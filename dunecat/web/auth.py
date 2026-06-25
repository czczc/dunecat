"""Lazy server-side renewal for the two credentials dunecat uses:

* OIDC bearer token at ``BEARER_TOKEN_FILE`` (default ``/tmp/bt_u<uid>``),
  minted by ``htgettoken``.
* Metacat session token in ``~/.token_library``, minted by
  ``metacat auth login -m token``.

Both tokens carry their own ``exp`` claim, so we never hardcode a
lifetime — we just read what the server gave us. Per-request cost is
two ``datetime`` comparisons; subprocess work only fires at renewal.

If the vault token itself is dead (>10 d since interactive login),
htgettoken needs a browser and can't run from this process. We surface
that as :class:`VaultExpiredError` and let the API layer convert it into
the verbatim "run ``dunecat login`` in a terminal" remediation copy.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import shutil
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

log = logging.getLogger("uvicorn.error")

# Renew when this much time (or less) remains, capped at 10% of the
# token's own lifetime so very short-lived tokens don't thrash.
_DEFAULT_BUFFER = timedelta(minutes=5)

# How long we wait for a renewal subprocess before giving up.
_SUBPROCESS_TIMEOUT = 5.0


class AuthRenewError(Exception):
    """Renewal failed in a way the user can probably retry."""


class VaultExpiredError(AuthRenewError):
    """Vault refresh token is expired; the user must run ``dunecat login``
    in a terminal to drive the OIDC device-code (browser) flow."""


class MetacatRejectError(AuthRenewError):
    """Metacat refused the OIDC bearer — server config / unprovisioned
    account / audience mismatch. Surface stderr to the user."""


@dataclass
class _TokenState:
    expires_at: datetime | None = None  # None == unknown, treat as expired
    lifetime: timedelta | None = None  # for adaptive buffer


_bearer = _TokenState()
_metacat = _TokenState()
_lock = threading.Lock()


# --- JWT helpers ------------------------------------------------------------


def _jwt_claims(token: str) -> dict:
    """Decode (without verifying) the middle segment of a JWT."""
    parts = token.split(".")
    if len(parts) != 3:
        raise ValueError("not a JWT")
    body = parts[1]
    body += "=" * (-len(body) % 4)  # restore base64 padding
    return json.loads(base64.urlsafe_b64decode(body))


def _exp_of(token: str) -> datetime:
    claims = _jwt_claims(token)
    return datetime.fromtimestamp(int(claims["exp"]), UTC)


def _iat_of(token: str) -> datetime | None:
    claims = _jwt_claims(token)
    iat = claims.get("iat") or claims.get("nbf")
    if iat is None:
        return None
    return datetime.fromtimestamp(int(iat), UTC)


# --- Bearer (htgettoken) ----------------------------------------------------


def _bearer_path() -> Path:
    # WLCG Bearer Token Discovery: honor an explicit BEARER_TOKEN_FILE, else
    # $XDG_RUNTIME_DIR/bt_u<uid> on Linux (e.g. /run/user/1000), else
    # /tmp/bt_u<uid> (macOS). Must match where htgettoken writes and xrdcp
    # reads — see scripts/dune-xrdcp.sh, which uses the same precedence.
    if raw := os.environ.get("BEARER_TOKEN_FILE"):
        return Path(raw)
    runtime = os.environ.get("XDG_RUNTIME_DIR") or "/tmp"
    return Path(runtime) / f"bt_u{os.geteuid()}"


def _read_bearer_from_disk() -> None:
    """Populate the cached bearer expiry from the file on disk."""
    path = _bearer_path()
    try:
        token = path.read_text().strip()
        exp = _exp_of(token)
        iat = _iat_of(token)
    except (OSError, ValueError, KeyError) as e:
        log.debug("auth: cannot read bearer at %s: %s", path, e)
        _bearer.expires_at = None
        _bearer.lifetime = None
        return
    _bearer.expires_at = exp
    if iat is not None:
        _bearer.lifetime = exp - iat


def _run_htgettoken() -> None:
    """Drive htgettoken to refresh the bearer. Silent when the vault
    token is alive; raises :class:`VaultExpiredError` when it isn't."""
    htget = shutil.which("htgettoken") or shutil.which(
        "htgettoken", path=str(Path(sys.prefix) / "bin"),
    )
    if not htget:
        raise AuthRenewError(
            "htgettoken not found on PATH. Run `uv run dunecat login` "
            "from a terminal."
        )
    cmd = [
        htget,
        "--vaulttokenttl=10d",
        "--vaultserver=htvaultprod.fnal.gov",
        "--issuer=dune",
    ]
    started = time.monotonic()
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=_SUBPROCESS_TIMEOUT,
        )
    except subprocess.TimeoutExpired:
        log.warning("auth: htgettoken timed out after %ss", _SUBPROCESS_TIMEOUT)
        raise AuthRenewError("htgettoken timed out; retry later") from None
    elapsed = time.monotonic() - started
    if result.returncode != 0:
        err = (result.stderr or result.stdout or "").strip()
        # htgettoken refuses OIDC device flow in non-interactive mode with a
        # distinctive message; treat as vault-expired so we surface the
        # "run dunecat login" remediation.
        if "Disabling oidc" in err or "device flow" in err.lower():
            log.info("auth: vault expired (htgettoken needs browser)")
            raise VaultExpiredError(
                "Vault token expired. Run: uv run dunecat login (browser)."
            )
        log.warning("auth: htgettoken failed (%.2fs): %s", elapsed, err)
        raise AuthRenewError(f"htgettoken failed: {err}")
    log.info("auth: bearer renewed via htgettoken in %.2fs", elapsed)
    _read_bearer_from_disk()


# --- Metacat session (.token_library) --------------------------------------


def _token_library_path() -> Path:
    return Path.home() / ".token_library"


def _read_metacat_from_disk() -> None:
    """Populate the cached metacat-session expiry by reading the JWT for
    the (server, user) we're configured against."""
    server = os.environ.get("METACAT_SERVER_URL", "")
    path = _token_library_path()
    try:
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) < 2:
                continue
            if parts[0] != server:
                continue
            token = parts[1]
            exp = _exp_of(token)
            iat = _iat_of(token)
            _metacat.expires_at = exp
            if iat is not None:
                _metacat.lifetime = exp - iat
            return
    except (OSError, ValueError, KeyError) as e:
        log.debug("auth: cannot read metacat session: %s", e)
    _metacat.expires_at = None
    _metacat.lifetime = None


def _run_metacat_login() -> None:
    """Mint a fresh metacat session token from the current bearer."""
    user = os.environ.get("METACAT_USER")
    server = os.environ.get("METACAT_SERVER_URL")
    auth = os.environ.get("METACAT_AUTH_SERVER_URL")
    if not (user and server and auth):
        raise AuthRenewError(
            "METACAT_USER / METACAT_SERVER_URL / METACAT_AUTH_SERVER_URL "
            "missing from environment."
        )
    metacat = shutil.which("metacat")
    if not metacat:
        raise AuthRenewError("metacat CLI not found on PATH.")
    cmd = [metacat, "-s", server, "-a", auth, "auth", "login", "-m", "token", user]
    started = time.monotonic()
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=_SUBPROCESS_TIMEOUT,
        )
    except subprocess.TimeoutExpired:
        log.warning("auth: metacat login timed out after %ss", _SUBPROCESS_TIMEOUT)
        raise AuthRenewError("metacat auth login timed out; retry later") from None
    elapsed = time.monotonic() - started
    if result.returncode != 0:
        err = (result.stderr or result.stdout or "").strip()
        log.warning("auth: metacat login failed (%.2fs): %s", elapsed, err)
        raise MetacatRejectError(
            f"Metacat refused OIDC bearer: {err or 'no detail'}"
        )
    log.info("auth: metacat session renewed in %.2fs", elapsed)
    _read_metacat_from_disk()


# --- Public API -------------------------------------------------------------


def _buffer_for(state: _TokenState) -> timedelta:
    """Renew when ``now + buffer >= exp``. Capped at 10% of the token's
    own lifetime so a 10-min token doesn't thrash."""
    if state.lifetime is None:
        return _DEFAULT_BUFFER
    return min(_DEFAULT_BUFFER, state.lifetime * 0.10)


def _is_stale(state: _TokenState) -> bool:
    if state.expires_at is None:
        return True
    return datetime.now(UTC) + _buffer_for(state) >= state.expires_at


def ensure_fresh_bearer() -> None:
    """Renew the bearer if its cached expiry is approaching."""
    if not _is_stale(_bearer):
        return
    with _lock:
        if not _is_stale(_bearer):
            return  # another thread renewed while we were waiting
        _run_htgettoken()


def ensure_fresh_metacat_session() -> None:
    """Renew the metacat session if its cached expiry is approaching.
    Also tops up the bearer first since metacat-login consumes it."""
    if not _is_stale(_metacat):
        return
    with _lock:
        if not _is_stale(_metacat):
            return
        if _is_stale(_bearer):
            _run_htgettoken()
        _run_metacat_login()


def invalidate_metacat_cache() -> None:
    """Force the next ensure_fresh_metacat_session() call to re-mint. Use
    when the server returns a 401 despite our cache thinking we're good."""
    _metacat.expires_at = None


def invalidate_bearer_cache() -> None:
    """Same idea for the bearer (Rucio side)."""
    _bearer.expires_at = None


def prime() -> None:
    """Populate caches at app startup from whatever is on disk. Safe to
    call repeatedly; falls through quietly if files are missing."""
    _read_bearer_from_disk()
    _read_metacat_from_disk()
