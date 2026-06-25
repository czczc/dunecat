"""Thin wrapper around rucio-clients for DUNE replica lookups.

Bootstraps a transient rucio.cfg from environment variables (the user
shouldn't have to maintain a separate config file when they've already
got everything they need in ``.env``). Auth is OIDC bearer-token only —
the token comes from ``htgettoken --issuer=dune`` and lives at
``BEARER_TOKEN_FILE``. When the token is missing/expired, raises
:class:`RucioAuthError` with verbatim remediation copy that the API
layer surfaces straight to the UI.
"""

from __future__ import annotations

import logging
import os
import sys
import textwrap
from pathlib import Path
from typing import Any

from . import auth

log = logging.getLogger("uvicorn.error")

HTGETTOKEN_CMD = "uv run dunecat login"


class RucioAuthError(Exception):
    """Token missing/expired/rejected. Message includes remediation."""


class RucioError(Exception):
    """Any other Rucio-side error."""


_client = None  # cached ReplicaClient

# Disk sites before tape sites when ordering the grouped result.
_TYPE_RANK = {"DISK": 0, "TAPE": 1}


def _group_replicas(pfns: dict[str, Any]) -> list[dict[str, Any]]:
    """Group Rucio's per-PFN info (the ``pfns`` map) by site into
    ``[{rse, type, pfns: [{scheme, pfn, priority}]}]``.

    Disk sites come before tape sites; within a site the doors are ordered
    by Rucio's own priority, which ranks ``root://`` (typically faster)
    ahead of ``davs://``.
    """
    sites: dict[str, dict[str, Any]] = {}
    for pfn, info in pfns.items():
        rse = info.get("rse") or "?"
        site = sites.setdefault(
            rse, {"rse": rse, "type": (info.get("type") or "").upper(), "pfns": []}
        )
        site["pfns"].append(
            {
                "scheme": pfn.split("://", 1)[0],
                "pfn": pfn,
                "priority": info.get("priority"),
            }
        )
    for site in sites.values():
        site["pfns"].sort(key=lambda p: (p["priority"] is None, p["priority"] or 0))
    return sorted(
        sites.values(), key=lambda s: (_TYPE_RANK.get(s["type"], 9), s["rse"])
    )


def _ensure_config() -> None:
    """Write ``~/.dunecat/rucio/etc/rucio.cfg`` from env vars unless RUCIO_HOME
    is already set by the operator. Idempotent."""
    if os.environ.get("RUCIO_HOME"):
        return
    account = os.environ.get("RUCIO_ACCOUNT")
    if not account:
        raise RucioAuthError(
            "RUCIO_ACCOUNT not set in environment. Set it to your DUNE Rucio "
            "account name (typically your FNAL username) and restart."
        )
    rucio_host = os.environ.get("RUCIO_HOST", "https://dune-rucio.fnal.gov")
    auth_host = os.environ.get("RUCIO_AUTH_HOST", rucio_host)
    cfg_dir = Path.home() / ".dunecat" / "rucio"
    (cfg_dir / "etc").mkdir(parents=True, exist_ok=True)
    cfg_file = cfg_dir / "etc" / "rucio.cfg"
    cfg_file.write_text(
        textwrap.dedent(f"""\
            [client]
            rucio_host = {rucio_host}
            auth_host = {auth_host}
            auth_type = oidc
            account = {account}
            ca_cert = /etc/ssl/cert.pem
        """)
    )
    os.environ["RUCIO_HOME"] = str(cfg_dir)
    os.environ.setdefault("RUCIO_ACCOUNT", account)
    _link_venv_config(cfg_file)


def _link_venv_config(target: Path) -> None:
    """Symlink ``<venv>/etc/rucio.cfg`` to the canonical config so that the
    ``rucio`` CLI run inside the venv finds it without needing RUCIO_HOME."""
    if sys.prefix == sys.base_prefix:
        return  # not running inside a venv
    venv_cfg = Path(sys.prefix) / "etc" / "rucio.cfg"
    try:
        venv_cfg.parent.mkdir(parents=True, exist_ok=True)
        if venv_cfg.is_symlink():
            if venv_cfg.resolve() == target.resolve():
                return
            venv_cfg.unlink()
        elif venv_cfg.exists():
            return  # operator wrote a real file here; leave it alone
        venv_cfg.symlink_to(target)
    except OSError as e:
        log.warning("Could not link %s -> %s: %s", venv_cfg, target, e)


def _token_path() -> Path:
    raw = os.environ.get("BEARER_TOKEN_FILE")
    if not raw:
        raise RucioAuthError(
            f"BEARER_TOKEN_FILE not set in environment. Run: {HTGETTOKEN_CMD}"
        )
    return Path(raw)


def _get_client():
    global _client
    _ensure_config()
    # Top up the bearer in-memory expiry cache before we hand out a client.
    # Silent re-mint when the vault token is alive; raises VaultExpiredError
    # otherwise so the API layer surfaces the "browser login needed" copy.
    try:
        auth.ensure_fresh_bearer()
    except auth.VaultExpiredError as e:
        raise RucioAuthError(str(e)) from e
    except auth.AuthRenewError as e:
        # Other transient renewal failure — still try the existing token if
        # the file is there; otherwise propagate.
        log.warning("rucio: bearer renew failed (%s); falling back to disk", e)
    if _client is not None:
        return _client
    token = _token_path()
    if not token.exists():
        raise RucioAuthError(
            f"Bearer token file {token} not found. Run: {HTGETTOKEN_CMD}"
        )
    from rucio.client.replicaclient import ReplicaClient

    _client = ReplicaClient()
    return _client


def reset_client() -> None:
    """Drop the cached client so the next call re-reads the token file."""
    global _client
    _client = None


def list_replicas(scope: str, name: str) -> dict[str, Any] | None:
    """Look up replicas for one DID. Returns ``None`` if Rucio knows the file
    but has no replicas, or if the DID itself is unknown."""
    client = _get_client()
    try:
        # All common schemes so each RSE shows both its xrootd (root://) and
        # WebDAV (davs://) doors (same bytes, different token support per site).
        results = list(
            client.list_replicas(
                [{"scope": scope, "name": name}],
                schemes=["root", "davs", "https"],
            )
        )
    except Exception as e:  # noqa: BLE001  — narrow below
        from rucio.common.exception import (  # noqa: PLC0415
            CannotAuthenticate,
            DataIdentifierNotFound,
        )

        if isinstance(e, CannotAuthenticate):
            reset_client()
            raise RucioAuthError(
                f"Rucio rejected token ({e}). Run: {HTGETTOKEN_CMD}"
            ) from e
        if isinstance(e, DataIdentifierNotFound):
            return None
        raise RucioError(str(e)) from e

    if not results:
        return None
    r = results[0]
    replicas = _group_replicas(r.get("pfns") or {})
    return {
        "scope": r.get("scope"),
        "name": r.get("name"),
        "bytes": r.get("bytes"),
        "md5": r.get("md5"),
        "adler32": r.get("adler32"),
        "replicas": replicas,
    }
