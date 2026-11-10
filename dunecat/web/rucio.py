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
_did_client = None  # cached DIDClient (rule lookups)

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


# System CA bundles, first match wins: macOS/BSD, Debian/Ubuntu, RHEL/Alma.
_CA_BUNDLES = (
    "/etc/ssl/cert.pem",
    "/etc/ssl/certs/ca-certificates.crt",
    "/etc/pki/tls/certs/ca-bundle.crt",
)


def ca_cert_path() -> str:
    """CA bundle for talking to Rucio: ``RUCIO_CA_CERT`` if set, else the
    first system bundle that exists, else certifi's."""
    explicit = os.environ.get("RUCIO_CA_CERT")
    if explicit:
        return explicit
    for path in _CA_BUNDLES:
        if os.path.isfile(path):
            return path
    import certifi

    return certifi.where()


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
            ca_cert = {ca_cert_path()}
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


def _prepare() -> None:
    """Config + fresh bearer + token-file check, shared by every client."""
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
    if _client is not None or _did_client is not None:
        return
    token = _token_path()
    if not token.exists():
        raise RucioAuthError(
            f"Bearer token file {token} not found. Run: {HTGETTOKEN_CMD}"
        )


def _get_client():
    global _client
    _prepare()
    if _client is None:
        from rucio.client.replicaclient import ReplicaClient

        _client = ReplicaClient()
    return _client


def _get_did_client():
    global _did_client
    _prepare()
    if _did_client is None:
        from rucio.client.didclient import DIDClient

        _did_client = DIDClient()
    return _did_client


def reset_client() -> None:
    """Drop the cached clients so the next call re-reads the token file."""
    global _client, _did_client
    _client = None
    _did_client = None


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


_RULE_FIELDS = (
    "id",
    "account",
    "scope",
    "name",
    "state",
    "locks_ok_cnt",
    "locks_replicating_cnt",
    "locks_stuck_cnt",
    "rse_expression",
    "copies",
    "bytes",
    "expires_at",
    "created_at",
)


def rule_rows(rules: Any) -> list[dict[str, Any]]:
    """Trim Rucio rule records to the fields `rucio rule list` shows.
    Timestamps become ``YYYY-MM-DD HH:MM:SS`` strings (UTC)."""
    rows = []
    for rule in rules:
        row = {k: rule.get(k) for k in _RULE_FIELDS}
        for k in ("expires_at", "created_at"):
            if row[k] is not None:
                row[k] = str(row[k])
        rows.append(row)
    return rows


def list_rules(scope: str, name: str) -> list[dict[str, Any]]:
    """Replication rules on one DID (dataset or container). Empty when
    the DID has no rules or Rucio doesn't know it."""
    client = _get_did_client()
    try:
        return rule_rows(client.list_did_rules(scope, name))
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
            return []
        raise RucioError(str(e)) from e
