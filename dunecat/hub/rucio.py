"""Per-user Rucio access for the hub.

Rucio's ``ReplicaClient`` reads its bearer via the WLCG token-discovery
chain (``BEARER_TOKEN`` env → ``BEARER_TOKEN_FILE`` → XDG paths). The
env var is process-global, so to use a per-user bearer we set it
briefly inside a thread-wide lock while we instantiate the client,
then immediately restore. After ``__init__`` the client has the
bearer baked into ``self.auth_token`` and ``self.headers``, so
``list_replicas`` runs lock-free.

Lock contention is bounded by the ReplicaClient constructor (~ms);
the actual Rucio HTTP call (~10–100 ms) happens outside the lock so
concurrent users don't queue on each other.
"""

from __future__ import annotations

import logging
import os
import threading
from typing import Any

from .auth.bearer import bearer_for
from .auth.session import User

log = logging.getLogger("uvicorn.error")

# Boundary error types mirror dunecat/web/rucio.py so the route can
# raise the same shapes.


class RucioAuthError(Exception):
    """Bearer rejected or vault unavailable. Surface a 401."""


class RucioError(Exception):
    """Other Rucio-side error. Surface a 502."""


_construction_lock = threading.Lock()

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


def _replica_client_for(user: User):
    """Build a fresh ReplicaClient bound to this user's bearer.

    All Rucio config lives in env vars (`.env`) — RUCIO_HOST,
    RUCIO_AUTH_HOST. No rucio.cfg file is written or read; everything
    is passed via constructor args.
    """
    from rucio.client.replicaclient import ReplicaClient

    rucio_host = os.environ.get("RUCIO_HOST", "https://dune-rucio.fnal.gov")
    auth_host = os.environ.get("RUCIO_AUTH_HOST", rucio_host)
    ca_cert = os.environ.get("RUCIO_CA_CERT", "/etc/ssl/cert.pem")

    bearer = bearer_for(user)

    with _construction_lock:
        prev = os.environ.get("BEARER_TOKEN")
        os.environ["BEARER_TOKEN"] = bearer
        try:
            return ReplicaClient(
                rucio_host=rucio_host,
                auth_host=auth_host,
                account=user.metacat_username,
                auth_type="oidc",
                ca_cert=ca_cert,
                timeout=30,
            )
        finally:
            if prev is None:
                os.environ.pop("BEARER_TOKEN", None)
            else:
                os.environ["BEARER_TOKEN"] = prev


def list_replicas_for(
    user: User, scope: str, name: str
) -> dict[str, Any] | None:
    """Return replica info for ``scope:name`` as the given user.

    Returns None when the DID is unknown or has no replicas.
    Raises :class:`RucioAuthError` when the bearer is rejected.
    """
    try:
        client = _replica_client_for(user)
    except Exception as e:
        log.warning("hub: building rucio client failed for %s: %s",
                    user.metacat_username, e)
        raise RucioAuthError(str(e))

    try:
        # Ask for all common schemes so each RSE shows both its xrootd
        # (root://) and WebDAV (davs://) doors — they reach the same bytes
        # but differ in token support per site.
        results = list(
            client.list_replicas(
                [{"scope": scope, "name": name}],
                schemes=["root", "davs", "https"],
            )
        )
    except Exception as e:
        from rucio.common.exception import (  # noqa: PLC0415
            CannotAuthenticate,
            DataIdentifierNotFound,
        )

        if isinstance(e, CannotAuthenticate):
            raise RucioAuthError(f"Rucio rejected bearer: {e}") from e
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
