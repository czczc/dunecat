"""Per-user bearer minting and MetaCatClient construction.

The hub doesn't keep bearers around: each route that calls metacat /
Rucio mints a fresh ~3h bearer from the user's encrypted vault token
in the DB. Mint cost is one HTTPS round-trip to vault (~50ms).

When the vault token itself has expired or decrypt fails, we surface a
401 the SPA's fetch wrapper converts into a redirect to /hub/login.
"""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime

from fastapi import HTTPException
from metacat.webapi import MetaCatClient

from .. import crypto, db
from . import flow
from .session import User

log = logging.getLogger("uvicorn.error")


def bearer_for(user: User) -> str:
    """Decrypt the user's stored vault token and mint a fresh bearer.

    Raises ``HTTPException(401)`` when the vault token is missing or
    has expired — the SPA reads this as "your session is over, sign in
    again."
    """
    with db.connect() as conn:
        row = conn.execute(
            "SELECT ciphertext, nonce, expires_at FROM vault_tokens "
            "WHERE user_id = ?",
            (user.id,),
        ).fetchone()
    if row is None:
        raise HTTPException(
            status_code=401, detail="no vault token on file; sign in again"
        )
    if row["expires_at"] <= datetime.now(UTC).isoformat():
        raise HTTPException(
            status_code=401, detail="vault token expired; sign in again"
        )
    try:
        vault_token = crypto.decrypt(
            bytes(row["ciphertext"]), bytes(row["nonce"])
        ).decode()
    except Exception as e:
        log.warning("hub: vault token decrypt failed for user %s: %s", user.id, e)
        raise HTTPException(
            status_code=401, detail="vault token unreadable; sign in again"
        )
    try:
        return flow.mint_bearer(vault_token, user.metacat_username)
    except Exception as e:
        log.warning(
            "hub: bearer mint failed for user %s (%s): %s",
            user.id,
            user.metacat_username,
            e,
        )
        # Likely the vault token revoked or vault unreachable; treat
        # as "sign in again" rather than 500 — the user can fix it.
        raise HTTPException(
            status_code=401, detail="could not mint bearer; sign in again"
        )


def metacat_for(user: User) -> MetaCatClient:
    """Build a fresh MetaCatClient ready to use for this user.

    Two-step dance — same as what ``metacat auth login -m token``
    does locally, just in-process:
      1. Mint an OIDC bearer from the user's vault token.
      2. Exchange that bearer for a metacat-format SignedToken via
         ``login_token(username, bearer)``. Metacat data endpoints
         require the SignedToken, not the OIDC bearer.

    Total cost per request: two HTTPS round-trips to FNAL (~150 ms).
    Per-session caching of the SignedToken is a future optimisation.
    """
    server = os.environ.get("METACAT_SERVER_URL")
    auth = os.environ.get("METACAT_AUTH_SERVER_URL")
    if not (server and auth):
        raise HTTPException(
            status_code=500,
            detail="METACAT_SERVER_URL / METACAT_AUTH_SERVER_URL not configured",
        )
    bearer = bearer_for(user)
    # Use a unique scratch TokenLib path so we never read or write the
    # user's ~/.token_library by accident. The hub's auth is purely
    # in-process; nothing on disk.
    client = MetaCatClient(
        server_url=server,
        auth_server_url=auth,
        token_library="/dev/null",
    )
    try:
        client.login_token(user.metacat_username, bearer)
    except Exception as e:
        log.warning(
            "hub: metacat login_token failed for user %s: %s",
            user.metacat_username,
            e,
        )
        raise HTTPException(
            status_code=401,
            detail="metacat refused bearer; sign in again",
        )
    return client
