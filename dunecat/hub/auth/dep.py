"""FastAPI dependency: ``current_user``.

Reads the session cookie, looks up the user, raises 401 if the cookie
is missing or the session is dead.
"""

from __future__ import annotations

from fastapi import Cookie, HTTPException

from .. import db
from . import session as session_mod
from .session import COOKIE_NAME, User


def current_user(
    dunecat_session: str | None = Cookie(default=None, alias=COOKIE_NAME),
) -> User:
    if not dunecat_session:
        raise HTTPException(status_code=401, detail="not logged in")
    with db.connect() as conn:
        user = session_mod.load_session(conn, dunecat_session)
    if user is None:
        raise HTTPException(status_code=401, detail="session expired")
    return user
