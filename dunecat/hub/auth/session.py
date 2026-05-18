"""Server-side session management.

The browser holds an opaque ``dunecat_session`` cookie. The server
maps that to a row in ``sessions``, which maps to a row in ``users``.
Sliding 7-day TTL: every successful lookup bumps ``last_seen_at`` and
extends ``expires_at``.
"""

from __future__ import annotations

import secrets
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

SESSION_LIFETIME = timedelta(days=7)
DEVICE_FLOW_LIFETIME = timedelta(minutes=10)
COOKIE_NAME = "dunecat_session"


@dataclass(frozen=True)
class User:
    id: int
    oidc_sub: str
    metacat_username: str


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _expires(td: timedelta) -> str:
    return (datetime.now(UTC) + td).isoformat()


def upsert_user(
    conn: sqlite3.Connection, *, oidc_sub: str, metacat_username: str
) -> int:
    """Insert a new user row or update an existing one (matched by oidc_sub).
    Returns the user's id."""
    now = _now()
    row = conn.execute(
        "SELECT id FROM users WHERE oidc_sub = ?", (oidc_sub,)
    ).fetchone()
    if row is not None:
        conn.execute(
            "UPDATE users SET metacat_username = ?, last_seen_at = ? WHERE id = ?",
            (metacat_username, now, row["id"]),
        )
        return row["id"]
    cur = conn.execute(
        "INSERT INTO users (oidc_sub, metacat_username, created_at, last_seen_at) "
        "VALUES (?, ?, ?, ?)",
        (oidc_sub, metacat_username, now, now),
    )
    return cur.lastrowid


def create_session(conn: sqlite3.Connection, *, user_id: int) -> str:
    """Insert a new session row, return its id (the opaque cookie value)."""
    sid = secrets.token_urlsafe(32)
    now = _now()
    expires = _expires(SESSION_LIFETIME)
    conn.execute(
        "INSERT INTO sessions (id, user_id, created_at, last_seen_at, expires_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (sid, user_id, now, now, expires),
    )
    return sid


def load_session(conn: sqlite3.Connection, session_id: str) -> User | None:
    """Look up the user behind a session id. Returns None if missing or
    expired. Slides the session expiry as a side effect on success."""
    row = conn.execute(
        "SELECT u.id, u.oidc_sub, u.metacat_username, s.expires_at "
        "FROM sessions s JOIN users u ON s.user_id = u.id "
        "WHERE s.id = ?",
        (session_id,),
    ).fetchone()
    if row is None:
        return None
    if row["expires_at"] <= _now():
        # Expired session — clean up and report nothing.
        conn.execute("DELETE FROM sessions WHERE id = ?", (session_id,))
        return None
    now = _now()
    expires = _expires(SESSION_LIFETIME)
    conn.execute(
        "UPDATE sessions SET last_seen_at = ?, expires_at = ? WHERE id = ?",
        (now, expires, session_id),
    )
    return User(
        id=row["id"],
        oidc_sub=row["oidc_sub"],
        metacat_username=row["metacat_username"],
    )


def delete_session(conn: sqlite3.Connection, session_id: str) -> None:
    conn.execute("DELETE FROM sessions WHERE id = ?", (session_id,))


def gc_expired(conn: sqlite3.Connection) -> None:
    """Delete expired sessions and device-flows. Cheap; called from the
    periodic background task."""
    now = _now()
    conn.execute("DELETE FROM sessions WHERE expires_at <= ?", (now,))
    conn.execute("DELETE FROM device_flows WHERE expires_at <= ?", (now,))
