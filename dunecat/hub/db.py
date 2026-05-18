"""SQLite connection + schema for the multi-user hub.

Separate file (and separate DB path) from the local app's database;
the two never share rows or schemas.
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

_SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    oidc_sub           TEXT NOT NULL UNIQUE,
    metacat_username   TEXT NOT NULL,
    created_at         TEXT NOT NULL,
    last_seen_at       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
    id            TEXT PRIMARY KEY,
    user_id       INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at    TEXT NOT NULL,
    last_seen_at  TEXT NOT NULL,
    expires_at    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS sessions_user_idx ON sessions (user_id);
CREATE INDEX IF NOT EXISTS sessions_expires_idx ON sessions (expires_at);

CREATE TABLE IF NOT EXISTS vault_tokens (
    user_id      INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    ciphertext   BLOB NOT NULL,
    nonce        BLOB NOT NULL,
    expires_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS device_flows (
    id           TEXT PRIMARY KEY,
    poll_body    TEXT NOT NULL,
    expires_at   TEXT NOT NULL,
    status       TEXT NOT NULL CHECK (status IN ('pending', 'complete', 'expired'))
);
CREATE INDEX IF NOT EXISTS device_flows_expires_idx ON device_flows (expires_at);
"""


def db_path() -> Path:
    raw = os.environ.get("DUNECAT_HUB_DB")
    if raw:
        return Path(raw).expanduser()
    return Path.home() / ".dunecat" / "hub.sqlite"


def connect() -> sqlite3.Connection:
    """Open a fresh connection. Caller owns it.

    WAL + busy_timeout + foreign_keys are set per-connection because
    SQLite doesn't persist some of these across connections.
    """
    path = db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, isolation_level=None)  # autocommit
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_schema() -> None:
    with connect() as conn:
        conn.executescript(_SCHEMA)
