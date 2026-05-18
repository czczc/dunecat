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

-- Global caches (Q8): metacat / condb / Rucio responses don't vary
-- per DUNE user, so one row serves everyone.
CREATE TABLE IF NOT EXISTS datasets_cache (
    cache_key  TEXT PRIMARY KEY,
    body       TEXT NOT NULL,
    fetched_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS condb_cache (
    folder     TEXT NOT NULL,
    tv         INTEGER NOT NULL,
    body       TEXT,
    fetched_at TEXT NOT NULL,
    PRIMARY KEY (folder, tv)
);

CREATE TABLE IF NOT EXISTS rucio_cache (
    scope      TEXT NOT NULL,
    name       TEXT NOT NULL,
    body       TEXT,
    fetched_at TEXT NOT NULL,
    PRIMARY KEY (scope, name)
);

-- Saved queries are per-user (Q8).
CREATE TABLE IF NOT EXISTS saved_queries (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    mql         TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    last_run_at TEXT,
    UNIQUE (user_id, name)
);
CREATE INDEX IF NOT EXISTS saved_queries_user_idx ON saved_queries (user_id);
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
