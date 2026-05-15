import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

DB_PATH = Path.home() / ".dunecat" / "dunecat.db"


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(DB_PATH)


def init_db() -> None:
    with _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS datasets_cache (
                cache_key  TEXT PRIMARY KEY,
                body       TEXT NOT NULL,
                fetched_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS saved_queries (
                id          INTEGER PRIMARY KEY,
                name        TEXT NOT NULL UNIQUE,
                mql         TEXT NOT NULL,
                created_at  TEXT NOT NULL,
                last_run_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS condb_cache (
                folder     TEXT NOT NULL,
                tv         INTEGER NOT NULL,
                body       TEXT,
                fetched_at TEXT NOT NULL,
                PRIMARY KEY (folder, tv)
            )
            """
        )


def get_condb_cached(folder: str, tv: int) -> dict[str, Any] | None | str:
    """Return a cached condb row, ``None`` if the cache stored a negative
    result (run was checked but had no row), or the sentinel ``"MISS"`` if
    the cache has no entry at all. ``"MISS"`` distinguishes "we've never
    asked" from "we asked and got nothing."
    """
    with _connect() as conn:
        row = conn.execute(
            "SELECT body FROM condb_cache WHERE folder = ? AND tv = ?",
            (folder, tv),
        ).fetchone()
    if row is None:
        return "MISS"
    if row[0] is None:
        return None
    return json.loads(row[0])


def set_condb_cached(folder: str, tv: int, body: dict[str, Any] | None) -> None:
    with _connect() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO condb_cache (folder, tv, body, fetched_at) "
            "VALUES (?, ?, ?, ?)",
            (
                folder,
                tv,
                json.dumps(body, default=str) if body is not None else None,
                datetime.now(UTC).isoformat(),
            ),
        )


def connect() -> sqlite3.Connection:
    """Public alias for callers outside this module."""
    return _connect()


def _key(namespace: str) -> str:
    return f"datasets:ns={namespace}"


def get_cached(namespace: str) -> tuple[list[dict[str, Any]], datetime] | None:
    with _connect() as conn:
        row = conn.execute(
            "SELECT body, fetched_at FROM datasets_cache WHERE cache_key = ?",
            (_key(namespace),),
        ).fetchone()
    if row is None:
        return None
    body = json.loads(row[0])
    fetched_at = datetime.fromisoformat(row[1])
    return body, fetched_at


def set_cached(namespace: str, body: list[dict[str, Any]]) -> datetime:
    fetched_at = datetime.now(UTC)
    with _connect() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO datasets_cache (cache_key, body, fetched_at) "
            "VALUES (?, ?, ?)",
            (_key(namespace), json.dumps(body, default=str), fetched_at.isoformat()),
        )
    return fetched_at


def invalidate(namespace: str) -> None:
    with _connect() as conn:
        conn.execute(
            "DELETE FROM datasets_cache WHERE cache_key = ?",
            (_key(namespace),),
        )


def get_or_fetch(
    namespace: str, fetch: callable
) -> tuple[list[dict[str, Any]], datetime]:
    cached = get_cached(namespace)
    if cached is not None:
        return cached
    body = list(fetch(namespace))
    fetched_at = set_cached(namespace, body)
    return body, fetched_at
