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
