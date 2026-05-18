"""Catalog / replicas / conditions caches for the hub.

Mirrors `dunecat/web/cache.py` but writes to the hub's own SQLite at
``DUNECAT_HUB_DB`` instead of the local user's DB. Per grilling Q8,
these caches are **global across users** in the hub — metacat /
condb / Rucio responses don't vary by which DUNE user asked, so one
cache row serves everyone.

Saved queries live in this module too, but the schema is per-user
(``UNIQUE(user_id, name)``) and every CRUD path filters by
``user_id``.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any

from . import db

RUCIO_TTL = timedelta(hours=1)


# ---- datasets cache (global) ----------------------------------------------


def _ds_key(namespace: str) -> str:
    return f"datasets:ns={namespace}"


def get_datasets_cached(
    namespace: str,
) -> tuple[list[dict[str, Any]], datetime] | None:
    with db.connect() as conn:
        row = conn.execute(
            "SELECT body, fetched_at FROM datasets_cache WHERE cache_key = ?",
            (_ds_key(namespace),),
        ).fetchone()
    if row is None:
        return None
    body = json.loads(row["body"])
    fetched_at = datetime.fromisoformat(row["fetched_at"])
    return body, fetched_at


def set_datasets_cached(namespace: str, body: list[dict[str, Any]]) -> datetime:
    fetched_at = datetime.now(UTC)
    with db.connect() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO datasets_cache (cache_key, body, fetched_at) "
            "VALUES (?, ?, ?)",
            (
                _ds_key(namespace),
                json.dumps(body, default=str),
                fetched_at.isoformat(),
            ),
        )
    return fetched_at


def invalidate_datasets(namespace: str) -> None:
    with db.connect() as conn:
        conn.execute(
            "DELETE FROM datasets_cache WHERE cache_key = ?",
            (_ds_key(namespace),),
        )


def get_or_fetch_datasets(
    namespace: str, fetch: callable
) -> tuple[list[dict[str, Any]], datetime]:
    cached = get_datasets_cached(namespace)
    if cached is not None:
        return cached
    body = list(fetch(namespace))
    fetched_at = set_datasets_cached(namespace, body)
    return body, fetched_at


# ---- condb cache (global) -------------------------------------------------


def get_condb_cached(folder: str, tv: int) -> dict[str, Any] | None | str:
    with db.connect() as conn:
        row = conn.execute(
            "SELECT body FROM condb_cache WHERE folder = ? AND tv = ?",
            (folder, tv),
        ).fetchone()
    if row is None:
        return "MISS"
    if row["body"] is None:
        return None
    return json.loads(row["body"])


def set_condb_cached(
    folder: str, tv: int, body: dict[str, Any] | None
) -> None:
    with db.connect() as conn:
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


# ---- rucio cache (global, 1h TTL) ----------------------------------------


def get_rucio_cached(scope: str, name: str) -> dict[str, Any] | None:
    with db.connect() as conn:
        row = conn.execute(
            "SELECT body, fetched_at FROM rucio_cache WHERE scope = ? AND name = ?",
            (scope, name),
        ).fetchone()
    if row is None or row["body"] is None:
        return None
    fetched_at = datetime.fromisoformat(row["fetched_at"])
    if datetime.now(UTC) - fetched_at > RUCIO_TTL:
        return None
    return json.loads(row["body"])


def set_rucio_cached(scope: str, name: str, body: dict[str, Any]) -> None:
    with db.connect() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO rucio_cache (scope, name, body, fetched_at) "
            "VALUES (?, ?, ?, ?)",
            (
                scope,
                name,
                json.dumps(body, default=str),
                datetime.now(UTC).isoformat(),
            ),
        )


# ---- saved queries (per-user) ---------------------------------------------


def _saved_query_row(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "name": row["name"],
        "mql": row["mql"],
        "created_at": row["created_at"],
        "last_run_at": row["last_run_at"],
    }


def list_saved_queries_for(user_id: int) -> list[dict[str, Any]]:
    with db.connect() as conn:
        rows = conn.execute(
            "SELECT id, name, mql, created_at, last_run_at FROM saved_queries "
            "WHERE user_id = ? ORDER BY name",
            (user_id,),
        ).fetchall()
    return [_saved_query_row(r) for r in rows]


def get_saved_query_for(
    user_id: int, query_id: int
) -> dict[str, Any] | None:
    with db.connect() as conn:
        row = conn.execute(
            "SELECT id, name, mql, created_at, last_run_at FROM saved_queries "
            "WHERE id = ? AND user_id = ?",
            (query_id, user_id),
        ).fetchone()
    return _saved_query_row(row) if row else None


def create_saved_query(
    user_id: int, *, name: str, mql: str
) -> dict[str, Any]:
    now = datetime.now(UTC).isoformat()
    with db.connect() as conn:
        cur = conn.execute(
            "INSERT INTO saved_queries (user_id, name, mql, created_at) "
            "VALUES (?, ?, ?, ?)",
            (user_id, name, mql, now),
        )
        new_id = cur.lastrowid
        row = conn.execute(
            "SELECT id, name, mql, created_at, last_run_at FROM saved_queries "
            "WHERE id = ?",
            (new_id,),
        ).fetchone()
    return _saved_query_row(row)


def update_saved_query(
    user_id: int,
    query_id: int,
    *,
    name: str | None = None,
    mql: str | None = None,
) -> dict[str, Any] | None:
    sets = []
    args: list[Any] = []
    if name is not None:
        sets.append("name = ?")
        args.append(name)
    if mql is not None:
        sets.append("mql = ?")
        args.append(mql)
    if not sets:
        return get_saved_query_for(user_id, query_id)
    args.extend([query_id, user_id])
    with db.connect() as conn:
        cur = conn.execute(
            f"UPDATE saved_queries SET {', '.join(sets)} "
            "WHERE id = ? AND user_id = ?",
            args,
        )
        if cur.rowcount == 0:
            return None
    return get_saved_query_for(user_id, query_id)


def delete_saved_query(user_id: int, query_id: int) -> bool:
    with db.connect() as conn:
        cur = conn.execute(
            "DELETE FROM saved_queries WHERE id = ? AND user_id = ?",
            (query_id, user_id),
        )
        return cur.rowcount > 0


def touch_saved_query(user_id: int, query_id: int) -> None:
    """Bump last_run_at when a saved query is executed."""
    with db.connect() as conn:
        conn.execute(
            "UPDATE saved_queries SET last_run_at = ? "
            "WHERE id = ? AND user_id = ?",
            (datetime.now(UTC).isoformat(), query_id, user_id),
        )
