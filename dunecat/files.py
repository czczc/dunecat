from collections.abc import Iterator
from typing import Any

from .client import get_client
from .filters import FileFilters


def find_files(
    did: str,
    filters: FileFilters,
    with_metadata: bool = False,
    batch_size: int = 1000,
) -> Iterator[dict[str, Any]]:
    mql = build_mql(did, filters)
    client = get_client()
    yield from client.query(
        mql,
        with_metadata=with_metadata,
        batch_size=batch_size,
    )


def build_mql(did: str, filters: FileFilters) -> str:
    parts = [f"files from {did}"]
    clauses = filters.to_mql_where_clauses()
    if clauses:
        parts.append("where " + " and ".join(clauses))
    return " ".join(parts)


def file_did(item: dict[str, Any]) -> str:
    return f"{item['namespace']}:{item['name']}"
