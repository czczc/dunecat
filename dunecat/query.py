from collections.abc import Iterator
from typing import Any

from .client import get_client


def run_query(
    mql: str, with_metadata: bool = False, batch_size: int = 1000
) -> Iterator[dict[str, Any]]:
    client = get_client()
    yield from client.query(
        mql, with_metadata=with_metadata, batch_size=batch_size
    )
