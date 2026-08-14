from collections.abc import Iterator
from typing import Any

from metacat.webapi import MetaCatClient

from . import mql_lint
from .client import get_client


def run_query(
    mql: str,
    with_metadata: bool = False,
    batch_size: int = 1000,
    *,
    client: MetaCatClient | None = None,
) -> Iterator[dict[str, Any]]:
    client = client or get_client()
    if batch_size > 0 and mql_lint.breaks_when_wrapped(mql):
        # With batch_size the client rewrites the query as
        # "(<mql>) ordered skip N limit B" — which metacat 4.1.4 fails on
        # when <mql> is a limit over a compound query. Fetch unbatched.
        batch_size = 0
    yield from client.query(
        mql, with_metadata=with_metadata, batch_size=batch_size
    )
