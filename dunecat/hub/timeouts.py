"""Per-request timeout helper for blocking FNAL calls.

FastAPI runs sync route handlers in its thread pool. Wrap any
blocking call to metacat / Rucio / condb in ``with_timeout(...)`` and
the handler raises a 504 if the upstream takes too long, freeing the
worker thread.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from typing import Any, Callable, TypeVar

from fastapi import HTTPException

log = logging.getLogger("uvicorn.error")

T = TypeVar("T")

# Defaults from grilling Q11.
METACAT_TIMEOUT_S = 60.0
RUCIO_TIMEOUT_S = 30.0
CONDB_TIMEOUT_S = 30.0

# One shared executor is fine; we just need a worker we can wait on
# with a timeout. Pool size is conservative; if it bottlenecks under
# real load we'll widen it.
_pool = ThreadPoolExecutor(max_workers=16, thread_name_prefix="hub-timeout")


def with_timeout(
    fn: Callable[..., T],
    *args: Any,
    timeout: float = METACAT_TIMEOUT_S,
    label: str = "external call",
    **kwargs: Any,
) -> T:
    """Run ``fn(*args, **kwargs)`` in a worker thread; abort if it
    takes longer than ``timeout`` seconds. Raises ``HTTPException(504)``
    on timeout."""
    fut = _pool.submit(fn, *args, **kwargs)
    try:
        return fut.result(timeout=timeout)
    except FuturesTimeout:
        log.warning(
            "hub: timeout (%.1fs) waiting on %s — abandoning the worker",
            timeout,
            label,
        )
        # We deliberately don't cancel the future — Python can't actually
        # cancel a running blocking call, and "leaked" worker eventually
        # completes or dies. The HTTP request, at least, returns promptly.
        raise HTTPException(
            status_code=504,
            detail=f"{label} timed out after {timeout:.0f}s; retry later",
        )
