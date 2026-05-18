"""Hub variant of `dunecat/web/detectors.py`.

Two structural differences from the local-app version:
  * the cache writes to the hub's SQLite, not the local-user DB
  * metacat calls go through a caller-provided client (per-user
    bearer), not the cached singleton

YAML loading and the `apply_default_filters` predicate are shared
with the local app via direct import — the YAML file is project-wide
config, not local-app-specific.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any

from metacat.webapi import MetaCatClient

from dunecat.web.detectors import (  # noqa: F401 — re-exported for callers
    apply_default_filters,
    detector_by_id,
    load_detectors,
)

from . import cache


def datasets_for_namespace(
    namespace: str, *, client: MetaCatClient
) -> tuple[list[dict[str, Any]], datetime]:
    def _fetch(ns: str) -> list[dict[str, Any]]:
        return list(client.list_datasets(namespace_pattern=ns))

    return cache.get_or_fetch_datasets(namespace, _fetch)


def datasets_for_detector(
    namespaces: list[str], *, client: MetaCatClient
) -> tuple[list[dict[str, Any]], datetime]:
    with ThreadPoolExecutor(max_workers=8) as ex:
        results = list(
            ex.map(lambda ns: datasets_for_namespace(ns, client=client), namespaces)
        )

    seen: set[tuple[str, str]] = set()
    out: list[dict[str, Any]] = []
    oldest: datetime | None = None
    for items, fetched_at in results:
        if oldest is None or fetched_at < oldest:
            oldest = fetched_at
        for ds in items:
            key = (ds["namespace"], ds["name"])
            if key in seen:
                continue
            seen.add(key)
            out.append(ds)
    assert oldest is not None
    return out, oldest
