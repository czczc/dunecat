from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from dunecat.client import get_client

from . import cache

_YAML_PATH = Path(__file__).parent / "detectors.yaml"


def load_detectors() -> list[dict[str, Any]]:
    with _YAML_PATH.open() as f:
        data = yaml.safe_load(f) or []
    for entry in data:
        if "id" not in entry or "name" not in entry or "namespaces" not in entry:
            raise ValueError(f"detectors.yaml entry missing required keys: {entry!r}")
    return data


def detector_by_id(detector_id: str) -> dict[str, Any] | None:
    for d in load_detectors():
        if d["id"] == detector_id:
            return d
    return None


def datasets_for_namespace(namespace: str) -> tuple[list[dict[str, Any]], datetime]:
    return cache.get_or_fetch(namespace, _fetch_from_metacat)


def _fetch_from_metacat(namespace: str) -> list[dict[str, Any]]:
    return list(get_client().list_datasets(namespace_pattern=namespace))


def datasets_for_detector(
    namespaces: list[str],
) -> tuple[list[dict[str, Any]], datetime]:
    seen: set[tuple[str, str]] = set()
    out: list[dict[str, Any]] = []
    oldest: datetime | None = None
    for ns in namespaces:
        items, fetched_at = datasets_for_namespace(ns)
        if oldest is None or fetched_at < oldest:
            oldest = fetched_at
        for ds in items:
            key = (ds["namespace"], ds["name"])
            if key in seen:
                continue
            seen.add(key)
            out.append(ds)
    assert oldest is not None  # namespaces is always non-empty per YAML schema
    return out, oldest
