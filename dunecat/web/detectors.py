from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

from dunecat.client import get_client

_YAML_PATH = Path(__file__).parent / "detectors.yaml"


def load_detectors() -> list[dict[str, Any]]:
    with _YAML_PATH.open() as f:
        data = yaml.safe_load(f) or []
    for entry in data:
        if "id" not in entry or "name" not in entry or "namespaces" not in entry:
            raise ValueError(f"detectors.yaml entry missing required keys: {entry!r}")
    return data


@lru_cache(maxsize=64)
def _datasets_for_namespace(namespace: str) -> list[dict[str, Any]]:
    return list(get_client().list_datasets(namespace_pattern=namespace))


def datasets_for_detector(namespaces: list[str]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str]] = set()
    out: list[dict[str, Any]] = []
    for ns in namespaces:
        for ds in _datasets_for_namespace(ns):
            key = (ds["namespace"], ds["name"])
            if key in seen:
                continue
            seen.add(key)
            out.append(ds)
    return out


def clear_cache() -> None:
    _datasets_for_namespace.cache_clear()
