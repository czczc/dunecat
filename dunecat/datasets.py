from collections.abc import Iterator
from typing import Any

from .client import get_client
from .errors import DatasetNotFoundError
from .files import find_files
from .filters import FileFilters


def show_dataset(did: str) -> dict[str, Any]:
    client = get_client()
    ds = client.get_dataset(did=did)
    if ds is None:
        raise DatasetNotFoundError(f"Dataset not found: {did}")
    return ds


def dataset_values(did: str, field: str) -> set[Any]:
    seen: set[Any] = set()
    for item in find_files(did, FileFilters(), with_metadata=True):
        metadata = item.get("metadata") or {}
        if field not in metadata:
            continue
        value = metadata[field]
        if value is None:
            continue
        if isinstance(value, (list, tuple)):
            seen.update(v for v in value if v is not None)
        else:
            seen.add(value)
    return seen


def list_datasets(
    pattern: str | None = None,
    namespace: str | None = None,
) -> Iterator[str]:
    namespace_pattern, name_pattern = _split_pattern(pattern, namespace)
    client = get_client()
    for ds in client.list_datasets(
        namespace_pattern=namespace_pattern,
        name_pattern=name_pattern,
    ):
        yield f"{ds['namespace']}:{ds['name']}"


def _split_pattern(
    pattern: str | None, namespace: str | None
) -> tuple[str | None, str | None]:
    if pattern is None:
        return namespace, None
    if ":" in pattern:
        ns, name = pattern.split(":", 1)
        if namespace and namespace != ns:
            from .errors import ConfigError

            raise ConfigError(
                f"--namespace={namespace!r} conflicts with pattern namespace {ns!r}."
            )
        return ns, name or None
    return namespace, pattern
