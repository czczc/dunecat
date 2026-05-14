from collections.abc import Iterator

from .client import get_client


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
