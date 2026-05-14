from dataclasses import dataclass, field


@dataclass(frozen=True)
class FileFilters:
    runs: tuple[int, ...] = ()
    run_range: tuple[int, int] | None = None
    namespace: str | None = None
    meta: tuple[tuple[str, str], ...] = ()

    def to_mql_where_clauses(self) -> list[str]:
        clauses: list[str] = []
        if self.runs:
            joined = ",".join(str(r) for r in self.runs)
            clauses.append(f"core.runs in ({joined})")
        if self.run_range is not None:
            lo, hi = self.run_range
            clauses.append(f"core.runs >= {lo} and core.runs <= {hi}")
        if self.namespace:
            clauses.append(f"namespace = '{_escape(self.namespace)}'")
        for key, value in self.meta:
            clauses.append(f"{key} = {_format_value(value)}")
        return clauses


def parse_runs(value: str | None) -> tuple[int, ...]:
    if value is None:
        return ()
    parts = [p.strip() for p in value.split(",") if p.strip()]
    return tuple(int(p) for p in parts)


def parse_run_range(value: str | None) -> tuple[int, int] | None:
    if value is None:
        return None
    if "-" not in value:
        raise ValueError(f"--run-range expects 'MIN-MAX', got {value!r}")
    lo_s, hi_s = value.split("-", 1)
    lo, hi = int(lo_s.strip()), int(hi_s.strip())
    if lo > hi:
        raise ValueError(f"--run-range MIN must not exceed MAX, got {value!r}")
    return (lo, hi)


def parse_meta(values: list[str] | None) -> tuple[tuple[str, str], ...]:
    if not values:
        return ()
    pairs: list[tuple[str, str]] = []
    for raw in values:
        if "=" not in raw:
            raise ValueError(
                f"--meta expects 'KEY=VALUE', got {raw!r}"
            )
        key, value = raw.split("=", 1)
        key = key.strip()
        if not key:
            raise ValueError(f"--meta has empty key in {raw!r}")
        pairs.append((key, value))
    return tuple(pairs)


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _format_value(value: str) -> str:
    try:
        return str(int(value))
    except ValueError:
        pass
    try:
        f = float(value)
    except ValueError:
        pass
    else:
        return repr(f)
    return f"'{_escape(value)}'"


def value_matches(metadata_value, user_value: str) -> bool:
    if metadata_value is None:
        return False
    try:
        return float(metadata_value) == float(user_value)
    except (ValueError, TypeError):
        pass
    return str(metadata_value) == user_value
