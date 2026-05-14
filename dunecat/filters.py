from dataclasses import dataclass


@dataclass(frozen=True)
class FileFilters:
    runs: tuple[int, ...] = ()
    namespace: str | None = None

    def to_mql_where_clauses(self) -> list[str]:
        clauses: list[str] = []
        if self.runs:
            joined = ",".join(str(r) for r in self.runs)
            clauses.append(f"core.runs in ({joined})")
        if self.namespace:
            clauses.append(f"namespace = '{_escape(self.namespace)}'")
        return clauses


def parse_runs(value: str | None) -> tuple[int, ...]:
    if value is None:
        return ()
    parts = [p.strip() for p in value.split(",") if p.strip()]
    return tuple(int(p) for p in parts)


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")
