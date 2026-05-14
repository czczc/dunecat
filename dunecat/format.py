from datetime import UTC, datetime
from typing import Any

from rich.console import Console
from rich.table import Table

_console = Console()


def render_dataset_table(ds: dict[str, Any]) -> None:
    table = Table(title=f"{ds['namespace']}:{ds['name']}", show_header=True)
    table.add_column("Field", style="bold")
    table.add_column("Value", overflow="fold")

    table.add_row("Creator", _str(ds.get("creator")))
    table.add_row("Create timestamp", _fmt_ts(ds.get("created_timestamp")))
    table.add_row("Updated by", _str(ds.get("updated_by")))
    table.add_row("Update timestamp", _fmt_ts(ds.get("updated_timestamp")))
    table.add_row("Frozen", _str(ds.get("frozen")))
    table.add_row("Monotonic", _str(ds.get("monotonic")))
    table.add_row("File count", _str(ds.get("file_count")))

    metadata = ds.get("metadata") or {}
    if metadata:
        table.add_section()
        for key in sorted(metadata):
            table.add_row(key, _str(metadata[key]))

    _console.print(table)


def _fmt_ts(value: Any) -> str:
    if value is None:
        return ""
    return datetime.fromtimestamp(float(value), tz=UTC).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )


def _str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "yes" if value else "no"
    return str(value)
