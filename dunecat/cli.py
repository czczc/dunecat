import json
import logging
import os
import re
import sys
from typing import Any

import typer
from metacat.webapi import AuthenticationError, MCWebAPIError

from .client import get_client
from .datasets import dataset_values, list_datasets, show_dataset
from .errors import (
    ConfigError,
    DatasetNotFoundError,
    DunecatError,
    FileDIDNotFoundError,
)
from .files import file_datasets, file_did, find_files
from .filters import FileFilters, parse_meta, parse_run_range, parse_runs
from .format import render_dataset_table
from .query import run_query
from .timestamps import (
    DEFAULT_FORMAT,
    DEFAULT_MAX_CANDIDATES,
    DEFAULT_REGEX,
    CandidateLimitExceeded,
    apply_date_range,
    apply_one_per_day,
    parse_date_range,
)

app = typer.Typer(no_args_is_help=True, add_completion=False)
dataset_app = typer.Typer(no_args_is_help=True, add_completion=False)
file_app = typer.Typer(no_args_is_help=True, add_completion=False)
app.add_typer(dataset_app, name="dataset")
app.add_typer(file_app, name="file")


@app.callback()
def _root(
    server: str | None = typer.Option(
        None, "--server", help="Override METACAT_SERVER_URL for this invocation."
    ),
    auth_server: str | None = typer.Option(
        None,
        "--auth-server",
        help="Override METACAT_AUTH_SERVER_URL for this invocation.",
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable debug logging on stderr."
    ),
) -> None:
    if server is not None:
        os.environ["METACAT_SERVER_URL"] = server
    if auth_server is not None:
        os.environ["METACAT_AUTH_SERVER_URL"] = auth_server
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.WARNING,
        stream=sys.stderr,
        format="%(levelname)s %(name)s: %(message)s",
    )
    get_client.cache_clear()


@dataset_app.command("list")
def dataset_list(
    pattern: str | None = typer.Argument(
        None,
        help=(
            "fnmatch pattern, optionally namespaced as 'NAMESPACE:NAME_PATTERN'. "
            "Omit to list every dataset visible on the server."
        ),
    ),
    namespace: str | None = typer.Option(
        None, "--namespace", "-n", help="Restrict to this namespace."
    ),
    meta: list[str] | None = typer.Option(
        None,
        "--meta",
        help="Metadata equality filter 'KEY=VALUE'. Repeatable; applied client-side over dataset metadata.",
    ),
) -> None:
    try:
        meta_pairs = parse_meta(meta)
    except ValueError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(2)
    with _handled_errors():
        for did in list_datasets(
            pattern=pattern, namespace=namespace, meta=meta_pairs
        ):
            typer.echo(did)


@file_app.command("datasets")
def file_datasets_cmd(
    did: str = typer.Argument(..., help="File DID as 'NAMESPACE:NAME'."),
    json_out: bool = typer.Option(
        False, "--json", help="Emit a JSON array of dataset DIDs."
    ),
) -> None:
    with _handled_errors():
        datasets = file_datasets(did)
    if json_out:
        typer.echo(json.dumps(datasets))
    else:
        for d in datasets:
            typer.echo(d)


@app.command("query")
def query_cmd(
    mql: str = typer.Argument(..., help="Raw MQL query string."),
    with_metadata: bool = typer.Option(
        False, "--with-metadata", help="Include file metadata in output."
    ),
    json_out: bool = typer.Option(
        False, "--json", help="Emit JSONL (one JSON object per line)."
    ),
    batch_size: int = typer.Option(
        1000, "--batch-size", help="Server-side batch size for streaming."
    ),
) -> None:
    with _handled_errors():
        for item in run_query(mql, with_metadata=with_metadata, batch_size=batch_size):
            if json_out:
                typer.echo(json.dumps(item, default=str))
            else:
                typer.echo(_render_query_item(item))


def _render_query_item(item: dict[str, Any]) -> str:
    if "namespace" in item and "name" in item:
        return f"{item['namespace']}:{item['name']}"
    return json.dumps(item, default=str)


@dataset_app.command("files")
def dataset_files(
    did: str = typer.Argument(..., help="Dataset DID as 'NAMESPACE:NAME'."),
    runs: str | None = typer.Option(
        None, "--runs", help="Comma-separated list of run numbers."
    ),
    run_range: str | None = typer.Option(
        None, "--run-range", help="Inclusive run range as 'MIN-MAX'."
    ),
    namespace: str | None = typer.Option(
        None,
        "--namespace",
        "-n",
        help="Restrict results to files in this namespace.",
    ),
    meta: list[str] | None = typer.Option(
        None,
        "--meta",
        help="Metadata equality filter 'KEY=VALUE'. Repeatable; multiple filters AND together.",
    ),
    date_range: str | None = typer.Option(
        None,
        "--date-range",
        help="UTC date range 'YYYY-MM-DD:YYYY-MM-DD' against the run timestamp extracted from each filename. Inclusive.",
    ),
    one_per_day: bool = typer.Option(
        False,
        "--one-per-day",
        help="Keep the first file streamed for each UTC calendar date (run-time).",
    ),
    filename_time_regex: str | None = typer.Option(
        None,
        "--filename-time-regex",
        help="Override the regex used to extract the run timestamp from filenames.",
    ),
    filename_time_format: str = typer.Option(
        DEFAULT_FORMAT,
        "--filename-time-format",
        help="strptime format for the timestamp captured by --filename-time-regex.",
    ),
    date_range_max_candidates: int = typer.Option(
        DEFAULT_MAX_CANDIDATES,
        "--date-range-max-candidates",
        help="Abort if more than N candidates stream in before the client-side date/one-per-day filter.",
    ),
    with_metadata: bool = typer.Option(
        False, "--with-metadata", help="Include file metadata in output."
    ),
    json_out: bool = typer.Option(
        False, "--json", help="Emit JSONL (one JSON object per line)."
    ),
    batch_size: int = typer.Option(
        1000, "--batch-size", help="Server-side batch size for streaming."
    ),
) -> None:
    try:
        filters = FileFilters(
            runs=parse_runs(runs),
            run_range=parse_run_range(run_range),
            namespace=namespace,
            meta=parse_meta(meta),
        )
        parsed_date_range = parse_date_range(date_range) if date_range else None
        regex = (
            re.compile(filename_time_regex)
            if filename_time_regex
            else DEFAULT_REGEX
        )
    except (ValueError, re.error) as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(2)

    with _handled_errors():
        stream = find_files(
            did, filters, with_metadata=with_metadata, batch_size=batch_size
        )
        if parsed_date_range is not None:
            stream = apply_date_range(
                stream,
                parsed_date_range,
                regex=regex,
                fmt=filename_time_format,
                max_candidates=date_range_max_candidates,
            )
        if one_per_day:
            stream = apply_one_per_day(
                stream,
                regex=regex,
                fmt=filename_time_format,
                max_candidates=date_range_max_candidates,
            )
        try:
            for item in stream:
                if json_out:
                    typer.echo(json.dumps(item, default=str))
                else:
                    typer.echo(file_did(item))
        except CandidateLimitExceeded as e:
            typer.echo(str(e), err=True)
            raise typer.Exit(1)


@dataset_app.command("values")
def dataset_values_cmd(
    did: str = typer.Argument(..., help="Dataset DID as 'NAMESPACE:NAME'."),
    field: str = typer.Argument(..., help="Metadata key, e.g. 'core.runs'."),
    json_out: bool = typer.Option(
        False, "--json", help="Emit a JSON array of the sorted distinct values."
    ),
) -> None:
    with _handled_errors():
        values = dataset_values(did, field)
    sorted_values = sorted(values, key=_sort_key)
    if json_out:
        typer.echo(json.dumps(sorted_values, default=str))
    else:
        for v in sorted_values:
            typer.echo(str(v))


def _sort_key(value: Any) -> tuple[int, Any]:
    if isinstance(value, bool):
        return (0, int(value))
    if isinstance(value, (int, float)):
        return (0, value)
    return (1, str(value))


@dataset_app.command("show")
def dataset_show(
    did: str = typer.Argument(..., help="Dataset DID as 'NAMESPACE:NAME'."),
    json_out: bool = typer.Option(
        False, "--json", help="Emit the full dataset record as one-line JSON."
    ),
) -> None:
    with _handled_errors():
        ds = show_dataset(did)
    if json_out:
        typer.echo(json.dumps(ds, default=str))
    else:
        render_dataset_table(ds)


class _handled_errors:
    def __enter__(self) -> "_handled_errors":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc is None:
            return False
        if isinstance(exc, ConfigError):
            typer.echo(str(exc), err=True)
            raise typer.Exit(2)
        if isinstance(exc, (DatasetNotFoundError, FileDIDNotFoundError)):
            typer.echo(str(exc), err=True)
            raise typer.Exit(1)
        if isinstance(exc, AuthenticationError):
            typer.echo(_token_expired_message(exc), err=True)
            raise typer.Exit(2)
        if isinstance(exc, MCWebAPIError):
            typer.echo(str(exc), err=True)
            raise typer.Exit(1)
        if isinstance(exc, DunecatError):
            typer.echo(str(exc), err=True)
            raise typer.Exit(1)
        return False


def _token_expired_message(exc: AuthenticationError) -> str:
    server = os.environ.get("METACAT_SERVER_URL", "<METACAT_SERVER_URL>")
    auth = os.environ.get("METACAT_AUTH_SERVER_URL", "<METACAT_AUTH_SERVER_URL>")
    return (
        f"Token missing or expired ({exc}). Run:\n"
        f"  metacat -s {server} -a {auth} auth login -m <method> <username>"
    )
