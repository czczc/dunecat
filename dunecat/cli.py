import json
import logging
import os
import sys

import typer
from metacat.webapi import AuthenticationError

from .client import get_client
from .datasets import list_datasets, show_dataset
from .errors import ConfigError, DatasetNotFoundError, DunecatError
from .files import file_did, find_files
from .filters import FileFilters, parse_meta, parse_run_range, parse_runs
from .format import render_dataset_table

app = typer.Typer(no_args_is_help=True, add_completion=False)
dataset_app = typer.Typer(no_args_is_help=True, add_completion=False)
app.add_typer(dataset_app, name="dataset")


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
) -> None:
    with _handled_errors():
        for did in list_datasets(pattern=pattern, namespace=namespace):
            typer.echo(did)


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
    except ValueError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(2)
    with _handled_errors():
        for item in find_files(
            did, filters, with_metadata=with_metadata, batch_size=batch_size
        ):
            if json_out:
                typer.echo(json.dumps(item, default=str))
            else:
                typer.echo(file_did(item))


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
        if isinstance(exc, DatasetNotFoundError):
            typer.echo(str(exc), err=True)
            raise typer.Exit(1)
        if isinstance(exc, AuthenticationError):
            typer.echo(_token_expired_message(exc), err=True)
            raise typer.Exit(2)
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
