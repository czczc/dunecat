import logging
import os
import sys

import typer
from metacat.webapi import AuthenticationError

from .client import get_client
from .datasets import list_datasets
from .errors import ConfigError, DunecatError

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


class _handled_errors:
    def __enter__(self) -> "_handled_errors":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc is None:
            return False
        if isinstance(exc, ConfigError):
            typer.echo(str(exc), err=True)
            raise typer.Exit(2)
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
