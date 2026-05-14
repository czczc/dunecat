import os
from functools import cache
from pathlib import Path

from dotenv import load_dotenv
from metacat.webapi import MetaCatClient

from .errors import ConfigError


def _load_env() -> None:
    load_dotenv()


@cache
def get_client() -> MetaCatClient:
    _load_env()
    server_url = os.environ.get("METACAT_SERVER_URL")
    auth_server_url = os.environ.get("METACAT_AUTH_SERVER_URL")
    if not server_url:
        raise ConfigError(
            "METACAT_SERVER_URL is not set. "
            "Set it in .env or pass --server."
        )
    if not auth_server_url:
        raise ConfigError(
            "METACAT_AUTH_SERVER_URL is not set. "
            "Set it in .env or pass --auth-server."
        )
    return MetaCatClient(
        server_url=server_url,
        auth_server_url=auth_server_url,
        token_library=str(Path.home() / ".token_library"),
    )
