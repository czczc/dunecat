import pytest

from dunecat import client
from dunecat.errors import ConfigError


@pytest.fixture(autouse=True)
def _clear_cache():
    client.get_client.cache_clear()
    yield
    client.get_client.cache_clear()


def test_missing_server_url_raises_config_error(monkeypatch):
    monkeypatch.delenv("METACAT_SERVER_URL", raising=False)
    monkeypatch.setenv("METACAT_AUTH_SERVER_URL", "https://auth.example/auth")
    monkeypatch.setattr(client, "_load_env", lambda: None)
    with pytest.raises(ConfigError, match="METACAT_SERVER_URL"):
        client.get_client()


def test_missing_auth_server_url_raises_config_error(monkeypatch):
    monkeypatch.setenv("METACAT_SERVER_URL", "https://meta.example/app")
    monkeypatch.delenv("METACAT_AUTH_SERVER_URL", raising=False)
    monkeypatch.setattr(client, "_load_env", lambda: None)
    with pytest.raises(ConfigError, match="METACAT_AUTH_SERVER_URL"):
        client.get_client()


def test_get_client_constructs_metacat_client(monkeypatch):
    captured = {}

    class FakeMetaCatClient:
        def __init__(self, server_url, auth_server_url, token_library):
            captured["server_url"] = server_url
            captured["auth_server_url"] = auth_server_url
            captured["token_library"] = token_library

    monkeypatch.setattr(client, "MetaCatClient", FakeMetaCatClient)
    monkeypatch.setattr(client, "_load_env", lambda: None)
    monkeypatch.setenv("METACAT_SERVER_URL", "https://meta.example/app")
    monkeypatch.setenv("METACAT_AUTH_SERVER_URL", "https://auth.example/auth")

    result = client.get_client()

    assert isinstance(result, FakeMetaCatClient)
    assert captured["server_url"] == "https://meta.example/app"
    assert captured["auth_server_url"] == "https://auth.example/auth"
    assert captured["token_library"].endswith(".token_library")
