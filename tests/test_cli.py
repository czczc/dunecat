import pytest
from typer.testing import CliRunner

from dunecat import cli, client
from dunecat.errors import ConfigError


runner = CliRunner()


@pytest.fixture(autouse=True)
def _clear_client_cache():
    client.get_client.cache_clear()
    yield
    client.get_client.cache_clear()


def test_dataset_list_prints_one_did_per_line(monkeypatch):
    def fake_list_datasets(pattern, namespace):
        assert pattern == "hd-protodune-det-reco:*cosmic*"
        assert namespace is None
        yield "hd-protodune-det-reco:alpha_cosmic"
        yield "hd-protodune-det-reco:beta_cosmic"

    monkeypatch.setattr(cli, "list_datasets", fake_list_datasets)

    result = runner.invoke(
        cli.app, ["dataset", "list", "hd-protodune-det-reco:*cosmic*"]
    )

    assert result.exit_code == 0, result.output
    assert result.stdout.splitlines() == [
        "hd-protodune-det-reco:alpha_cosmic",
        "hd-protodune-det-reco:beta_cosmic",
    ]


def test_config_error_exits_2_with_stderr_message(monkeypatch):
    def fake_list_datasets(pattern, namespace):
        raise ConfigError("METACAT_SERVER_URL is not set.")
        yield  # pragma: no cover - generator marker

    monkeypatch.setattr(cli, "list_datasets", fake_list_datasets)

    result = runner.invoke(cli.app, ["dataset", "list", "anything"])

    assert result.exit_code == 2
    assert "METACAT_SERVER_URL is not set." in result.stderr


def test_authentication_error_exits_2_with_login_instructions(monkeypatch):
    from metacat.webapi import AuthenticationError

    def fake_list_datasets(pattern, namespace):
        raise AuthenticationError("token expired")
        yield  # pragma: no cover

    monkeypatch.setattr(cli, "list_datasets", fake_list_datasets)
    monkeypatch.setenv("METACAT_SERVER_URL", "https://meta.example/app")
    monkeypatch.setenv("METACAT_AUTH_SERVER_URL", "https://auth.example/auth")

    result = runner.invoke(cli.app, ["dataset", "list", "anything"])

    assert result.exit_code == 2
    assert "Token missing or expired" in result.stderr
    assert "metacat -s https://meta.example/app" in result.stderr
    assert "auth login" in result.stderr


def test_server_flag_overrides_env(monkeypatch):
    captured = {}

    def fake_list_datasets(pattern, namespace):
        captured["server"] = __import__("os").environ.get("METACAT_SERVER_URL")
        return iter(())

    monkeypatch.setattr(cli, "list_datasets", fake_list_datasets)
    monkeypatch.setenv("METACAT_SERVER_URL", "https://from-env.example/app")
    monkeypatch.setenv("METACAT_AUTH_SERVER_URL", "https://auth.example/auth")

    result = runner.invoke(
        cli.app,
        [
            "--server",
            "https://from-flag.example/app",
            "dataset",
            "list",
            "anything",
        ],
    )

    assert result.exit_code == 0, result.output
    assert captured["server"] == "https://from-flag.example/app"
