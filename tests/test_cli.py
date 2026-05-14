import json

import pytest
from typer.testing import CliRunner

from dunecat import cli, client
from dunecat.errors import ConfigError, DatasetNotFoundError


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


_SAMPLE_DATASET = {
    "namespace": "hd-protodune-det-reco",
    "name": "example",
    "frozen": True,
    "monotonic": False,
    "creator": "dunepro",
    "created_timestamp": 1738712710.5,
    "updated_by": None,
    "updated_timestamp": None,
    "file_count": 4,
    "metadata": {"core.runs": "(27731, 27732)"},
}


def test_dataset_show_renders_table(monkeypatch):
    monkeypatch.setattr(cli, "show_dataset", lambda did: _SAMPLE_DATASET)
    result = runner.invoke(cli.app, ["dataset", "show", "hd-protodune-det-reco:example"])

    assert result.exit_code == 0, result.output
    assert "hd-protodune-det-reco:example" in result.stdout
    assert "dunepro" in result.stdout
    assert "core.runs" in result.stdout
    assert "(27731, 27732)" in result.stdout


def test_dataset_show_json_emits_single_line(monkeypatch):
    monkeypatch.setattr(cli, "show_dataset", lambda did: _SAMPLE_DATASET)
    result = runner.invoke(
        cli.app, ["dataset", "show", "hd-protodune-det-reco:example", "--json"]
    )

    assert result.exit_code == 0, result.output
    lines = [ln for ln in result.stdout.splitlines() if ln.strip()]
    assert len(lines) == 1
    parsed = json.loads(lines[0])
    assert parsed["namespace"] == "hd-protodune-det-reco"
    assert parsed["metadata"]["core.runs"] == "(27731, 27732)"


_FILE_ITEMS = [
    {
        "fid": "abc1",
        "namespace": "hd-protodune-det-reco",
        "name": "np04hd_raw_run027731_0001.root",
        "metadata": {"core.runs": [27731]},
    },
    {
        "fid": "abc2",
        "namespace": "hd-protodune-det-reco",
        "name": "np04hd_raw_run027732_0001.root",
        "metadata": {"core.runs": [27732]},
    },
]


def test_dataset_files_plain_output_streams_dids(monkeypatch):
    captured = {}

    def fake_find_files(did, filters, with_metadata=False, batch_size=1000):
        captured["did"] = did
        captured["filters"] = filters
        captured["with_metadata"] = with_metadata
        captured["batch_size"] = batch_size
        for item in _FILE_ITEMS:
            yield item

    monkeypatch.setattr(cli, "find_files", fake_find_files)

    result = runner.invoke(
        cli.app,
        [
            "dataset",
            "files",
            "hd-protodune-det-reco:ds",
            "--runs",
            "27731,27732",
        ],
    )

    assert result.exit_code == 0, result.output
    assert result.stdout.splitlines() == [
        "hd-protodune-det-reco:np04hd_raw_run027731_0001.root",
        "hd-protodune-det-reco:np04hd_raw_run027732_0001.root",
    ]
    assert captured["did"] == "hd-protodune-det-reco:ds"
    assert captured["filters"].runs == (27731, 27732)
    assert captured["with_metadata"] is False
    assert captured["batch_size"] == 1000


def test_dataset_files_json_emits_one_object_per_line(monkeypatch):
    monkeypatch.setattr(
        cli, "find_files", lambda *a, **kw: iter(_FILE_ITEMS)
    )

    result = runner.invoke(
        cli.app,
        ["dataset", "files", "hd-protodune-det-reco:ds", "--json"],
    )

    assert result.exit_code == 0, result.output
    lines = [ln for ln in result.stdout.splitlines() if ln.strip()]
    assert len(lines) == 2
    assert json.loads(lines[0])["fid"] == "abc1"
    assert json.loads(lines[1])["fid"] == "abc2"


def test_dataset_files_with_metadata_flag_passed_through(monkeypatch):
    captured = {}

    def fake_find_files(did, filters, with_metadata=False, batch_size=1000):
        captured["with_metadata"] = with_metadata
        return iter(())

    monkeypatch.setattr(cli, "find_files", fake_find_files)

    result = runner.invoke(
        cli.app,
        [
            "dataset",
            "files",
            "hd-protodune-det-reco:ds",
            "--with-metadata",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert captured["with_metadata"] is True


def test_dataset_files_empty_result_exits_zero(monkeypatch):
    monkeypatch.setattr(cli, "find_files", lambda *a, **kw: iter(()))
    result = runner.invoke(cli.app, ["dataset", "files", "ns:ds"])
    assert result.exit_code == 0
    assert result.stdout == ""


_DATED_FILES = [
    {"namespace": "ns", "name": "a_20250101T120000.root"},
    {"namespace": "ns", "name": "b_20250215T120000.root"},
    {"namespace": "ns", "name": "c_20250215T130000.root"},
    {"namespace": "ns", "name": "d_20250301T120000.root"},
]


def test_dataset_files_date_range_filters_by_run_time(monkeypatch):
    monkeypatch.setattr(cli, "find_files", lambda *a, **kw: iter(_DATED_FILES))
    result = runner.invoke(
        cli.app,
        [
            "dataset",
            "files",
            "ns:ds",
            "--date-range",
            "2025-02-01:2025-02-28",
        ],
    )
    assert result.exit_code == 0, result.output
    assert result.stdout.splitlines() == [
        "ns:b_20250215T120000.root",
        "ns:c_20250215T130000.root",
    ]


def test_dataset_files_one_per_day_dedupes_by_utc_date(monkeypatch):
    monkeypatch.setattr(cli, "find_files", lambda *a, **kw: iter(_DATED_FILES))
    result = runner.invoke(
        cli.app, ["dataset", "files", "ns:ds", "--one-per-day"]
    )
    assert result.exit_code == 0, result.output
    assert result.stdout.splitlines() == [
        "ns:a_20250101T120000.root",
        "ns:b_20250215T120000.root",
        "ns:d_20250301T120000.root",
    ]


def test_dataset_files_date_range_max_candidates_exits_1(monkeypatch):
    big_stream = (
        {"namespace": "ns", "name": f"f_2025010{i % 10}T120000.root"}
        for i in range(100)
    )
    monkeypatch.setattr(cli, "find_files", lambda *a, **kw: big_stream)
    result = runner.invoke(
        cli.app,
        [
            "dataset",
            "files",
            "ns:ds",
            "--date-range",
            "1900-01-01:1901-01-01",  # nothing will match → all become candidates
            "--date-range-max-candidates",
            "5",
        ],
    )
    assert result.exit_code == 1
    assert "Exceeded --date-range-max-candidates" in result.stderr


def test_dataset_files_filename_time_regex_override(monkeypatch):
    custom = [
        {"namespace": "ns", "name": "foo_run20250215_x.root"},
        {"namespace": "ns", "name": "foo_run20250301_x.root"},
    ]
    monkeypatch.setattr(cli, "find_files", lambda *a, **kw: iter(custom))
    result = runner.invoke(
        cli.app,
        [
            "dataset",
            "files",
            "ns:ds",
            "--date-range",
            "2025-02-01:2025-02-28",
            "--filename-time-regex",
            r"run(\d{8})_",
            "--filename-time-format",
            "%Y%m%d",
        ],
    )
    assert result.exit_code == 0, result.output
    assert result.stdout.splitlines() == ["ns:foo_run20250215_x.root"]


def test_dataset_files_bad_filename_regex_exits_2(monkeypatch):
    monkeypatch.setattr(cli, "find_files", lambda *a, **kw: iter(()))
    result = runner.invoke(
        cli.app,
        ["dataset", "files", "ns:ds", "--filename-time-regex", "(unclosed"],
    )
    assert result.exit_code == 2


def test_dataset_values_prints_sorted_one_per_line(monkeypatch):
    monkeypatch.setattr(
        cli, "dataset_values", lambda did, field: {27732, 27734, 27731, 27740}
    )
    result = runner.invoke(cli.app, ["dataset", "values", "ns:ds", "core.runs"])

    assert result.exit_code == 0, result.output
    assert result.stdout.splitlines() == ["27731", "27732", "27734", "27740"]


def test_dataset_values_json_array(monkeypatch):
    monkeypatch.setattr(
        cli, "dataset_values", lambda did, field: {"confirmed", "rejected"}
    )
    result = runner.invoke(
        cli.app, ["dataset", "values", "ns:ds", "dune.output_status", "--json"]
    )

    assert result.exit_code == 0, result.output
    parsed = json.loads(result.stdout)
    assert parsed == ["confirmed", "rejected"]


def test_dataset_values_empty_field_exits_zero(monkeypatch):
    monkeypatch.setattr(cli, "dataset_values", lambda did, field: set())
    result = runner.invoke(cli.app, ["dataset", "values", "ns:ds", "nope"])
    assert result.exit_code == 0
    assert result.stdout == ""


def test_dataset_show_missing_did_exits_1(monkeypatch):
    def raises(did):
        raise DatasetNotFoundError(f"Dataset not found: {did}")

    monkeypatch.setattr(cli, "show_dataset", raises)
    result = runner.invoke(cli.app, ["dataset", "show", "nope:not-here"])

    assert result.exit_code == 1
    assert "Dataset not found: nope:not-here" in result.stderr
