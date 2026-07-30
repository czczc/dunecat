import json

import pytest
from typer.testing import CliRunner

import dunecat.slowcontrol as sc
from dunecat import cli

runner = CliRunner()


# ---- catalog ----------------------------------------------------------------


def test_catalog_loads_both_detectors():
    for det in sc.DETECTORS:
        sensors = sc.list_sensors(det)
        assert len(sensors) > 100
        assert any(s.unit == "K" for s in sensors)


def test_subsystems_counts_match_sensor_list():
    counts = sc.subsystems("np02")
    assert "cryogenics" in counts
    assert counts["cryogenics"] == sum(
        1 for s in sc.list_sensors("np02") if "cryogenics" in s.subsystems
    )


# ---- sensor resolution -------------------------------------------------------


def test_resolve_by_id():
    s = sc.resolve_sensor("np02", "47910796417819")
    assert s.name == "NP02_TT0100AI"


def test_resolve_by_name_case_insensitive():
    assert sc.resolve_sensor("np02", "np02_tt0100ai").id == "47910796417819"


def test_resolve_by_label_leading_token():
    # full label is 'TE0516 (0.034m)'
    s = sc.resolve_sensor("np02", "TE0516")
    assert s.label.startswith("TE0516")
    assert s.unit == "K"


def test_resolve_ambiguous_lists_candidates():
    with pytest.raises(sc.AmbiguousSensorError) as exc:
        sc.resolve_sensor("np02", "TE05")
    assert len(exc.value.candidates) > 1


def test_resolve_not_found():
    with pytest.raises(sc.SensorNotFoundError):
        sc.resolve_sensor("np02", "NOSUCHSENSOR")


def test_unknown_detector():
    with pytest.raises(sc.DunecatError):
        sc.list_sensors("np03")


# ---- date parsing ------------------------------------------------------------


def test_parse_when_bare_date():
    assert sc.parse_when("2024-07-01") == "2024-07-01T00:00:00"
    assert sc.parse_when("2024-07-01", end_of_day=True) == "2024-07-01T23:59:59"


def test_parse_when_full_timestamp():
    assert sc.parse_when("2024-07-01T12:30:00") == "2024-07-01T12:30:00"
    assert sc.parse_when("2024-07-01 12:30:00", end_of_day=True) == "2024-07-01T12:30:00"


def test_parse_when_rejects_garbage():
    with pytest.raises(sc.DunecatError):
        sc.parse_when("julio")


# ---- history fetch (mocked HTTP) ----------------------------------------------


class _FakeResponse:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload or {}
        self.ok = status_code < 400

    def json(self):
        return self._payload


def test_fetch_history_sorts_and_casts(monkeypatch):
    def fake_get(url, timeout):
        assert "/np04histogram/id1/2024-07-01T00:00:00/2024-07-01T23:59:59" in url
        return _FakeResponse(payload={"200": 2.0, "100": 1.0})

    monkeypatch.setattr(sc.requests, "get", fake_get)
    data = sc.fetch_history(
        "np04", "id1", "2024-07-01T00:00:00", "2024-07-01T23:59:59"
    )
    assert list(data.items()) == [(100, 1.0), (200, 2.0)]


def test_fetch_history_average_endpoint(monkeypatch):
    seen = {}

    def fake_get(url, timeout):
        seen["url"] = url
        return _FakeResponse(payload={})

    monkeypatch.setattr(sc.requests, "get", fake_get)
    sc.fetch_history("np02", "id1", "a", "b", average=True)
    assert "/np02histogram_average/" in seen["url"]


def test_fetch_history_504_is_friendly(monkeypatch):
    monkeypatch.setattr(
        sc.requests, "get", lambda url, timeout: _FakeResponse(status_code=504)
    )
    with pytest.raises(sc.SlowControlAPIError, match="server-side outage"):
        sc.fetch_history("np02", "id1", "a", "b")


# ---- CLI ----------------------------------------------------------------------


def test_cli_sensors_filter_and_json():
    result = runner.invoke(
        cli.app, ["slowcontrol", "sensors", "-D", "np02", "-s", "cryogenics", "--json"]
    )
    assert result.exit_code == 0
    rows = [json.loads(line) for line in result.output.splitlines()]
    assert all("cryogenics" in r["subsystems"] for r in rows)


def test_cli_sensors_unknown_subsystem_errors():
    result = runner.invoke(
        cli.app, ["slowcontrol", "sensors", "-s", "nosuchsubsystem"]
    )
    assert result.exit_code == 1


def test_cli_history_csv_output(monkeypatch):
    monkeypatch.setattr(
        sc.requests,
        "get",
        lambda url, timeout: _FakeResponse(payload={"1719792001889": 87.72}),
    )
    result = runner.invoke(
        cli.app,
        [
            "slowcontrol", "history", "TE0516",
            "--start", "2024-07-01", "--end", "2024-07-01",
        ],
    )
    assert result.exit_code == 0
    lines = result.output.splitlines()
    assert lines[0].startswith("# sensor=TE0516") and "unit=K" in lines[0]
    assert lines[1] == "timestamp,value"
    assert lines[2] == "2024-07-01T00:00:01.889,87.72"
