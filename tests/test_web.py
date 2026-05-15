import pytest
from fastapi.testclient import TestClient
from metacat.webapi import AuthenticationError

from dunecat.web import app
from dunecat.web import cache as cache_mod
from dunecat.web import detectors as det_mod


@pytest.fixture(autouse=True)
def _isolated_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(cache_mod, "DB_PATH", tmp_path / "test.db")
    cache_mod.init_db()
    yield


@pytest.fixture
def client():
    return TestClient(app)


def _fake_list(items_by_ns):
    def _impl(namespace_pattern=None, name_pattern=None):
        yield from items_by_ns.get(namespace_pattern, [])

    return _impl


def test_detectors_endpoint_returns_yaml_only(client):
    response = client.get("/api/detectors")
    assert response.status_code == 200
    payload = response.json()
    assert {d["id"] for d in payload} == {
        "protodune-hd",
        "protodune-vd",
        "fardet-hd",
        "fardet-vd",
    }
    # Counts must NOT be on this endpoint (it's now YAML-only / instant)
    for d in payload:
        assert "datasets_count" not in d
        assert "files_count" not in d
        assert "namespaces" in d


def test_detector_counts_endpoint_applies_default_filters(monkeypatch, client):
    items = {
        "hd-protodune": [
            {  # official, non-empty
                "namespace": "hd-protodune", "name": "a",
                "file_count": 10, "creator": "dunepro",
            },
            {  # non-official → excluded by default
                "namespace": "hd-protodune", "name": "b",
                "file_count": 5, "creator": "someone",
            },
            {  # empty → always excluded
                "namespace": "hd-protodune", "name": "c",
                "file_count": 0, "creator": "dunepro",
            },
        ],
        "hd-protodune-det-reco": [
            {
                "namespace": "hd-protodune-det-reco", "name": "d",
                "file_count": 3, "creator": "dunepro",
            },
        ],
        "vd-protodune": [],
        "fardet-hd": [],
        "fardet-vd": [],
        "fd_vd_mc_reco": [],
    }

    class FakeClient:
        list_datasets = staticmethod(_fake_list(items))

    monkeypatch.setattr(
        "dunecat.web.detectors.get_client", lambda: FakeClient(), raising=False
    )

    response = client.get("/api/detectors/counts")
    assert response.status_code == 200
    by_id = {d["id"]: d for d in response.json()}
    # protodune-hd: only a (10) + d (3) survive; b non-official, c empty
    assert by_id["protodune-hd"]["datasets_count"] == 2
    assert by_id["protodune-hd"]["files_count"] == 13


def test_authentication_error_surfaces_as_401(monkeypatch, client):
    def raising(self, namespace_pattern=None, name_pattern=None):
        raise AuthenticationError("token expired")
        yield  # pragma: no cover

    class FakeClient:
        list_datasets = raising

    monkeypatch.setattr(
        "dunecat.web.detectors.get_client", lambda: FakeClient(), raising=False
    )
    monkeypatch.setenv("METACAT_SERVER_URL", "https://m.example/app")
    monkeypatch.setenv("METACAT_AUTH_SERVER_URL", "https://m.example/auth")

    response = client.get("/api/detectors/counts")

    assert response.status_code == 401
    body = response.json()
    assert "Token missing or expired" in body["detail"]
    assert "metacat -s https://m.example/app" in body["detail"]


# --- /api/datasets ---------------------------------------------------------


_DS_FIXTURES = {
    "hd-protodune": [
        {
            "namespace": "hd-protodune",
            "name": "alpha_cosmic",
            "file_count": 10,
            "creator": "dunepro",
            "metadata": {"core.data_tier": "raw"},
        },
        {
            "namespace": "hd-protodune",
            "name": "beta_beam",
            "file_count": 5,
            "creator": "dunepro",
            "metadata": {"core.data_tier": "raw"},
        },
    ],
    "hd-protodune-det-reco": [
        {
            "namespace": "hd-protodune-det-reco",
            "name": "gamma_cosmic_reco",
            "file_count": 3,
            "creator": "dunepro",
            "metadata": {
                "core.data_tier": "full-reconstructed",
                "dune.output_status": "confirmed",
            },
        },
        {
            "namespace": "hd-protodune-det-reco",
            "name": "delta_user_test",
            "file_count": 2,
            "creator": "chaoz",
            "metadata": {"core.data_tier": "full-reconstructed"},
        },
        {
            "namespace": "hd-protodune-det-reco",
            "name": "epsilon_empty",
            "file_count": 0,
            "creator": "dunepro",
            "metadata": {"core.data_tier": "full-reconstructed"},
        },
    ],
    "vd-protodune": [],
    "fardet-hd": [],
    "fardet-vd": [],
    "fd_vd_mc_reco": [],
}


def _install_fake_client(monkeypatch, calls=None):
    if calls is None:
        calls = []

    class FakeClient:
        def list_datasets(self, namespace_pattern=None, name_pattern=None):
            calls.append(namespace_pattern)
            yield from _DS_FIXTURES.get(namespace_pattern, [])

    monkeypatch.setattr(
        "dunecat.web.detectors.get_client", lambda: FakeClient(), raising=False
    )
    return calls


def test_datasets_basic_pagination(monkeypatch, client):
    _install_fake_client(monkeypatch)

    response = client.get(
        "/api/datasets",
        params={"detector": "protodune-hd", "page": 1, "page_size": 2},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 3
    assert body["page"] == 1
    assert body["page_size"] == 2
    assert len(body["rows"]) == 2
    assert {r["did"] for r in body["rows"]} <= {
        "hd-protodune:alpha_cosmic",
        "hd-protodune:beta_beam",
        "hd-protodune-det-reco:gamma_cosmic_reco",
    }


def test_datasets_pagination_boundary(monkeypatch, client):
    _install_fake_client(monkeypatch)
    response = client.get(
        "/api/datasets",
        params={"detector": "protodune-hd", "page": 2, "page_size": 2},
    )
    body = response.json()
    assert body["page"] == 2
    assert len(body["rows"]) == 1  # 3 total - 2 on page 1


def test_datasets_pattern_filter(monkeypatch, client):
    _install_fake_client(monkeypatch)
    response = client.get(
        "/api/datasets",
        params={"detector": "protodune-hd", "pattern": "*cosmic*"},
    )
    body = response.json()
    names = [r["name"] for r in body["rows"]]
    assert sorted(names) == ["alpha_cosmic", "gamma_cosmic_reco"]


def test_datasets_tier_filter(monkeypatch, client):
    _install_fake_client(monkeypatch)
    response = client.get(
        "/api/datasets",
        params={"detector": "protodune-hd", "tier": "full-reconstructed"},
    )
    body = response.json()
    assert [r["name"] for r in body["rows"]] == ["gamma_cosmic_reco"]


def test_datasets_meta_filter_ands_multiple(monkeypatch, client):
    _install_fake_client(monkeypatch)
    response = client.get(
        "/api/datasets",
        params={
            "detector": "protodune-hd",
            "meta": [
                "core.data_tier=full-reconstructed",
                "dune.output_status=confirmed",
            ],
        },
    )
    body = response.json()
    assert [r["name"] for r in body["rows"]] == ["gamma_cosmic_reco"]


def test_datasets_meta_filter_bad_syntax_400(monkeypatch, client):
    _install_fake_client(monkeypatch)
    response = client.get(
        "/api/datasets",
        params={"detector": "protodune-hd", "meta": ["no-equals"]},
    )
    assert response.status_code == 400


def test_datasets_unknown_detector_404(monkeypatch, client):
    _install_fake_client(monkeypatch)
    response = client.get("/api/datasets", params={"detector": "nope"})
    assert response.status_code == 404


def test_datasets_cache_hit_skips_metacat(monkeypatch, client):
    calls = _install_fake_client(monkeypatch)

    client.get("/api/datasets", params={"detector": "protodune-hd"})
    assert "hd-protodune" in calls and "hd-protodune-det-reco" in calls
    calls.clear()

    client.get("/api/datasets", params={"detector": "protodune-hd"})
    assert calls == []  # all served from cache


def test_datasets_refresh_invalidates_cache(monkeypatch, client):
    calls = _install_fake_client(monkeypatch)

    client.get("/api/datasets", params={"detector": "protodune-hd"})
    calls.clear()

    refresh = client.post(
        "/api/datasets/refresh", params={"detector": "protodune-hd"}
    )
    assert refresh.status_code == 204

    client.get("/api/datasets", params={"detector": "protodune-hd"})
    assert "hd-protodune" in calls and "hd-protodune-det-reco" in calls


def test_datasets_response_includes_fetched_at(monkeypatch, client):
    _install_fake_client(monkeypatch)
    response = client.get("/api/datasets", params={"detector": "protodune-hd"})
    body = response.json()
    assert "fetched_at" in body
    # ISO 8601 with timezone
    assert "T" in body["fetched_at"]


def test_datasets_drops_empty_datasets_always(monkeypatch, client):
    _install_fake_client(monkeypatch)
    response = client.get(
        "/api/datasets",
        params={"detector": "protodune-hd", "official_only": "false"},
    )
    body = response.json()
    # All non-empty datasets returned (alpha, beta, gamma, delta); epsilon (0 files) dropped.
    assert body["total"] == 4
    assert "epsilon_empty" not in [r["name"] for r in body["rows"]]


def test_datasets_official_only_default_drops_non_dunepro(monkeypatch, client):
    _install_fake_client(monkeypatch)
    response = client.get(
        "/api/datasets", params={"detector": "protodune-hd"}
    )
    # default official_only=true: alpha, beta, gamma. delta (chaoz) dropped, epsilon (0 files) dropped.
    body = response.json()
    assert body["total"] == 3
    names = {r["name"] for r in body["rows"]}
    assert "delta_user_test" not in names
    assert "epsilon_empty" not in names


def test_datasets_official_only_false_keeps_non_dunepro(monkeypatch, client):
    _install_fake_client(monkeypatch)
    response = client.get(
        "/api/datasets",
        params={"detector": "protodune-hd", "official_only": "false"},
    )
    body = response.json()
    names = {r["name"] for r in body["rows"]}
    assert "delta_user_test" in names
    # Empty still dropped
    assert "epsilon_empty" not in names


# --- /api/files ------------------------------------------------------------


class _FakeQueryClient:
    """Mimics MetaCatClient.query for /api/files tests.

    Returns a count summary for summary='count', else streams from a
    fixture list applying naive skip/limit parsing of the MQL.
    """

    def __init__(self, items, raises=None):
        self._items = items
        self._raises = raises
        self.queries: list[dict] = []

    def query(self, mql, summary=None, with_metadata=False, batch_size=0):
        self.queries.append(
            {"mql": mql, "summary": summary, "with_metadata": with_metadata}
        )
        if self._raises is not None:
            raise self._raises
        if summary == "count":
            yield {"count": len(self._items), "total_size": 0}
            return
        # Crude parse of "(...) ordered skip N limit M" to slice the fixture
        skip = 0
        limit = len(self._items)
        if "skip" in mql and "limit" in mql:
            tail = mql.split("ordered ", 1)[1]
            parts = tail.split()
            skip = int(parts[1])
            limit = int(parts[3])
        for item in self._items[skip : skip + limit]:
            yield item


def _install_query_client(monkeypatch, items, raises=None):
    fake = _FakeQueryClient(items, raises=raises)
    monkeypatch.setattr(
        "dunecat.web.routes._get_metacat_client", lambda: fake, raising=False
    )
    return fake


def _file(name, run=None, size=1000):
    item = {
        "namespace": "hd-protodune-det-reco",
        "name": name,
        "fid": f"fid-{name}",
        "size": size,
        "created_timestamp": 1.0,
    }
    if run is not None:
        item["metadata"] = {"core.runs": [run]}
    return item


def test_files_pagination_returns_total_and_page_rows(monkeypatch, client):
    items = [_file(f"f{i}.root", run=27731 + i) for i in range(250)]
    fake = _install_query_client(monkeypatch, items)

    response = client.get(
        "/api/files",
        params={
            "dataset": "hd-protodune-det-reco:ds",
            "page": 2,
            "page_size": 100,
        },
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["total"] == 250
    assert body["page"] == 2
    assert body["page_size"] == 100
    assert len(body["rows"]) == 100
    assert body["rows"][0]["name"] == "f100.root"
    # First call must be summary=count, second the paged fetch
    assert fake.queries[0]["summary"] == "count"
    assert "skip 100 limit 100" in fake.queries[1]["mql"]


def test_files_runs_filter_composes_into_mql(monkeypatch, client):
    fake = _install_query_client(monkeypatch, [_file("a.root", run=27731)])
    response = client.get(
        "/api/files",
        params={
            "dataset": "hd-protodune-det-reco:ds",
            "runs": "27731,27732",
        },
    )
    assert response.status_code == 200
    assert "core.runs in (27731,27732)" in fake.queries[0]["mql"]


def test_files_run_range_and_meta_filters_compose(monkeypatch, client):
    fake = _install_query_client(monkeypatch, [])
    response = client.get(
        "/api/files",
        params={
            "dataset": "hd-protodune-det-reco:ds",
            "run_range": "27000-28000",
            "meta": ["dune.output_status=confirmed"],
        },
    )
    assert response.status_code == 200
    mql = fake.queries[0]["mql"]
    assert "core.runs >= 27000 and core.runs <= 27999" not in mql  # confirm inclusive range
    assert "core.runs >= 27000 and core.runs <= 28000" in mql
    assert "dune.output_status = 'confirmed'" in mql


def test_files_with_metadata_flag_passes_through(monkeypatch, client):
    items = [_file("a.root", run=27731)]
    fake = _install_query_client(monkeypatch, items)
    response = client.get(
        "/api/files",
        params={
            "dataset": "hd-protodune-det-reco:ds",
            "with_metadata": "true",
        },
    )
    body = response.json()
    assert body["rows"][0]["metadata"] == {"core.runs": [27731]}
    # Second query was the paged fetch with with_metadata=True
    assert fake.queries[1]["with_metadata"] is True


def test_files_bad_run_range_400(monkeypatch, client):
    _install_query_client(monkeypatch, [])
    response = client.get(
        "/api/files",
        params={
            "dataset": "hd-protodune-det-reco:ds",
            "run_range": "not-a-range",
        },
    )
    assert response.status_code == 400


def test_files_bad_meta_400(monkeypatch, client):
    _install_query_client(monkeypatch, [])
    response = client.get(
        "/api/files",
        params={
            "dataset": "hd-protodune-det-reco:ds",
            "meta": ["nope"],
        },
    )
    assert response.status_code == 400


def test_files_mql_error_surfaces_as_400(monkeypatch, client):
    from unittest.mock import Mock

    from metacat.webapi import MCWebAPIError

    fake_response = Mock(status_code=400, text="MQL syntax error at col 5")
    fake_response.headers = {}
    _install_query_client(
        monkeypatch, [], raises=MCWebAPIError("https://meta.example/q", fake_response)
    )

    response = client.get(
        "/api/files",
        params={"dataset": "hd-protodune-det-reco:ds"},
    )
    assert response.status_code == 400
    assert "MQL syntax error" in response.json()["detail"]
