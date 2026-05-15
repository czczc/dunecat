import pytest
from fastapi.testclient import TestClient
from metacat.webapi import AuthenticationError

from dunecat.web import app
from dunecat.web import detectors as det_mod


@pytest.fixture(autouse=True)
def _clear_detector_cache():
    det_mod.clear_cache()
    yield
    det_mod.clear_cache()


@pytest.fixture
def client():
    return TestClient(app)


def _fake_list(items_by_ns):
    def _impl(namespace_pattern=None, name_pattern=None):
        yield from items_by_ns.get(namespace_pattern, [])

    return _impl


def test_detectors_returns_yaml_with_live_counts(monkeypatch, client):
    items = {
        "hd-protodune": [
            {"namespace": "hd-protodune", "name": "a", "file_count": 10},
            {"namespace": "hd-protodune", "name": "b", "file_count": 5},
        ],
        "hd-protodune-det-reco": [
            {"namespace": "hd-protodune-det-reco", "name": "c", "file_count": 3},
        ],
        "vd-protodune": [
            {"namespace": "vd-protodune", "name": "d", "file_count": 1},
        ],
        "fardet-hd": [],
        "fardet-vd": [],
        "fd_vd_mc_reco": [],
    }

    class FakeClient:
        list_datasets = staticmethod(_fake_list(items))

    monkeypatch.setattr(
        "dunecat.web.detectors.get_client", lambda: FakeClient(), raising=False
    )
    # detectors.py imports get_client lazily inside the function so we
    # also need to patch the actual source module attribute that the
    # closure resolves through. The monkeypatch above handles the
    # `dunecat.web.detectors.get_client` reference once it's been
    # bound via the lazy import.

    response = client.get("/api/detectors")

    assert response.status_code == 200
    payload = response.json()
    assert {d["id"] for d in payload} == {
        "protodune-hd",
        "protodune-vd",
        "fardet-hd",
        "fardet-vd",
    }
    by_id = {d["id"]: d for d in payload}
    assert by_id["protodune-hd"]["datasets_count"] == 3
    assert by_id["protodune-hd"]["files_count"] == 18
    assert by_id["protodune-vd"]["datasets_count"] == 1
    assert by_id["protodune-vd"]["files_count"] == 1
    assert by_id["fardet-hd"]["datasets_count"] == 0


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

    response = client.get("/api/detectors")

    assert response.status_code == 401
    body = response.json()
    assert "Token missing or expired" in body["detail"]
    assert "metacat -s https://m.example/app" in body["detail"]
