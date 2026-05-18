"""Tests for the catalog routes the hub picked up from `dunecat/web/routes.py`.

All FNAL HTTP is mocked: `metacat_for` is replaced with a stub
MetaCatClient that returns canned data, so these tests run offline.
"""

from __future__ import annotations

import base64
import json
import secrets
from typing import Any
from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def hub_env(tmp_path, monkeypatch):
    monkeypatch.setenv("DUNECAT_HUB_DB", str(tmp_path / "hub.sqlite"))
    monkeypatch.setenv(
        "DUNECAT_HUB_SECRET_KEY",
        base64.b64encode(secrets.token_bytes(32)).decode(),
    )
    # Configure metacat URLs so metacat_for() doesn't bail on missing config.
    monkeypatch.setenv("METACAT_SERVER_URL", "https://mock-metacat.test/app")
    monkeypatch.setenv(
        "METACAT_AUTH_SERVER_URL", "https://mock-metacat.test/auth"
    )
    yield


def _fake_bearer(sub: str) -> str:
    header = base64.urlsafe_b64encode(b'{"alg":"none"}').rstrip(b"=").decode()
    body = (
        base64.urlsafe_b64encode(
            json.dumps({"sub": sub, "exp": 9999999999}).encode()
        )
        .rstrip(b"=")
        .decode()
    )
    return f"{header}.{body}.sig"


def _stub_response(status: int, payload: dict | None = None):
    class _R:
        status_code = status

        def json(self) -> Any:
            return payload

        def raise_for_status(self) -> None:
            if status >= 400:
                raise RuntimeError(f"HTTP {status}")

    return _R()


@pytest.fixture
def client(hub_env):
    from fastapi.testclient import TestClient

    from dunecat.hub import app as hub_app_module

    with TestClient(hub_app_module.app) as c:
        yield c


def _drive_login(client, *, sub: str, credkey: str) -> None:
    from dunecat.hub.auth import flow as flow_mod

    auth_url = "https://cilogon.org/device/?user_code=TEST-CODE"
    start_resp = _stub_response(
        200,
        {
            "data": {
                "auth_url": auth_url,
                "user_code": "TEST-CODE",
                "state": "vault-state",
                "poll_interval": 3,
            }
        },
    )
    poll_resp = _stub_response(
        200,
        {
            "auth": {
                "client_token": f"vault-token-for-{credkey}",
                "lease_duration": 2419200,
                "metadata": {"credkey": credkey},
            }
        },
    )
    bearer_resp = _stub_response(
        200, {"data": {"access_token": _fake_bearer(sub)}}
    )

    def fake_post(url: str, body: dict):
        if url.endswith("/auth_url"):
            return start_resp
        if url.endswith("/poll"):
            return poll_resp
        raise AssertionError(f"unexpected POST {url}")

    def fake_get(url: str, headers: dict, params: dict):
        if "/v1/secret/oauth/creds/" in url:
            return bearer_resp
        raise AssertionError(f"unexpected GET {url}")

    with (
        patch.object(flow_mod, "_vault_post", side_effect=fake_post),
        patch.object(flow_mod, "_vault_get", side_effect=fake_get),
    ):
        r = client.get("/hub/login")
        marker = "const flowId = "
        i = r.text.find(marker) + len(marker)
        flow_id = json.loads(r.text[i : r.text.find(";", i)])
        r = client.get(f"/hub/login/poll?flow_id={flow_id}")
        assert r.status_code == 200, r.text
        assert r.json() == {"status": "ok"}


# ---- saved queries --------------------------------------------------------


def test_saved_queries_are_per_user(client):
    """Alice's queries must not be visible to Bob, and the
    UNIQUE(user_id, name) constraint should allow both to use the
    same name."""
    _drive_login(client, sub="uuid-alice", credkey="alice")
    r = client.post("/api/queries", json={"name": "test", "mql": "files from x:y"})
    assert r.status_code == 201
    assert r.json()["name"] == "test"

    # Alice sees her own query.
    r = client.get("/api/queries")
    assert r.status_code == 200
    alice_rows = r.json()
    assert len(alice_rows) == 1
    assert alice_rows[0]["name"] == "test"

    # Switch to Bob.
    client.post("/hub/logout")
    _drive_login(client, sub="uuid-bob", credkey="bob")

    # Bob sees nothing.
    r = client.get("/api/queries")
    assert r.status_code == 200
    assert r.json() == []

    # Bob can create a query with the same name (per-user uniqueness).
    r = client.post("/api/queries", json={"name": "test", "mql": "files from p:q"})
    assert r.status_code == 201

    # Bob can't see or touch Alice's query by id either.
    alice_id = alice_rows[0]["id"]
    r = client.get(f"/api/queries")
    bob_ids = [q["id"] for q in r.json()]
    assert alice_id not in bob_ids

    r = client.put(f"/api/queries/{alice_id}", json={"name": "hijack"})
    assert r.status_code == 404
    r = client.delete(f"/api/queries/{alice_id}")
    assert r.status_code == 404


def test_saved_query_crud_round_trip(client):
    _drive_login(client, sub="uuid-carol", credkey="carol")

    # Create.
    r = client.post(
        "/api/queries",
        json={"name": "q1", "mql": "files from a:b"},
    )
    assert r.status_code == 201
    qid = r.json()["id"]

    # Read (list).
    r = client.get("/api/queries")
    assert r.status_code == 200
    assert any(q["id"] == qid for q in r.json())

    # Update.
    r = client.put(
        f"/api/queries/{qid}",
        json={"name": "q1-renamed", "mql": "files from a:c"},
    )
    assert r.status_code == 200
    assert r.json()["name"] == "q1-renamed"
    assert r.json()["mql"] == "files from a:c"

    # Conflict: another query, same name.
    r = client.post(
        "/api/queries", json={"name": "q1-renamed", "mql": "files from x:y"}
    )
    assert r.status_code == 409

    # Delete.
    r = client.delete(f"/api/queries/{qid}")
    assert r.status_code == 204
    r = client.get("/api/queries")
    assert not any(q["id"] == qid for q in r.json())


# ---- one catalog route through mocked metacat ----------------------------


def test_dataset_endpoint_with_mocked_metacat(client):
    """End-to-end: hub login → /api/dataset → mocked MetaCatClient
    returns canned record. Validates that current_user + metacat_for
    + the timeout wrapper compose cleanly without anyone touching
    FNAL."""
    _drive_login(client, sub="uuid-dave", credkey="dave")

    fake_record = {
        "namespace": "hd-protodune",
        "name": "demo-dataset",
        "file_count": 42,
        "metadata": {"core.data_tier": "raw"},
    }

    class FakeMetaCat:
        def get_dataset(self, did: str):
            assert did == "hd-protodune:demo-dataset"
            return fake_record

    with patch(
        "dunecat.hub.routes.catalog.metacat_for", return_value=FakeMetaCat()
    ):
        r = client.get(
            "/api/dataset", params={"did": "hd-protodune:demo-dataset"}
        )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["did"] == "hd-protodune:demo-dataset"
    assert body["file_count"] == 42


def test_catalog_routes_require_auth(client):
    """A representative sample of catalog routes 401 without a cookie."""
    for path, method in [
        ("/api/detectors/counts", "GET"),
        ("/api/datasets?detector=protodune-hd", "GET"),
        ("/api/dataset?did=x:y", "GET"),
        ("/api/file?did=x:y", "GET"),
        ("/api/files?dataset=x:y", "GET"),
        ("/api/queries", "GET"),
        ("/api/query/run", "POST"),
    ]:
        if method == "GET":
            r = client.get(path)
        else:
            r = client.post(path, json={"mql": "files from x:y"})
        assert r.status_code == 401, f"{method} {path} → {r.status_code}"
