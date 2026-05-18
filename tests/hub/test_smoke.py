"""End-to-end smoke test for the hub, with all vault HTTP calls mocked.

Covers:
  - GET /api/me without a cookie → 401
  - GET /hub/login renders the polling page
  - GET /hub/login/poll, first call → pending
  - GET /hub/login/poll, second call → ok + Set-Cookie + DB rows persisted
  - GET /api/me with cookie → 200 with identity
  - second login for the same user does not duplicate the users row
  - POST /hub/logout → 200, cookie cleared
  - GET /api/me after logout → 401
"""

from __future__ import annotations

import base64
import json
import os
import secrets
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def hub_env(tmp_path, monkeypatch):
    """One isolated SQLite + crypto key per test."""
    monkeypatch.setenv("DUNECAT_HUB_DB", str(tmp_path / "hub.sqlite"))
    monkeypatch.setenv(
        "DUNECAT_HUB_SECRET_KEY",
        base64.b64encode(secrets.token_bytes(32)).decode(),
    )
    yield


def _fake_bearer(sub: str, preferred_username: str | None = None) -> str:
    """Construct an unsigned 3-segment 'JWT' the hub's decoder accepts.
    The hub never verifies the signature; it only base64-decodes the body."""
    header = base64.urlsafe_b64encode(b'{"alg":"none"}').rstrip(b"=").decode()
    claims: dict[str, Any] = {"sub": sub, "exp": 9999999999}
    if preferred_username:
        claims["preferred_username"] = preferred_username
    body = (
        base64.urlsafe_b64encode(json.dumps(claims).encode()).rstrip(b"=").decode()
    )
    sig = "sig"  # ignored
    return f"{header}.{body}.{sig}"


def _stub_response(status: int, payload: dict | None = None):
    """Minimal duck-typed stand-in for a requests.Response."""

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
    """Spin up the hub FastAPI app with the test-time env baked in.
    `with TestClient(...)` runs the lifespan (schema init + crypto load)."""
    from fastapi.testclient import TestClient

    from dunecat.hub import app as hub_app_module

    with TestClient(hub_app_module.app) as c:
        yield c


def _drive_login(
    client, *, sub: str, credkey: str, pending_first: bool = True
) -> dict:
    """Walk the login flow end-to-end, returning the final poll JSON."""
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

    poll_responses: list[Any] = []
    if pending_first:
        poll_responses.append(
            _stub_response(400, {"errors": ["authorization_pending"]})
        )
    poll_responses.append(
        _stub_response(
            200,
            {
                "auth": {
                    "client_token": f"vault-token-for-{credkey}",
                    "lease_duration": 2419200,
                    "metadata": {"credkey": credkey},
                }
            },
        )
    )
    bearer_resp = _stub_response(
        200, {"data": {"access_token": _fake_bearer(sub, credkey)}}
    )

    def fake_post(url: str, body: dict):
        if url.endswith("/auth_url"):
            return start_resp
        if url.endswith("/poll"):
            return poll_responses.pop(0)
        raise AssertionError(f"unexpected POST {url}")

    def fake_get(url: str, headers: dict, params: dict):
        # bearer mint
        if "/v1/secret/oauth/creds/" in url:
            return bearer_resp
        raise AssertionError(f"unexpected GET {url}")

    with (
        patch.object(flow_mod, "_vault_post", side_effect=fake_post),
        patch.object(flow_mod, "_vault_get", side_effect=fake_get),
    ):
        r = client.get("/hub/login")
        assert r.status_code == 200
        assert auth_url in r.text
        # Extract the flow_id from the inline script.
        marker = 'const flowId = '
        i = r.text.find(marker) + len(marker)
        flow_id = json.loads(r.text[i : r.text.find(";", i)])

        if pending_first:
            r = client.get(f"/hub/login/poll?flow_id={flow_id}")
            assert r.status_code == 200
            assert r.json() == {"status": "pending"}

        r = client.get(f"/hub/login/poll?flow_id={flow_id}")
        assert r.status_code == 200, r.text
        return r.json()


def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


def test_me_unauthenticated(client):
    r = client.get("/api/me")
    assert r.status_code == 401


def test_home_redirects_to_login_when_unauthenticated(client):
    r = client.get("/", follow_redirects=False)
    assert r.status_code == 303
    assert r.headers["location"] == "/hub/login"


def test_home_renders_when_authenticated(client):
    _drive_login(client, sub="uuid-dave", credkey="dave")
    r = client.get("/")
    assert r.status_code == 200
    assert "Signed in as <strong>dave</strong>" in r.text
    # Logout button should be present.
    assert 'id="logout"' in r.text


def test_login_then_me_then_logout(client):
    out = _drive_login(client, sub="uuid-alice", credkey="alice")
    assert out == {"status": "ok"}
    assert "dunecat_session" in client.cookies

    r = client.get("/api/me")
    assert r.status_code == 200, r.text
    me = r.json()
    assert me["oidc_sub"] == "uuid-alice"
    assert me["metacat_username"] == "alice"
    assert me["vault_token_expires_at"] is not None

    r = client.post("/hub/logout")
    assert r.status_code == 200

    # Cookie now gone client-side; /api/me should be 401.
    r = client.get("/api/me")
    assert r.status_code == 401


def test_second_login_same_user_does_not_duplicate(client):
    _drive_login(client, sub="uuid-bob", credkey="bob")
    client.post("/hub/logout")
    # second flow, identical sub
    _drive_login(client, sub="uuid-bob", credkey="bob")

    # Inspect the DB directly to verify there's exactly one users row.
    from dunecat.hub import db as hub_db

    with hub_db.connect() as conn:
        n = conn.execute(
            "SELECT COUNT(*) AS n FROM users WHERE oidc_sub = ?", ("uuid-bob",)
        ).fetchone()["n"]
    assert n == 1


def test_vault_token_stored_encrypted(client):
    _drive_login(client, sub="uuid-carol", credkey="carol")
    from dunecat.hub import db as hub_db

    with hub_db.connect() as conn:
        row = conn.execute(
            "SELECT vt.ciphertext FROM vault_tokens vt "
            "JOIN users u ON vt.user_id = u.id "
            "WHERE u.oidc_sub = ?",
            ("uuid-carol",),
        ).fetchone()
    assert row is not None
    # The plaintext vault token would be "vault-token-for-carol"; the
    # ciphertext must not contain that substring.
    assert b"vault-token-for-carol" not in bytes(row["ciphertext"])


def test_missing_secret_key_auto_generates(tmp_path, monkeypatch):
    """No env, no existing file → app boots and writes a fresh key to disk."""
    db_path = tmp_path / "hub.sqlite"
    monkeypatch.setenv("DUNECAT_HUB_DB", str(db_path))
    monkeypatch.delenv("DUNECAT_HUB_SECRET_KEY", raising=False)

    from fastapi.testclient import TestClient

    from dunecat.hub import app as hub_app_module

    with TestClient(hub_app_module.app) as c:
        assert c.get("/health").status_code == 200

    key_file = db_path.parent / "hub.key"
    assert key_file.exists()
    # 32 random bytes, base64-encoded ≈ 44 chars + newline.
    assert len(base64.b64decode(key_file.read_text().strip())) == 32
    # File should be private to the user.
    assert (key_file.stat().st_mode & 0o777) == 0o600


def test_malformed_secret_key_env_is_fatal(tmp_path, monkeypatch):
    """Configuration errors (not just absence) still fail fast."""
    monkeypatch.setenv("DUNECAT_HUB_DB", str(tmp_path / "hub.sqlite"))
    monkeypatch.setenv("DUNECAT_HUB_SECRET_KEY", "not-base64-and-too-short")

    from fastapi.testclient import TestClient

    from dunecat.hub import app as hub_app_module
    from dunecat.hub.crypto import HubCryptoError

    with pytest.raises(HubCryptoError):
        with TestClient(hub_app_module.app):
            pass
