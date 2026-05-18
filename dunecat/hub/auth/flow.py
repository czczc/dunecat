"""Per-user OIDC device-flow driver against htvaultprod.fnal.gov.

Validated by .idea/spike/vault_device_flow.py — this module is the
production port of that script. Endpoints mirror htgettoken's source
(see .venv/.../htgettoken/__init__.py).

The driver is split into two halves so it composes cleanly with the
hub's `device_flows` table:

- ``start()`` performs the auth_url POST against vault, returns enough
  context to (a) show the user the auth URL and (b) re-call the poll
  endpoint later. The hub stores the poll context in `device_flows`.
- ``poll(poll_body)`` performs one POST against vault's /poll endpoint
  and classifies the result: pending / slow_down / complete / error.
- ``mint_bearer(vault_token, credkey)`` GETs the secret path and
  returns the bearer JWT string.

The HTTP calls go through ``_vault_post`` / ``_vault_get`` at module
scope so tests can monkey-patch them without dragging in a full
``responses``/``httpretty`` dependency.
"""

from __future__ import annotations

import base64
import json
import logging
import secrets
from dataclasses import dataclass
from typing import Literal

import requests

log = logging.getLogger("uvicorn.error")

VAULT = "https://htvaultprod.fnal.gov:8200"
ISSUER = "dune"
ROLE = "default"
OIDC_PATH = f"auth/oidc-{ISSUER}/oidc"
REDIRECT_URI = f"{VAULT}/v1/{OIDC_PATH}/callback"

_TIMEOUT = 10.0


# ----- HTTP boundary (patched in tests) ------------------------------------


def _vault_post(url: str, body: dict) -> requests.Response:
    return requests.post(url, json=body, timeout=_TIMEOUT)


def _vault_get(url: str, headers: dict, params: dict) -> requests.Response:
    return requests.get(url, headers=headers, params=params, timeout=_TIMEOUT)


# ----- Data shapes ---------------------------------------------------------


@dataclass(frozen=True)
class StartResult:
    auth_url: str
    user_code: str | None
    poll_body: dict  # opaque blob, post verbatim to /poll later


PollOutcome = Literal["pending", "slow_down", "complete"]


@dataclass(frozen=True)
class PollResult:
    outcome: PollOutcome
    auth: dict | None  # populated only when outcome == "complete"


@dataclass(frozen=True)
class LoginResult:
    """Everything a successful login produces, from vault's perspective."""

    vault_token: str
    vault_lease_seconds: int
    metacat_username: str  # vault metadata.credkey
    bearer: str  # JWT, what metacat/Rucio consume
    bearer_claims: dict  # decoded JWT body
    oidc_sub: str  # bearer_claims["sub"]


# ----- Public driver -------------------------------------------------------


def start() -> StartResult:
    """Begin a device flow. Returns the auth URL plus a serialisable
    poll body to hand to ``poll()`` later."""
    nonce = secrets.token_urlsafe()
    body = {
        "role": ROLE,
        "client_nonce": nonce,
        "redirect_uri": REDIRECT_URI,
    }
    r = _vault_post(f"{VAULT}/v1/{OIDC_PATH}/auth_url", body)
    r.raise_for_status()
    data = r.json()["data"]
    auth_url = data.pop("auth_url")
    user_code = data.pop("user_code", None)
    if not auth_url:
        raise RuntimeError("vault returned empty auth_url")
    # Everything else (state, poll_interval, etc.) goes verbatim to /poll,
    # plus the client_nonce we generated.
    poll_body = {**data, "client_nonce": nonce}
    return StartResult(auth_url=auth_url, user_code=user_code, poll_body=poll_body)


def poll(poll_body: dict) -> PollResult:
    """Do one /poll request. Caller drives the loop."""
    r = _vault_post(f"{VAULT}/v1/{OIDC_PATH}/poll", poll_body)
    if r.status_code == 400:
        errs = (r.json() or {}).get("errors", [])
        if errs and errs[0] == "authorization_pending":
            return PollResult(outcome="pending", auth=None)
        if errs and errs[0] == "slow_down":
            return PollResult(outcome="slow_down", auth=None)
        raise RuntimeError(f"vault returned 400 with errors: {errs}")
    r.raise_for_status()
    return PollResult(outcome="complete", auth=r.json().get("auth"))


def mint_bearer(vault_token: str, credkey: str) -> str:
    """Read the secret path and return the bearer JWT."""
    path = f"secret/oauth/creds/{ISSUER}/{credkey}:{ROLE}"
    r = _vault_get(
        f"{VAULT}/v1/{path}",
        headers={"X-Vault-Token": vault_token},
        params={"minimum_seconds": 60},
    )
    r.raise_for_status()
    return r.json()["data"]["access_token"]


def complete(auth: dict) -> LoginResult:
    """Given vault's /poll success response, finish the login: mint the
    bearer and decode it. Returns everything the hub needs to persist."""
    vault_token = auth["client_token"]
    lease = int(auth.get("lease_duration", 0))
    metadata = auth.get("metadata") or {}
    credkey = metadata.get("credkey")
    if not credkey:
        raise RuntimeError(
            "vault response missing metadata.credkey; cannot identify user"
        )
    bearer = mint_bearer(vault_token, credkey)
    claims = jwt_claims(bearer)
    sub = claims.get("sub")
    if not sub:
        raise RuntimeError("bearer JWT missing 'sub' claim")
    return LoginResult(
        vault_token=vault_token,
        vault_lease_seconds=lease,
        metacat_username=credkey,
        bearer=bearer,
        bearer_claims=claims,
        oidc_sub=sub,
    )


# ----- JWT helper ----------------------------------------------------------


def jwt_claims(token: str) -> dict:
    parts = token.split(".")
    if len(parts) != 3:
        raise RuntimeError("not a JWT (expected 3 dot-separated segments)")
    body = parts[1]
    body += "=" * (-len(body) % 4)
    return json.loads(base64.urlsafe_b64decode(body))
