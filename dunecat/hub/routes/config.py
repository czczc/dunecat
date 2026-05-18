"""GET /api/config — boot signal for the SPA.

Unauthenticated by design: the SPA fetches this before any login state
exists, to learn whether it's running against the hub or the local app.
"""

from __future__ import annotations

from fastapi import APIRouter

router = APIRouter()


@router.get("/api/config")
def get_config() -> dict:
    return {"mode": "hub", "login_url": "/hub/login"}
