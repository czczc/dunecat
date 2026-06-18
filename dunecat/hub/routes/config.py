"""GET /api/config — boot signal for the SPA.

Unauthenticated by design: the SPA fetches this before any login state
exists, to learn whether it's running against the hub or the local app.
"""

from __future__ import annotations

from fastapi import APIRouter, Request

from dunecat import llm

router = APIRouter()


@router.get("/api/config")
def get_config(request: Request) -> dict:
    # When the app is mounted under a URL prefix (uvicorn --root-path),
    # the externally-visible login URL needs the prefix too.
    root = request.scope.get("root_path", "")
    return {
        "mode": "hub",
        "login_url": f"{root}/hub/login",
        "llm_enabled": llm.is_enabled(),
    }
