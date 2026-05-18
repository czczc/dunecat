"""Catalog routes ported from the local app, with per-request auth.

This file is the first vertical slice of #27. /api/detectors is the
simplest catalog route — pure YAML read, no metacat call — so it's the
right place to verify Depends(current_user) works end-to-end before
porting routes that hit FNAL services.

Note on imports: `load_detectors` and friends currently live in
`dunecat/web/detectors.py` because the local app got there first. The
YAML they read (`dunecat/web/detectors.yaml`) is project-wide config,
not local-app-specific, so importing it from the hub is intentional
read-only coupling. If we ever need to evolve the format separately,
the cleanup is to lift the YAML loaders into a shared
`dunecat/detectors_yaml.py` module — out of scope for this slice.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends

from dunecat.web.detectors import load_detectors

from ..auth.dep import current_user
from ..auth.session import User

router = APIRouter()


@router.get("/api/detectors")
def list_detectors(_user: User = Depends(current_user)) -> list[dict[str, Any]]:
    """Detector names + namespaces from `dunecat/web/detectors.yaml`.

    Auth-gated even though it doesn't touch metacat: the hub's
    invariant is "every API call is on behalf of a real user."
    """
    return [
        {
            "id": d["id"],
            "name": d["name"],
            "namespaces": d["namespaces"],
            "condb_folder": d.get("condb_folder"),
            "wiki": d.get("wiki"),
            "chip": d.get("chip", True),
        }
        for d in load_detectors()
    ]
