import fnmatch
import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from metacat.webapi import AuthenticationError, MCWebAPIError

from dunecat.filters import value_matches

from . import cache
from .detectors import datasets_for_detector, detector_by_id, load_detectors


@asynccontextmanager
async def lifespan(_: FastAPI):
    cache.init_db()
    yield


app = FastAPI(
    title="dunecat",
    docs_url="/api/docs",
    openapi_url="/api/openapi.json",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)


@app.exception_handler(AuthenticationError)
async def _auth_error(_: Request, exc: AuthenticationError) -> JSONResponse:
    server = os.environ.get("METACAT_SERVER_URL", "<METACAT_SERVER_URL>")
    auth = os.environ.get("METACAT_AUTH_SERVER_URL", "<METACAT_AUTH_SERVER_URL>")
    return JSONResponse(
        status_code=401,
        content={
            "detail": (
                f"Token missing or expired ({exc}). Run: "
                f"metacat -s {server} -a {auth} auth login -m <method> <username>"
            )
        },
    )


@app.exception_handler(MCWebAPIError)
async def _metacat_error(_: Request, exc: MCWebAPIError) -> JSONResponse:
    return JSONResponse(status_code=400, content={"detail": str(exc)})


@app.get("/api/detectors")
def list_detectors() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for entry in load_detectors():
        datasets, _ = datasets_for_detector(entry["namespaces"])
        out.append(
            {
                "id": entry["id"],
                "name": entry["name"],
                "namespaces": entry["namespaces"],
                "datasets_count": len(datasets),
                "files_count": sum(ds.get("file_count") or 0 for ds in datasets),
            }
        )
    return out


@app.get("/api/datasets")
def list_datasets(
    detector: str = Query(...),
    pattern: str | None = Query(None),
    tier: str | None = Query(None),
    meta: list[str] = Query(default_factory=list),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=500),
) -> dict[str, Any]:
    det = detector_by_id(detector)
    if det is None:
        raise HTTPException(status_code=404, detail=f"Unknown detector: {detector}")

    items, fetched_at = datasets_for_detector(det["namespaces"])
    filtered = _apply_filters(items, pattern=pattern, tier=tier, meta=meta)

    total = len(filtered)
    start = (page - 1) * page_size
    end = start + page_size
    rows = [_row(ds) for ds in filtered[start:end]]

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "fetched_at": fetched_at.isoformat(),
        "rows": rows,
    }


@app.post("/api/datasets/refresh")
def refresh_datasets(detector: str = Query(...)) -> Response:
    det = detector_by_id(detector)
    if det is None:
        raise HTTPException(status_code=404, detail=f"Unknown detector: {detector}")
    for ns in det["namespaces"]:
        cache.invalidate(ns)
    return Response(status_code=204)


def _row(ds: dict[str, Any]) -> dict[str, Any]:
    return {
        "did": f"{ds['namespace']}:{ds['name']}",
        "namespace": ds["namespace"],
        "name": ds["name"],
        "file_count": ds.get("file_count"),
        "created_timestamp": ds.get("created_timestamp"),
        "updated_timestamp": ds.get("updated_timestamp"),
        "metadata": ds.get("metadata") or {},
    }


def _apply_filters(
    items: list[dict[str, Any]],
    pattern: str | None,
    tier: str | None,
    meta: list[str],
) -> list[dict[str, Any]]:
    meta_pairs = _parse_meta(meta)
    out = []
    for ds in items:
        if pattern and not fnmatch.fnmatch(ds["name"], pattern):
            continue
        md = ds.get("metadata") or {}
        if tier and not value_matches(md.get("core.data_tier"), tier):
            continue
        if not all(value_matches(md.get(k), v) for k, v in meta_pairs):
            continue
        out.append(ds)
    return out


def _parse_meta(values: list[str]) -> list[tuple[str, str]]:
    pairs = []
    for raw in values:
        if "=" not in raw:
            raise HTTPException(
                status_code=400, detail=f"--meta expects 'KEY=VALUE', got {raw!r}"
            )
        key, value = raw.split("=", 1)
        key = key.strip()
        if not key:
            raise HTTPException(
                status_code=400, detail=f"--meta has empty key in {raw!r}"
            )
        pairs.append((key, value))
    return pairs
