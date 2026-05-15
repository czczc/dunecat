import fnmatch
import os
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from metacat.webapi import AuthenticationError, MCWebAPIError

from dunecat.client import get_client as _get_metacat_client
from dunecat.files import build_mql
from dunecat.filters import (
    FileFilters,
    parse_run_range,
    parse_runs,
    value_matches,
)

from . import cache
from .detectors import (
    apply_default_filters,
    datasets_for_detector,
    datasets_for_namespace,
    detector_by_id,
    load_detectors,
)


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
    """Detector names + namespaces only. Instant (YAML-only)."""
    return [
        {"id": d["id"], "name": d["name"], "namespaces": d["namespaces"]}
        for d in load_detectors()
    ]


@app.get("/api/detectors/counts")
def detector_counts() -> list[dict[str, Any]]:
    """Live datasets/files counts per detector (official-only, non-empty).

    Pulls every unique namespace in parallel and shares results across
    detectors that overlap, so cold-cache wall time is roughly max(per-ns)
    rather than sum(per-ns).
    """
    entries = load_detectors()
    all_namespaces: list[str] = sorted(
        {ns for e in entries for ns in e["namespaces"]}
    )

    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor(max_workers=8) as ex:
        ns_results = dict(
            zip(all_namespaces, ex.map(datasets_for_namespace, all_namespaces))
        )

    out: list[dict[str, Any]] = []
    for e in entries:
        seen: set[tuple[str, str]] = set()
        merged: list[dict[str, Any]] = []
        for ns in e["namespaces"]:
            items, _ = ns_results[ns]
            for ds in items:
                key = (ds["namespace"], ds["name"])
                if key in seen:
                    continue
                seen.add(key)
                merged.append(ds)
        filtered = apply_default_filters(merged, official_only=True)
        out.append(
            {
                "id": e["id"],
                "datasets_count": len(filtered),
                "files_count": sum(ds.get("file_count") or 0 for ds in filtered),
            }
        )
    return out


@app.get("/api/datasets")
def list_datasets(
    detector: str = Query(...),
    pattern: str | None = Query(None),
    tier: str | None = Query(None),
    meta: list[str] = Query(default_factory=list),
    official_only: bool = Query(True),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=500),
) -> dict[str, Any]:
    det = detector_by_id(detector)
    if det is None:
        raise HTTPException(status_code=404, detail=f"Unknown detector: {detector}")

    items, fetched_at = datasets_for_detector(det["namespaces"])
    items = apply_default_filters(items, official_only=official_only)
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


@app.get("/api/files")
def list_files(
    dataset: str = Query(...),
    runs: str | None = Query(None),
    run_range: str | None = Query(None),
    namespace: str | None = Query(None),
    meta: list[str] = Query(default_factory=list),
    with_metadata: bool = Query(False),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=500),
) -> dict[str, Any]:
    try:
        filters = FileFilters(
            runs=parse_runs(runs),
            run_range=parse_run_range(run_range),
            namespace=namespace,
            meta=tuple(_parse_meta(meta)),
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    base_mql = build_mql(dataset, filters)
    offset = (page - 1) * page_size
    paged_mql = f"({base_mql}) ordered skip {offset} limit {page_size}"
    client = _get_metacat_client()

    def fetch_total() -> int:
        for summary in client.query(base_mql, summary="count"):
            return summary.get("count", 0) if isinstance(summary, dict) else 0
        return 0

    def fetch_rows() -> list[dict[str, Any]]:
        return [
            _file_row(item)
            for item in client.query(paged_mql, with_metadata=with_metadata)
        ]

    with ThreadPoolExecutor(max_workers=2) as ex:
        total_future = ex.submit(fetch_total)
        rows_future = ex.submit(fetch_rows)
        total = total_future.result()
        rows = rows_future.result()

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "rows": rows,
    }


def _file_row(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "did": f"{item['namespace']}:{item['name']}",
        "namespace": item["namespace"],
        "name": item["name"],
        "fid": item.get("fid"),
        "size": item.get("size"),
        "created_timestamp": item.get("created_timestamp"),
        "updated_timestamp": item.get("updated_timestamp"),
        "checksums": item.get("checksums"),
        "metadata": item.get("metadata"),
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
