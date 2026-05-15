import fnmatch
import logging
import os
import sqlite3
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

log = logging.getLogger("uvicorn.error")

from fastapi import Body, FastAPI, HTTPException, Query, Request
from pydantic import BaseModel, Field
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


def _build_file_filters(
    runs: str | None,
    run_range: str | None,
    namespace: str | None,
    meta: list[str],
) -> FileFilters:
    try:
        return FileFilters(
            runs=parse_runs(runs),
            run_range=parse_run_range(run_range),
            namespace=namespace,
            meta=tuple(_parse_meta(meta)),
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


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
    filters = _build_file_filters(runs, run_range, namespace, meta)
    base_mql = build_mql(dataset, filters)
    offset = (page - 1) * page_size
    paged_mql = f"({base_mql}) ordered skip {offset} limit {page_size}"
    client = _get_metacat_client()

    start = time.monotonic()
    rows = [
        _file_row(item)
        for item in client.query(paged_mql, with_metadata=with_metadata)
    ]
    log.info(
        "/api/files page=%d rows=%d with_metadata=%s took=%.2fs",
        page, len(rows), with_metadata, time.monotonic() - start,
    )

    return {
        "page": page,
        "page_size": page_size,
        "rows": rows,
        "has_more": len(rows) == page_size,
    }


class _QueryRequest(BaseModel):
    mql: str


class _QueryRunRequest(BaseModel):
    mql: str
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=100, ge=1, le=500)
    saved_query_id: int | None = None


class _SavedQueryCreate(BaseModel):
    name: str
    mql: str


class _SavedQueryUpdate(BaseModel):
    name: str | None = None
    mql: str | None = None


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _saved_query_row(row: tuple) -> dict[str, Any]:
    return {
        "id": row[0],
        "name": row[1],
        "mql": row[2],
        "created_at": row[3],
        "last_run_at": row[4],
    }


@app.post("/api/query/run")
def query_run(req: _QueryRunRequest) -> dict[str, Any]:
    mql = req.mql.strip()
    if not mql:
        raise HTTPException(status_code=400, detail="mql is required")
    paged_mql = f"({mql}) ordered skip {(req.page - 1) * req.page_size} limit {req.page_size}"
    client = _get_metacat_client()
    start = time.monotonic()
    rows = [_file_row(item) for item in client.query(paged_mql)]
    log.info(
        "/api/query/run page=%d rows=%d took=%.2fs",
        req.page, len(rows), time.monotonic() - start,
    )
    if req.saved_query_id is not None:
        with cache.connect() as conn:
            conn.execute(
                "UPDATE saved_queries SET last_run_at = ? WHERE id = ?",
                (_now_iso(), req.saved_query_id),
            )
    return {
        "page": req.page,
        "page_size": req.page_size,
        "rows": rows,
        "has_more": len(rows) == req.page_size,
    }


# --- saved queries ---------------------------------------------------------


@app.get("/api/queries")
def list_saved_queries() -> list[dict[str, Any]]:
    with cache.connect() as conn:
        rows = conn.execute(
            "SELECT id, name, mql, created_at, last_run_at "
            "FROM saved_queries ORDER BY LOWER(name)"
        ).fetchall()
    return [_saved_query_row(r) for r in rows]


@app.post("/api/queries", status_code=201)
def create_saved_query(req: _SavedQueryCreate) -> dict[str, Any]:
    name = req.name.strip()
    mql = req.mql.strip()
    if not name:
        raise HTTPException(status_code=400, detail="name is required")
    if not mql:
        raise HTTPException(status_code=400, detail="mql is required")
    try:
        with cache.connect() as conn:
            cur = conn.execute(
                "INSERT INTO saved_queries (name, mql, created_at) VALUES (?, ?, ?)",
                (name, mql, _now_iso()),
            )
            new_id = cur.lastrowid
            row = conn.execute(
                "SELECT id, name, mql, created_at, last_run_at FROM saved_queries WHERE id = ?",
                (new_id,),
            ).fetchone()
    except sqlite3.IntegrityError:
        raise HTTPException(
            status_code=409, detail=f"A query named {name!r} already exists"
        )
    return _saved_query_row(row)


@app.put("/api/queries/{query_id}")
def update_saved_query(query_id: int, req: _SavedQueryUpdate) -> dict[str, Any]:
    with cache.connect() as conn:
        existing = conn.execute(
            "SELECT id, name, mql, created_at, last_run_at FROM saved_queries WHERE id = ?",
            (query_id,),
        ).fetchone()
        if existing is None:
            raise HTTPException(status_code=404, detail=f"Saved query {query_id} not found")
        new_name = (req.name.strip() if req.name is not None else existing[1])
        new_mql = (req.mql.strip() if req.mql is not None else existing[2])
        if not new_name:
            raise HTTPException(status_code=400, detail="name cannot be empty")
        if not new_mql:
            raise HTTPException(status_code=400, detail="mql cannot be empty")
        try:
            conn.execute(
                "UPDATE saved_queries SET name = ?, mql = ? WHERE id = ?",
                (new_name, new_mql, query_id),
            )
        except sqlite3.IntegrityError:
            raise HTTPException(
                status_code=409, detail=f"A query named {new_name!r} already exists"
            )
        row = conn.execute(
            "SELECT id, name, mql, created_at, last_run_at FROM saved_queries WHERE id = ?",
            (query_id,),
        ).fetchone()
    return _saved_query_row(row)


@app.delete("/api/queries/{query_id}", status_code=204)
def delete_saved_query(query_id: int) -> Response:
    with cache.connect() as conn:
        cur = conn.execute("DELETE FROM saved_queries WHERE id = ?", (query_id,))
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail=f"Saved query {query_id} not found")
    return Response(status_code=204)


@app.post("/api/query/count")
def query_count(req: _QueryRequest) -> dict[str, Any]:
    mql = req.mql.strip()
    if not mql:
        raise HTTPException(status_code=400, detail="mql is required")
    client = _get_metacat_client()
    start = time.monotonic()
    total = 0
    total_size = 0
    for summary in client.query(mql, summary="count"):
        if isinstance(summary, dict):
            total = summary.get("count", 0)
            total_size = summary.get("total_size", 0)
        break
    log.info(
        "/api/query/count total=%d took=%.2fs",
        total, time.monotonic() - start,
    )
    return {"total": total, "total_size": total_size}


@app.post("/api/query/validate")
def query_validate(req: _QueryRequest) -> dict[str, Any]:
    mql = req.mql.strip()
    if not mql:
        return {"ok": False, "error": "MQL is empty"}
    client = _get_metacat_client()
    try:
        for _ in client.query(mql, summary="count"):
            break
    except MCWebAPIError as e:
        return {"ok": False, "error": str(e)}
    return {"ok": True}


@app.get("/api/file")
def get_file(did: str = Query(...)) -> dict[str, Any]:
    client = _get_metacat_client()
    start = time.monotonic()
    record = client.get_file(
        did=did, with_metadata=True, with_provenance=True, with_datasets=True
    )
    log.info("/api/file did=%s took=%.2fs", did, time.monotonic() - start)
    if record is None:
        raise HTTPException(status_code=404, detail=f"File not found: {did}")
    record["did"] = f"{record['namespace']}:{record['name']}"
    return record


@app.get("/api/dataset")
def get_dataset(did: str = Query(...)) -> dict[str, Any]:
    client = _get_metacat_client()
    start = time.monotonic()
    record = client.get_dataset(did=did)
    log.info("/api/dataset did=%s took=%.2fs", did, time.monotonic() - start)
    if record is None:
        raise HTTPException(status_code=404, detail=f"Dataset not found: {did}")
    record["did"] = f"{record['namespace']}:{record['name']}"
    return record


@app.get("/api/files/count")
def count_files(
    dataset: str = Query(...),
    runs: str | None = Query(None),
    run_range: str | None = Query(None),
    namespace: str | None = Query(None),
    meta: list[str] = Query(default_factory=list),
) -> dict[str, Any]:
    filters = _build_file_filters(runs, run_range, namespace, meta)
    client = _get_metacat_client()
    start = time.monotonic()

    # Fast path: no filters → dataset's own file_count, fetched in one
    # cheap call instead of a summary=count MQL query that can take
    # minutes on large datasets.
    if not (filters.runs or filters.run_range or filters.namespace or filters.meta):
        ds = client.get_dataset(did=dataset)
        if ds is None:
            raise HTTPException(status_code=404, detail=f"Dataset not found: {dataset}")
        total = ds.get("file_count") or 0
        log.info(
            "/api/files/count fast-path dataset=%s total=%d took=%.2fs",
            dataset, total, time.monotonic() - start,
        )
        return {"total": total, "source": "dataset.file_count"}

    base_mql = build_mql(dataset, filters)
    total = 0
    for summary in client.query(base_mql, summary="count"):
        total = summary.get("count", 0) if isinstance(summary, dict) else 0
        break
    log.info(
        "/api/files/count slow-path dataset=%s total=%d took=%.2fs",
        dataset, total, time.monotonic() - start,
    )
    return {"total": total, "source": "summary=count"}


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
