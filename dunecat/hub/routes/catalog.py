"""Catalog endpoints ported from `dunecat/web/routes.py` with two
structural changes per handler:

  * `Depends(current_user)` — every request is identified as a real
    user, so the metacat call carries that user's bearer.
  * `metacat_for(user)` — builds a fresh per-request MetaCatClient
    (vault → bearer → metacat session). The local app's cached
    singleton is irrelevant here.

Routes live in one file for now because they share a thicket of
imports and helpers; split per area when this file grows past ~600
lines.

Deferred:
  * `/api/replicas` — Rucio's auth model assumes a global bearer
    file, which doesn't fit per-user. Tracked as a separate follow-up.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from collections import Counter
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response
from metacat.webapi import MCWebAPIError
from pydantic import BaseModel, Field

from dunecat import llm
from dunecat.files import build_mql
from dunecat.filters import FileFilters, parse_run_range, parse_runs, value_matches
from dunecat.web import condb

from .. import cache as hub_cache
from .. import rucio as hub_rucio
from ..auth.bearer import metacat_for
from ..auth.dep import current_user
from ..auth.session import User
from ..detectors import (
    apply_default_filters,
    datasets_for_detector,
    datasets_for_namespace,
    detector_by_id,
)
from ..timeouts import (
    CONDB_TIMEOUT_S,
    METACAT_TIMEOUT_S,
    RUCIO_TIMEOUT_S,
    with_timeout,
)

log = logging.getLogger("uvicorn.error")
router = APIRouter()


# ---- helpers --------------------------------------------------------------


def _parse_meta_pairs(values: list[str]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for raw in values:
        if "=" not in raw:
            raise HTTPException(
                status_code=400,
                detail=f"--meta expects 'KEY=VALUE', got {raw!r}",
            )
        k, v = raw.split("=", 1)
        out.append((k.strip(), v.strip()))
    return out


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
            meta=tuple(_parse_meta_pairs(meta)),
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


def _apply_filters(
    items: list[dict[str, Any]],
    pattern: str | None,
    tier: str | None,
    file_type: str | None,
    meta: list[str],
) -> list[dict[str, Any]]:
    meta_pairs = _parse_meta_pairs(meta)
    needle = pattern.lower() if pattern else None
    out = []
    for ds in items:
        if needle and needle not in ds["name"].lower():
            continue
        md = ds.get("metadata") or {}
        if tier and not value_matches(md.get("core.data_tier"), tier):
            continue
        if file_type and not value_matches(md.get("core.file_type"), file_type):
            continue
        if not all(value_matches(md.get(k), v) for k, v in meta_pairs):
            continue
        out.append(ds)
    return out


def _dataset_row(ds: dict[str, Any]) -> dict[str, Any]:
    return {
        "did": f"{ds['namespace']}:{ds['name']}",
        "namespace": ds["namespace"],
        "name": ds["name"],
        "file_count": ds.get("file_count"),
        "created_timestamp": ds.get("created_timestamp"),
        "updated_timestamp": ds.get("updated_timestamp"),
        "metadata": ds.get("metadata") or {},
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


def _resolve_provenance_fids(
    client, entries: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    if not entries:
        return []
    fids = [e["fid"] for e in entries if "fid" in e]
    if not fids:
        return entries
    lookup = [{"fid": f} for f in fids]
    resolved = {}
    for item in client.get_files(
        lookup, with_metadata=False, with_provenance=False
    ):
        resolved[item["fid"]] = {
            "fid": item["fid"],
            "namespace": item["namespace"],
            "name": item["name"],
            "did": f"{item['namespace']}:{item['name']}",
            "size": item.get("size"),
            "created_timestamp": item.get("created_timestamp"),
        }
    return [resolved.get(e["fid"], e) for e in entries if "fid" in e]


# ---- detector counts + datasets ------------------------------------------


@router.get("/api/detectors/counts")
def detector_counts(
    user: User = Depends(current_user),
) -> list[dict[str, Any]]:
    """Live datasets/files counts per detector. Caches in-process so
    overlapping namespaces share the per-ns fetch."""
    from dunecat.web.detectors import load_detectors

    entries = load_detectors()
    all_namespaces = sorted({ns for e in entries for ns in e["namespaces"]})

    client = metacat_for(user)

    def _all_ns_in_parallel() -> dict[str, tuple[list[dict[str, Any]], datetime]]:
        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(max_workers=8) as ex:
            results = list(
                ex.map(
                    lambda ns: datasets_for_namespace(ns, client=client),
                    all_namespaces,
                )
            )
        return dict(zip(all_namespaces, results))

    ns_results = with_timeout(
        _all_ns_in_parallel,
        timeout=METACAT_TIMEOUT_S,
        label="metacat datasets_for_namespace (counts)",
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
                "files_count": sum(
                    ds.get("file_count") or 0 for ds in filtered
                ),
            }
        )
    return out


@router.get("/api/datasets")
def list_datasets(
    detector: str = Query(...),
    namespace: str | None = Query(None),
    pattern: str | None = Query(None),
    tier: str | None = Query(None),
    file_type: str | None = Query(None),
    meta: list[str] = Query(default_factory=list),
    official_only: bool = Query(True),
    with_metadata_only: bool = Query(True),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=500),
    user: User = Depends(current_user),
) -> dict[str, Any]:
    det = detector_by_id(detector)
    if det is None:
        raise HTTPException(
            status_code=404, detail=f"Unknown detector: {detector}"
        )
    client = metacat_for(user)
    items, fetched_at = with_timeout(
        datasets_for_detector,
        det["namespaces"],
        client=client,
        timeout=METACAT_TIMEOUT_S,
        label="datasets_for_detector",
    )
    if namespace:
        items = [ds for ds in items if ds.get("namespace") == namespace]
    items = apply_default_filters(
        items,
        official_only=official_only,
        with_metadata_only=with_metadata_only,
    )
    filtered = _apply_filters(
        items, pattern=pattern, tier=tier, file_type=file_type, meta=meta
    )
    total = len(filtered)
    start = (page - 1) * page_size
    end = start + page_size
    rows = [_dataset_row(ds) for ds in filtered[start:end]]
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "fetched_at": fetched_at.isoformat(),
        "rows": rows,
    }


@router.get("/api/datasets/facets")
def datasets_facets(
    detector: str = Query(...),
    namespace: str | None = Query(None),
    tier: str | None = Query(None),
    file_type: str | None = Query(None),
    official_only: bool = Query(True),
    with_metadata_only: bool = Query(True),
    user: User = Depends(current_user),
) -> dict[str, list[dict[str, Any]]]:
    det = detector_by_id(detector)
    if det is None:
        raise HTTPException(
            status_code=404, detail=f"Unknown detector: {detector}"
        )
    client = metacat_for(user)
    items, _ = with_timeout(
        datasets_for_detector,
        det["namespaces"],
        client=client,
        timeout=METACAT_TIMEOUT_S,
        label="datasets_for_detector (facets)",
    )
    if namespace:
        items = [ds for ds in items if ds.get("namespace") == namespace]
    items = apply_default_filters(
        items,
        official_only=official_only,
        with_metadata_only=with_metadata_only,
    )

    tier_counts: Counter = Counter()
    type_counts: Counter = Counter()
    for ds in items:
        md = ds.get("metadata") or {}
        ds_tier = md.get("core.data_tier")
        ds_type = md.get("core.file_type")
        type_match = not file_type or value_matches(ds_type, file_type)
        tier_match = not tier or value_matches(ds_tier, tier)
        if ds_tier is not None and type_match:
            tier_counts[ds_tier] += 1
        if ds_type is not None and tier_match:
            type_counts[ds_type] += 1

    def _format(counter: Counter) -> list[dict[str, Any]]:
        return [{"value": k, "count": c} for k, c in counter.most_common()]

    return {
        "tiers": _format(tier_counts),
        "file_types": _format(type_counts),
    }


@router.post("/api/datasets/refresh")
def refresh_datasets(
    detector: str = Query(...),
    _user: User = Depends(current_user),
) -> Response:
    det = detector_by_id(detector)
    if det is None:
        raise HTTPException(
            status_code=404, detail=f"Unknown detector: {detector}"
        )
    for ns in det["namespaces"]:
        hub_cache.invalidate_datasets(ns)
    return Response(status_code=204)


@router.get("/api/replicas")
def get_replicas(
    did: str = Query(..., description="scope:name DID"),
    user: User = Depends(current_user),
) -> dict[str, Any]:
    """Rucio replica lookup for one file DID, scoped to the
    requesting user's bearer. Cached for 1h (global cache; replica
    info doesn't vary per DUNE user)."""
    if ":" not in did:
        raise HTTPException(status_code=400, detail="did must be 'scope:name'")
    scope, name = did.split(":", 1)
    if not scope or not name:
        raise HTTPException(status_code=400, detail="did must be 'scope:name'")

    cached = hub_cache.get_rucio_cached(scope, name)
    if cached is not None:
        return {"cached": True, **cached}

    start = time.monotonic()
    try:
        body = with_timeout(
            hub_rucio.list_replicas_for,
            user,
            scope,
            name,
            timeout=RUCIO_TIMEOUT_S,
            label=f"rucio list_replicas {did}",
        )
    except HTTPException:
        raise
    except hub_rucio.RucioAuthError as e:
        raise HTTPException(status_code=401, detail=str(e))
    except hub_rucio.RucioError as e:
        log.warning("/api/replicas did=%s: rucio error: %s", did, e)
        raise HTTPException(status_code=502, detail=f"Rucio error: {e}")
    log.info(
        "/api/replicas did=%s replicas=%d took=%.2fs",
        did,
        len(body["replicas"]) if body else 0,
        time.monotonic() - start,
    )
    if body is None:
        raise HTTPException(status_code=404, detail=f"No replicas for {did}")
    hub_cache.set_rucio_cached(scope, name, body)
    return {"cached": False, **body}


@router.get("/api/dataset")
def get_dataset(
    did: str = Query(...),
    user: User = Depends(current_user),
) -> dict[str, Any]:
    client = metacat_for(user)
    start = time.monotonic()
    record = with_timeout(
        client.get_dataset,
        did,
        timeout=METACAT_TIMEOUT_S,
        label=f"get_dataset {did}",
    )
    log.info(
        "/api/dataset did=%s took=%.2fs", did, time.monotonic() - start
    )
    if record is None:
        raise HTTPException(status_code=404, detail=f"Dataset not found: {did}")
    record["did"] = f"{record['namespace']}:{record['name']}"
    return record


# ---- files ----------------------------------------------------------------


@router.get("/api/files")
def list_files(
    dataset: str = Query(...),
    runs: str | None = Query(None),
    run_range: str | None = Query(None),
    namespace: str | None = Query(None),
    meta: list[str] = Query(default_factory=list),
    with_metadata: bool = Query(False),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=500),
    user: User = Depends(current_user),
) -> dict[str, Any]:
    filters = _build_file_filters(runs, run_range, namespace, meta)
    base_mql = build_mql(dataset, filters)
    offset = (page - 1) * page_size
    paged_mql = f"({base_mql}) ordered skip {offset} limit {page_size}"
    client = metacat_for(user)
    start = time.monotonic()
    rows = with_timeout(
        lambda: [
            _file_row(item)
            for item in client.query(paged_mql, with_metadata=with_metadata)
        ],
        timeout=METACAT_TIMEOUT_S,
        label=f"query (files page={page})",
    )
    log.info(
        "/api/files page=%d rows=%d with_metadata=%s took=%.2fs",
        page,
        len(rows),
        with_metadata,
        time.monotonic() - start,
    )
    return {
        "page": page,
        "page_size": page_size,
        "rows": rows,
        "has_more": len(rows) == page_size,
    }


@router.get("/api/files/count")
def count_files(
    dataset: str = Query(...),
    runs: str | None = Query(None),
    run_range: str | None = Query(None),
    namespace: str | None = Query(None),
    meta: list[str] = Query(default_factory=list),
    user: User = Depends(current_user),
) -> dict[str, Any]:
    filters = _build_file_filters(runs, run_range, namespace, meta)
    client = metacat_for(user)
    start = time.monotonic()

    if not (filters.runs or filters.run_range or filters.namespace or filters.meta):
        ds = with_timeout(
            client.get_dataset,
            dataset,
            timeout=METACAT_TIMEOUT_S,
            label=f"get_dataset {dataset}",
        )
        if ds is None:
            raise HTTPException(
                status_code=404, detail=f"Dataset not found: {dataset}"
            )
        total = ds.get("file_count") or 0
        log.info(
            "/api/files/count fast dataset=%s total=%d took=%.2fs",
            dataset,
            total,
            time.monotonic() - start,
        )
        return {"total": total, "source": "dataset.file_count"}

    base_mql = build_mql(dataset, filters)

    def _count() -> int:
        for summary in client.query(base_mql, summary="count"):
            if isinstance(summary, dict):
                return summary.get("count", 0)
            break
        return 0

    total = with_timeout(
        _count, timeout=METACAT_TIMEOUT_S, label="query summary=count"
    )
    log.info(
        "/api/files/count slow dataset=%s total=%d took=%.2fs",
        dataset,
        total,
        time.monotonic() - start,
    )
    return {"total": total, "source": "summary=count"}


@router.get("/api/file")
def get_file(
    did: str = Query(...),
    user: User = Depends(current_user),
) -> dict[str, Any]:
    client = metacat_for(user)
    start = time.monotonic()
    record = with_timeout(
        lambda: client.get_file(
            did=did,
            with_metadata=True,
            with_provenance=True,
            with_datasets=True,
        ),
        timeout=METACAT_TIMEOUT_S,
        label=f"get_file {did}",
    )
    log.info("/api/file did=%s took=%.2fs", did, time.monotonic() - start)
    if record is None:
        raise HTTPException(status_code=404, detail=f"File not found: {did}")
    record["did"] = f"{record['namespace']}:{record['name']}"
    record["parents"] = _resolve_provenance_fids(
        client, record.get("parents") or []
    )
    record["children"] = _resolve_provenance_fids(
        client, record.get("children") or []
    )
    return record


# ---- run summary ----------------------------------------------------------


@router.get("/api/run/{run_number}")
def get_run(
    run_number: int,
    user: User = Depends(current_user),
) -> dict[str, Any]:
    client = metacat_for(user)
    start = time.monotonic()
    items = with_timeout(
        lambda: list(
            client.query(
                f"files where core.runs in ({run_number})",
                with_metadata=True,
            )
        ),
        timeout=METACAT_TIMEOUT_S,
        label=f"query run={run_number}",
    )
    log.info(
        "/api/run %d files=%d took=%.2fs",
        run_number,
        len(items),
        time.monotonic() - start,
    )
    if not items:
        raise HTTPException(
            status_code=404, detail=f"No files found for run {run_number}"
        )

    by_tier: dict[str, int] = {}
    raw_starts: list[float] = []
    raw_ends: list[float] = []
    sample_raw: list[dict[str, Any]] = []
    for it in items:
        md = it.get("metadata") or {}
        tier = md.get("core.data_tier") or "unknown"
        by_tier[tier] = by_tier.get(tier, 0) + 1
        if tier == "raw":
            s = md.get("core.start_time")
            e = md.get("core.end_time")
            if isinstance(s, (int, float)):
                raw_starts.append(float(s))
            if isinstance(e, (int, float)):
                raw_ends.append(float(e))
            if len(sample_raw) < 10:
                sample_raw.append(
                    {
                        "did": f"{it['namespace']}:{it['name']}",
                        "name": it["name"],
                        "size": it.get("size"),
                    }
                )

    start_time = min(raw_starts) if raw_starts else None
    end_time = max(raw_ends) if raw_ends else None
    duration = (
        (end_time - start_time)
        if (start_time is not None and end_time is not None)
        else None
    )

    return {
        "run": run_number,
        "files_total": len(items),
        "files_by_tier": dict(sorted(by_tier.items(), key=lambda kv: -kv[1])),
        "start_time": start_time,
        "end_time": end_time,
        "duration_seconds": duration,
        "sample_raw_files": sample_raw,
    }


# ---- conditions DB --------------------------------------------------------


@router.get("/api/detectors/{detector_id}/condb-columns")
def get_condb_columns(
    detector_id: str,
    _user: User = Depends(current_user),
) -> list[dict[str, str]]:
    det = detector_by_id(detector_id)
    if det is None:
        raise HTTPException(
            status_code=404, detail=f"Unknown detector: {detector_id}"
        )
    folder = det.get("condb_folder")
    if not folder:
        return []
    return condb.CUSTOM_COLUMNS.get(folder, [])


@router.get("/api/runs/{detector}/{run}/conditions")
def get_run_conditions(
    detector: str,
    run: int,
    _user: User = Depends(current_user),
) -> dict[str, Any]:
    det = detector_by_id(detector)
    if det is None:
        raise HTTPException(
            status_code=404, detail=f"Unknown detector: {detector}"
        )
    folder = det.get("condb_folder")
    if not folder:
        raise HTTPException(
            status_code=400,
            detail=f"Detector {detector} has no condb_folder configured.",
        )
    try:
        row = with_timeout(
            condb.fetch_run, folder, run,
            cache_mod=hub_cache,
            timeout=CONDB_TIMEOUT_S,
            label=f"condb fetch_run {folder}/{run}",
        )
    except HTTPException:
        raise
    except Exception as e:
        log.warning(
            "/api/runs/%s/%d/conditions: condb fetch failed: %s",
            detector,
            run,
            e,
        )
        raise HTTPException(status_code=502, detail="condb unreachable")
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"No conditions on file for run {run} in {folder}.",
        )
    return {"folder": folder, "run": run, "row": row}


@router.get("/api/runs/{detector}/conditions")
def get_runs_conditions(
    detector: str,
    run_min: int | None = Query(None, ge=0),
    run_max: int | None = Query(None, ge=0),
    runs: list[int] | None = Query(None),
    start: str | None = Query(None),
    stop: str | None = Query(None),
    run_type: str = Query("PROD"),
    data_stream: str = Query("any"),
    beam_setp_min: float | None = Query(None, ge=0),
    beam_setp_max: float | None = Query(None, ge=0),
    polarity: str = Query("any"),
    cond: list[str] | None = Query(None),
    _user: User = Depends(current_user),
) -> dict[str, Any]:
    runs_set: set[int] | None = None
    if runs:
        if len(runs) > 5000:
            raise HTTPException(
                status_code=400,
                detail="Too many explicit runs (max 5000); use a plain range.",
            )
        if any(r < 0 for r in runs):
            raise HTTPException(
                status_code=400, detail="run numbers must be >= 0"
            )
        runs_set = set(runs)
        if run_min is None:
            run_min = min(runs_set)
        if run_max is None:
            run_max = max(runs_set)

    if run_min is not None and run_max is not None and run_max < run_min:
        raise HTTPException(
            status_code=400, detail="run_max must be >= run_min"
        )
    if (run_min is None) != (run_max is None):
        raise HTTPException(
            status_code=400,
            detail="Pass both run_min and run_max, or neither.",
        )
    if (
        beam_setp_min is not None
        and beam_setp_max is not None
        and beam_setp_max < beam_setp_min
    ):
        raise HTTPException(
            status_code=400, detail="beam_setp_max must be >= beam_setp_min"
        )
    polarity_norm = polarity.lower() if polarity else "any"
    if polarity_norm not in ("any", "positive", "negative"):
        raise HTTPException(
            status_code=400,
            detail="polarity must be 'positive', 'negative', or 'any'.",
        )
    ds_norm = data_stream.lower() if data_stream else "any"
    if ds_norm not in ("any", "cosmics", "calibration", "physics"):
        raise HTTPException(
            status_code=400,
            detail=(
                "data_stream must be 'cosmics', 'calibration', 'physics', "
                "or 'any'."
            ),
        )

    start_unix: float | None = None
    stop_unix: float | None = None
    try:
        if start:
            start_dt = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=UTC)
            start_unix = start_dt.timestamp()
        if stop:
            stop_dt = datetime.strptime(stop, "%Y-%m-%d").replace(tzinfo=UTC)
            stop_unix = stop_dt.timestamp() + 86400
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="start/stop must be in YYYY-MM-DD format.",
        )
    if start_unix is not None and stop_unix is not None and stop_unix <= start_unix:
        raise HTTPException(
            status_code=400, detail="stop date must be >= start date"
        )

    have_run_range = run_min is not None and run_max is not None
    have_date_range = start_unix is not None or stop_unix is not None
    have_beam_filter = (
        beam_setp_min is not None
        or beam_setp_max is not None
        or polarity_norm != "any"
    )
    have_stream_filter = ds_norm != "any"
    have_custom = bool(cond)
    if (
        not have_run_range
        and not have_date_range
        and not have_beam_filter
        and not have_stream_filter
        and not have_custom
    ):
        raise HTTPException(
            status_code=400,
            detail=(
                "At least one filter is required: run range, date range, "
                "beam (momentum / polarity), data stream, or custom condition."
            ),
        )

    det = detector_by_id(detector)
    if det is None:
        raise HTTPException(
            status_code=404, detail=f"Unknown detector: {detector}"
        )
    folder = det.get("condb_folder")
    if not folder:
        raise HTTPException(
            status_code=400,
            detail=f"Detector {detector} has no condb_folder configured.",
        )
    rt = None if run_type.upper() == "ALL" else run_type
    pol = None if polarity_norm == "any" else polarity_norm
    ds = None if ds_norm == "any" else ds_norm
    validated_extra: list[str] = []
    if cond:
        try:
            validated_extra = [
                condb.validate_custom_cond(folder, c) for c in cond
            ]
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
    try:
        rows = with_timeout(
            condb.fetch_runs,
            folder,
            run_min=run_min,
            run_max=run_max,
            runs=runs_set,
            start_unix=start_unix,
            stop_unix=stop_unix,
            run_type=rt,
            data_stream=ds,
            beam_setp_min=beam_setp_min,
            beam_setp_max=beam_setp_max,
            polarity=pol,
            extra_conds=validated_extra or None,
            cache_mod=hub_cache,
            timeout=CONDB_TIMEOUT_S,
            label=f"condb fetch_runs {folder}",
        )
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except condb.CondBError as e:
        raise HTTPException(status_code=400, detail=f"condb rejected: {e}")
    except Exception as e:
        log.warning(
            "/api/runs/%s/conditions: condb fetch failed: %s", detector, e
        )
        raise HTTPException(status_code=502, detail="condb unreachable")
    return {
        "folder": folder,
        "run_min": run_min,
        "run_max": run_max,
        "runs": sorted(runs_set) if runs_set is not None else None,
        "start": start,
        "stop": stop,
        "run_type": rt,
        "data_stream": ds,
        "beam_setp_min": beam_setp_min,
        "beam_setp_max": beam_setp_max,
        "polarity": pol,
        "custom_conds": validated_extra,
        "rows": rows,
    }


# ---- raw query + saved queries -------------------------------------------


class _QueryRequest(BaseModel):
    mql: str


class _QueryRunRequest(BaseModel):
    mql: str
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=100, ge=1, le=500)
    saved_query_id: int | None = None


class _FromEnglishRequest(BaseModel):
    english: str


class _SavedQueryCreate(BaseModel):
    name: str
    mql: str


class _SavedQueryUpdate(BaseModel):
    name: str | None = None
    mql: str | None = None


@router.post("/api/query/run")
def query_run(
    req: _QueryRunRequest,
    user: User = Depends(current_user),
) -> dict[str, Any]:
    mql = req.mql.strip()
    if not mql:
        raise HTTPException(status_code=400, detail="mql is required")
    paged_mql = (
        f"({mql}) ordered skip {(req.page - 1) * req.page_size} "
        f"limit {req.page_size}"
    )
    client = metacat_for(user)
    start = time.monotonic()
    rows = with_timeout(
        lambda: [_file_row(item) for item in client.query(paged_mql)],
        timeout=METACAT_TIMEOUT_S,
        label=f"query (run page={req.page})",
    )
    log.info(
        "/api/query/run page=%d rows=%d took=%.2fs",
        req.page,
        len(rows),
        time.monotonic() - start,
    )
    if req.saved_query_id is not None:
        hub_cache.touch_saved_query(user.id, req.saved_query_id)
    return {
        "page": req.page,
        "page_size": req.page_size,
        "rows": rows,
        "has_more": len(rows) == req.page_size,
    }


@router.post("/api/query/count")
def query_count(
    req: _QueryRequest,
    user: User = Depends(current_user),
) -> dict[str, Any]:
    mql = req.mql.strip()
    if not mql:
        raise HTTPException(status_code=400, detail="mql is required")
    client = metacat_for(user)
    start = time.monotonic()

    def _count() -> tuple[int, int]:
        for summary in client.query(mql, summary="count"):
            if isinstance(summary, dict):
                return (summary.get("count", 0), summary.get("total_size", 0))
            break
        return (0, 0)

    total, total_size = with_timeout(
        _count, timeout=METACAT_TIMEOUT_S, label="query summary=count"
    )
    log.info(
        "/api/query/count total=%d took=%.2fs",
        total,
        time.monotonic() - start,
    )
    return {"total": total, "total_size": total_size}


@router.post("/api/query/validate")
def query_validate(
    req: _QueryRequest,
    user: User = Depends(current_user),
) -> dict[str, Any]:
    mql = req.mql.strip()
    if not mql:
        return {"ok": False, "error": "MQL is empty"}
    client = metacat_for(user)

    def _probe() -> None:
        for _ in client.query(mql, summary="count"):
            break

    try:
        with_timeout(
            _probe, timeout=METACAT_TIMEOUT_S, label="query validate"
        )
    except MCWebAPIError as e:
        return {"ok": False, "error": str(e)}
    except HTTPException:
        raise
    return {"ok": True}


@router.post("/api/query/from-english")
def query_from_english(
    req: _FromEnglishRequest,
    _user: User = Depends(current_user),
) -> dict[str, str]:
    if not llm.is_enabled():
        raise HTTPException(
            status_code=503,
            detail="English-to-MQL is not enabled on this server",
        )
    english = req.english.strip()
    if not english:
        raise HTTPException(status_code=400, detail="english is required")
    start = time.monotonic()
    try:
        result = llm.generate_mql(english)
    except Exception as e:
        log.warning("/api/query/from-english failed: %s", e)
        raise HTTPException(
            status_code=502, detail="Couldn't generate a query from that"
        )
    log.info(
        "/api/query/from-english took=%.2fs mql=%r",
        time.monotonic() - start,
        result["mql"][:120],
    )
    return result


@router.get("/api/queries")
def list_saved_queries(
    user: User = Depends(current_user),
) -> list[dict[str, Any]]:
    return hub_cache.list_saved_queries_for(user.id)


@router.post("/api/queries", status_code=201)
def create_saved_query(
    req: _SavedQueryCreate,
    user: User = Depends(current_user),
) -> dict[str, Any]:
    name = req.name.strip()
    mql = req.mql.strip()
    if not name:
        raise HTTPException(status_code=400, detail="name is required")
    if not mql:
        raise HTTPException(status_code=400, detail="mql is required")
    try:
        return hub_cache.create_saved_query(user.id, name=name, mql=mql)
    except sqlite3.IntegrityError:
        raise HTTPException(
            status_code=409,
            detail=f"A query named {name!r} already exists",
        )


@router.put("/api/queries/{query_id}")
def update_saved_query(
    query_id: int,
    req: _SavedQueryUpdate,
    user: User = Depends(current_user),
) -> dict[str, Any]:
    existing = hub_cache.get_saved_query_for(user.id, query_id)
    if existing is None:
        raise HTTPException(
            status_code=404, detail=f"Saved query {query_id} not found"
        )
    name = req.name.strip() if req.name is not None else existing["name"]
    mql = req.mql.strip() if req.mql is not None else existing["mql"]
    if not name:
        raise HTTPException(status_code=400, detail="name cannot be empty")
    if not mql:
        raise HTTPException(status_code=400, detail="mql cannot be empty")
    try:
        updated = hub_cache.update_saved_query(
            user.id, query_id, name=name, mql=mql
        )
    except sqlite3.IntegrityError:
        raise HTTPException(
            status_code=409,
            detail=f"A query named {name!r} already exists",
        )
    if updated is None:
        raise HTTPException(
            status_code=404, detail=f"Saved query {query_id} not found"
        )
    return updated


@router.delete("/api/queries/{query_id}", status_code=204)
def delete_saved_query(
    query_id: int,
    user: User = Depends(current_user),
) -> Response:
    if not hub_cache.delete_saved_query(user.id, query_id):
        raise HTTPException(
            status_code=404, detail=f"Saved query {query_id} not found"
        )
    return Response(status_code=204)
