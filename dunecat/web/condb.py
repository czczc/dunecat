"""HTTP client for the DUNE conditions DB.

Talks to the public read endpoint described in `.idea/condb-findings.md`.
Only `/search` is used here; `cond=` predicates let us pull a single row
by `tv` (run number) without dragging the whole folder.
"""
from __future__ import annotations

import ast
import json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from . import cache

log = logging.getLogger("uvicorn.error")

DEFAULT_BASE_URL = "https://dbdata0vm.fnal.gov:9443/dune_runcon_prod"
_TIMEOUT = 15.0


def base_url() -> str:
    return os.environ.get("CONDB_SERVER_URL") or DEFAULT_BASE_URL


def _fetch_json(url: str) -> dict[str, Any]:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_run_range(
    folder: str,
    run_min: int,
    run_max: int,
    run_type: str | None = None,
) -> list[dict[str, Any]]:
    """Return normalized condb rows with ``run_min <= tv <= run_max``.

    Uses the ``/get?t0=&t1=`` range query (``cond=tv...`` is rejected by the
    server — ``tv`` is a timeline-key column, not a data column). The server
    falls back to "latest row at or before t1" when no rows are in range, so
    we always re-filter the response client-side to enforce exact bounds.

    ``/get`` doesn't accept ``cond=`` either, so additional column filters
    (here: ``run_type``) are applied client-side after the range fetch.
    """
    params = urllib.parse.urlencode(
        [
            ("folder", folder),
            ("format", "json"),
            ("t0", str(run_min)),
            ("t1", str(run_max)),
        ]
    )
    url = f"{base_url()}/get?{params}"
    payload = _fetch_json(url)
    rows = payload.get("rows") or []
    out: list[dict[str, Any]] = []
    for r in rows:
        tv = r.get("tv")
        if tv is None:
            continue
        tv_int = int(tv)
        if tv_int < run_min or tv_int > run_max:
            continue
        if r.get("channel") not in (0, None):
            continue
        norm = _normalize(r)
        # Side effect: populate the per-row cache so a later single-run lookup
        # for any of these doesn't hit the network. We cache the unfiltered
        # row so different run_type filters can share the cache.
        cache.set_condb_cached(folder, tv_int, norm)
        if run_type and norm.get("run_type") != run_type:
            continue
        out.append(norm)
    return out


def fetch_run(folder: str, run: int) -> dict[str, Any] | None:
    """Return one normalized condb row for ``(folder, run)``, or ``None``.

    Cached indefinitely in the SQLite store — condb rows for past runs are
    immutable. Negative results (run not in folder) are cached too so we
    don't re-hit the server for known misses.
    """
    cached = cache.get_condb_cached(folder, run)
    if cached != "MISS":
        return cached  # dict or None

    # /search?t=N returns the row at tv=N, or the closest tv<=N if N doesn't
    # exist. We validate `tv == run` exactly to avoid silently returning a
    # different run's row.
    params = urllib.parse.urlencode(
        [("folder", folder), ("format", "json"), ("t", str(run))]
    )
    url = f"{base_url()}/search?{params}"
    try:
        payload = _fetch_json(url)
    except urllib.error.URLError as e:
        log.warning("condb fetch failed (%s): %s", url, e)
        # Don't cache transient failures.
        raise

    rows = payload.get("rows") or []
    body: dict[str, Any] | None = None
    for r in rows:
        tv = r.get("tv")
        if tv is None:
            continue
        if int(tv) == run:
            body = _normalize(r)
            break

    cache.set_condb_cached(folder, run, body)
    return body


def _normalize(row: dict[str, Any]) -> dict[str, Any]:
    """Coerce string sentinels to JSON null and parse config_files."""
    out: dict[str, Any] = {}
    for k, v in row.items():
        if isinstance(v, str) and v in ("None", "null", ""):
            out[k] = None
        else:
            out[k] = v

    cf = out.get("config_files")
    if isinstance(cf, str) and cf:
        # The server returns a Python repr() of a dict ({'np04_daq': '...'}),
        # not JSON. Use literal_eval so single-quoted keys/values parse.
        try:
            parsed = ast.literal_eval(cf)
        except (SyntaxError, ValueError):
            parsed = None
        if isinstance(parsed, dict):
            out["config_files"] = parsed
        else:
            out["config_files"] = None
    return out
