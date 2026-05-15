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


def fetch_runs(
    folder: str,
    *,
    run_min: int | None = None,
    run_max: int | None = None,
    start_unix: float | None = None,
    stop_unix: float | None = None,  # exclusive upper bound
    run_type: str | None = None,
) -> list[dict[str, Any]]:
    """Return normalized condb rows under the given filters.

    Server limitations dictate the path we take:

    - ``/get?t0&t1`` returns rows in a tv (run number) range but does **not**
      accept ``cond=`` predicates.
    - ``/search?cond=...`` accepts column predicates (``start_time``,
      ``run_type``, etc.) but does **not** accept ``t0/t1``, and rejects
      ``cond=tv...`` because ``tv`` is a timeline-key column, not a data
      column.

    So:

    - If any date bound is set, use ``/search?cond=start_time...`` and apply
      the tv range client-side.
    - Otherwise (run range only), use ``/get?t0&t1`` and apply the run_type
      filter client-side.
    """
    use_search = start_unix is not None or stop_unix is not None
    if use_search:
        rows = _search_by_conds(
            folder,
            start_unix=start_unix,
            stop_unix=stop_unix,
            run_type=run_type,
        )
    else:
        if run_min is None or run_max is None:
            raise ValueError("Either a date range or a run range is required.")
        rows = _get_by_tv_range(folder, run_min, run_max)

    out: list[dict[str, Any]] = []
    for r in rows:
        tv = r.get("tv")
        if tv is None:
            continue
        tv_int = int(tv)
        if run_min is not None and tv_int < run_min:
            continue
        if run_max is not None and tv_int > run_max:
            continue
        if r.get("channel") not in (0, None):
            continue
        norm = _normalize(r)
        # Cache the unfiltered row keyed on (folder, tv) so drilling into a
        # single run from the results table is free.
        cache.set_condb_cached(folder, tv_int, norm)
        if run_type and norm.get("run_type") != run_type:
            continue
        out.append(norm)
    return out


def _get_by_tv_range(
    folder: str, run_min: int, run_max: int
) -> list[dict[str, Any]]:
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
    return payload.get("rows") or []


def _search_by_conds(
    folder: str,
    *,
    start_unix: float | None,
    stop_unix: float | None,
    run_type: str | None,
) -> list[dict[str, Any]]:
    conds: list[str] = []
    if start_unix is not None:
        conds.append(f"start_time >= {int(start_unix)}")
    if stop_unix is not None:
        conds.append(f"start_time < {int(stop_unix)}")
    if run_type:
        # String values in cond= require single quotes per the server.
        conds.append(f"run_type = '{run_type}'")
    params: list[tuple[str, str]] = [
        ("folder", folder),
        ("format", "json"),
    ]
    params.extend(("cond", c) for c in conds)
    url = f"{base_url()}/search?{urllib.parse.urlencode(params)}"
    payload = _fetch_json(url)
    return payload.get("rows") or []


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
