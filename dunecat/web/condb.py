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
import re
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from . import cache

log = logging.getLogger("uvicorn.error")

DEFAULT_BASE_URL = "https://dbdata0vm.fnal.gov:9443/dune_runcon_prod"
_TIMEOUT = 15.0

# Curated per-folder list of columns exposed to the "Custom conditions"
# UI. Excludes columns covered by dedicated filters (run_type, data_stream,
# polarity, beam_*, start_time, stop_time) and admin columns (channel, tv,
# tr, upload_time, data_type, detector_id). Type hint is informational only;
# the server parses values on its own.
CUSTOM_COLUMNS: dict[str, list[dict[str, str]]] = {
    "pdunesp.run_conditionstest": [
        {"name": "software_version", "type": "string"},
        {"name": "data_quality", "type": "string"},
        {"name": "detector_type", "type": "string"},
        {"name": "ac_couple", "type": "string"},
        {"name": "gain", "type": "number"},
        {"name": "peak_time", "type": "number"},
        {"name": "baseline", "type": "number"},
        {"name": "leak", "type": "number"},
        {"name": "detector_hv", "type": "number"},
        {"name": "detector_hvset", "type": "number"},
        {"name": "lar_purity", "type": "number"},
        {"name": "lar_top_temp_mean", "type": "number"},
        {"name": "lar_bottom_temp_mean", "type": "number"},
        {"name": "pulser", "type": "bool"},
        {"name": "cold", "type": "bool"},
    ],
    "pdunesp.run_conditions_vd": [
        {"name": "software_version", "type": "string"},
        {"name": "data_quality", "type": "string"},
        {"name": "ac_couple", "type": "string"},
        {"name": "gain", "type": "number"},
        {"name": "peak_time", "type": "number"},
        {"name": "baseline", "type": "number"},
        {"name": "detector_hv_mean", "type": "number"},
        {"name": "detector_hv_std", "type": "number"},
        {"name": "detector_set", "type": "number"},
        {"name": "apas", "type": "string"},
        {"name": "hv_induction_plane", "type": "number"},
        {"name": "hv_collection_plane", "type": "number"},
        {"name": "lar_top_temp_mean", "type": "number"},
        {"name": "lar_bottom_temp_mean", "type": "number"},
        {"name": "pulser", "type": "bool"},
        {"name": "test_cap", "type": "bool"},
    ],
}

_CUSTOM_OPS = frozenset({"<", "<=", "=", "!=", ">=", ">"})


class CondBError(Exception):
    """Server-side error from condb, with a user-presentable message."""


def validate_custom_cond(folder: str, cond: str) -> str:
    """Validate a user-submitted ``cond=`` predicate string.

    The format mirrors what condb's server-side parser expects:
    ``<column> <op> <value>``, three whitespace-separated tokens.

    Returns the cleaned string ready to forward to condb. Raises
    ``ValueError`` on any rejection (unknown column, bad op, etc.).
    """
    parts = cond.strip().split(None, 2)
    if len(parts) != 3:
        raise ValueError(
            f"Custom condition must be 'COL OP VALUE' (got: {cond!r})"
        )
    col, op, value = parts
    if op not in _CUSTOM_OPS:
        raise ValueError(
            f"Operator must be one of <, <=, =, !=, >=, > (got: {op!r})"
        )
    allowed = {c["name"] for c in CUSTOM_COLUMNS.get(folder, ())}
    if col not in allowed:
        raise ValueError(
            f"Column {col!r} not exposed for custom conditions in {folder!r}"
        )
    return f"{col} {op} {value}"

# Per-folder column naming. HD uses `beam_setmomentum` (the live folder); VD
# uses `beam_momentum_set` (a different schema). Filtering against the wrong
# column 400s the server with "Unrecognized data column".
_BEAM_SETP_COL = {
    "pdunesp.run_conditionstest": "beam_setmomentum",
    "pdunesp.run_conditions_vd": "beam_momentum_set",
}

# Polarity literal casing differs per folder — HD stores 'Negative' (capital
# N), VD stores 'negative' (lowercase). Server matches strings exactly.
_POLARITY_BY_FOLDER: dict[str, dict[str, str]] = {
    "pdunesp.run_conditionstest": {"positive": "positive", "negative": "Negative"},
    "pdunesp.run_conditions_vd": {"positive": "positive", "negative": "negative"},
}


def base_url() -> str:
    return os.environ.get("CONDB_SERVER_URL") or DEFAULT_BASE_URL


def _fetch_json(url: str) -> dict[str, Any]:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        # 400 / 500 from condb usually carry a useful message we want to
        # bubble up — either a plain "Invalid value for X" or an HTML body
        # with the underlying exception's text in an <h3> tag.
        body = b""
        try:
            body = e.read() or b""
        except Exception:
            pass
        msg = _extract_condb_error(body.decode("utf-8", errors="replace")) \
            or f"HTTP {e.code}"
        raise CondBError(msg) from None


_ERROR_PATTERNS = [
    re.compile(r"<h3>(.*?)</h3>", re.S),
    re.compile(r"Invalid value for [^\n<]+"),
    re.compile(r"Can not parse value: [^\n<]+"),
    re.compile(r"Unrecognized data column: [^\n<]+"),
]


def _extract_condb_error(body: str) -> str | None:
    for pat in _ERROR_PATTERNS:
        m = pat.search(body)
        if m:
            return (m.group(1) if m.lastindex else m.group(0)).strip()
    return None


def fetch_runs(
    folder: str,
    *,
    run_min: int | None = None,
    run_max: int | None = None,
    start_unix: float | None = None,
    stop_unix: float | None = None,  # exclusive upper bound
    run_type: str | None = None,
    data_stream: str | None = None,
    beam_setp_min: float | None = None,
    beam_setp_max: float | None = None,
    polarity: str | None = None,  # "positive" or "negative" (case-insensitive); None = any
    extra_conds: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Return normalized condb rows under the given filters.

    Server limitations dictate the path we take:

    - ``/get?t0&t1`` returns rows in a tv (run number) range but does **not**
      accept ``cond=`` predicates.
    - ``/search?cond=...`` accepts column predicates (``start_time``,
      ``run_type``, beam columns, etc.) but does **not** accept ``t0/t1``,
      and rejects ``cond=tv...`` because ``tv`` is a timeline-key column.

    So:

    - If any column predicate is set (date, beam, polarity), use
      ``/search?cond=...`` and apply the tv range client-side.
    - Otherwise (run range only), use ``/get?t0&t1`` and apply the run_type
      filter client-side.
    """
    has_column_filter = (
        start_unix is not None
        or stop_unix is not None
        or data_stream is not None
        or beam_setp_min is not None
        or beam_setp_max is not None
        or polarity is not None
        or bool(extra_conds)
    )
    if has_column_filter:
        rows = _search_with_bounds(
            folder,
            start_unix=start_unix,
            stop_unix=stop_unix,
            run_type=run_type,
            data_stream=data_stream,
            beam_setp_min=beam_setp_min,
            beam_setp_max=beam_setp_max,
            polarity=polarity,
            extra_conds=extra_conds,
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


def _search_with_bounds(
    folder: str,
    *,
    start_unix: float | None,
    stop_unix: float | None,
    run_type: str | None,
    data_stream: str | None,
    beam_setp_min: float | None,
    beam_setp_max: float | None,
    polarity: str | None,
    extra_conds: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Build server-side ``cond=`` predicates, with magnitude-based beam
    bounds.

    Beam setpoint is stored signed in both folders: negative-polarity
    beams have negative setpoint. The user-facing bounds are magnitudes
    (>= 0), so this function maps them onto the correct sign of the
    storage column:

    - ``polarity="positive"``: ``setp in [min, max]`` AND polarity literal.
    - ``polarity="negative"``: ``setp in [-max, -min]`` AND polarity literal
      (note bounds flip when negated).
    - ``polarity=None``: with no beam bounds, no constraint. With beam
      bounds, two server queries (positive side + negative side) unioned.
    """
    common: list[str] = []
    if start_unix is not None:
        common.append(f"start_time >= {int(start_unix)}")
    if stop_unix is not None:
        common.append(f"start_time < {int(stop_unix)}")
    if run_type:
        common.append(f"run_type = '{run_type}'")
    if data_stream:
        common.append(f"data_stream = '{data_stream}'")
    if extra_conds:
        common.extend(extra_conds)

    have_beam_bounds = beam_setp_min is not None or beam_setp_max is not None
    pol_map = _POLARITY_BY_FOLDER.get(folder, {})

    def query_side(side: str | None) -> list[dict[str, Any]]:
        """Run /search for one polarity side (or polarity-agnostic)."""
        conds = list(common)
        if side is not None:
            literal = pol_map.get(side)
            if literal is None:
                raise ValueError(
                    f"Polarity {side!r} not configured for folder {folder!r}"
                )
            conds.append(f"beam_polarity = '{literal}'")
        if have_beam_bounds:
            col = _BEAM_SETP_COL.get(folder)
            if col is None:
                raise ValueError(
                    f"Beam setpoint filter not supported for folder {folder!r}"
                )
            if side == "negative":
                # Flip & swap: |setp| in [min, max] ↔ setp in [-max, -min].
                if beam_setp_max is not None:
                    conds.append(f"{col} >= {-beam_setp_max}")
                if beam_setp_min is not None:
                    conds.append(f"{col} <= {-beam_setp_min}")
            else:
                if beam_setp_min is not None:
                    conds.append(f"{col} >= {beam_setp_min}")
                if beam_setp_max is not None:
                    conds.append(f"{col} <= {beam_setp_max}")
        params: list[tuple[str, str]] = [
            ("folder", folder),
            ("format", "json"),
        ]
        params.extend(("cond", c) for c in conds)
        url = f"{base_url()}/search?{urllib.parse.urlencode(params)}"
        return _fetch_json(url).get("rows") or []

    if polarity in ("positive", "negative"):
        return query_side(polarity)
    # polarity is None / "any".
    if have_beam_bounds:
        # Magnitude semantics: union positive-side and negative-side.
        return query_side("positive") + query_side("negative")
    return query_side(None)


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
