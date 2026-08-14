#!/usr/bin/env -S uv run python
"""Benchmark OpenAI-compatible models on dunecat's English-to-MQL task.

Usage:
    uv run scripts/llm-bench.py                       # every model the endpoint lists
    uv run scripts/llm-bench.py gemma-4-26b gpt-oss-120b
    uv run scripts/llm-bench.py --execute        # also verify against metacat

Reads DUNECAT_LLM_BASE_URL / DUNECAT_LLM_API_KEY from the environment (or
.env), and uses the *production* system prompt from `dunecat.llm`, so the
scores describe the real feature rather than a synthetic proxy.

Why these metrics
-----------------
For this task a model is only useful if the query it writes is one we can
actually run, so the scoring is layered from cheapest to most expensive
signal:

  1. json          -- did it obey the response contract at all
  2. parses        -- offline, against metacat's own Lark grammar
  3. semantics     -- does it mean what was asked (per-case assertions)
  4. grounded      -- no invented namespace / metadata key / filter, where
                      the filter whitelist was derived from the live server
                      once, so "every_nth isn't installed" is caught offline
  5. executes      -- opt-in (--execute): the live server accepts it. Off by
                      default; metacat response time dominates the loop
  6. adherence     -- obeys prompt rules a valid query can still break
                      (e.g. the pathologically slow dataset-wildcard shape)
  7. repair        -- when it does emit a syntax error, can it fix it when
                      handed the parser message
  8. latency       -- p50 / p95 wall clock, and token usage when reported
  9. determinism   -- same prompt, temperature 0, three times: stable?
 10. api_compat    -- does it accept the request params we send

A model can score 100% on `parses` and still be useless (right syntax,
wrong meaning), or perfect on semantics and unusable (60s latency), which
is why none of these collapses into a single number.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import requests
from dotenv import load_dotenv

from dunecat import llm, mql_lint

SERVER_TIMEOUT_S = 25.0
REQUEST_TIMEOUT_S = 180.0
DETERMINISM_REPEATS = 3


@dataclass
class Case:
    """One English request plus what a correct answer must look like."""

    tier: str
    ask: str
    # every string must appear in the mql (case-sensitive)
    required: list[str] = field(default_factory=list)
    # at least one member of each group must appear (alternative phrasings)
    required_any: list[list[str]] = field(default_factory=list)
    # none of these may appear — invented keys, unavailable filters, ...
    forbidden: list[str] = field(default_factory=list)
    # True when the only correct answer is a refusal (empty mql)
    expect_empty: bool = False
    # skip the server round-trip (placeholder datasets that don't exist)
    execute: bool = True
    note: str = ""


HD = "hd-protodune"
DSA = f"{HD}:dsA"
DSB = f"{HD}:dsB"

CASES: list[Case] = [
    # -- tier 1: name a dataset, no filtering -------------------------------
    Case("1-trivial", "all files in the dataset np04_reco_v1 in hd-protodune-det-reco",
         required=["hd-protodune-det-reco:np04_reco_v1"], execute=False),
    Case("1-trivial", "show me 3 files from run 27361",
         required=["27361", "limit 3"]),

    # -- tier 2: metadata filters + vocabulary ------------------------------
    Case("2-filters", "raw files from run 27731 in ProtoDUNE horizontal drift",
         required=["27731", "'raw'"], required_any=[[f"namespace = '{HD}'", f"{HD}:"]]),
    Case("2-filters", "fully reconstructed data for runs 27731 and 27732",
         required=["full-reconstructed", "27731", "27732"]),
    Case("2-filters", "reco files with more than 1000 events",
         required=["full-reconstructed"], required_any=[["core.events > 1000", "core.events>1000"]]),
    Case("2-filters", f"files from {DSA} with run numbers between 27000 and 28000",
         required_any=[["27000:28000", "core.runs >= 27000"]], execute=False),

    # -- tier 3: dates, presence, ordering ----------------------------------
    Case("3-dates", "the first 10 raw files in iceberg taken after April 2024",
         required=["iceberg", "datetime", "limit 10"]),
    Case("3-dates", f"files from {DSA} with no output status recorded",
         required=["not present"], execute=False),

    # -- tier 4: detector -> namespace grounding ----------------------------
    Case("4-grounding", "raw files from Iceberg",
         required=["iceberg", "'raw'"]),
    Case("4-grounding", "files from ProtoDUNE vertical drift",
         required_any=[["vd-protodune", "vd-protodune-det-reco", "vd-protodune-top",
                        "dc25-vd-protodune"]]),
    Case("4-grounding", "raw files from the NP04 beam instrumentation",
         required=["ehn1-beam-np04"]),

    # -- tier 5: set operations and provenance ------------------------------
    Case("5-advanced", f"files in {DSA} but not in {DSB}",
         required=["-"], execute=False),
    Case("5-advanced", f"files from either {DSA} or {DSB}",
         required=["union"], execute=False),
    Case("5-advanced", f"files present in both {DSA} and {DSB}",
         required=["join"], execute=False),
    Case("5-advanced", "the parent files of the reco files in hd-protodune-det-reco:np04_reco_v1",
         required=["parents"], execute=False),
    Case("5-advanced", f"children of the raw files from {DSA}",
         required=["children"], execute=False),

    # -- tier 6: external filters (server-verified availability) ------------
    Case("6-filters", f"a 10 percent sample of raw files from {DSA}",
         required=["sample"], execute=False),
    Case("6-filters", f"every 3rd file from {DSA}",
         forbidden=["every_nth"], execute=False,
         note="every_nth is documented upstream but NOT installed on the DUNE "
              "server; hash(3,i) is the available equivalent"),

    # -- tier 7: traps ------------------------------------------------------
    Case("7-traps", "files from run 27731 where the beam energy is 7 GeV",
         required=["27731"], forbidden=["beam", "energy"],
         note="no metadata key for beam energy — must omit it, not invent one"),
    Case("7-traps", "raw files from the SBND detector",
         expect_empty=True, forbidden=["sbnd", "SBND"], execute=False,
         note="SBND is not a DUNE detector and has no namespace here, so a "
              "refusal is the correct answer — inventing namespace='sbnd' "
              "yields a query that runs and silently matches nothing"),
    Case("7-traps", "everything from ProtoDUNE horizontal drift",
         forbidden=["datasets matching"],
         note="the dataset-wildcard shape expands to 22k datasets and times "
              "out; namespace = '...' is the fast equivalent"),
    Case("7-traps", "give me files from run 27731 in hd-protodune, dataset name unknown",
         forbidden=["<", ">"],
         note="angle-bracket placeholder is a syntax error metacat rejects"),

    # -- tier 8: refusal calibration ----------------------------------------
    Case("8-refusal", "what is the weather in Chicago tomorrow",
         expect_empty=True, execute=False),
    Case("8-refusal", "delete all files from run 27731",
         expect_empty=True, execute=False,
         note="MQL is read-only; must refuse rather than invent DDL"),

    # -- tier 9: instruction adherence under pressure -----------------------
    Case("9-adherence", "list files from run 27731 and explain your reasoning "
                        "step by step in detail before answering",
         required=["27731"],
         note="must still return only the JSON object"),
]

DETERMINISM_CASES = [
    "raw files from run 27731 in ProtoDUNE horizontal drift",
    f"files in {DSA} but not in {DSB}",
    "reco files with more than 1000 events",
]


# --- endpoint plumbing ------------------------------------------------------


def list_models() -> list[str]:
    r = requests.get(f"{llm._base_url()}/models", headers=llm._headers(), timeout=30)
    r.raise_for_status()
    return sorted(m["id"] for m in r.json().get("data", []))


def chat(model: str, messages: list[dict], *, reasoning: bool) -> dict:
    """One raw request. Returns {ok, content, latency_s, usage, error}."""
    body: dict = {
        "model": model,
        "temperature": 0.0,
        "response_format": {"type": "json_object"},
        "messages": messages,
    }
    if reasoning:
        body["reasoning_effort"] = "none"
    t0 = time.monotonic()
    try:
        r = requests.post(
            f"{llm._base_url()}/chat/completions",
            json=body, headers=llm._headers(), timeout=REQUEST_TIMEOUT_S,
        )
    except requests.RequestException as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}",
                "latency_s": time.monotonic() - t0}
    dt = time.monotonic() - t0
    if r.status_code >= 400:
        return {"ok": False, "error": f"HTTP {r.status_code}: {r.text[:200]}",
                "latency_s": dt}
    payload = r.json()
    return {
        "ok": True,
        "content": payload["choices"][0]["message"]["content"],
        "usage": payload.get("usage") or {},
        "latency_s": dt,
    }


def probe_api_compat(model: str) -> dict:
    """Which request params does this model accept? A gateway that rejects
    one with HTTP 400 makes the whole request fail, so this is a hard
    compatibility fact, not a nicety."""
    ping = [{"role": "user", "content": 'Reply with {"ok": 1}'}]
    with_re = chat(model, ping, reasoning=True)
    without = chat(model, ping, reasoning=False)
    return {
        "accepts_reasoning_effort": with_re["ok"],
        "reasoning_effort_error": None if with_re["ok"] else with_re.get("error"),
        "accepts_json_object": without["ok"],
        "baseline_error": None if without["ok"] else without.get("error"),
    }


# --- server execution check (deduped across models) ------------------------

_exec_cache: dict[str, dict] = {}


def executes(mql: str, client) -> dict:
    """Does metacat accept this query? `(mql) limit 1` is safe for every
    shape and lets the server stop early."""
    if mql in _exec_cache:
        return _exec_cache[mql]
    from concurrent.futures import ThreadPoolExecutor, TimeoutError as FTimeout

    def _run() -> tuple[bool, str | None, bool]:
        try:
            for _ in client.query(f"({mql}) limit 1"):
                return (True, None, True)
            return (True, None, False)
        except Exception as e:
            return (False, str(e).replace("\n", " ")[:160], False)

    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(_run)
        try:
            ok, err, matched = fut.result(timeout=SERVER_TIMEOUT_S)
            out = {"ok": ok, "error": err, "matched": matched,
                   "seconds": round(time.monotonic() - t0, 2)}
        except FTimeout:
            out = {"ok": False, "error": f"timeout >{SERVER_TIMEOUT_S:.0f}s",
                   "matched": False, "timed_out": True,
                   "seconds": SERVER_TIMEOUT_S}
    _exec_cache[mql] = out
    return out


# --- scoring ----------------------------------------------------------------


def score(case: Case, mql: str, warnings: list[str]) -> dict:
    """Everything checkable without the server."""
    parse_err = mql_lint.syntax_error(mql) if mql else None
    refused = mql == ""
    if case.expect_empty:
        # Still check grounding: a model that answers an unanswerable ask by
        # inventing a namespace is worse than one that just declines.
        invented = [s for s in case.forbidden if s in mql]
        return {
            "parses": None if refused else parse_err is None,
            "semantics": refused,
            "grounded": not invented and not warnings,
            "invented": invented,
            "warnings": warnings,
            "adherence": True,
            "refusal_correct": refused,
        }
    missing = [s for s in case.required if s not in mql]
    for group in case.required_any:
        if not any(s in mql for s in group):
            missing.append(" | ".join(group))
    present_forbidden = [s for s in case.forbidden if s in mql]
    return {
        "parses": (parse_err is None) if mql else False,
        "parse_error": parse_err,
        "semantics": not missing and not refused,
        "missing": missing,
        # A namespace/filter warning from the linter *is* a hallucination.
        "grounded": not present_forbidden and not warnings,
        "invented": present_forbidden,
        "warnings": warnings,
        "adherence": "datasets matching" not in mql and "<" not in mql,
        "over_refused": refused,
    }


def run_model(model: str, client, *, execute: bool) -> dict:
    system = llm._build_advanced_prompt()
    compat = probe_api_compat(model)
    if not compat["accepts_json_object"]:
        return {"model": model, "api_compat": compat, "unusable": True,
                "cases": [], "determinism": None}
    reasoning = compat["accepts_reasoning_effort"]

    rows, latencies, prompt_tokens, completion_tokens = [], [], [], []
    for case in CASES:
        messages = [{"role": "system", "content": system},
                    {"role": "user", "content": case.ask}]
        resp = chat(model, messages, reasoning=reasoning)
        if not resp["ok"]:
            rows.append({"tier": case.tier, "ask": case.ask, "transport_error": resp["error"]})
            continue
        latencies.append(resp["latency_s"])
        usage = resp.get("usage") or {}
        if usage.get("prompt_tokens"):
            prompt_tokens.append(usage["prompt_tokens"])
        if usage.get("completion_tokens"):
            completion_tokens.append(usage["completion_tokens"])

        parsed = llm._parse_json_object(resp["content"])
        if parsed is None:
            rows.append({"tier": case.tier, "ask": case.ask, "json": False,
                         "raw": resp["content"][:200], "latency_s": resp["latency_s"]})
            continue
        mql = (parsed.get("mql") or "").strip()
        warnings = mql_lint.lint(mql, llm.known_namespaces())["warnings"] if mql else []
        sc = score(case, mql, warnings)

        # repair round, exactly as production does it
        repaired = None
        if mql and sc.get("parse_error"):
            messages += [
                {"role": "assistant", "content": json.dumps(parsed)},
                {"role": "user", "content":
                 f"That query does not parse: {sc['parse_error']}\n"
                 f"Fix it and reply in the same JSON format."},
            ]
            again = chat(model, messages, reasoning=reasoning)
            if again["ok"]:
                p2 = llm._parse_json_object(again["content"]) or {}
                m2 = (p2.get("mql") or "").strip()
                repaired = {"mql": m2, "parses": bool(m2) and mql_lint.syntax_error(m2) is None}

        row = {"tier": case.tier, "ask": case.ask, "json": True, "mql": mql,
               "notes": (parsed.get("notes") or "")[:300],
               "latency_s": round(resp["latency_s"], 2), "repaired": repaired, **sc}
        effective = repaired["mql"] if (repaired and repaired["parses"]) else mql
        if execute and case.execute and effective and sc["parses"] is not False:
            row["execution"] = executes(effective, client)
        rows.append(row)
        print(f"    {case.tier:12s} {'ok ' if sc.get('semantics') else 'MISS'} "
              f"{resp['latency_s']:5.1f}s  {case.ask[:44]:44s} -> {mql[:60]}", flush=True)

    # determinism: identical request N times at temperature 0
    det = []
    for ask in DETERMINISM_CASES:
        outs = set()
        for _ in range(DETERMINISM_REPEATS):
            r = chat(model, [{"role": "system", "content": system},
                             {"role": "user", "content": ask}], reasoning=reasoning)
            if r["ok"]:
                p = llm._parse_json_object(r["content"]) or {}
                outs.add((p.get("mql") or "").strip())
        det.append({"ask": ask, "distinct": len(outs)})

    return {
        "model": model,
        "api_compat": compat,
        "cases": rows,
        "determinism": det,
        "latency": {
            "p50": round(statistics.median(latencies), 2) if latencies else None,
            "p95": round(sorted(latencies)[int(len(latencies) * 0.95) - 1], 2)
            if len(latencies) >= 2 else None,
            "max": round(max(latencies), 2) if latencies else None,
        },
        "tokens": {
            "prompt_median": statistics.median(prompt_tokens) if prompt_tokens else None,
            "completion_median": statistics.median(completion_tokens) if completion_tokens else None,
        },
    }


def summarise(result: dict) -> dict:
    rows = [r for r in result["cases"] if not r.get("transport_error")]
    n = len(rows) or 1
    ex = [r for r in rows if "execution" in r]
    return {
        "model": result["model"],
        "n": len(rows),
        "json": sum(1 for r in rows if r.get("json")),
        "parses": sum(1 for r in rows if r.get("parses") in (True, None)),
        "semantics": sum(1 for r in rows if r.get("semantics")),
        "grounded": sum(1 for r in rows if r.get("grounded")),
        "adherence": sum(1 for r in rows if r.get("adherence")),
        "executes": f"{sum(1 for r in ex if r['execution']['ok'])}/{len(ex)}" if ex else "n/a",
        "repairs_ok": sum(1 for r in rows if r.get("repaired", {}) and r["repaired"]["parses"])
        if any(r.get("repaired") for r in rows) else 0,
        "repairs_attempted": sum(1 for r in rows if r.get("repaired")),
        "overall_pct": round(
            100 * sum(1 for r in rows
                      if r.get("json") and r.get("parses") in (True, None)
                      and r.get("semantics") and r.get("grounded") and r.get("adherence")) / n
        ),
        "latency_p50": result["latency"]["p50"],
        "latency_p95": result["latency"]["p95"],
        "deterministic": all(d["distinct"] == 1 for d in (result["determinism"] or [])),
        "accepts_reasoning_effort": result["api_compat"]["accepts_reasoning_effort"],
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("models", nargs="*", help="default: every model the endpoint lists")
    ap.add_argument("--out", default="llm-bench-results.json")
    ap.add_argument("--execute", action="store_true",
                    help="also check each query against the live metacat "
                         "server. Off by default: a broad catalog query can "
                         "run past the 25s cap, so this dominates the runtime "
                         "and loads a shared production service. The offline "
                         "filter/namespace whitelists already encode what the "
                         "server accepts.")
    args = ap.parse_args()

    load_dotenv()
    if not llm.is_enabled():
        sys.exit("DUNECAT_LLM_BASE_URL is not set")

    client = None
    if args.execute:
        from dunecat.client import get_client
        from dunecat.web import auth
        auth.prime()
        auth.ensure_fresh_metacat_session()
        client = get_client()

    models = args.models or list_models()
    print(f"endpoint: {llm._base_url()}")
    print(f"models:   {', '.join(models)}")
    print(f"cases:    {len(CASES)} across {len(sorted({c.tier for c in CASES}))} tiers\n")

    results = []
    for model in models:
        print(f"  === {model}")
        results.append(run_model(model, client, execute=args.execute))
        print()

    summaries = [summarise(r) for r in results]
    Path(args.out).write_text(json.dumps(
        {"endpoint": llm._base_url(), "cases": len(CASES),
         "results": results, "summary": summaries}, indent=2))

    print(f"{'model':30s} {'overall':>8s} {'sem':>5s} {'grnd':>5s} {'adh':>5s} "
          f"{'exec':>7s} {'p50':>6s} {'p95':>6s} {'det':>4s}")
    for s in summaries:
        print(f"{s['model']:30s} {s['overall_pct']:7d}% {s['semantics']:3d}/{s['n']:<2d}"
              f"{s['grounded']:4d} {s['adherence']:5d} {s['executes']:>8s} "
              f"{str(s['latency_p50']):>6s} {str(s['latency_p95']):>6s} "
              f"{'y' if s['deterministic'] else 'n':>4s}")
    print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()
