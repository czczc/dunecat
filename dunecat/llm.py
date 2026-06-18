"""English -> MQL translation via a local, OpenAI-compatible LLM.

The ``/api/query/from-english`` routes are thin wrappers over
:func:`generate_mql`. We target an OpenAI-compatible chat endpoint
(Ollama, vLLM, llama.cpp, ...) so "local" is just a base-URL config —
swap ``DUNECAT_LLM_BASE_URL`` to move from a dev Ollama to a hub-hosted
model without touching this code.

Enablement is opt-in: the feature is OFF unless ``DUNECAT_LLM_BASE_URL``
is set, so a plain hub stays dark until an operator points it at a model.

Grounding follows the "start simple" decision: the namespace list is
built live from ``detectors.yaml`` (a local read, always current), while
the metadata keys and the slang->value vocabulary are hand-curated here.
Injecting live facet values for tier/file_type is a deliberate follow-up.

This module deliberately keeps error handling thin (happy path); the
full failure taxonomy + fence-strip fallback land in a separate slice.
"""

from __future__ import annotations

import json
import logging
import os
import re

import requests

from dunecat.web.detectors import load_detectors

log = logging.getLogger("uvicorn.error")

DEFAULT_MODEL = "qwen3.5:4b"
DEFAULT_TIMEOUT_S = 45.0


class LLMError(Exception):
    """Base for English-to-MQL failures the route maps to HTTP codes."""


class LLMUnreachable(LLMError):
    """The model endpoint refused the connection / DNS failed."""


class LLMTimeout(LLMError):
    """Generation took longer than DUNECAT_LLM_TIMEOUT."""


class LLMModelNotFound(LLMError):
    """The configured model isn't available on the server."""

    def __init__(self, model: str) -> None:
        super().__init__(f"model {model!r} not found")
        self.model = model


class LLMBadResponse(LLMError):
    """The model returned something we couldn't parse into {mql, notes}."""


def is_enabled() -> bool:
    """The feature is on iff an endpoint is configured."""
    return bool(os.environ.get("DUNECAT_LLM_BASE_URL"))


def _base_url() -> str:
    url = os.environ.get("DUNECAT_LLM_BASE_URL")
    if not url:
        raise RuntimeError("DUNECAT_LLM_BASE_URL is not set")
    return url.rstrip("/")


def _model() -> str:
    return os.environ.get("DUNECAT_LLM_MODEL") or DEFAULT_MODEL


def _timeout() -> float:
    raw = os.environ.get("DUNECAT_LLM_TIMEOUT")
    return float(raw) if raw else DEFAULT_TIMEOUT_S


# --- grounding -------------------------------------------------------------

# Curated metadata keys (hand-maintained). Values shown are real observed
# examples, not an exhaustive enum.
_METADATA_KEYS = """\
core.runs         integer (or list of integers)   run number(s),         e.g. core.runs in (27731, 27732)
core.data_tier    string                          processing stage,      e.g. 'raw', 'full-reconstructed'
core.file_type    string                          file format/category
core.events       integer                         event count,           e.g. core.events > 1000
core.start_time   unix timestamp (use datetime()) start of data taking
core.end_time     unix timestamp (use datetime()) end of data taking
dune.output_status string                         e.g. 'confirmed', 'rejected'"""


def _namespaces_block() -> str:
    """Detector -> namespaces, built live from detectors.yaml."""
    lines = []
    for det in load_detectors():
        lines.append(f"{det['name']}: {', '.join(det['namespaces'])}")
    return "\n".join(lines)


def _build_system_prompt() -> str:
    return f"""\
You translate a physicist's plain-English request into a MetaCat Query \
Language (MQL) query for the DUNE data catalog.

Respond with ONLY a JSON object, no prose around it:
  {{"mql": "<the query, or empty string>", "notes": "<short explanation>"}}

HARD RULES (do not break these):
- NEVER invent a namespace. Use only namespaces from the list below.
- NEVER invent a metadata key. Use only keys from the list below. If the
  request needs a concept with no matching key, do NOT substitute a different
  key -- leave it out and say so in notes.
- A dataset/detector is NOT required. If the user names neither, write a
  catalog-wide query: "files where <conditions>". A missing dataset or
  detector is NEVER a reason to return empty mql.
- If the user names a detector but no dataset, use that detector's namespace
  with the <dataset-name> placeholder. If the user gives an explicit dataset
  name, use it verbatim (no brackets). Never guess dataset names.
- ONLY return an EMPTY mql string when the request needs ADVANCED MQL not in
  the supported subset below (set operations like union/join/subtraction,
  parent/child provenance, sampling filters, regex dataset matching). In that
  case explain in notes and point to the full reference.

SUPPORTED MQL SUBSET (only generate these forms):
  files from <namespace>:<dataset-name>            all files in one dataset
  files from <namespace>:<dataset-name> where ...  filtered within a dataset
  files where ...                                   across the whole catalog
  Conditions:
    key = 'string'                 string equality (single quotes)
    key = 42                       numeric equality
    key in (a, b, c)               list membership / OR
    key > n , key < n , >=, <=     numeric comparison & ranges
    key in lo:hi                   inclusive range
    and / or / not                 boolean operators
    core.start_time > datetime("2024-04-01")   dates via datetime("YYYY-MM-DD")
  Tail clauses:
    ordered                        deterministic order
    limit N                        cap result count
    skip N                         offset

NAMESPACES (grouped by detector; pick the namespace, not the detector name):
{_namespaces_block()}

METADATA KEYS (only these):
{_METADATA_KEYS}

VOCABULARY (map the physicist's shorthand to the canonical value):
- "raw" -> core.data_tier = 'raw'
- "reco", "reconstructed", "fully reconstructed", "full reco"
      -> core.data_tier = 'full-reconstructed'
If a term clearly refers to a data tier but you are unsure of the exact
canonical value, use your best guess and flag the assumption in notes.

EXAMPLES (each shows a different construct):

User: raw files from run 27731 in ProtoDUNE horizontal drift
{{"mql": "files from hd-protodune:<dataset-name> where core.runs in (27731) and core.data_tier = 'raw'", "notes": "Used the hd-protodune namespace. Left the dataset as a placeholder -- pick the dataset you want."}}

User: fully reconstructed data for runs 27731 and 27732
{{"mql": "files where core.runs in (27731, 27732) and core.data_tier = 'full-reconstructed'", "notes": "No detector specified, so this searches the whole catalog. Add 'files from <ns>:<dataset>' to narrow it."}}

User: show me 3 files from run 27361
{{"mql": "files where core.runs in (27361) limit 3", "notes": "No dataset or detector given, so this is a catalog-wide search. limit 3 caps the result."}}

User: all files in the dataset np04_reco_v1 in hd-protodune-det-reco
{{"mql": "files from hd-protodune-det-reco:np04_reco_v1", "notes": "Used the dataset name you gave verbatim."}}

User: the first 10 raw files in iceberg, taken after April 2024
{{"mql": "files from iceberg:<dataset-name> where core.data_tier = 'raw' and core.start_time > datetime(\\"2024-04-01\\") ordered limit 10", "notes": "Date via datetime(); ordered+limit for a deterministic first 10."}}

User: files that are in dataset A but not dataset B
{{"mql": "", "notes": "This needs set subtraction, which is advanced MQL outside this tool's supported subset. See https://fermitools.github.io/metacat/mql.html"}}
"""


def _parse_json_object(content: str) -> dict | None:
    """Parse the model's reply into a JSON object, tolerating a model
    that wraps it in ```json fences or stray prose. Returns None if no
    JSON object can be recovered."""
    try:
        obj = json.loads(content)
        return obj if isinstance(obj, dict) else None
    except json.JSONDecodeError:
        pass
    # Fallback: grab the first {...} span (handles fences / leading prose).
    match = re.search(r"\{.*\}", content, re.DOTALL)
    if not match:
        return None
    try:
        obj = json.loads(match.group(0))
        return obj if isinstance(obj, dict) else None
    except json.JSONDecodeError:
        return None


def generate_mql(english: str) -> dict[str, str]:
    """Translate one English request into {mql, notes}.

    Raises a specific :class:`LLMError` subclass on failure; the caller
    maps each to an HTTP status code.
    """
    try:
        resp = requests.post(
            f"{_base_url()}/chat/completions",
            json={
                "model": _model(),
                # Greedy decoding: this is faithful translation, not creative
                # generation. At 0.1 the model intermittently (~50% on some
                # borderline queries) talked itself into wrongly refusing a
                # valid catalog-wide query; 0.0 makes it deterministic.
                "temperature": 0.0,
                # qwen3.5 is a hybrid reasoning model; without this it spends
                # 10-50s emitting reasoning tokens before the JSON. Must be a
                # request param -- the "/no_think" prompt switch is ignored.
                "reasoning_effort": "none",
                "response_format": {"type": "json_object"},
                "messages": [
                    {"role": "system", "content": _build_system_prompt()},
                    {"role": "user", "content": english},
                ],
            },
            timeout=_timeout(),
        )
    except requests.Timeout as e:
        raise LLMTimeout(str(e)) from e
    except requests.ConnectionError as e:
        raise LLMUnreachable(str(e)) from e
    except requests.RequestException as e:
        raise LLMError(str(e)) from e

    # A missing model comes back as 404 on the OpenAI-compatible endpoint.
    if resp.status_code == 404:
        raise LLMModelNotFound(_model())
    if resp.status_code >= 400:
        raise LLMError(f"model endpoint returned HTTP {resp.status_code}")

    content = resp.json()["choices"][0]["message"]["content"]
    parsed = _parse_json_object(content)
    if parsed is None:
        log.warning("llm: unparseable response: %r", content[:500])
        raise LLMBadResponse("could not parse model response")
    return {
        "mql": (parsed.get("mql") or "").strip(),
        "notes": (parsed.get("notes") or "").strip(),
    }
