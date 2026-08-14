"""English -> MQL translation via an OpenAI-compatible LLM.

The ``/api/query/from-english`` routes are thin wrappers over
:func:`generate_mql`. We target an OpenAI-compatible chat endpoint
(Ollama, vLLM, a hosted LiteLLM gateway, ...) so the endpoint is just a
base-URL config — swap ``DUNECAT_LLM_BASE_URL`` to move from a dev Ollama
to BNL's inference service without touching this code.

Enablement is opt-in: the feature is OFF unless ``DUNECAT_LLM_BASE_URL``
is set, so a plain hub stays dark until an operator points it at a model.

Two prompt modes:

* **advanced** (default) grounds the model in metacat's *own* Lark grammar
  — the whole language, including set operations, provenance and filters.
  Generated queries are checked with :mod:`dunecat.mql_lint` and one
  repair round is attempted before giving up.
* **subset** (``DUNECAT_LLM_ADVANCED=0``) is the original hand-curated
  grammar-free prompt, which refuses advanced constructs. Kept as an
  escape hatch for small local models that drown in the full grammar.

Grounding: the namespace list is built live from ``detectors.yaml`` (a
local read, always current), while the metadata keys and the
slang->value vocabulary are hand-curated here. Injecting live facet
values for tier/file_type is a deliberate follow-up.
"""

from __future__ import annotations

import json
import logging
import os
import re

import requests

from dunecat import mql_lint
from dunecat.web.detectors import load_detectors

log = logging.getLogger("uvicorn.error")

DEFAULT_MODEL = "qwen3.5:4b"
DEFAULT_TIMEOUT_S = 45.0
DEFAULT_REASONING_EFFORT = "none"


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


def _headers() -> dict[str, str]:
    """Bearer auth for hosted gateways (BNL's LiteLLM proxy, OpenAI, ...).
    Local Ollama/vLLM need no key, so the header is omitted when unset."""
    key = os.environ.get("DUNECAT_LLM_API_KEY")
    return {"Authorization": f"Bearer {key}"} if key else {}


def _advanced() -> bool:
    """Full-grammar prompt + lint/repair. On by default; set
    DUNECAT_LLM_ADVANCED=0 to fall back to the curated-subset prompt."""
    return os.environ.get("DUNECAT_LLM_ADVANCED", "1").strip() not in ("0", "false")


def _reasoning_effort() -> str | None:
    """qwen3.5 is a hybrid reasoning model; without "none" it spends 10-50s
    emitting reasoning tokens before the JSON, and the "/no_think" prompt
    switch is ignored -- so it must be a request param. But LiteLLM-fronted
    models that don't support the param reject the whole request with HTTP
    400, so an empty DUNECAT_LLM_REASONING_EFFORT omits the field."""
    raw = os.environ.get("DUNECAT_LLM_REASONING_EFFORT", DEFAULT_REASONING_EFFORT)
    return raw.strip() or None


# --- grounding -------------------------------------------------------------

# Curated metadata keys (hand-maintained). Values shown are real observed
# examples, not an exhaustive enum.
_METADATA_KEYS = """\
namespace         string (file attribute)         the detector's namespace,
                                                  e.g. namespace = 'hd-protodune'
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


def known_namespaces() -> set[str]:
    """Every namespace in detectors.yaml, for the lint whitelist."""
    return {ns for det in load_detectors() for ns in det["namespaces"]}


def _build_system_prompt() -> str:
    return f"""\
You translate a physicist's plain-English request into a MetaCat Query \
Language (MQL) query for the DUNE data catalog.

Respond with ONLY a JSON object, no prose around it:
  {{"mql": "<the query, or empty string>", "notes": "<short explanation>"}}

DECISION PROCEDURE (follow in this exact order):
1. Does the request require an ADVANCED construct -- set operations
   (union/join/subtraction), parent/child provenance, sampling filters, or
   regex dataset matching? If YES: return an empty mql, explain in notes,
   and STOP. This is the ONLY case where mql is empty.
2. Otherwise you MUST produce a query. A missing dataset or detector is
   normal and fine -- it is NEVER a reason to return empty mql:
   - detector named, no dataset -> files where namespace = 'NAMESPACE' and ...
     (substitute the real namespace, e.g. namespace = 'hd-protodune')
   - neither named            -> files where ...        (catalog-wide)
   A run number with no detector (e.g. "3 reco files from run 27362") is a
   plain catalog-wide query: files where core.runs in (27362) ...

HARD RULES (do not break these):
- NEVER invent a namespace. Use only namespaces from the list below.
- NEVER invent a metadata key. Use only keys from the list below. If the
  request needs a concept with no matching key, do NOT substitute a different
  key -- leave it out and say so in notes.
- A dataset/detector is NOT required. If the user names neither, write a
  catalog-wide query: "files where <conditions>". A missing dataset or
  detector is NEVER a reason to return empty mql.
- NEVER write angle brackets in a query. An angle-bracket placeholder is a
  SYNTAX ERROR that metacat rejects. If the user names a detector but no
  dataset, filter on the namespace attribute instead, e.g.
  "files where namespace = 'hd-protodune' and ...". If the user gives an
  explicit dataset name, use it verbatim. Never guess dataset names.
- Do NOT write "files from datasets matching NAMESPACE:*" to mean "everything
  from this detector". It is correct MQL but pathologically slow -- it expands
  to every dataset in the namespace (22k+ for hd-protodune) and takes minutes
  or times out. Use "namespace = '...'" instead. Only use "datasets matching"
  when the user explicitly asks to match dataset NAMES by pattern.
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
{{"mql": "files where namespace = 'hd-protodune' and core.runs in (27731) and core.data_tier = 'raw'", "notes": "No dataset given, so this searches the whole hd-protodune namespace. Narrow it by naming a dataset."}}

User: fully reconstructed data for runs 27731 and 27732
{{"mql": "files where core.runs in (27731, 27732) and core.data_tier = 'full-reconstructed'", "notes": "No detector specified, so this searches the whole catalog. Add 'files from <ns>:<dataset>' to narrow it."}}

User: show me 3 files from run 27361
{{"mql": "files where core.runs in (27361) limit 3", "notes": "No dataset or detector given, so this is a catalog-wide search. limit 3 caps the result."}}

User: all files in the dataset np04_reco_v1 in hd-protodune-det-reco
{{"mql": "files from hd-protodune-det-reco:np04_reco_v1", "notes": "Used the dataset name you gave verbatim."}}

User: the first 10 raw files in iceberg, taken after April 2024
{{"mql": "files where namespace = 'iceberg' and core.data_tier = 'raw' and core.start_time > datetime(\\"2024-04-01\\") ordered limit 10", "notes": "Date via datetime(); ordered+limit for a deterministic first 10."}}

User: files that are in dataset A but not dataset B
{{"mql": "", "notes": "This needs set subtraction, which is advanced MQL outside this tool's supported subset. See https://fermitools.github.io/metacat/mql.html"}}
"""


# --- advanced prompt (full grammar) ----------------------------------------

# The grammar constrains syntax but names no filters -- `filter FNAME(...)`
# accepts any identifier -- so the registered filters have to be spelled out.
# This list is verified against the production server, not the MQL reference;
# see mql_lint.VERIFIED_FILTERS.
_FILTERS_BLOCK = """\
filter sample(f)(<query>)    f is a fraction 0..1; keeps ~that fraction
filter hash(n, i)(<query>)   keeps bucket i of n, by hash of the file name;
                             different i never overlap"""

_ADVANCED_EXAMPLES = """\
User: files in hd-protodune:dsA but not in hd-protodune:dsB
{"mql": "files from hd-protodune:dsA - files from hd-protodune:dsB", "notes": "Set subtraction with the - operator."}

User: files from either hd-protodune:dsA or hd-protodune:dsB
{"mql": "union(files from hd-protodune:dsA, files from hd-protodune:dsB)", "notes": "union() merges both file sets."}

User: files present in both hd-protodune:dsA and hd-protodune:dsB
{"mql": "join(files from hd-protodune:dsA, files from hd-protodune:dsB)", "notes": "join() is the intersection."}

User: the parent files of the reco files in hd-protodune-det-reco:np04_reco_v1
{"mql": "parents(files from hd-protodune-det-reco:np04_reco_v1)", "notes": "parents() walks provenance up one level; children() walks down."}

User: a 10 percent sample of raw files from hd-protodune:dsA
{"mql": "filter sample(0.1)(files from hd-protodune:dsA where core.data_tier = 'raw')", "notes": "sample(0.1) keeps roughly one file in ten."}

User: files from hd-protodune:dsA that have no output status recorded
{"mql": "files from hd-protodune:dsA where dune.output_status not present", "notes": "'not present' matches files missing the key entirely."}

User: files from hd-protodune:dsA with run numbers between 27000 and 28000
{"mql": "files from hd-protodune:dsA where core.runs in 27000:28000", "notes": "lo:hi is an inclusive range."}"""


def _build_advanced_prompt() -> str:
    """Ground the model in metacat's own grammar instead of a curated subset.

    The grammar comes from the installed metacat package, so it always
    matches the parser the server runs. It only constrains *syntax*, which
    is why the namespace/key/filter lists below still matter.
    """
    return f"""\
You translate a physicist's plain-English request into a MetaCat Query \
Language (MQL) query for the DUNE data catalog.

Respond with ONLY a JSON object, no prose around it:
  {{"mql": "<the query, or empty string>", "notes": "<short explanation>"}}

Every query you emit MUST parse against this grammar (Lark syntax). It is the
authoritative definition of MQL -- prefer the simplest form that answers the
request, and do not invent syntax that isn't here.

--- BEGIN MQL GRAMMAR ---
{mql_lint.grammar()}
--- END MQL GRAMMAR ---

The grammar accepts more than this server supports. Obey these too:

- Emit a FILE query ("files ..."). A top-level "datasets ..." query is NOT
  accepted by the endpoint we call, even though the grammar allows it. To
  filter by detector, use "files where namespace = 'NAMESPACE' and ...".
- NEVER write angle brackets in a query. An angle-bracket placeholder is a
  SYNTAX ERROR. When the user names a detector but no dataset, filter on the
  namespace attribute, e.g. "files where namespace = 'hd-protodune'".
- Do NOT write "files from datasets matching NAMESPACE:*" to mean "everything
  from this detector". Valid MQL, but it expands to every dataset in the
  namespace (22k+ for hd-protodune) and takes minutes or times out. Only use
  "datasets matching" when the user explicitly asks to match dataset NAMES.
- NEVER invent a namespace. Use only the namespaces listed below.
- NEVER invent a metadata key. Use only the keys listed below. If the request
  needs a concept with no matching key, leave it out and say so in notes.
- Never guess a dataset name. Use one only if the user gave it.
- Only these external filters exist on this server:
{_FILTERS_BLOCK}
  Do NOT use any other filter name (every_nth, for example, is documented
  upstream but is NOT installed here).
- Return an empty mql string only if the request genuinely cannot be
  expressed in MQL at all. Explain why in notes.

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

EXAMPLES:

User: raw files from run 27731 in ProtoDUNE horizontal drift
{{"mql": "files where namespace = 'hd-protodune' and core.runs in (27731) and core.data_tier = 'raw'", "notes": "No dataset given, so this searches the whole hd-protodune namespace."}}

User: show me 3 files from run 27361
{{"mql": "files where core.runs in (27361) limit 3", "notes": "Catalog-wide search; limit 3 caps the result."}}

{_ADVANCED_EXAMPLES}
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


def _chat(messages: list[dict[str, str]]) -> dict:
    """One round-trip to the chat endpoint, returning the parsed JSON object
    the model was asked to produce."""
    body = {
        "model": _model(),
        # Greedy decoding: this is faithful translation, not creative
        # generation. At 0.1 the model intermittently (~50% on some
        # borderline queries) talked itself into wrongly refusing a
        # valid catalog-wide query; 0.0 makes it deterministic.
        "temperature": 0.0,
        "response_format": {"type": "json_object"},
        "messages": messages,
    }
    if (effort := _reasoning_effort()) is not None:
        body["reasoning_effort"] = effort
    try:
        resp = requests.post(
            f"{_base_url()}/chat/completions",
            json=body,
            headers=_headers(),
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
        # Hosted gateways explain themselves in the body (bad key, param the
        # backing model doesn't support); log it or the 502 is undebuggable.
        log.warning(
            "llm: HTTP %s from %s: %s",
            resp.status_code, _base_url(), resp.text[:300],
        )
        raise LLMError(f"model endpoint returned HTTP {resp.status_code}")

    content = resp.json()["choices"][0]["message"]["content"]
    parsed = _parse_json_object(content)
    if parsed is None:
        log.warning("llm: unparseable response: %r", content[:500])
        raise LLMBadResponse("could not parse model response")
    return parsed


def _result_of(parsed: dict) -> dict:
    return {
        "mql": (parsed.get("mql") or "").strip(),
        "notes": (parsed.get("notes") or "").strip(),
    }


def generate_mql(english: str) -> dict:
    """Translate one English request into ``{mql, notes, warnings, parses}``.

    In advanced mode the generated query is linted offline (see
    :mod:`dunecat.mql_lint`) and one repair round is attempted if it fails
    to parse. A query that still won't parse is returned as-is with the
    parse error in ``warnings`` — the user gets to see and fix it, which
    beats swallowing the attempt.

    ``parses`` is the offline syntax verdict on whatever we ended up
    returning. The UI uses it to decide whether it can go straight on to
    running the query, so it's reported in both modes even though only
    advanced mode lints for warnings and repairs.

    Raises a specific :class:`LLMError` subclass on transport failures;
    the caller maps each to an HTTP status code.
    """
    system = _build_advanced_prompt() if _advanced() else _build_system_prompt()
    messages = [
        {"role": "system", "content": system},
        {"role": "user", "content": english},
    ]
    parsed = _chat(messages)
    result = _result_of(parsed)
    if not _advanced() or not result["mql"]:
        return {
            **result,
            "warnings": [],
            "parses": bool(result["mql"])
            and mql_lint.syntax_error(result["mql"]) is None,
        }

    checked = mql_lint.lint(result["mql"], known_namespaces())
    if checked["error"] is None:
        return {**result, "warnings": checked["warnings"], "parses": True}

    # One repair round: the parse error is precise and mechanical, exactly
    # the kind of feedback a model fixes on the first retry.
    log.info("llm: repairing unparseable MQL: %s", checked["error"])
    messages += [
        {"role": "assistant", "content": json.dumps(parsed)},
        {
            "role": "user",
            "content": (
                f"That query does not parse: {checked['error']}\n"
                f"Fix it and reply in the same JSON format."
            ),
        },
    ]
    repaired = _result_of(_chat(messages))
    if not repaired["mql"]:
        return {
            **result,
            "warnings": [f"does not parse: {checked['error']}"],
            "parses": False,
        }
    again = mql_lint.lint(repaired["mql"], known_namespaces())
    warnings = list(again["warnings"])
    if again["error"] is not None:
        warnings.insert(0, f"does not parse: {again['error']}")
    return {**repaired, "warnings": warnings, "parses": again["error"] is None}
