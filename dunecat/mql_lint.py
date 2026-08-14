"""Offline MQL checks, used to vet generated queries before they cost a
metacat round-trip.

Layer 1 is metacat's *own* Lark grammar, which ships inside the installed
``metacat`` package — so it cannot drift from the parser the server runs.
Layers 2-3 cover what a grammar can't: a syntactically perfect query can
still name a namespace that doesn't exist (the server answers "0 files",
not an error) or a filter the server hasn't registered.

None of this replaces asking the server. Measured against production, the
local parse agreed with metacat on 17/20 queries and every disagreement
was local-accepts / server-rejects. Treat this as a cheap prefilter.
"""

from __future__ import annotations

import re
import sys
import types
from functools import cache
from pathlib import Path

from lark import Lark, LarkError

# External filters the DUNE production server actually has registered,
# verified by running each one against metacat.fnal.gov. This is NOT the
# list in the MQL reference: `every_nth` is documented as a standard
# MetaCat filter but production rejects it with
# ``MQLExecutionError: 'every_nth'``. Re-verify before adding a name here.
VERIFIED_FILTERS = frozenset({"sample", "hash"})

# `files from ns:name`, `datasets matching ns:*`, ... — the namespace is the
# token to the left of a colon, and must start with a letter so numeric
# ranges (`core.runs in 27000:28000`) don't match.
_NAMESPACE_RE = re.compile(r"\b([A-Za-z][\w.-]*)\s*:")
# `namespace = 'ns'` — the file-attribute form, which is how a detector-wide
# query is written (the `datasets matching ns:*` alternative expands to every
# dataset in the namespace and times out on large ones).
_NAMESPACE_ATTR_RE = re.compile(r"\bnamespace\s*=\s*['\"]([^'\"]+)['\"]")
_FILTER_RE = re.compile(r"\bfilter\s+([A-Za-z_]\w*)\s*\(")


@cache
def _parser() -> Lark:
    """metacat's grammar, with ``top_file_query`` as the start rule.

    NOT ``query``: that also admits ``datasets ...`` and ``files selected
    by ...``, which the /query endpoint we call rejects outright. Starting
    at ``top_file_query`` makes the local verdict match the endpoint's.
    """
    import metacat

    # metacat.mql/__init__ pulls in the server-side DB stack (wsdbtools),
    # which the client install doesn't ship. The grammar subpackage is pure
    # strings, so stub the parent package to import it on its own. Guarded
    # so we never clobber a real metacat.mql if one is importable.
    if "metacat.mql" not in sys.modules:
        stub = types.ModuleType("metacat.mql")
        stub.__path__ = [str(Path(metacat.__path__[0]) / "mql")]
        sys.modules["metacat.mql"] = stub
    from metacat.mql.grammar import MQL_Grammar

    return Lark(MQL_Grammar, start="top_file_query")


def grammar() -> str:
    """The raw grammar text, for grounding the model's prompt."""
    import metacat  # noqa: F401  (import for the side effect above)

    _parser()  # ensure the stub is installed
    from metacat.mql.grammar import MQL_Grammar

    return MQL_Grammar


def syntax_error(mql: str) -> str | None:
    """First line of the parse error, or None when the query parses."""
    try:
        _parser().parse(mql)
    except LarkError as e:
        return str(e).split("\n")[0].strip()
    return None


# Set operations, provenance and filters — the "compound" constructs.
_COMPOUND_RULES = frozenset(
    {"minus", "union", "join", "parents_of", "children_of", "filter", "named_query"}
)


def breaks_when_wrapped(mql: str) -> bool:
    """True when wrapping this query in ``(...) ordered skip N`` would make
    metacat 4.1.4 fail.

    The trigger, isolated against production, is a ``limit`` applied to a
    *compound* query with ``ordered`` or ``skip`` layered on top:

        (A - B limit 3) ordered skip 0        -> MQLCompilationError
        (A - B limit 3) ordered skip 0 limit 100 -> MQLSyntaxError
        A - B limit 3                         -> fine
        (A - B) ordered skip 0 limit 100      -> fine  (no inner limit)
        (union(A limit 5, B)) ordered skip 0 limit 10 -> fine  (limit is
                                                 inside a branch, not on
                                                 the compound result)

    So the test is: does any ``limit`` node have a compound construct
    beneath it? Those queries have to be sent verbatim and paged in
    Python. Everything else can keep the server-side skip/limit pushdown.

    Unparseable input returns False — let the server be the one to
    complain about it rather than silently changing how we page.
    """
    try:
        tree = _parser().parse(mql)
    except LarkError:
        return False
    for node in tree.iter_subtrees():
        if node.data != "limit":
            continue
        if any(d.data in _COMPOUND_RULES for d in node.iter_subtrees()):
            return True
    return False


def unknown_namespaces(mql: str, known: set[str]) -> list[str]:
    """Namespace-looking tokens that aren't in ``known``.

    Regex-based, so it can misfire on a colon inside a quoted string —
    which is why callers surface these as warnings, not hard failures.
    """
    found = {m.group(1) for m in _NAMESPACE_RE.finditer(mql)}
    found |= {m.group(1) for m in _NAMESPACE_ATTR_RE.finditer(mql)}
    return sorted(n for n in found if n not in known)


def unknown_filters(mql: str) -> list[str]:
    """`filter <name>(...)` names the server hasn't registered."""
    found = {m.group(1) for m in _FILTER_RE.finditer(mql)}
    return sorted(f for f in found if f not in VERIFIED_FILTERS)


def lint(mql: str, known_namespaces: set[str] | None = None) -> dict:
    """Run every offline layer. Returns ``{error, warnings}`` where
    ``error`` is a hard syntax failure (None when clean) and ``warnings``
    are things that parse but will not behave as the user expects."""
    mql = mql.strip()
    if not mql:
        return {"error": "MQL is empty", "warnings": []}
    err = syntax_error(mql)
    if err:
        return {"error": err, "warnings": []}
    warnings = []
    for name in unknown_filters(mql):
        warnings.append(
            f"filter {name!r} is not registered on the metacat server "
            f"(available: {', '.join(sorted(VERIFIED_FILTERS))})"
        )
    if known_namespaces is not None:
        for ns in unknown_namespaces(mql, known_namespaces):
            warnings.append(
                f"namespace {ns!r} is not a known DUNE namespace — the query "
                f"will run but match nothing"
            )
    return {"error": None, "warnings": warnings}
