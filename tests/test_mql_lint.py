"""Offline MQL checks.

The expected verdicts here were all confirmed against the production
metacat server (v4.1.4) — see the module docstring in dunecat.mql_lint.
"""

from dunecat import mql_lint

A = "hd-protodune:dsA"
B = "hd-protodune:dsB"


class TestSyntaxError:
    def test_accepts_a_plain_query(self):
        assert mql_lint.syntax_error(f"files from {A}") is None

    def test_rejects_angle_bracket_placeholder(self):
        # The bug that shipped in the LLM prompt: metacat rejects this too.
        err = mql_lint.syntax_error("files from hd-protodune:<dataset-name>")
        assert err is not None
        assert "'<'" in err

    def test_rejects_a_dataset_query(self):
        # Grammatical MQL, but the /query endpoint we call only takes file
        # queries — so the start rule must reject it, matching the server.
        assert mql_lint.syntax_error("datasets matching hd-protodune:*") is not None

    def test_accepts_advanced_constructs(self):
        for mql in [
            f"files from {A} - files from {B}",
            f"union(files from {A}, files from {B})",
            f"join(files from {A}, files from {B})",
            f"parents(files from {A})",
            f"filter sample(0.1)(files from {A})",
            f"files from {A} where dune.output_status not present",
            f"files from {A} where core.runs in 27000:28000",
        ]:
            assert mql_lint.syntax_error(mql) is None, mql


class TestBreaksWhenWrapped:
    """metacat 4.1.4 fails on `ordered`/`skip` layered over a `limit` that
    sits on a compound query. Each verdict below was checked against the
    production server."""

    def test_safe_without_a_limit(self):
        # No inner limit -> the paging wrapper is fine, keep the pushdown.
        for mql in [
            f"files from {A}",
            f"files from {A} - files from {B}",
            f"union(files from {A}, files from {B})",
            f"parents(files from {A})",
            f"filter sample(0.1)(files from {A})",
        ]:
            assert mql_lint.breaks_when_wrapped(mql) is False, mql

    def test_safe_when_limit_is_on_a_plain_query(self):
        for mql in [
            f"files from {A} limit 5",
            f"files from {A} where core.runs in (1) limit 5",
            "files where core.runs in (27731) limit 3",
        ]:
            assert mql_lint.breaks_when_wrapped(mql) is False, mql

    def test_fragile_when_limit_sits_on_a_compound_query(self):
        for mql in [
            f"files from {A} - files from {B} limit 3",
            f"(files from {A} - files from {B}) limit 3",
            f"union(files from {A}, files from {B}) limit 3",
            f"parents(files from {A}) limit 3",
            f"filter sample(0.1)(files from {A}) limit 3",
        ]:
            assert mql_lint.breaks_when_wrapped(mql) is True, mql

    def test_safe_when_limit_is_inside_a_branch(self):
        # The limit applies to one input, not to the compound result, so
        # the server handles the wrap fine.
        assert (
            mql_lint.breaks_when_wrapped(
                f"union(files from {A} limit 5, files from {B})"
            )
            is False
        )
        assert (
            mql_lint.breaks_when_wrapped(f"parents(files from {A} limit 5)") is False
        )

    def test_unparseable_is_not_fragile(self):
        # Don't change how we page based on a query the server will reject.
        assert mql_lint.breaks_when_wrapped("files from <ns>:<ds>") is False


class TestWhitelists:
    def test_flags_filter_the_server_does_not_have(self):
        # Documented upstream as a standard filter; production rejects it.
        assert mql_lint.unknown_filters(f"filter every_nth(3,0)(files from {A})") == [
            "every_nth"
        ]

    def test_accepts_verified_filters(self):
        assert mql_lint.unknown_filters(f"filter sample(0.1)(files from {A})") == []
        assert mql_lint.unknown_filters(f"filter hash(3,0)(files from {A})") == []

    def test_flags_unknown_namespace(self):
        assert mql_lint.unknown_namespaces(
            "files from nosuchns:ds", {"hd-protodune"}
        ) == ["nosuchns"]

    def test_numeric_range_is_not_a_namespace(self):
        assert (
            mql_lint.unknown_namespaces(
                f"files from {A} where core.runs in 27000:28000", {"hd-protodune"}
            )
            == []
        )


class TestLint:
    def test_empty(self):
        assert mql_lint.lint("")["error"] == "MQL is empty"

    def test_syntax_error_short_circuits_warnings(self):
        out = mql_lint.lint("files from hd-protodune:<ds>")
        assert out["error"] is not None
        assert out["warnings"] == []

    def test_clean_query(self):
        out = mql_lint.lint(f"files from {A}", {"hd-protodune"})
        assert out == {"error": None, "warnings": []}

    def test_warns_without_failing(self):
        out = mql_lint.lint(f"filter every_nth(3,0)(files from {A})", {"hd-protodune"})
        assert out["error"] is None
        assert len(out["warnings"]) == 1
        assert "every_nth" in out["warnings"][0]
