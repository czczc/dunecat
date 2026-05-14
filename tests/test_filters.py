import pytest

from dunecat.filters import FileFilters, parse_meta, parse_run_range, parse_runs


def test_parse_runs_empty():
    assert parse_runs(None) == ()
    assert parse_runs("") == ()


def test_parse_runs_single():
    assert parse_runs("27731") == (27731,)


def test_parse_runs_comma_separated():
    assert parse_runs("27731,27732,27734") == (27731, 27732, 27734)


def test_parse_runs_strips_whitespace():
    assert parse_runs(" 27731 , 27732 ") == (27731, 27732)


def test_no_filters_yields_no_clauses():
    assert FileFilters().to_mql_where_clauses() == []


def test_runs_filter_emits_core_runs_in_clause():
    f = FileFilters(runs=(27731, 27732))
    assert f.to_mql_where_clauses() == ["core.runs in (27731,27732)"]


def test_namespace_filter_emits_quoted_clause():
    f = FileFilters(namespace="hd-protodune-det-reco")
    assert f.to_mql_where_clauses() == ["namespace = 'hd-protodune-det-reco'"]


def test_namespace_with_apostrophe_is_escaped():
    f = FileFilters(namespace="o'malley")
    assert f.to_mql_where_clauses() == [r"namespace = 'o\'malley'"]


def test_both_filters_combine_as_separate_clauses():
    f = FileFilters(runs=(27731,), namespace="hd-protodune-det-reco")
    assert f.to_mql_where_clauses() == [
        "core.runs in (27731)",
        "namespace = 'hd-protodune-det-reco'",
    ]


def test_parse_run_range_simple():
    assert parse_run_range("27000-28000") == (27000, 28000)


def test_parse_run_range_strips_whitespace():
    assert parse_run_range(" 27000 - 28000 ") == (27000, 28000)


def test_parse_run_range_none_returns_none():
    assert parse_run_range(None) is None


def test_parse_run_range_missing_dash_raises():
    with pytest.raises(ValueError, match="MIN-MAX"):
        parse_run_range("27000")


def test_parse_run_range_min_greater_than_max_raises():
    with pytest.raises(ValueError, match="MIN must not exceed MAX"):
        parse_run_range("28000-27000")


def test_run_range_filter_emits_range_clause():
    f = FileFilters(run_range=(27000, 28000))
    assert f.to_mql_where_clauses() == [
        "core.runs >= 27000 and core.runs <= 28000"
    ]


def test_parse_meta_empty():
    assert parse_meta(None) == ()
    assert parse_meta([]) == ()


def test_parse_meta_single_pair():
    assert parse_meta(["core.data_tier=full-reconstructed"]) == (
        ("core.data_tier", "full-reconstructed"),
    )


def test_parse_meta_multiple_pairs_preserves_order():
    assert parse_meta(
        ["dune.output_status=confirmed", "core.data_tier=full-reconstructed"]
    ) == (
        ("dune.output_status", "confirmed"),
        ("core.data_tier", "full-reconstructed"),
    )


def test_parse_meta_no_equals_raises():
    with pytest.raises(ValueError, match="KEY=VALUE"):
        parse_meta(["nope"])


def test_parse_meta_empty_key_raises():
    with pytest.raises(ValueError, match="empty key"):
        parse_meta(["=value"])


def test_parse_meta_keeps_equals_inside_value():
    assert parse_meta(["k=a=b"]) == (("k", "a=b"),)


def test_meta_string_value_is_single_quoted():
    f = FileFilters(meta=(("dune.output_status", "confirmed"),))
    assert f.to_mql_where_clauses() == [
        "dune.output_status = 'confirmed'"
    ]


def test_meta_integer_value_unquoted():
    f = FileFilters(meta=(("core.events", "42"),))
    assert f.to_mql_where_clauses() == ["core.events = 42"]


def test_meta_float_value_unquoted():
    f = FileFilters(meta=(("size_gb", "3.14"),))
    assert f.to_mql_where_clauses() == ["size_gb = 3.14"]


def test_meta_hyphenated_string_kept_as_string():
    f = FileFilters(meta=(("core.data_tier", "full-reconstructed"),))
    assert f.to_mql_where_clauses() == [
        "core.data_tier = 'full-reconstructed'"
    ]


def test_meta_value_with_apostrophe_escaped():
    f = FileFilters(meta=(("k", "o'malley"),))
    assert f.to_mql_where_clauses() == [r"k = 'o\'malley'"]


def test_all_filters_combine_with_and_semantics():
    f = FileFilters(
        runs=(27731,),
        run_range=(27000, 28000),
        namespace="hd-protodune-det-reco",
        meta=(
            ("dune.output_status", "confirmed"),
            ("core.events", "42"),
        ),
    )
    assert f.to_mql_where_clauses() == [
        "core.runs in (27731)",
        "core.runs >= 27000 and core.runs <= 28000",
        "namespace = 'hd-protodune-det-reco'",
        "dune.output_status = 'confirmed'",
        "core.events = 42",
    ]
