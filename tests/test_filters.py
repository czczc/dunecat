from dunecat.filters import FileFilters, parse_runs


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
