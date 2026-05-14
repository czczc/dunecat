import re
from datetime import UTC, date, datetime

import pytest

from dunecat.timestamps import (
    DEFAULT_FORMAT,
    DEFAULT_REGEX,
    CandidateLimitExceeded,
    apply_date_range,
    apply_one_per_day,
    extract_run_time,
    parse_date_range,
)


RAW_HDF5 = (
    "np04hd_run027731_0001_dataflow0_datawriter_0_dataflow0_datawriter_0"
    "_20250112T220317.hdf5"
)
RECO_ROOT = (
    "np04hd_raw_run027731_0000_dataflow0_datawriter_0_20240705T122140"
    "_reco_stage1_reco_stage2_20241004T205934_keepup.root"
)


def test_extract_run_time_raw_hdf5():
    assert extract_run_time(RAW_HDF5) == datetime(
        2025, 1, 12, 22, 3, 17, tzinfo=UTC
    )


def test_extract_run_time_reco_root_picks_first_match():
    # Two timestamps in the filename: raw write time (first) and reco
    # processing time (second). We want the first — the run/data-taking
    # time, before any reco stages.
    assert extract_run_time(RECO_ROOT) == datetime(
        2024, 7, 5, 12, 21, 40, tzinfo=UTC
    )


def test_extract_run_time_no_match():
    assert extract_run_time("plain_filename.root") is None


def test_extract_run_time_bad_calendar_value_returns_none():
    # 13th month would not parse as %m
    assert extract_run_time("file_20251301T000000.root") is None


def test_extract_run_time_custom_regex():
    regex = re.compile(r"run(\d{8})_")
    fmt = "%Y%m%d"
    assert extract_run_time("foo_run20250112_x.root", regex, fmt) == datetime(
        2025, 1, 12, tzinfo=UTC
    )


def test_extract_run_time_regex_without_capture_group_falls_back_to_full_match():
    regex = re.compile(r"\d{8}T\d{6}")
    assert extract_run_time(RAW_HDF5, regex, DEFAULT_FORMAT) == datetime(
        2025, 1, 12, 22, 3, 17, tzinfo=UTC
    )


def test_parse_date_range_inclusive_bounds():
    assert parse_date_range("2025-02-01:2025-02-28") == (
        date(2025, 2, 1),
        date(2025, 2, 28),
    )


def test_parse_date_range_missing_colon_raises():
    with pytest.raises(ValueError, match="FROM:TO"):
        parse_date_range("2025-02-01")


def test_parse_date_range_bad_format_raises():
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        parse_date_range("2025/02/01:2025-02-28")


def _make_item(name: str) -> dict:
    return {"namespace": "ns", "name": name}


def test_apply_date_range_keeps_in_window_drops_out_of_window():
    stream = [
        _make_item("a_20250101T120000.root"),
        _make_item("b_20250215T120000.root"),
        _make_item("c_20250301T120000.root"),
    ]
    result = list(apply_date_range(iter(stream), (date(2025, 2, 1), date(2025, 2, 28))))
    assert [i["name"] for i in result] == ["b_20250215T120000.root"]


def test_apply_date_range_skips_no_match_with_warning(capsys):
    stream = [
        _make_item("a_20250215T120000.root"),
        _make_item("no_timestamp.root"),
    ]
    result = list(
        apply_date_range(iter(stream), (date(2025, 1, 1), date(2025, 12, 31)))
    )
    assert [i["name"] for i in result] == ["a_20250215T120000.root"]
    captured = capsys.readouterr()
    assert "warning" in captured.err
    assert "no_timestamp.root" in captured.err


def test_apply_date_range_aborts_when_candidates_exceed_limit():
    stream = (_make_item(f"f_2025010{i % 10}T120000.root") for i in range(20))
    with pytest.raises(CandidateLimitExceeded, match="10"):
        list(
            apply_date_range(
                stream,
                (date(2025, 1, 1), date(2025, 1, 31)),
                max_candidates=10,
            )
        )


def test_apply_one_per_day_keeps_first_per_date():
    stream = [
        _make_item("a_20250215T100000.root"),
        _make_item("b_20250215T110000.root"),
        _make_item("c_20250216T100000.root"),
        _make_item("d_20250216T120000.root"),
        _make_item("e_20250217T100000.root"),
    ]
    result = list(apply_one_per_day(iter(stream)))
    assert [i["name"] for i in result] == [
        "a_20250215T100000.root",
        "c_20250216T100000.root",
        "e_20250217T100000.root",
    ]


def test_apply_one_per_day_streams_lazily():
    pulls: list[str] = []

    def gen():
        for n in ["a_20250215T100000.root", "b_20250216T100000.root", "c_20250217T100000.root"]:
            pulls.append(n)
            yield _make_item(n)

    it = apply_one_per_day(gen())
    next(it)
    assert pulls == ["a_20250215T100000.root"]
    next(it)
    assert pulls[:2] == [
        "a_20250215T100000.root",
        "b_20250216T100000.root",
    ]


def test_apply_one_per_day_aborts_when_over_limit():
    stream = (_make_item(f"f_2025010{i % 9 + 1}T120000.root") for i in range(20))
    with pytest.raises(CandidateLimitExceeded):
        list(apply_one_per_day(stream, max_candidates=5))
