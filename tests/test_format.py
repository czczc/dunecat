from rich.console import Console

from dunecat import format as fmt
from dunecat.format import _fmt_ts, _str, render_dataset_table


def test_fmt_ts_formats_unix_epoch_as_utc():
    assert _fmt_ts(1738712710.5) == "2025-02-04 23:45:10 UTC"


def test_fmt_ts_none_returns_empty():
    assert _fmt_ts(None) == ""


def test_str_booleans():
    assert _str(True) == "yes"
    assert _str(False) == "no"


def test_str_none_returns_empty():
    assert _str(None) == ""


def test_render_dataset_table_emits_all_required_fields(monkeypatch, capsys):
    captured_console = Console(force_terminal=False, width=200, record=True)
    monkeypatch.setattr(fmt, "_console", captured_console)

    record = {
        "namespace": "hd-protodune-det-reco",
        "name": "example",
        "frozen": True,
        "monotonic": False,
        "creator": "dunepro",
        "created_timestamp": 1738712710.5,
        "updated_by": None,
        "updated_timestamp": None,
        "file_count": 148,
        "metadata": {
            "core.runs": "(27731, 27732)",
            "core.data_tier": "full-reconstructed",
        },
    }
    render_dataset_table(record)
    text = captured_console.export_text()

    assert "hd-protodune-det-reco:example" in text
    assert "Creator" in text and "dunepro" in text
    assert "Create timestamp" in text and "2025-02-04" in text
    assert "Frozen" in text and "yes" in text
    assert "Monotonic" in text and "no" in text
    assert "File count" in text and "148" in text
    assert "core.runs" in text and "(27731, 27732)" in text
    assert "core.data_tier" in text and "full-reconstructed" in text
