import pytest

from dunecat import files
from dunecat.errors import FileDIDNotFoundError
from dunecat.files import build_mql, file_datasets, file_did, find_files
from dunecat.filters import FileFilters


def test_build_mql_no_filters():
    assert build_mql("ns:n", FileFilters()) == "files from ns:n"


def test_build_mql_with_runs():
    assert (
        build_mql("ns:n", FileFilters(runs=(27731, 27732)))
        == "files from ns:n where core.runs in (27731,27732)"
    )


def test_build_mql_with_runs_and_namespace():
    mql = build_mql(
        "ns:n", FileFilters(runs=(27731,), namespace="hd-protodune-det-reco")
    )
    assert mql == (
        "files from ns:n where core.runs in (27731) "
        "and namespace = 'hd-protodune-det-reco'"
    )


def test_file_did_joins_namespace_and_name():
    item = {"namespace": "ns", "name": "f.root"}
    assert file_did(item) == "ns:f.root"


class FakeClient:
    def __init__(self, items):
        self._items = items
        self.calls: list[dict] = []

    def query(self, mql, with_metadata=False, batch_size=0):
        self.calls.append(
            {"mql": mql, "with_metadata": with_metadata, "batch_size": batch_size}
        )
        for item in self._items:
            yield item


def test_find_files_passes_filters_through_and_yields_items(monkeypatch):
    items = [
        {"namespace": "ns", "name": "a.root"},
        {"namespace": "ns", "name": "b.root"},
    ]
    fc = FakeClient(items)
    monkeypatch.setattr(files, "get_client", lambda: fc)

    result = list(
        find_files(
            "ns:ds",
            FileFilters(runs=(27731,)),
            with_metadata=True,
            batch_size=500,
        )
    )

    assert result == items
    assert fc.calls == [
        {
            "mql": "files from ns:ds where core.runs in (27731)",
            "with_metadata": True,
            "batch_size": 500,
        }
    ]


class FakeFileClient:
    def __init__(self, file_record=None):
        self._record = file_record
        self.calls: list[dict] = []

    def get_file(self, did=None, with_datasets=False, **kwargs):
        self.calls.append({"did": did, "with_datasets": with_datasets})
        return self._record


def test_file_datasets_returns_parent_dataset_dids(monkeypatch):
    record = {
        "namespace": "hd-protodune-det-reco",
        "name": "np04hd_raw_run027731_X.root",
        "datasets": [
            {"namespace": "dune", "name": "all"},
            {"namespace": "hd-protodune-det-reco", "name": "batch-set0"},
            {"namespace": "hd-protodune-det-reco", "name": "cosmic_runchunk4"},
        ],
    }
    fc = FakeFileClient(file_record=record)
    monkeypatch.setattr(files, "get_client", lambda: fc)

    result = file_datasets(
        "hd-protodune-det-reco:np04hd_raw_run027731_X.root"
    )

    assert result == [
        "dune:all",
        "hd-protodune-det-reco:batch-set0",
        "hd-protodune-det-reco:cosmic_runchunk4",
    ]
    assert fc.calls == [
        {
            "did": "hd-protodune-det-reco:np04hd_raw_run027731_X.root",
            "with_datasets": True,
        }
    ]


def test_file_datasets_missing_raises(monkeypatch):
    fc = FakeFileClient(file_record=None)
    monkeypatch.setattr(files, "get_client", lambda: fc)

    with pytest.raises(FileDIDNotFoundError, match="ns:nope.root"):
        file_datasets("ns:nope.root")


def test_file_datasets_empty_list_when_no_parents(monkeypatch):
    record = {"namespace": "ns", "name": "f.root", "datasets": []}
    fc = FakeFileClient(file_record=record)
    monkeypatch.setattr(files, "get_client", lambda: fc)

    assert file_datasets("ns:f.root") == []


def test_find_files_streams_lazily(monkeypatch):
    events: list[str] = []

    class StreamingClient:
        def query(self, mql, with_metadata=False, batch_size=0):
            for i in range(3):
                events.append(f"yield-{i}")
                yield {"namespace": "ns", "name": f"f{i}.root"}

    monkeypatch.setattr(files, "get_client", lambda: StreamingClient())

    iterator = find_files("ns:ds", FileFilters())
    next(iterator)
    assert events == ["yield-0"]
    next(iterator)
    assert events == ["yield-0", "yield-1"]
