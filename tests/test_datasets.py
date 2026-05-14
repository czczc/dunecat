import pytest

from dunecat import datasets
from dunecat.datasets import _split_pattern
from dunecat.errors import ConfigError, DatasetNotFoundError


class FakeClient:
    def __init__(self, items=None, dataset=None):
        self._items = items or []
        self._dataset = dataset
        self.calls: list[tuple[str | None, str | None]] = []
        self.get_calls: list[str] = []

    def list_datasets(self, namespace_pattern=None, name_pattern=None):
        self.calls.append((namespace_pattern, name_pattern))
        for item in self._items:
            # M1 fake yielded {namespace, name} only; here we also support
            # callers that pre-populate full dataset dicts for --meta tests.
            yield item if isinstance(item, dict) and "namespace" in item else item

    def get_dataset(self, did=None, namespace=None, name=None, exact_file_count=False):
        self.get_calls.append(did)
        return self._dataset


@pytest.fixture
def fake_client(monkeypatch):
    items = [
        {"namespace": "hd-protodune-det-reco", "name": "alpha_cosmic"},
        {"namespace": "hd-protodune-det-reco", "name": "beta_cosmic"},
    ]
    fc = FakeClient(items)
    monkeypatch.setattr(datasets, "get_client", lambda: fc)
    return fc


def test_list_datasets_yields_dids(fake_client):
    result = list(datasets.list_datasets(pattern="hd-protodune-det-reco:*cosmic*"))
    assert result == [
        "hd-protodune-det-reco:alpha_cosmic",
        "hd-protodune-det-reco:beta_cosmic",
    ]


def test_pattern_split_passes_namespace_and_name(fake_client):
    list(datasets.list_datasets(pattern="hd-protodune-det-reco:*cosmic*"))
    assert fake_client.calls == [("hd-protodune-det-reco", "*cosmic*")]


def test_pattern_without_colon_treated_as_name_pattern(fake_client):
    list(datasets.list_datasets(pattern="*cosmic*"))
    assert fake_client.calls == [(None, "*cosmic*")]


def test_namespace_flag_overrides_when_pattern_missing(fake_client):
    list(datasets.list_datasets(namespace="hd-protodune-det-reco"))
    assert fake_client.calls == [("hd-protodune-det-reco", None)]


def test_namespace_flag_with_name_pattern(fake_client):
    list(
        datasets.list_datasets(
            pattern="*cosmic*", namespace="hd-protodune-det-reco"
        )
    )
    assert fake_client.calls == [("hd-protodune-det-reco", "*cosmic*")]


def test_conflicting_namespace_raises():
    with pytest.raises(ConfigError, match="conflicts"):
        _split_pattern("a:foo", namespace="b")


def test_split_pattern_none_inputs():
    assert _split_pattern(None, None) == (None, None)


def test_show_dataset_returns_dict(monkeypatch):
    record = {
        "namespace": "ns",
        "name": "n",
        "frozen": True,
        "monotonic": False,
        "metadata": {"core.runs": "(1, 2, 3)"},
        "creator": "alice",
        "created_timestamp": 1738712710.5,
        "file_count": 4,
    }
    fc = FakeClient(dataset=record)
    monkeypatch.setattr(datasets, "get_client", lambda: fc)

    assert datasets.show_dataset("ns:n") == record
    assert fc.get_calls == ["ns:n"]


def test_show_dataset_raises_when_missing(monkeypatch):
    fc = FakeClient(dataset=None)
    monkeypatch.setattr(datasets, "get_client", lambda: fc)
    with pytest.raises(DatasetNotFoundError, match="ns:n"):
        datasets.show_dataset("ns:n")


def _file(name: str, **meta) -> dict:
    return {"namespace": "ns", "name": name, "metadata": meta}


def test_dataset_values_flattens_list_metadata(monkeypatch):
    items = [
        _file("a.root", **{"core.runs": [27731]}),
        _file("b.root", **{"core.runs": [27732, 27734]}),
        _file("c.root", **{"core.runs": [27731, 27740]}),
    ]
    monkeypatch.setattr(datasets, "find_files", lambda *a, **kw: iter(items))

    assert datasets.dataset_values("ns:ds", "core.runs") == {27731, 27732, 27734, 27740}


def test_dataset_values_scalar_values_collected_directly(monkeypatch):
    items = [
        _file("a.root", **{"dune.output_status": "confirmed"}),
        _file("b.root", **{"dune.output_status": "confirmed"}),
        _file("c.root", **{"dune.output_status": "rejected"}),
    ]
    monkeypatch.setattr(datasets, "find_files", lambda *a, **kw: iter(items))

    assert datasets.dataset_values("ns:ds", "dune.output_status") == {
        "confirmed",
        "rejected",
    }


def test_dataset_values_skips_none_and_missing(monkeypatch):
    items = [
        _file("a.root", **{"some.field": None}),
        {"namespace": "ns", "name": "b.root", "metadata": {}},
        _file("c.root", **{"some.field": "x"}),
        _file("d.root", **{"some.field": [None, "y"]}),
    ]
    monkeypatch.setattr(datasets, "find_files", lambda *a, **kw: iter(items))

    assert datasets.dataset_values("ns:ds", "some.field") == {"x", "y"}


def test_list_datasets_meta_filter_drops_non_matching(monkeypatch):
    items = [
        {
            "namespace": "ns",
            "name": "a",
            "metadata": {"core.data_tier": "full-reconstructed"},
        },
        {
            "namespace": "ns",
            "name": "b",
            "metadata": {"core.data_tier": "raw"},
        },
        {
            "namespace": "ns",
            "name": "c",
            "metadata": {"core.data_tier": "full-reconstructed"},
        },
    ]
    fc = FakeClient(items=items)
    monkeypatch.setattr(datasets, "get_client", lambda: fc)

    result = list(
        datasets.list_datasets(
            meta=(("core.data_tier", "full-reconstructed"),)
        )
    )
    assert result == ["ns:a", "ns:c"]


def test_list_datasets_meta_filter_ands_multiple_pairs(monkeypatch):
    items = [
        {
            "namespace": "ns",
            "name": "a",
            "metadata": {
                "core.data_tier": "full-reconstructed",
                "dune.output_status": "confirmed",
            },
        },
        {
            "namespace": "ns",
            "name": "b",
            "metadata": {
                "core.data_tier": "full-reconstructed",
                "dune.output_status": "rejected",
            },
        },
    ]
    fc = FakeClient(items=items)
    monkeypatch.setattr(datasets, "get_client", lambda: fc)

    result = list(
        datasets.list_datasets(
            meta=(
                ("core.data_tier", "full-reconstructed"),
                ("dune.output_status", "confirmed"),
            )
        )
    )
    assert result == ["ns:a"]


def test_dataset_values_absent_field_returns_empty(monkeypatch):
    items = [_file("a.root"), _file("b.root")]
    monkeypatch.setattr(datasets, "find_files", lambda *a, **kw: iter(items))

    assert datasets.dataset_values("ns:ds", "nope") == set()
