import pytest

from dunecat import datasets
from dunecat.datasets import _split_pattern
from dunecat.errors import ConfigError


class FakeClient:
    def __init__(self, items):
        self._items = items
        self.calls: list[tuple[str | None, str | None]] = []

    def list_datasets(self, namespace_pattern=None, name_pattern=None):
        self.calls.append((namespace_pattern, name_pattern))
        for item in self._items:
            yield item


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
