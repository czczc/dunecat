from dunecat import query


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


def test_run_query_passes_args_and_yields_items(monkeypatch):
    items = [{"namespace": "ns", "name": "a"}, {"namespace": "ns", "name": "b"}]
    fc = FakeClient(items)
    monkeypatch.setattr(query, "get_client", lambda: fc)

    result = list(
        query.run_query("files from ns:ds", with_metadata=True, batch_size=500)
    )

    assert result == items
    assert fc.calls == [
        {
            "mql": "files from ns:ds",
            "with_metadata": True,
            "batch_size": 500,
        }
    ]


def test_run_query_streams_lazily(monkeypatch):
    pulled = []

    class StreamingClient:
        def query(self, mql, with_metadata=False, batch_size=0):
            for i in range(3):
                pulled.append(i)
                yield {"namespace": "ns", "name": f"f{i}"}

    monkeypatch.setattr(query, "get_client", lambda: StreamingClient())
    it = query.run_query("files from ns:ds")
    next(it)
    assert pulled == [0]
