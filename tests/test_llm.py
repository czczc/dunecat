"""English -> MQL: request shaping, and the lint/repair loop.

Two families here. First, the knobs a hosted gateway needs: a bearer key,
and omitting ``reasoning_effort`` (BNL's LiteLLM proxy rejects the whole
request with HTTP 400 for models that don't support it). Second, the
advanced path, where a query that doesn't parse gets one repair round.
"""

import json

from dunecat import llm


class FakeResponse:
    status_code = 200

    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return {"choices": [{"message": {"content": json.dumps(self._payload)}}]}


def _capture(monkeypatch, *replies):
    """Swap requests.post for a recorder that returns ``replies`` in order.

    Returns a list of the request bodies seen, so a test can assert on both
    the first attempt and the repair round.
    """
    seen: list[dict] = []
    queue = list(replies) or [{"mql": "files where core.events > 1", "notes": "ok"}]

    def fake_post(url, json=None, headers=None, timeout=None):
        seen.append({"url": url, "body": json, "headers": headers})
        return FakeResponse(queue.pop(0) if len(queue) > 1 else queue[0])

    monkeypatch.setattr(llm.requests, "post", fake_post)
    monkeypatch.setenv("DUNECAT_LLM_BASE_URL", "https://gateway.example/v1")
    return seen


def test_hosted_gateway_sends_bearer_and_omits_reasoning_effort(monkeypatch):
    seen = _capture(monkeypatch)
    monkeypatch.setenv("DUNECAT_LLM_API_KEY", "sk-test")
    monkeypatch.setenv("DUNECAT_LLM_REASONING_EFFORT", "")

    result = llm.generate_mql("files with more than one event")

    assert seen[0]["headers"] == {"Authorization": "Bearer sk-test"}
    assert "reasoning_effort" not in seen[0]["body"]
    assert result["mql"] == "files where core.events > 1"
    assert result["warnings"] == []


def test_local_endpoint_sends_no_key_and_keeps_reasoning_effort(monkeypatch):
    seen = _capture(monkeypatch)
    monkeypatch.delenv("DUNECAT_LLM_API_KEY", raising=False)
    monkeypatch.delenv("DUNECAT_LLM_REASONING_EFFORT", raising=False)

    llm.generate_mql("files with more than one event")

    assert seen[0]["headers"] == {}
    assert seen[0]["body"]["reasoning_effort"] == llm.DEFAULT_REASONING_EFFORT


class TestAdvancedMode:
    def test_grammar_is_in_the_prompt(self, monkeypatch):
        seen = _capture(monkeypatch)
        llm.generate_mql("anything")
        system = seen[0]["body"]["messages"][0]["content"]
        # The authoritative grammar, not a curated subset.
        assert "BEGIN MQL GRAMMAR" in system
        assert "parents_of" in system

    def test_unparseable_query_triggers_one_repair_round(self, monkeypatch):
        seen = _capture(
            monkeypatch,
            {"mql": "files from ns:<dataset-name>", "notes": "first try"},
            {"mql": "files from datasets matching hd-protodune:*", "notes": "fixed"},
        )

        result = llm.generate_mql("raw files from protodune hd")

        assert len(seen) == 2, "should have retried exactly once"
        # The repair prompt must carry the actual parse error.
        repair = seen[1]["body"]["messages"][-1]["content"]
        assert "does not parse" in repair
        assert "'<'" in repair
        assert result["mql"] == "files from datasets matching hd-protodune:*"
        assert result["warnings"] == []

    def test_still_unparseable_after_repair_is_surfaced_not_swallowed(self, monkeypatch):
        _capture(
            monkeypatch,
            {"mql": "files from ns:<a>", "notes": "bad"},
            {"mql": "files from ns:<b>", "notes": "still bad"},
        )

        result = llm.generate_mql("something")

        # The user sees the query and the reason it won't run.
        assert result["mql"] == "files from ns:<b>"
        assert result["warnings"]
        assert "does not parse" in result["warnings"][0]

    def test_unregistered_filter_warns_without_a_retry(self, monkeypatch):
        seen = _capture(
            monkeypatch,
            {"mql": "filter every_nth(3,0)(files from hd-protodune:dsA)", "notes": ""},
        )

        result = llm.generate_mql("every third file")

        assert len(seen) == 1, "parses fine — no repair round needed"
        assert any("every_nth" in w for w in result["warnings"])

    def test_parses_flag_gates_the_auto_run(self, monkeypatch):
        """The UI runs the query straight away when `parses` is true, so the
        flag has to be right in every outcome."""
        _capture(monkeypatch, {"mql": "files from hd-protodune:dsA", "notes": ""})
        assert llm.generate_mql("x")["parses"] is True

    def test_parses_false_when_repair_also_fails(self, monkeypatch):
        _capture(
            monkeypatch,
            {"mql": "files from ns:<a>", "notes": ""},
            {"mql": "files from ns:<b>", "notes": ""},
        )
        assert llm.generate_mql("x")["parses"] is False

    def test_parses_false_on_a_deliberate_refusal(self, monkeypatch):
        # Empty mql means "can't express this" — nothing to auto-run.
        _capture(monkeypatch, {"mql": "", "notes": "not expressible"})
        result = llm.generate_mql("x")
        assert result["parses"] is False
        assert result["mql"] == ""

    def test_parses_reported_in_subset_mode_too(self, monkeypatch):
        _capture(monkeypatch, {"mql": "files from hd-protodune:dsA", "notes": ""})
        monkeypatch.setenv("DUNECAT_LLM_ADVANCED", "0")
        assert llm.generate_mql("x")["parses"] is True

    def test_subset_mode_skips_the_grammar_and_the_lint(self, monkeypatch):
        seen = _capture(
            monkeypatch, {"mql": "files from ns:<dataset-name>", "notes": ""}
        )
        monkeypatch.setenv("DUNECAT_LLM_ADVANCED", "0")

        result = llm.generate_mql("anything")

        assert len(seen) == 1, "subset mode does not lint or repair"
        assert "BEGIN MQL GRAMMAR" not in seen[0]["body"]["messages"][0]["content"]
        assert result["warnings"] == []
