"""kb_app passthrough tests: committee spans corpora, chat stays scoped."""
from unittest.mock import MagicMock, patch

import services.deal_committee.orchestrator as orch
import services.hermes.market_agent_bridge as bridge


class _Block:
    def __init__(self, type_, **kw):
        self.type = type_
        for k, v in kw.items():
            setattr(self, k, v)


class _Resp:
    def __init__(self, stop_reason, content):
        self.stop_reason = stop_reason
        self.content = content


class _OneShotSearchClient:
    """Returns one tool_use (search_reference_docs), then a final answer."""

    def __init__(self):
        self.calls = 0

    @property
    def messages(self):
        return self

    def create(self, **kw):
        self.calls += 1
        if self.calls == 1 and kw.get("tools"):
            return _Resp("tool_use", [
                _Block("tool_use", name="search_reference_docs", id="t1",
                       input={"query": "蒙西储能", "limit": 5}),
            ])
        return _Resp("end_turn", [_Block("text", text="answer")])


def _run_and_capture(kb_app):
    client = _OneShotSearchClient()
    captured = {}
    patches = [
        patch("shared.anthropic_client.make_client", return_value=client),
        patch("services.knowledge_pool.expert_memory.get_relevant_insights", return_value=[]),
        patch("services.knowledge_pool.expert_memory.inject_expert_memory", return_value=""),
        patch("services.knowledge_pool.expert_memory.extract_spot_insights", return_value=None),
        patch("services.spot_mcp.tools.get_spot_prices", return_value={}),
        patch("services.spot_mcp.tools.get_interprov_flow", return_value={}),
        patch("services.spot_mcp.tools.get_market_summaries", return_value={}),
        patch("services.spot_mcp.tools.get_market_fundamentals", return_value={}),
    ]
    import services.knowledge_pool.knowledge_docs as kd
    orig_srd = kd.search_reference_docs

    def fake_srd(**kw):
        captured.update(kw)
        return []

    for p in patches:
        p.start()
    with patch.object(kd, "search_reference_docs", side_effect=fake_srd):
        bridge._run_spot_query("测试", api_key="k", kb_app=kb_app)
    for p in patches:
        p.stop()
    return captured


def test_bridge_default_stays_strategist_scoped():
    captured = _run_and_capture("strategist")
    assert captured["app"] == "strategist"


def test_bridge_none_spans_all_corpora():
    captured = _run_and_capture(None)
    assert captured["app"] is None


def test_orchestrator_spot_spans_but_others_stay_scoped():
    calls = []

    def fake_rmq(market, question, api_key, pg_url="", kb_app="strategist"):
        calls.append((market, kb_app))
        return "ok"

    with patch("services.hermes.market_agent_bridge.run_market_query",
               side_effect=fake_rmq):
        orch.default_query_fn("spot", "q", "k")
        orch.default_query_fn("mengxi", "q", "k")

    assert ("spot", None) in calls
    assert ("mengxi", "strategist") in calls
