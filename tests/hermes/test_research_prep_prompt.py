# tests/hermes/test_research_prep_prompt.py
"""Research-prep (调研问题清单) prompt injection in HermesAgent.process().

User report 2026-10-15: a 安徽 field-research question-list request produced
generic any-province questions because the system prompt carried no domain
framing for research prep. The RESEARCH_PREP addendum must be injected for
research-prep intents and only for those.
"""
import json
from unittest.mock import patch

from services.hermes.agent import HermesAgent
from services.hermes.models import InboundMessage


class _FakeTasks:
    def get_setting(self, key): return None
    def set_setting(self, key, value): pass


def _make_agent(captured: dict) -> HermesAgent:
    agent = HermesAgent(tasks=_FakeTasks(), anthropic_api_key="k")

    def _fake_llm(system, user_text, preferred="auto"):
        captured["system"] = system
        return json.dumps({"action": "REPLY", "params": {}, "reply": "ok"})

    agent._call_llm = _fake_llm
    agent._retrieve_kb_context = lambda query: ""
    agent._get_memory = lambda: None
    return agent


def _process(agent, text):
    msg = InboundMessage(source="feishu", sender_id="u1", sender_name="U",
                         text=text, timestamp="2026-10-15T07:32:00+08:00")
    ps = []
    try:
        import services.knowledge_pool.vault_reader  # noqa: F401
        ps.append(patch(
            "services.knowledge_pool.vault_reader.retrieve_vault_context",
            return_value=""))
    except ImportError:
        pass
    for p in ps:
        p.start()
    try:
        return agent.process(msg, chat_id="")
    finally:
        for p in ps:
            p.stop()


def test_research_trip_question_list_triggers_addendum():
    captured: dict = {}
    agent = _make_agent(captured)
    _process(agent, "我准备去安徽调研电力市场，根据你对于新能源参与电力市场、售电、"
                    "电力交易、跨省交易的knowledge gap，给我一个问题清单，不超过20个问题")
    system = captured["system"]
    assert "RESEARCH PREP MODE" in system
    assert "136号文" in system
    assert "机制电价" in system
    # 省份调研主线框架 (Dipeng 2026-10-16) baked into the addendum
    assert "调研主线框架" in system
    assert "批零价差" in system
    assert "调频资金池" in system
    assert "利润上限" in system


def test_english_field_trip_triggers_addendum():
    captured: dict = {}
    agent = _make_agent(captured)
    _process(agent, "I'm visiting Anhui next week for a field trip — "
                    "please prepare research questions on the spot market")
    assert "RESEARCH PREP MODE" in captured["system"]


def test_plain_data_question_no_addendum():
    captured: dict = {}
    agent = _make_agent(captured)
    _process(agent, "今天山东现货价格怎么样")
    assert "RESEARCH PREP MODE" not in captured["system"]


def test_report_trigger_phrase_no_addendum():
    captured: dict = {}
    agent = _make_agent(captured)
    _process(agent, "蒙西储能日报")
    assert "RESEARCH PREP MODE" not in captured["system"]
