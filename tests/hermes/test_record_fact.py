# tests/hermes/test_record_fact.py
"""RECORD_FACT ("记一下") flow: extraction, draft writes, confirm flip.

Owner workflow (2026-10-17): field intel like "记一下:安徽调频池3000万/月,
规则未发布" should land as (a) draft row in the right province_* table,
(b) KB doc, (c) agent_memory — then flip to confirmed on 「确认」.
"""
import json
from unittest.mock import MagicMock, patch, call

from services.hermes import record_fact as rf


_CANNED = {
    "province": "安徽",
    "fact_type": "fr_pool",
    "value": 3.6,
    "unit": "亿元/年",
    "as_of_date": "2026-10-17",
    "effective_date": "2026-10-01",
    "metric": None,
    "note": "规则未发布,两个细则零和补偿,尚未传递到用户侧",
    "kb_title": "安徽调频资金池 3000万/月(规则未发布)",
}


def _tool_resp(payload):
    block = type("B", (), {"type": "tool_use", "name": "submit_fact", "input": payload})()
    return type("R", (), {"content": [block], "stop_reason": "end_turn"})()


class _FakeClient:
    def __init__(self, payload): self._payload = payload; self.calls = []
    @property
    def messages(self): return self
    def create(self, **kwargs): self.calls.append(kwargs); return _tool_resp(self._payload)


def test_extract_fact_forces_tool_and_parses():
    client = _FakeClient(_CANNED)
    fact = rf.extract_fact("记一下:安徽调频池3000万/月,规则未发布", client)
    assert fact["province"] == "安徽"
    assert fact["fact_type"] == "fr_pool"
    assert fact["value"] == 3.6
    call = client.calls[0]
    assert call["tool_choice"] == {"type": "tool", "name": "submit_fact"}
    assert call["model"].startswith("claude-haiku")          # cheap tier for extraction


def test_write_fact_drafts_routes_fr_pool():
    with patch("services.hermes.record_fact.write_fr_market_draft") as m_fr, \
         patch("services.hermes.record_fact.write_capcomp_draft") as m_cc, \
         patch("services.hermes.record_fact.write_ancillary_draft") as m_an, \
         patch("services.hermes.record_fact.write_pipeline_rows") as m_pipe:
        m_fr.return_value = "ok"
        results = rf.write_fact_drafts(dict(_CANNED), pg_url="pg", source="hermes-record-fact")
    m_fr.assert_called_once_with(
        province="安徽", effective_date="2026-10-01",
        fr_price_yuan_kw_h=None, fr_pool_billion_yuan=3.6,
        source="hermes-record-fact")
    m_cc.assert_not_called(); m_an.assert_not_called(); m_pipe.assert_not_called()
    assert results and "province_fr_market" in results[0]


def test_write_fact_drafts_other_type_skips_structured():
    fact = dict(_CANNED, fact_type="other")
    with patch("services.hermes.record_fact.write_fr_market_draft") as m_fr:
        results = rf.write_fact_drafts(fact, pg_url="pg", source="s")
    m_fr.assert_not_called()
    assert results == []


def test_confirm_pending_flips_drafts_and_deactivates():
    pending = {
        "writes": [
            {"table": "marketdata.province_fr_market",
             "key": {"province": "安徽", "effective_date": "2026-10-01",
                     "source": "hermes-record-fact"}},
        ],
        "memory_id": 42,
    }
    conn = MagicMock()
    cur = conn.cursor.return_value.__enter__.return_value
    with patch("psycopg2.connect", return_value=conn):
        summary = rf.confirm_pending(pending, pg_url="pg")
    sqls = [c.args[0] for c in cur.execute.call_args_list]
    assert any("UPDATE marketdata.province_fr_market" in s and "status = 'confirmed'" in s for s in sqls)
    assert any("agent_memory" in s and "active = FALSE" in s for s in sqls)
    assert "province_fr_market" in summary


def test_system_prompt_documents_record_fact():
    from services.hermes.agent import SYSTEM_PROMPT
    assert "RECORD_FACT" in SYSTEM_PROMPT
