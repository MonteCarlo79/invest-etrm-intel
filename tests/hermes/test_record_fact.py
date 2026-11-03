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


def test_norm_date_accepts_iso_and_rejects_junk():
    assert rf._norm_date("2026-10-01") == "2026-10-01"
    assert rf._norm_date("<UNKNOWN>") is None
    assert rf._norm_date("") is None
    assert rf._norm_date(None) is None
    assert rf._norm_date("未知") is None


def test_write_fact_drafts_junk_dates_fall_back(monkeypatch):
    """Live dogfood 2026-10-17: Haiku emitted "<UNKNOWN>" for dates and the
    raw string reached SQL (invalid input syntax for type date). Junk dates
    must normalise to None → fallback chain (effective→as_of→today)."""
    junk = dict(_CANNED, effective_date="<UNKNOWN>", as_of_date="未知")
    captured = {}
    monkeypatch.setattr(rf, "write_fr_market_draft",
                        lambda **kw: captured.update(kw) or "ok")
    rf.write_fact_drafts(junk, pg_url="pg", source="s")
    import datetime as _dt
    eff = captured["effective_date"]
    _dt.date.fromisoformat(eff)                    # must be a real date
    assert eff == _dt.date.today().isoformat()     # fallback = today


def test_extract_schema_constrains_date_format():
    schema = rf._SUBMIT_FACT_TOOL["input_schema"]["properties"]
    for field_name in ("as_of_date", "effective_date"):
        assert schema[field_name].get("pattern"), field_name


# ── fundamentals_stat (spec 2026-11-02 写入路径 3) ────────────────────────────

_CANNED_FUND = {
    "province": "安徽",
    "fact_type": "fundamentals_stat",
    "value": 8.0,
    "unit": "%",
    "as_of_date": "2026-11-03",
    "effective_date": "2026-06-01",
    "metric": "demand_yoy",
    "section": "demand",
    "note": "全社会用电量同比+8%",
    "kb_title": "安徽6月全社会用电量同比+8%",
}


def test_fundamentals_stat_registered_everywhere():
    assert "fundamentals_stat" in rf.FACT_TYPES
    schema = rf._SUBMIT_FACT_TOOL["input_schema"]
    assert "fundamentals_stat" in schema["properties"]["fact_type"]["enum"]
    assert "section" in schema["properties"]
    assert "section" in schema["required"]


def test_prompt_defines_fundamentals_stat_with_wordlist():
    """记一下:安徽6月全社会用电量同比+8% → demand/demand_yoy。Bullet 必须带
    section+metric 词表,并把调频池/容量补偿/辅助服务收入挡在此类型之外。"""
    tmpl = rf._PROMPT_TMPL
    bullet = tmpl[tmpl.index("- fundamentals_stat:"): tmpl.index("- other:")]
    for token in ("capacity", "generation", "util_hours", "demand",
                  "cap_total", "gen_total", "hours_total",
                  "demand_total", "demand_yoy", "effective_date",
                  "fr_pool", "cap_comp", "ancillary_revenue"):   # 边界:不归此
        assert token in bullet, token


def test_write_fact_drafts_routes_fundamentals_stat():
    with patch("services.hermes.record_fact.write_fr_market_draft") as m_fr, \
         patch("services.hermes.record_fact.write_capcomp_draft") as m_cc, \
         patch("services.hermes.record_fact.write_ancillary_draft") as m_an, \
         patch("services.hermes.record_fact.write_fundamentals_draft") as m_fund:
        m_fund.return_value = "written 1, skipped 0"
        results = rf.write_fact_drafts(dict(_CANNED_FUND), pg_url="pg",
                                       source="hermes-record-fact")
    m_fund.assert_called_once_with(
        province="安徽",
        rows=[{"month": "2026-06-01", "section": "demand", "metric": "demand_yoy",
               "value": 8.0, "unit": "%"}],
        source="hermes-record-fact")
    m_fr.assert_not_called(); m_cc.assert_not_called(); m_an.assert_not_called()
    assert results and "province_fundamentals_monthly" in results[0]


def test_write_fact_drafts_fundamentals_month_falls_back_to_as_of():
    """effective_date 缺失 → 数据月取 as_of 所在月(沿用 _fact_dates 链)。"""
    fact = dict(_CANNED_FUND, effective_date=None, as_of_date="2026-11-03")
    captured = {}
    with patch("services.hermes.record_fact.write_fundamentals_draft",
               lambda **kw: captured.update(kw) or "written 1, skipped 0"):
        rf.write_fact_drafts(fact, pg_url="pg", source="s")
    assert captured["rows"][0]["month"] == "2026-11-01"


def test_pending_writes_for_fundamentals_stat_natural_key():
    writes = rf.pending_writes_for(dict(_CANNED_FUND), "hermes-record-fact")
    assert writes == [{
        "table": "marketdata.province_fundamentals_monthly",
        "key": {"province": "安徽", "month": "2026-06-01", "section": "demand",
                "metric": "demand_yoy", "source": "hermes-record-fact"}}]


def test_pending_writes_for_fundamentals_missing_keys_returns_empty():
    """section/metric 缺失时写库分支跳过(KB-only)— 确认翻无可翻。"""
    assert rf.pending_writes_for(dict(_CANNED_FUND, section=None), "s") == []
    assert rf.pending_writes_for(dict(_CANNED_FUND, metric=None), "s") == []


def test_write_fact_drafts_fundamentals_missing_keys_kb_only():
    """词表外输入(如「山东7月电网负荷创新高」)haiku 可能给出 section/metric=null
    → 跳过结构化写、不报错,事实仍走 KB(调用方 agent._record_fact 在
    write_fact_drafts 之后做 KB ingest,前提是不抛异常)。
    回归:None section/metric 曾穿过 writer 的 FundRow 校验(只查
    period/unit/province)直到 INSERT 撞 NOT NULL → IntegrityError 炸掉 KB 写入。"""
    with patch("services.hermes.record_fact.write_fundamentals_draft") as m_fund:
        for bad in (dict(_CANNED_FUND, section=None),
                    dict(_CANNED_FUND, metric=None)):
            assert rf.write_fact_drafts(bad, pg_url="pg", source="s") == []
    m_fund.assert_not_called()
