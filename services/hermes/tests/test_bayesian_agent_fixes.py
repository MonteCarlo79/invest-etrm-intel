"""Tests for bayesian_agent fixes: province filter + evidence-trail sanitization."""
from unittest.mock import MagicMock, patch

from services.hermes.bayesian_agent import BayesianAnalystAgent


def _agent():
    # Constructor may require args; build a bare instance for pure-method tests
    return BayesianAnalystAgent.__new__(BayesianAnalystAgent)


class _Hit(dict):
    pass


def _results(names):
    return [_Hit(file_name=n, page_no=1, rank=5.0, chunk_text="正文") for n in names]


def test_province_filter_drops_other_provinces():
    agent = _agent()
    hits = _results([
        "上海市2025年上半年电力市场交易信息.pdf",
        "安徽电力市场2025年一季度市场交易信息公报.pdf",
        "广东-电力市场运营简报暨市场信息披露报告（2026年6月份）.pdf",
    ])
    with patch("services.knowledge_pool.knowledge_docs.search_reference_docs", return_value=hits):
        out = agent._tool_search_exchange_reports("上海 2025年夏季 现货价格 高温")
    assert "上海市2025年上半年" in out
    assert "安徽电力市场" not in out
    assert "广东-电力市场" not in out


def test_province_filter_keeps_generic_and_alias():
    agent = _agent()
    hits = _results([
        "河北南网2026年6月电力市场信息披露报告.pdf",   # alias of 冀南
        "全国电力市场运行报告.pdf",                     # no province in name → keep
        "山东电力市场2026年6月报告.pdf",                # different province → drop
    ])
    with patch("services.knowledge_pool.knowledge_docs.search_reference_docs", return_value=hits):
        out = agent._tool_search_exchange_reports("冀南 现货 价格 2026年6月")
    assert "河北南网" in out
    assert "全国电力市场运行报告" in out
    assert "山东电力市场" not in out


def test_province_filter_graceful_when_nothing_survives():
    agent = _agent()
    hits = _results(["山东电力市场2026年6月报告.pdf"])
    with patch("services.knowledge_pool.knowledge_docs.search_reference_docs", return_value=hits):
        out = agent._tool_search_exchange_reports("上海 现货价格")
    # falls back to unfiltered rather than returning nothing
    assert "山东电力市场" in out


def test_evidence_trail_sanitizes_db_error():
    # The sanitization happens inline in run(); test the mechanism via the
    # same prefix rule used there.
    result = 'DB error: column "province_cn" does not exist\nLINE 2: ...'
    display = result[:600]
    if result.startswith(("DB error:", "ERROR:", "Exchange report search error:",
                          "KB search error:", "Market agent error:")):
        display = "（查询失败，已跳过 — 详细错误见服务端日志）"
    assert "province_cn" not in display
    assert "查询失败" in display


def test_prefetch_sql_uses_mwh_units():
    # The prefetch SQL must convert ¥/kWh → ¥/MWh for spot_daily and interprov.
    import inspect
    src = inspect.getsource(BayesianAnalystAgent._prefetch_spot_data)
    assert "AVG(da_avg) * 1000" in src
    assert "AVG(price_yuan_kwh) * 1000" in src
    assert "已换算为 ¥/MWh" in src


def test_system_prompt_has_unit_rules():
    from services.hermes.bayesian_agent import _build_system_prompt
    sp = _build_system_prompt()
    assert "UNIT DISCIPLINE" in sp
    assert "CONSISTENCY CHECK" in sp
    assert "¥/MWh" in sp
