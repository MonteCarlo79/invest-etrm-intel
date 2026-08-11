"""Regression tests for Hermes Bayesian-agent fixes (Xinjiang incident).

A. search_exchange_reports must not silently serve off-province reports when
   the named province has no reports in the knowledge base.
C. A malformed REPLY envelope (truncated JSON) must not leak the raw
   {"action": "REPLY", ...} wrapper to the user.
D. query_db tool description documents the new spot monthly tables.
"""
import json
from unittest.mock import patch

import pytest

from services.hermes.agent import _salvage_reply_text
from services.hermes.bayesian_agent import BayesianAnalystAgent, _TOOL_DEFS


def _make_agent() -> BayesianAnalystAgent:
    return BayesianAnalystAgent(
        anthropic_api_key="test-key",
        pg_url="postgresql://localhost/test",
    )


def _hit(file_name: str, chunk_text: str = "月报内容摘要", rank: float = 1.0) -> dict:
    return {
        "doc_id": 1,
        "file_name": file_name,
        "category": "monthly_report",
        "app": "strategist",
        "page_no": 3,
        "chunk_text": chunk_text,
        "rank": rank,
    }


_SEARCH_FN = "services.knowledge_pool.knowledge_docs.search_reference_docs"


class TestExchangeReportProvinceFallback:
    """Fix A — no silent province substitution."""

    def test_off_province_hits_rejected_with_explicit_message(self):
        agent = _make_agent()
        hits = [
            _hit("2025年9月份广东电力市场结算情况-附件.pdf"),
            _hit("2025年8月份江苏电力市场月度报告.pdf"),
        ]
        with patch(_SEARCH_FN, return_value=hits):
            result = agent._tool_search_exchange_reports("新疆 现货市场 峰谷价差 储能")
        assert result.startswith("未检索到【新疆】")
        assert "广东" not in result
        assert "江苏" not in result

    def test_on_province_hit_passes_filter(self):
        """Regression: the hard filter must keep genuine on-province hits."""
        agent = _make_agent()
        hits = [
            _hit("2025年9月份广东电力市场结算情况-附件.pdf"),
            _hit("新疆电力市场2026年5月月报.pdf", chunk_text="新疆现货均价与峰谷价差分析"),
        ]
        with patch(_SEARCH_FN, return_value=hits):
            result = agent._tool_search_exchange_reports("新疆 现货市场 峰谷价差 储能")
        assert "新疆电力市场2026年5月月报.pdf" in result
        assert "广东" not in result


class TestSalvageReplyText:
    """Fix C — malformed REPLY envelope is unwrapped before reaching the user."""

    def test_salvages_reply_from_malformed_envelope(self):
        raw = (
            '{"action": "REPLY", "params": {}, "reply": "'
            '第一行\\n第二行 with a stray \t control"}'
        )
        # Premise: strict JSON parsing fails on the raw control character,
        # so this input exercises the fallback path.
        with pytest.raises(Exception):
            json.loads(raw)

        salvaged = _salvage_reply_text(raw)
        assert "第一行\n第二行" in salvaged  # literal \n unescaped to a real newline
        assert "第一行\\n" not in salvaged
        assert '{"action"' not in salvaged

    def test_plain_text_passthrough(self):
        raw = "  这不是 JSON，只是一段被截断的普通回复。  "
        assert _salvage_reply_text(raw) == raw.strip()


class TestQueryDbToolDescription:
    """Fix D — new monthly tables are documented for the agent."""

    def test_monthly_tables_documented(self):
        desc = next(t for t in _TOOL_DEFS if t["name"] == "query_db")["description"]
        assert "spot_monthly_province" in desc
        assert "spot_monthly_national" in desc
