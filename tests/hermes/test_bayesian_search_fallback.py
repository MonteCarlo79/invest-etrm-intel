"""Regression tests for Hermes Bayesian-agent fixes (Xinjiang incident).

A. search_exchange_reports must not silently serve off-province reports when
   the named province has no reports in the knowledge base.
C. A malformed REPLY envelope (truncated JSON) must not leak the raw
   {"action": "REPLY", ...} wrapper to the user.
D. query_db tool description documents the new spot monthly tables.
"""
import json
from unittest.mock import MagicMock, patch

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

    def test_province_query_with_zero_results_returns_guidance(self):
        """With the province filter pushed into SQL, zero results means the
        province genuinely has no reports — the agent must get the
        no-substitution guidance, not the generic English line."""
        agent = _make_agent()
        with patch(_SEARCH_FN, return_value=[]):
            result = agent._tool_search_exchange_reports("新疆 现货市场 峰谷价差 储能")
        assert result.startswith("未检索到【新疆】")
        assert "spot_monthly_province" in result
        assert "exchange_monthly_metrics" in result

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


def _capture_search_sql(query: str, **kwargs) -> tuple[str, list]:
    """Run search_reference_docs with a mocked DB layer; return (sql, params)."""
    from services.knowledge_pool import knowledge_docs as kd

    captured: dict = {}
    cursor = MagicMock()
    cursor.description = [
        ("doc_id",), ("file_name",), ("category",), ("app",),
        ("page_no",), ("chunk_text",), ("rank",),
    ]
    cursor.fetchall.return_value = []
    cursor.execute.side_effect = lambda sql, params: captured.update(
        sql=sql, params=list(params)
    )
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor

    with patch.object(kd, "init_knowledge_tables"), patch.object(kd, "get_conn") as mock_gc:
        mock_gc.return_value.__enter__.return_value = conn
        kd.search_reference_docs(query, category="monthly_report", limit=5, **kwargs)
    return captured["sql"], captured["params"]


class TestFilenameContainsFilter:
    """Province filter pushed into the search SQL (opt-in filename_contains)."""

    def test_filename_contains_adds_ilike_condition_cjk(self):
        sql, params = _capture_search_sql("新疆 现货市场", filename_contains=("新疆",))
        assert "d.file_name ILIKE %s" in sql
        assert "%新疆%" in params

    def test_filename_contains_applies_to_latin_fts_branch(self):
        sql, params = _capture_search_sql("xinjiang spot price", filename_contains=("新疆",))
        assert "d.file_name ILIKE %s" in sql
        assert "%新疆%" in params

    def test_omitted_filename_contains_leaves_sql_without_it(self):
        # Note: a CJK query still yields %bigram% params for chunk_text ILIKEs,
        # so only the SQL shape (not the param values) distinguishes the filter.
        sql, _ = _capture_search_sql("新疆 现货市场")
        assert "file_name ILIKE" not in sql


class TestExchangeReportSearchPushesProvinceIntoSql:
    """_tool_search_exchange_reports passes filename_contains when a province is named."""

    def test_province_query_pushes_filename_filter(self):
        agent = _make_agent()
        hits = [_hit("新疆电力市场2026年5月月报.pdf")]
        with patch(_SEARCH_FN, return_value=hits) as mock_search:
            agent._tool_search_exchange_reports("新疆 现货市场 峰谷价差 储能")
        kwargs = mock_search.call_args.kwargs
        assert kwargs["filename_contains"] == ("新疆",)
        assert kwargs["limit"] == 5

    def test_provinceless_query_passes_none(self):
        agent = _make_agent()
        with patch(_SEARCH_FN, return_value=[]) as mock_search:
            result = agent._tool_search_exchange_reports("全国 现货 价格")
        assert mock_search.call_args.kwargs["filename_contains"] is None
        assert result == "No exchange reports found for this query."
