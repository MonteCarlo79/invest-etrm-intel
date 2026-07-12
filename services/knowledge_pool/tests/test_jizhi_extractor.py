# services/knowledge_pool/tests/test_jizhi_extractor.py
"""Unit tests for jizhi_extractor — all DB and API calls mocked."""
from __future__ import annotations
from unittest.mock import MagicMock, patch, call
import pytest


class TestEnsureTables:
    def test_executes_ddl_and_commits(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cur)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("psycopg2.connect", return_value=mock_conn):
            from services.knowledge_pool.jizhi_extractor import ensure_tables
            ensure_tables("postgresql://fake/db")

        mock_cur.execute.assert_called_once()
        ddl_arg = mock_cur.execute.call_args[0][0]
        assert "jizhi_bids" in ddl_arg
        assert "jizhi_bid_winners" in ddl_arg
        assert "jizhi_upcoming" in ddl_arg
        mock_conn.commit.assert_called_once()
        mock_conn.close.assert_called_once()


class TestExtractBids:
    def _mock_response(self, bids: list) -> MagicMock:
        block = MagicMock()
        block.type = "tool_use"
        block.name = "save_bid_results"
        block.input = {"bids": bids}
        resp = MagicMock()
        resp.content = [block]
        return resp

    def test_happy_path_returns_list_of_dicts(self):
        bids = [
            {"province": "广东", "year": 2025, "batch": "存量",
             "tech_type": "光伏", "cleared_price": 0.35, "cleared_volume_gwh": 100.0}
        ]
        with patch("anthropic.Anthropic") as mock_cls:
            mock_cls.return_value.messages.create.return_value = self._mock_response(bids)
            from services.knowledge_pool.jizhi_extractor import extract_bids
            result = extract_bids("some document text about 机制竞价", "test-key")

        assert len(result) == 1
        assert result[0]["province"] == "广东"
        assert result[0]["cleared_price"] == 0.35

    def test_empty_api_key_returns_empty_list(self):
        from services.knowledge_pool.jizhi_extractor import extract_bids
        assert extract_bids("text", "") == []

    def test_empty_text_returns_empty_list(self):
        from services.knowledge_pool.jizhi_extractor import extract_bids
        assert extract_bids("   ", "key") == []

    def test_api_failure_returns_empty_list(self):
        with patch("anthropic.Anthropic") as mock_cls:
            mock_cls.return_value.messages.create.side_effect = Exception("API down")
            from services.knowledge_pool.jizhi_extractor import extract_bids
            result = extract_bids("text", "key")
        assert result == []

    def test_no_tool_use_block_returns_empty_list(self):
        resp = MagicMock()
        resp.content = [MagicMock(type="text")]
        with patch("anthropic.Anthropic") as mock_cls:
            mock_cls.return_value.messages.create.return_value = resp
            from services.knowledge_pool.jizhi_extractor import extract_bids
            result = extract_bids("text", "key")
        assert result == []
