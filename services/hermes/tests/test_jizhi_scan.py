# services/hermes/tests/test_jizhi_scan.py
"""Unit tests for _run_jizhi_scan helper in services/hermes/app.py."""
from __future__ import annotations
from unittest.mock import MagicMock, patch
import pytest


class TestRunJizhiScan:

    def test_returns_counts_on_success(self):
        mock_feishu = MagicMock()
        with patch("services.hermes.app._jizhi_run_internet_query", return_value="some results text"), \
             patch("services.hermes.app._jizhi_extract_upcoming",
                   return_value=[{"province": "广东", "year": 2026, "batch": "增量_2026-12",
                                   "tech_type": "陆风", "bid_open_date": "2026-03-01"}]), \
             patch("services.hermes.app._jizhi_save_upcoming", return_value=1), \
             patch.dict("os.environ", {"PGURL": "postgresql://fake/db",
                                       "FEISHU_OWNER_OPEN_ID": "test_id"}):
            from services.hermes.app import _run_jizhi_scan
            result = _run_jizhi_scan("test-key", feishu=mock_feishu)

        assert result["new_upcoming"] >= 1
        assert isinstance(result["provinces"], list)

    def test_empty_api_key_returns_zeros(self):
        with patch("services.hermes.app._jizhi_run_internet_query") as mock_search:
            from services.hermes.app import _run_jizhi_scan
            result = _run_jizhi_scan("", feishu=None)

        mock_search.assert_not_called()
        assert result == {"new_upcoming": 0, "provinces": []}

    def test_internet_query_failure_doesnt_crash(self):
        with patch("services.hermes.app._jizhi_run_internet_query",
                   side_effect=RuntimeError("network error")), \
             patch("services.hermes.app._jizhi_extract_upcoming", return_value=[]), \
             patch("services.hermes.app._jizhi_save_upcoming", return_value=0), \
             patch.dict("os.environ", {"PGURL": "postgresql://fake/db"}):
            from services.hermes.app import _run_jizhi_scan
            result = _run_jizhi_scan("key", feishu=None)

        assert result["new_upcoming"] == 0

    def test_new_results_trigger_feishu_notification(self):
        mock_feishu = MagicMock()
        with patch("services.hermes.app._jizhi_run_internet_query", return_value="text"), \
             patch("services.hermes.app._jizhi_extract_upcoming",
                   return_value=[{"province": "山东", "year": 2026,
                                   "batch": "增量_2026-12", "tech_type": "光伏"}]), \
             patch("services.hermes.app._jizhi_save_upcoming", return_value=2), \
             patch.dict("os.environ", {"PGURL": "postgresql://fake/db",
                                       "FEISHU_OWNER_OPEN_ID": "ou_test123"}):
            from services.hermes.app import _run_jizhi_scan
            _run_jizhi_scan("key", feishu=mock_feishu)

        mock_feishu.send_card.assert_called_once()
        call_kwargs = mock_feishu.send_card.call_args
        assert call_kwargs[1]["open_id"] == "ou_test123"

    def test_no_new_results_no_feishu_notification(self):
        mock_feishu = MagicMock()
        with patch("services.hermes.app._jizhi_run_internet_query", return_value="text"), \
             patch("services.hermes.app._jizhi_extract_upcoming", return_value=[]), \
             patch("services.hermes.app._jizhi_save_upcoming", return_value=0), \
             patch.dict("os.environ", {"PGURL": "postgresql://fake/db"}):
            from services.hermes.app import _run_jizhi_scan
            _run_jizhi_scan("key", feishu=mock_feishu)

        mock_feishu.send_card.assert_not_called()
