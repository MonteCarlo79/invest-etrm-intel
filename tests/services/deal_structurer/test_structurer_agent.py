"""Structurer agent tests (tasks 1-6)."""
from unittest.mock import MagicMock

from services.deal_committee import library as lib


class TestLoadBrief:
    def test_found(self):
        engine = MagicMock()
        engine.connect.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = (
            7, "谷山梁二期", {"deal_name": "谷山梁二期", "province": "蒙西"},
            "2026-09-01 00:00:00",
        )
        row = lib.load_brief(engine, 7)
        assert row["id"] == 7 and row["brief"]["province"] == "蒙西"

    def test_missing_raises_keyerror(self):
        engine = MagicMock()
        engine.connect.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = None
        try:
            lib.load_brief(engine, 99)
            assert False, "should raise"
        except KeyError as e:
            assert "99" in str(e)


class TestCountResultsForBrief:
    def test_count(self):
        engine = MagicMock()
        engine.connect.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = (3,)
        assert lib.count_results_for_brief(engine, 7) == 3
