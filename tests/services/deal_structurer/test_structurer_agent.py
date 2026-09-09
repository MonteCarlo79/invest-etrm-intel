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


class TestUploadedDocs:
    def setup_method(self):
        from services.deal_structurer import structurer_agent as sa
        sa._UPLOADED_DOCS.clear()

    def test_add_and_page(self):
        from services.deal_structurer import structurer_agent as sa
        sa.add_uploaded_doc("daf.pdf", "A" * 4000 + "B" * 100)
        out = sa.tool_read_uploaded_doc("daf.pdf", page=2)
        assert out["total_pages"] == 2 and out["excerpt"] == "B" * 100

    def test_query_window(self):
        from services.deal_structurer import structurer_agent as sa
        sa.add_uploaded_doc("daf.pdf", "x" * 1000 + "容量补偿" + "y" * 2000)
        out = sa.tool_read_uploaded_doc("daf.pdf", query="容量补偿")
        assert "容量补偿" in out["excerpt"] and len(out["excerpt"]) <= 3100

    def test_unknown_filename_lists_available(self):
        from services.deal_structurer import structurer_agent as sa
        sa.add_uploaded_doc("a.pdf", "hello")
        out = sa.tool_read_uploaded_doc("nope.pdf")
        assert "error" in out and out["available"] == ["a.pdf"]


class TestDealReads:
    def _engine(self):
        return MagicMock()

    def test_list_deals(self, monkeypatch):
        from services.deal_structurer import structurer_agent as sa
        monkeypatch.setattr(
            "services.deal_structurer.structurer_agent._engine", lambda: self._engine())
        monkeypatch.setattr(
            "services.deal_committee.library.list_briefs",
            lambda engine, limit=10: [
                {"id": 7, "deal_name": "谷山梁二期", "confirmed": True,
                 "created_at": "2026-09-06 00:31", "brief": {"province": "蒙西", "asset_type": "bess"},
                 "result_id": 12, "recommendation": "有条件 GO", "daf_id": None},
            ])
        out = sa.tool_list_deals()
        assert out["deals"][0]["brief_id"] == 7
        assert out["deals"][0]["result_id"] == 12

    def test_get_result_by_name(self, monkeypatch):
        from services.deal_structurer import structurer_agent as sa
        monkeypatch.setattr(
            "services.deal_structurer.structurer_agent._engine", lambda: self._engine())
        monkeypatch.setattr(
            "services.deal_committee.library.list_briefs",
            lambda engine, limit=20: [
                {"id": 7, "deal_name": "谷山梁二期", "confirmed": True,
                 "created_at": "x", "brief": {"province": "蒙西", "asset_type": "bess"},
                 "result_id": 12, "recommendation": "有条件 GO", "daf_id": None},
            ])
        monkeypatch.setattr(
            "services.deal_committee.library.load_result",
            lambda engine, rid: {
                "brief": {"deal_name": "谷山梁二期", "province": "蒙西"},
                "sections": [{"key": "economics", "title": "经济性测算",
                              "status": "ok", "markdown": "M" * 1000}],
                "economics": {"revenue_p50": 2.8e8},
                "synthesis": "S" * 3000, "recommendation": "有条件 GO",
                "deal_name": "谷山梁二期", "daf_id": None,
            })
        out = sa.tool_get_deal_result("谷山梁二期")
        assert out["result_id"] == 12
        assert out["brief"]["province"] == "蒙西"
        assert len(out["sections"][0]["markdown_head"]) == 400
        assert len(out["synthesis"]) == 1500

    def test_get_result_unknown(self, monkeypatch):
        from services.deal_structurer import structurer_agent as sa
        monkeypatch.setattr(
            "services.deal_structurer.structurer_agent._engine", lambda: self._engine())
        monkeypatch.setattr(
            "services.deal_committee.library.list_briefs", lambda engine, limit=20: [])
        out = sa.tool_get_deal_result("不存在")
        assert "error" in out
