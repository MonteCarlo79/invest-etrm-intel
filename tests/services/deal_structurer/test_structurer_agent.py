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
