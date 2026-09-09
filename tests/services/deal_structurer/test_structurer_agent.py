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


class TestUpdateParameters:
    def _row(self):
        return {"id": 7, "deal_name": "谷山梁二期", "created_at": "x",
                "brief": {"deal_name": "谷山梁二期", "asset_type": "bess",
                          "province": "蒙西", "capacity_mw": 500.0,
                          "capacity_mwh": 2000.0, "capex_total_yuan": 13e8,
                          "confirmed": True}}

    def _patch(self, monkeypatch, count=1):
        from services.deal_structurer import structurer_agent as sa
        monkeypatch.setattr(
            "services.deal_structurer.structurer_agent._engine", lambda: MagicMock())
        monkeypatch.setattr(
            "services.deal_committee.library.load_brief",
            lambda engine, i: self._row())
        monkeypatch.setattr(
            "services.deal_committee.library.count_results_for_brief",
            lambda engine, i: count)
        self.saved = []
        monkeypatch.setattr(
            "services.deal_committee.library.update_brief",
            lambda engine, i, brief: self.saved.append(brief))

    def test_whitelist_patch(self, monkeypatch):
        self._patch(monkeypatch, count=1)
        from services.deal_structurer import structurer_agent as sa
        out = sa.tool_update_deal_parameters(
            7, {"comp_rate_yuan_mwh": 280.0}, challenge_note="费率下调")
        assert out["ok"] and out["changed"]["comp_rate_yuan_mwh"] == [None, 280.0]
        assert out["rev_name"] == "谷山梁二期 (rev 2)"
        assert self.saved[0].comp_rate_yuan_mwh == 280.0
        assert "费率下调" in (self.saved[0].structure_notes or "")

    def test_reset_comp_to_auto(self, monkeypatch):
        self._patch(monkeypatch)
        from services.deal_structurer import structurer_agent as sa
        out = sa.tool_update_deal_parameters(7, {"comp_rate_yuan_mwh": None})
        assert out["ok"] and self.saved[0].comp_rate_yuan_mwh is None

    def test_reject_non_whitelist(self, monkeypatch):
        self._patch(monkeypatch)
        from services.deal_structurer import structurer_agent as sa
        out = sa.tool_update_deal_parameters(7, {"structure_notes": "hack"})
        assert "error" in out and not self.saved

    def test_reject_bad_type(self, monkeypatch):
        self._patch(monkeypatch)
        from services.deal_structurer import structurer_agent as sa
        out = sa.tool_update_deal_parameters(7, {"capacity_mw": "五百"})
        assert "error" in out


class TestRerunAnalysis:
    def _setup(self, monkeypatch):
        from services.deal_structurer import structurer_agent as sa
        monkeypatch.setattr(
            "services.deal_structurer.structurer_agent._engine", lambda: MagicMock())
        monkeypatch.setattr(
            "services.deal_committee.library.load_brief",
            lambda engine, i: {"id": 7, "deal_name": "谷山梁二期",
                               "brief": {"deal_name": "谷山梁二期", "province": "蒙西",
                                         "capacity_mw": 500.0, "capacity_mwh": 2000.0,
                                         "capex_total_yuan": 13e8},
                               "created_at": "x"})
        monkeypatch.setattr(
            "services.deal_committee.library.count_results_for_brief",
            lambda engine, i: 1)
        monkeypatch.setattr(
            "services.deal_committee.library.list_results",
            lambda engine, limit=5: [{"id": 12, "deal_name": "谷山梁二期",
                                      "province": "蒙西", "asset_type": "bess",
                                      "recommendation": "GO", "created_at": "x",
                                      "daf_id": None, "filename": None}])
        monkeypatch.setattr(
            "services.deal_committee.library.load_result",
            lambda engine, rid: {
                "brief": {"deal_name": "谷山梁二期", "province": "蒙西",
                          "capacity_mw": 500.0, "capacity_mwh": 2000.0,
                          "capex_total_yuan": 13e8},
                "sections": [{"key": "economics", "title": "经济性测算",
                              "status": "ok", "markdown": "OLD"}],
                "economics": {"revenue_p50": 6.36e7},
                "synthesis": "OLD SYNTH", "recommendation": "NO-GO",
                "deal_name": "谷山梁二期", "daf_id": None})
        self.saved_results = []
        self.saved_dafs = []
        monkeypatch.setattr(
            "services.deal_committee.library.save_result",
            lambda engine, bid, result: self.saved_results.append(result) or 42)
        monkeypatch.setattr(
            "services.deal_committee.library.save_daf",
            lambda engine, bid, brief, pdf, fname, rec: self.saved_dafs.append(fname) or 77)
        monkeypatch.setattr(
            "services.deal_committee.library.link_result_pdf", lambda engine, rid, did: None)
        return sa

    def test_economics_scope(self, monkeypatch):
        sa = self._setup(monkeypatch)
        from services.deal_committee.economics import EconomicsResult
        fake_res = EconomicsResult(mc=None, monthly_price=[], n_price_hours=9504,
                                   n_simulations=500, model="ou",
                                   price_start="2025-08-01", price_end="2026-09-01",
                                   comp_rate_yuan_mwh=280.0, comp_annual_yuan=1.74e8)
        monkeypatch.setattr(
            "services.deal_committee.economics.run_economics",
            lambda brief, n_simulations=500: fake_res)
        monkeypatch.setattr(
            "services.deal_committee.economics.economics_section_markdown",
            lambda res, brief: "NEW ECON MD")
        out = sa.tool_rerun_analysis(7, "economics", api_key="k")
        assert out["ok"] and out["result_id"] == 42
        assert out["rev_name"] == "谷山梁二期 (rev 2)"
        saved = self.saved_results[0]
        assert saved.deal_name == "谷山梁二期 (rev 2)"
        econ_sec = next(s for s in saved.sections if s.key == "economics")
        assert econ_sec.markdown == "NEW ECON MD"

    def test_daf_scope_builds_pdf_no_new_result(self, monkeypatch):
        sa = self._setup(monkeypatch)
        from services.deal_committee.economics import EconomicsResult
        monkeypatch.setattr(
            "services.deal_committee.economics.run_economics",
            lambda brief, n_simulations=500: EconomicsResult(
                mc=None, monthly_price=[], n_price_hours=9504,
                n_simulations=500, model="ou", price_start="s", price_end="e"))
        monkeypatch.setattr(
            "services.deal_committee.daf_builder.build_daf", lambda result: b"%PDF-x")
        out = sa.tool_rerun_analysis(7, "daf", api_key="k")
        assert out["ok"] and out.get("daf_id") == 77
        assert self.saved_results == []  # daf scope saves no result row

    def test_full_scope_requires_confirmation(self, monkeypatch):
        sa = self._setup(monkeypatch)
        out = sa.tool_rerun_analysis(7, "full", api_key="k")
        assert "needs_confirmation" in out and self.saved_results == []

    def test_unknown_scope(self, monkeypatch):
        sa = self._setup(monkeypatch)
        out = sa.tool_rerun_analysis(7, "nonsense", api_key="k")
        assert "error" in out


class TestDispatchRouting:
    def test_schemas_present(self):
        from libs.deal_models.adapters.agent_tools import AGENT_TOOLS
        names = {t["name"] for t in AGENT_TOOLS}
        assert {"list_deals", "get_deal_result", "update_deal_parameters",
                "rerun_analysis", "read_uploaded_doc"} <= names

    def test_dispatch_read_uploaded_doc(self):
        from libs.deal_models.adapters.agent_tools import dispatch_tool
        from services.deal_structurer import structurer_agent as sa
        sa._UPLOADED_DOCS.clear()
        sa.add_uploaded_doc("daf.pdf", "hello daf content")
        import json as _json
        out = _json.loads(dispatch_tool("read_uploaded_doc",
                                        {"filename": "daf.pdf", "query": "daf"}))
        assert "daf" in out["excerpt"]

    def test_dispatch_unknown_still_errors(self):
        import json as _json
        from libs.deal_models.adapters.agent_tools import dispatch_tool
        out = _json.loads(dispatch_tool("nope_tool", {}))
        assert "error" in out

    def _rerun_fixture_with_result(self, monkeypatch):
        """list_briefs row WITH result_id + load_result with economics section
        → _load_current_result returns cur non-None."""
        from services.deal_structurer import structurer_agent as sa
        monkeypatch.setattr(
            "services.deal_structurer.structurer_agent._engine", lambda: MagicMock())
        monkeypatch.setattr(
            "services.deal_committee.library.load_brief",
            lambda engine, i: {"id": 7, "deal_name": "谷山梁二期",
                               "brief": {"deal_name": "谷山梁二期", "province": "蒙西",
                                         "capacity_mw": 500.0, "capacity_mwh": 2000.0,
                                         "capex_total_yuan": 13e8},
                               "created_at": "x"})
        monkeypatch.setattr(
            "services.deal_committee.library.count_results_for_brief",
            lambda engine, i: 1)
        monkeypatch.setattr(
            "services.deal_committee.library.list_results",
            lambda engine, limit=50: [])
        monkeypatch.setattr(
            "services.deal_committee.library.list_briefs",
            lambda engine, limit=20: [
                {"id": 7, "deal_name": "谷山梁二期", "confirmed": True,
                 "created_at": "x", "brief": {"province": "蒙西"},
                 "result_id": 12, "recommendation": "NO-GO", "daf_id": None}])
        monkeypatch.setattr(
            "services.deal_committee.library.load_result",
            lambda engine, rid: {
                "brief": {"deal_name": "谷山梁二期", "province": "蒙西"},
                "sections": [{"key": "economics", "title": "经济性测算",
                              "status": "ok", "markdown": "OLD ECON"}],
                "economics": None,
                "synthesis": "OLD SYNTH", "recommendation": "NO-GO",
                "deal_name": "谷山梁二期", "daf_id": None})
        self.saved_results = []
        monkeypatch.setattr(
            "services.deal_committee.library.save_result",
            lambda engine, bid, result: self.saved_results.append(result) or 42)
        monkeypatch.setattr(
            "services.deal_committee.library.save_daf",
            lambda engine, bid, brief, pdf, fname, rec: 77)
        return sa

    def test_rerun_economics_replaces_existing_section(self, monkeypatch):
        sa = self._rerun_fixture_with_result(monkeypatch)
        from services.deal_committee.economics import EconomicsResult
        fake_res = EconomicsResult(mc=None, monthly_price=[], n_price_hours=9504,
                                   n_simulations=500, model="ou",
                                   price_start="2025-08-01", price_end="2026-09-01",
                                   comp_rate_yuan_mwh=280.0, comp_annual_yuan=1.74e8)
        monkeypatch.setattr(
            "services.deal_committee.economics.run_economics",
            lambda brief, n_simulations=500: fake_res)
        monkeypatch.setattr(
            "services.deal_committee.economics.economics_section_markdown",
            lambda res, brief: "NEW ECON MD")
        out = sa.tool_rerun_analysis(7, "economics", api_key="k")
        assert out["ok"] and out["result_id"] == 42
        saved = self.saved_results[0]
        econ_secs = [s for s in saved.sections if s.key == "economics"]
        assert len(econ_secs) == 1  # replaced in place, not appended
        assert econ_secs[0].markdown == "NEW ECON MD"

    def test_rerun_daf_links_current_result(self, monkeypatch):
        sa = self._rerun_fixture_with_result(monkeypatch)
        from services.deal_committee.economics import EconomicsResult
        monkeypatch.setattr(
            "services.deal_committee.economics.run_economics",
            lambda brief, n_simulations=500: EconomicsResult(
                mc=None, monthly_price=[], n_price_hours=9504,
                n_simulations=500, model="ou", price_start="s", price_end="e"))
        monkeypatch.setattr(
            "services.deal_committee.daf_builder.build_daf", lambda result: b"%PDF-x")
        linked = []
        monkeypatch.setattr(
            "services.deal_committee.library.link_result_pdf",
            lambda engine, rid, did: linked.append((rid, did)))
        out = sa.tool_rerun_analysis(7, "daf", api_key="k")
        assert out["ok"] and out["daf_id"] == 77
        assert linked == [(12, 77)]  # current result id + new daf id


class TestDafScopeRefreshesEmptyPaths:
    def test_daf_scope_reruns_economics_when_paths_empty(self, monkeypatch):
        from unittest.mock import MagicMock
        from services.deal_structurer import structurer_agent as sa
        monkeypatch.setattr(
            "services.deal_structurer.structurer_agent._engine", lambda: MagicMock())
        monkeypatch.setattr(
            "services.deal_committee.library.load_brief",
            lambda engine, i: {"id": 7, "deal_name": "谷山梁二期",
                               "brief": {"deal_name": "谷山梁二期", "province": "蒙西",
                                         "capacity_mw": 500.0, "capacity_mwh": 2000.0,
                                         "capex_total_yuan": 13e8},
                               "created_at": "x"})
        monkeypatch.setattr(
            "services.deal_committee.library.count_results_for_brief",
            lambda engine, i: 1)
        # cur is None here (list_briefs unpatched) → economics None → refresh expected
        calls = []
        from services.deal_committee.economics import EconomicsResult
        fake_res = EconomicsResult(mc=None, monthly_price=[], n_price_hours=9504,
                                   n_simulations=500, model="ou",
                                   price_start="s", price_end="e")
        monkeypatch.setattr(
            "services.deal_committee.economics.run_economics",
            lambda brief, n_simulations=500: calls.append(brief) or fake_res)
        monkeypatch.setattr(
            "services.deal_committee.daf_builder.build_daf",
            lambda result: b"%PDF-x")
        monkeypatch.setattr(
            "services.deal_committee.library.save_daf",
            lambda engine, bid, brief, pdf, fname, rec: 77)
        monkeypatch.setattr(
            "services.deal_committee.library.link_result_pdf",
            lambda engine, rid, did: None)
        out = sa.tool_rerun_analysis(7, "daf", api_key="k")
        assert out["ok"] and out["daf_id"] == 77
        assert len(calls) == 1  # run_economics was invoked to refresh paths
