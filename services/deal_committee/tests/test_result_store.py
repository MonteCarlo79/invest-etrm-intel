"""Tests for services/deal_committee/result_store.py — CommitteeResult ↔ JSONB records."""
import json

from services.deal_committee.brief import DealBrief
from services.deal_committee.orchestrator import CommitteeResult
from services.deal_committee.sections import SectionResult
from services.deal_committee.result_store import (
    dict_to_economics, result_to_record, sections_from_dicts,
)
from services.deal_committee.tests.test_orchestrator import BRIEF, _fake_econ


def _result() -> CommitteeResult:
    return CommitteeResult(
        brief=BRIEF,
        sections=[
            SectionResult("market_background", "市场背景", "近12个月均价 320 元/MWh"),
            SectionResult("ops_mengxi", "运营实证 · 蒙西储能", "失败",
                          status="failed", error="timeout"),
        ],
        economics=_fake_econ(BRIEF),
        synthesis="## 交易摘要\n……\n## 风险分析\n……\n## 投资建议\n结论:有条件 GO",
        recommendation="有条件 GO",
    )


def test_result_to_record_is_json_serializable():
    rec = result_to_record(_result())
    # Must not raise — no numpy arrays or non-JSON types may leak into the record
    text = json.dumps(rec, ensure_ascii=False)
    assert "320 元/MWh" in text
    assert "有条件 GO" in text


def test_record_fields_cover_full_result():
    rec = result_to_record(_result())
    assert rec["brief"]["province"] == "蒙西"
    assert rec["sections"][0]["key"] == "market_background"
    assert rec["sections"][1]["status"] == "failed"
    assert rec["sections"][1]["error"] == "timeout"
    assert rec["economics"]["revenue_p50"] == 1e8
    assert rec["economics"]["equity_irr_p90"] == 0.13
    assert rec["economics"]["n_simulations"] == 10
    assert rec["synthesis"].startswith("## 交易摘要")
    assert rec["recommendation"] == "有条件 GO"


def test_record_handles_no_economics_and_empty_synthesis():
    r = CommitteeResult(brief=DealBrief(deal_name="最小", province="山东"),
                        sections=[], economics=None)
    rec = result_to_record(r)
    assert rec["economics"] is None
    assert rec["synthesis"] == ""
    json.dumps(rec)


def test_sections_from_dicts_roundtrip():
    sections = sections_from_dicts(result_to_record(_result())["sections"])
    assert sections[0].title == "市场背景"
    assert sections[0].markdown.startswith("近12个月")
    assert sections[1].status == "failed"


def test_dict_to_economics_reconstructs_scalars():
    econ = dict_to_economics(result_to_record(_result())["economics"])
    assert econ.mc.revenue_p50 == 1e8
    assert econ.mc.irr_prob_below_hurdle == 0.3
    assert econ.monthly_price == [("2026-08", 300.0)]
    assert dict_to_economics(None) is None
