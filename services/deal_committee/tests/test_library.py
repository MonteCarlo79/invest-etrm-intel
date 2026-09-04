# services/deal_committee/tests/test_library.py
from unittest.mock import MagicMock

from services.deal_committee.brief import DealBrief
from services.deal_committee.library import list_dafs, load_daf, save_brief, save_daf


def _engine_with(fetch_one=None, fetch_all=None):
    conn = MagicMock()
    if fetch_one is not None:
        conn.execute.return_value.fetchone.return_value = fetch_one
    if fetch_all is not None:
        conn.execute.return_value.fetchall.return_value = fetch_all
    engine = MagicMock()
    engine.begin.return_value.__enter__.return_value = conn
    engine.connect.return_value.__enter__.return_value = conn
    return engine, conn


def test_save_brief_inserts_jsonb_and_returns_id():
    engine, conn = _engine_with(fetch_one=(42,))
    brief = DealBrief(deal_name="蒙西储能一期", province="蒙西", confirmed=True)
    brief_id = save_brief(engine, brief)
    assert brief_id == 42
    sql, params = conn.execute.call_args[0][0], conn.execute.call_args[0][1]
    assert "marketdata.deal_briefs" in str(sql)
    assert "蒙西储能一期" in params["brief"]


def test_save_daf_stores_bytes_and_size():
    engine, conn = _engine_with(fetch_one=(7,))
    daf_id = save_daf(engine, 42, DealBrief(deal_name="x"), b"%PDF-fake", "DAF_x.pdf", "GO")
    assert daf_id == 7
    params = conn.execute.call_args[0][1]
    assert params["pdf"] == b"%PDF-fake"
    assert params["recommendation"] == "GO"


def test_list_dafs_returns_dicts():
    engine, conn = _engine_with(fetch_all=[(7, "蒙西储能一期", "DAF_a.pdf", 512, "GO", "2026-09-04")])
    rows = list_dafs(engine)
    assert rows[0]["deal_name"] == "蒙西储能一期"
    assert rows[0]["recommendation"] == "GO"


def test_load_daf_returns_bytes_and_filename():
    engine, conn = _engine_with(fetch_one=(b"%PDF-data", "DAF_a.pdf"))
    pdf, name = load_daf(engine, 7)
    assert pdf == b"%PDF-data" and name == "DAF_a.pdf"


# ── Full analysis results (deal_daf_results) ──────────────────────────────────

from services.deal_committee.library import (
    link_result_pdf, list_results, load_result, save_result,
)
from services.deal_committee.orchestrator import CommitteeResult
from services.deal_committee.sections import SectionResult
from services.deal_committee.tests.test_orchestrator import BRIEF, _fake_econ


def _result() -> CommitteeResult:
    return CommitteeResult(
        brief=BRIEF,
        sections=[SectionResult("market_background", "市场背景", "均价 320 元/MWh")],
        economics=_fake_econ(BRIEF),
        synthesis="## 交易摘要\n…", recommendation="有条件 GO",
    )


def test_save_result_inserts_jsonb_and_returns_id():
    engine, conn = _engine_with(fetch_one=(11,))
    rid = save_result(engine, 42, _result())
    assert rid == 11
    sql, params = conn.execute.call_args[0][0], conn.execute.call_args[0][1]
    assert "marketdata.deal_daf_results" in str(sql)
    assert "蒙西储能一期" in params["name"]
    assert "320" in params["sections"]            # JSONB-serialized sections
    assert "revenue_p50" in params["economics"]   # KPI scalars present
    assert params["recommendation"] == "有条件 GO"
    assert params["bid"] == 42


def test_save_result_handles_missing_economics():
    engine, conn = _engine_with(fetch_one=(12,))
    r = CommitteeResult(brief=BRIEF, sections=[], economics=None)
    rid = save_result(engine, None, r)
    assert rid == 12
    params = conn.execute.call_args[0][1]
    assert params["economics"] is None
    assert params["bid"] is None


def test_link_result_pdf_updates_daf_id():
    engine, conn = _engine_with()
    link_result_pdf(engine, 11, 7)
    params = conn.execute.call_args[0][1]
    assert params == {"d": 7, "i": 11}


def test_list_results_joins_pdf_filename():
    engine, conn = _engine_with(fetch_all=[
        (11, "蒙西储能一期", "蒙西", "bess", "有条件 GO", "2026-09-05", 7, "DAF_a.pdf")])
    rows = list_results(engine)
    assert rows[0]["daf_id"] == 7
    assert rows[0]["filename"] == "DAF_a.pdf"
    assert rows[0]["province"] == "蒙西"


def test_load_result_returns_full_record():
    engine, conn = _engine_with(fetch_one=(
        {"province": "蒙西"}, [{"key": "market_background", "title": "市场背景"}],
        {"revenue_p50": 1e8}, "## 交易摘要", "有条件 GO", "蒙西储能一期", 7))
    rec = load_result(engine, 11)
    assert rec["brief"]["province"] == "蒙西"
    assert rec["sections"][0]["key"] == "market_background"
    assert rec["economics"]["revenue_p50"] == 1e8
    assert rec["recommendation"] == "有条件 GO"
    assert rec["daf_id"] == 7


def test_load_result_missing_raises():
    engine, conn = _engine_with()
    conn.execute.return_value.fetchone.return_value = None
    import pytest
    with pytest.raises(KeyError):
        load_result(engine, 999)
