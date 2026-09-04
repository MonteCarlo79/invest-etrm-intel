# services/deal_committee/tests/test_orchestrator.py
import time

from services.deal_committee.economics import EconomicsResult
from services.deal_committee.orchestrator import run_committee, run_single_section
from services.deal_committee.brief import DealBrief
from libs.deal_models.contracts import MCResult
import numpy as np

BRIEF = DealBrief(deal_name="蒙西储能一期", asset_type="bess", province="蒙西",
                  capacity_mw=100, capacity_mwh=200, capex_total_yuan=1.2e9)


def _fake_mc():
    return MCResult(revenue_p10=8e7, revenue_p50=1e8, revenue_p90=1.2e8,
                    revenue_var_5pct=2e7, revenue_cvar_5pct=3e7,
                    equity_irr_p10=0.05, equity_irr_p50=0.09, equity_irr_p90=0.13,
                    irr_prob_below_hurdle=0.3, npv_p10=-1e7, npv_p50=2e7, npv_p90=5e7,
                    tornado=[], revenue_paths=np.zeros(10),
                    equity_irr_paths=np.zeros(10), npv_paths=np.zeros(10))


def _fake_econ(brief, **kw):
    return EconomicsResult(mc=_fake_mc(), monthly_price=[("2026-08", 300.0)],
                           n_price_hours=8760, n_simulations=10, model="ou")


def _fake_risk(brief, engine=None):
    from services.deal_committee.sections import SectionResult
    return SectionResult(key="risk", title="风险数据", markdown="| 台账 | VaR |\n|---|---|\n| A | 1M |")


def _fake_query(market, question, api_key):
    return f"[{market}] 回答:数据良好"


def test_run_committee_assembles_all_sections():
    res = run_committee(BRIEF, query_fn=_fake_query, econ_fn=_fake_econ, risk_fn=_fake_risk)
    assert [s.key for s in res.sections] == [
        "market_background", "policy", "economics",
        "ops_mengxi", "ops_asset_risk", "ops_retail_risk", "risk"]
    assert all(s.status == "ok" for s in res.sections)
    assert res.economics is not None
    assert "[spot]" in res.sections[0].markdown
    assert "P50" in res.sections[2].markdown  # economics markdown from real formatter


def test_failing_agent_marks_section_failed_and_continues():
    def boom(market, question, api_key):
        if market == "mengxi":
            raise RuntimeError("agent exploded")
        return "ok"
    res = run_committee(BRIEF, query_fn=boom, econ_fn=_fake_econ, risk_fn=_fake_risk)
    mengxi = next(s for s in res.sections if s.key == "ops_mengxi")
    assert mengxi.status == "failed" and "agent exploded" in mengxi.error
    others = [s for s in res.sections if s.key != "ops_mengxi"]
    assert all(s.status == "ok" for s in others)


def test_section_timeout_marks_failed():
    def slow(market, question, api_key):
        time.sleep(1.0)
        return "too late"
    res = run_committee(BRIEF, query_fn=slow, econ_fn=_fake_econ,
                        risk_fn=_fake_risk, timeout_s=0.1)
    agents = [s for s in res.sections if s.key.startswith(("market", "policy", "ops"))]
    assert all(s.status == "failed" for s in agents)
    assert all("超时" in s.error for s in agents)


def test_failing_economics_keeps_economics_none():
    def bad_econ(brief, **kw):
        raise ValueError("总投资")
    res = run_committee(BRIEF, query_fn=_fake_query, econ_fn=bad_econ, risk_fn=_fake_risk)
    econ = next(s for s in res.sections if s.key == "economics")
    assert econ.status == "failed"
    assert res.economics is None


def test_on_section_done_callback_fires_in_order():
    seen = []
    run_committee(BRIEF, query_fn=_fake_query, econ_fn=_fake_econ,
                  risk_fn=_fake_risk, on_section_done=lambda s: seen.append(s.key))
    assert seen == ["market_background", "policy", "economics",
                    "ops_mengxi", "ops_asset_risk", "ops_retail_risk", "risk"]


def test_run_single_section_agent():
    sec, econ = run_single_section("policy", BRIEF, _fake_query, api_key="")
    assert sec.status == "ok" and econ is None
    sec2, econ2 = run_single_section("economics", BRIEF, _fake_query, "", econ_fn=_fake_econ)
    assert sec2.status == "ok" and econ2 is not None
