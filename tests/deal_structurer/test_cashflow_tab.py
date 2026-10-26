# tests/deal_structurer/test_cashflow_tab.py
import numpy as np
from apps.deal_structurer.cashflow_tab import distribution_metrics

def test_distribution_metrics():
    irr = np.array([0.04, 0.08, 0.12, 0.16, 0.20])
    rev = np.array([80.0, 90.0, 100.0, 110.0, 120.0])
    m = distribution_metrics(irr, rev, hurdle=0.10)
    assert m["irr_p50"] == 0.12
    assert m["prob_below_hurdle"] == 2/5
    assert m["rev_var5"] <= m["rev_cvar5"] <= rev.mean()


def test_render_importable():
    # Rendering is verified by import smoke (no st mocks).
    import apps.deal_structurer.cashflow_tab as mod
    assert callable(mod.render)


def test_npv_tornado_directions_and_order():
    # ±10% one-at-a-time on {capex, interest, O&M, revenue scale} at the P50
    # revenue cashflow: capex↑ / interest↑ / O&M↑ push NPV down, revenue↑ up;
    # rows sorted by swing descending.
    from libs.deal_models.contracts import ProjectFinancials
    from apps.deal_structurer.cashflow_tab import npv_tornado

    fin = ProjectFinancials(
        capex_total_yuan=100e6, annual_revenue_yuan=[15e6] * 20,
        annual_om_yuan=3e6, project_life_years=20,
        debt_ratio=0.70, interest_rate=0.05, loan_term_years=10,
        grace_years=1, hurdle_rate=0.08,
    )
    rows = npv_tornado(fin, base_rev=15e6)
    swings = [r["swing"] for r in rows]
    assert swings == sorted(swings, reverse=True)
    by = {r["param"]: r for r in rows}
    assert set(by) == {"capex", "interest_rate", "om_cost", "revenue_scale"}
    assert by["capex"]["d_plus"] < 0 < by["capex"]["d_minus"]
    # Engine NPV is unlevered (tax on EBIT, no debt tax shield) → project
    # NPV is financing-invariant; the zero-swing interest bar is the honest
    # output of the ruled NPV basis, not a bug.
    assert by["interest_rate"]["d_plus"] == 0.0 == by["interest_rate"]["d_minus"]
    assert by["om_cost"]["d_plus"] < 0 < by["om_cost"]["d_minus"]
    assert by["revenue_scale"]["d_minus"] < 0 < by["revenue_scale"]["d_plus"]


def test_dist_cell_mismatch_flagged():
    # cf_dist_result carries its producing cell's identity; any drift vs the
    # current ov_result (province / asset / duration / asof / power) must be
    # flagged so cell-A's distribution never renders under cell-B's caption.
    from apps.deal_structurer.cashflow_tab import dist_cell_mismatch

    ov = {"province": "山东", "asset_type": "bess", "duration_h": 2.0,
          "asof_date": "2026-10-25", "power_mw": 100.0}
    same = {"cell": dict(ov)}
    assert not dist_cell_mismatch(same, ov)
    for drift in ({"province": "山西"}, {"asset_type": "wind"},
                  {"duration_h": 4.0}, {"asof_date": "2026-10-24"},
                  {"power_mw": 200.0}):
        cell = dict(ov)
        cell.update(drift)
        assert dist_cell_mismatch({"cell": cell}, ov), drift
    assert dist_cell_mismatch({}, ov)          # pre-fix shape: no identity
    assert not dist_cell_mismatch(None, ov)    # nothing rendered → no warning
    assert not dist_cell_mismatch(same, None)  # no current cell → no warning

