"""Pure overlay helpers: waterfall recon and diagnostics compare."""
from datetime import date

import pandas as pd

from libs.asset_risk.review.overlay import diagnostics_compare, reviewed_keys, waterfall_recon


def _rv(rows):
    return pd.DataFrame(rows, columns=["asset_code", "month", "metric", "value", "unit"])


def _stages():
    rows = []
    for code, mo, nom, clr, act in [("a", "2026-09", 100, 90, 80), ("b", "2026-09", 50, 40, 30)]:
        for stage, v in (("申报", nom), ("出清", clr), ("实际", act)):
            rows.append({"asset_code": code, "month": mo, "stage": stage, "arb_cny": v * 0.4, "cap_cny": v * 0.6})
    rows.append({"asset_code": "a", "month": "2026-09", "stage": "实际", "arb_cny": 0.0, "cap_cny": 0.0})
    return pd.DataFrame(rows)


def test_waterfall_recon_only_reviewed_assets_and_excludes_fees():
    d = date(2026, 9, 1)
    rv = _rv([("a", d, "strategy_revenue_cny", 110, "CNY"), ("a", d, "cleared_revenue_cny", 90, "CNY"),
              ("a", d, "executed_revenue_cny", 70, "CNY")])
    out = waterfall_recon(_stages(), rv).set_index("stage")
    assert out.loc["申报", "computed_cny"] == 100 and out.loc["申报", "diff_cny"] == 10
    assert abs(out.loc["申报", "diff_pct"] - 0.1) < 1e-9
    assert out.loc["实际", "diff_cny"] == -10
    assert reviewed_keys(rv) == {("a", "2026-09")}


def test_waterfall_recon_empty():
    assert waterfall_recon(_stages(), _rv([])).empty


def test_diagnostics_compare_signs():
    d = date(2026, 9, 1)
    rv = _rv([("a", d, "strategy_revenue_cny", 100, "CNY"), ("a", d, "cleared_revenue_cny", 90, "CNY"),
              ("a", d, "executed_revenue_cny", 70, "CNY")])
    bf = pd.DataFrame([{"asset": "a", "month": "2026-09", "dis_fail_cny": 8.0, "chg_fail_cny": 1.0}])
    eg = pd.DataFrame([{"asset": "a", "month": "2026-09", "dis_gap_cny": 15.0, "chg_gap_cny": -2.0}])
    r = diagnostics_compare(bf, eg, rv).iloc[0]
    assert r["reviewed_strategy_to_cleared"] == -10 and r["computed_bid_fail"] == -9
    assert r["unexplained_bid"] == -1
    assert r["reviewed_cleared_to_executed"] == -20 and r["computed_exec_gap"] == -17
    assert r["unexplained_exec"] == -3


def test_diagnostics_compare_missing_computed_is_nan():
    d = date(2026, 9, 1)
    rv = _rv([("a", d, "strategy_revenue_cny", 100, "CNY"), ("a", d, "cleared_revenue_cny", 90, "CNY")])
    r = diagnostics_compare(pd.DataFrame(), pd.DataFrame(), rv).iloc[0]
    assert pd.isna(r["computed_bid_fail"]) and pd.isna(r["reviewed_cleared_to_executed"])
