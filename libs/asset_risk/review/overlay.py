"""Pure overlay helpers: reviewed (复盘) figures vs computed legs. Overlay/reconciliation only.

Per controller ruling R4 the deck's strategy/cleared/executed_revenue_cny are spot + capacity
compensation BEFORE fees, so they are compared with computed arb + cap (fees excluded).
"""
from __future__ import annotations

import pandas as pd

STAGE_METRIC = {"申报": "strategy_revenue_cny", "出清": "cleared_revenue_cny", "实际": "executed_revenue_cny"}
RECON_COLUMNS = ["stage", "computed_cny", "reviewed_cny", "diff_cny", "diff_pct"]
DIAG_COLUMNS = ["asset", "month", "reviewed_strategy_to_cleared", "computed_bid_fail", "unexplained_bid",
                "reviewed_cleared_to_executed", "computed_exec_gap", "unexplained_exec"]


def _mkey(s) -> pd.Series:
    return pd.to_datetime(s.astype(str).str[:7] + "-01").dt.strftime("%Y-%m")


def _wide(review_long: pd.DataFrame) -> pd.DataFrame:
    if review_long is None or review_long.empty:
        return pd.DataFrame()
    rv = review_long.copy()
    rv["month"] = _mkey(rv["month"])
    return rv.pivot_table(index=["asset_code", "month"], columns="metric", values="value", aggfunc="sum")


def reviewed_keys(review_long: pd.DataFrame) -> set[tuple[str, str]]:
    """(asset_code, 'YYYY-MM') having at least one stage revenue metric."""
    w = _wide(review_long)
    cols = [m for m in STAGE_METRIC.values() if m in w.columns]
    if w.empty or not cols:
        return set()
    return set(w[w[cols].notna().any(axis=1)].index)


def waterfall_recon(stages_by_asset_month: pd.DataFrame, review_long: pd.DataFrame) -> pd.DataFrame:
    """Per stage (申报/出清/实际): computed arb+cap (no fees) vs reviewed revenue, in ¥.

    stages_by_asset_month: long rows asset_code, month ('YYYY-MM'), stage, arb_cny, cap_cny.
    Only asset-months that have the stage's reviewed metric are summed (both sides), so a
    partial review never skews the comparison. diff = reviewed - computed; pct vs |computed|.
    """
    w = _wide(review_long)
    if w.empty or stages_by_asset_month is None or stages_by_asset_month.empty:
        return pd.DataFrame(columns=RECON_COLUMNS)
    st = stages_by_asset_month.copy()
    st["month"] = _mkey(st["month"])
    st["ac"] = st["arb_cny"].fillna(0.0) + st["cap_cny"].fillna(0.0)
    rows = []
    for stage, metric in STAGE_METRIC.items():
        if metric not in w.columns:
            continue
        rv = w[metric].dropna()
        s = st[st["stage"] == stage].groupby(["asset_code", "month"])["ac"].sum()
        keys = rv.index.intersection(s.index)
        if len(keys) == 0:
            continue
        comp, rev = float(s.loc[keys].sum()), float(rv.loc[keys].sum())
        diff = rev - comp
        rows.append({"stage": stage, "computed_cny": comp, "reviewed_cny": rev, "diff_cny": diff,
                     "diff_pct": diff / abs(comp) if comp else None})
    return pd.DataFrame(rows, columns=RECON_COLUMNS)


def diagnostics_compare(bid_fail: pd.DataFrame, exec_gap: pd.DataFrame,
                        review_long: pd.DataFrame) -> pd.DataFrame:
    """Reviewed vs computed deviations per asset-month, ¥, all in REVENUE-IMPACT sign
    (negative = loss, positive = gain).

    reviewed_strategy_to_cleared = cleared - strategy; reviewed_cleared_to_executed = executed - cleared.
    Computed metrics are sign-flipped to the same convention:
      computed_bid_fail = -(dis_fail_cny + chg_fail_cny)   [bid_fail_monthly gives nominated - DA-cleared
                          revenue, a positive loss; DA-cleared is a proxy for the deck's 出清]
      computed_exec_gap = chg_gap_cny - dis_gap_cny         [exec_gap_monthly gives dis: cleared - actual
                          revenue (positive loss); chg: actual - cleared (= -(cleared - actual))]
    unexplained = reviewed - computed. 'asset' is the asset_code on both inputs' keys.
    bid_fail / exec_gap must carry 'asset' = asset_code and 'month' ('YYYY-MM').
    """
    w = _wide(review_long)
    if w.empty:
        return pd.DataFrame(columns=DIAG_COLUMNS)
    for m in STAGE_METRIC.values():
        if m not in w.columns:
            w[m] = float("nan")
    w = w.reset_index().rename(columns={"asset_code": "asset"})
    w["reviewed_strategy_to_cleared"] = w["cleared_revenue_cny"] - w["strategy_revenue_cny"]
    w["reviewed_cleared_to_executed"] = w["executed_revenue_cny"] - w["cleared_revenue_cny"]
    out = w[["asset", "month", "reviewed_strategy_to_cleared", "reviewed_cleared_to_executed"]]

    def _comp(df, expr, name):
        if df is None or df.empty:
            return pd.DataFrame(columns=["asset", "month", name])
        d = df.copy()
        d["month"] = _mkey(d["month"])
        d[name] = expr(d)
        return d[["asset", "month", name]]

    bf = _comp(bid_fail, lambda d: -(d["dis_fail_cny"].fillna(0.0) + d["chg_fail_cny"].fillna(0.0)),
               "computed_bid_fail")
    eg = _comp(exec_gap, lambda d: d["chg_gap_cny"].fillna(0.0) - d["dis_gap_cny"].fillna(0.0),
               "computed_exec_gap")
    out = out.merge(bf, on=["asset", "month"], how="left").merge(eg, on=["asset", "month"], how="left")
    out["unexplained_bid"] = out["reviewed_strategy_to_cleared"] - out["computed_bid_fail"]
    out["unexplained_exec"] = out["reviewed_cleared_to_executed"] - out["computed_exec_gap"]
    keep = out[["reviewed_strategy_to_cleared", "reviewed_cleared_to_executed"]].notna().any(axis=1)
    return out[keep][DIAG_COLUMNS].reset_index(drop=True)
