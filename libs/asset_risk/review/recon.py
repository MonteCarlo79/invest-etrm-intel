"""Reconciliation of reviewed (复盘) revenue against settlement-bill 实际 legs, like-for-like.

Pair 1: deck executed_revenue_cny (spot + capcomp, before fees) vs 实际 arb + cap.
Pair 2: deck total_revenue_cny (after 系统运行费/线损/调频分摊) vs 实际 arb + cap + fee + other.
spot_only_cny (arb) is informational and never flagged.
"""
from __future__ import annotations

import pandas as pd

THRESHOLD = 0.05
ACTUAL_COLUMNS = ["asset_code", "month", "arb_cny", "cap_cny", "fee_cny", "other_cny"]
RECON_COLUMNS = ["asset_code", "month",
                 "reviewed_executed_cny", "actual_arb_cap_cny", "diff_exec_cny", "diff_exec_pct", "flag_exec",
                 "reviewed_total_cny", "actual_total_cny", "diff_total_cny", "diff_total_pct", "flag_total",
                 "spot_only_cny", "flag"]


def _month_key(s) -> pd.Series:
    return pd.to_datetime(s.astype(str).str[:7] + "-01").dt.strftime("%Y-%m")


def _pct(diff, ref):
    if ref is None or pd.isna(ref) or ref == 0 or pd.isna(diff):
        return None
    return diff / abs(ref)


def reconcile(review: pd.DataFrame, actuals: pd.DataFrame, threshold: float = THRESHOLD) -> pd.DataFrame:
    """One row per reviewed (asset_code, month) having executed or total revenue.

    review: long rows (asset_code, month, metric, value). actuals: ACTUAL_COLUMNS in ¥ (fee_cny is
    signed, i.e. negative cost). Missing actuals stay NaN. diff = reviewed − actual; pct relative to actual.
    """
    if review is None or review.empty:
        return pd.DataFrame(columns=RECON_COLUMNS)
    rv = review[review["metric"].isin(["executed_revenue_cny", "total_revenue_cny"])].copy()
    if rv.empty:
        return pd.DataFrame(columns=RECON_COLUMNS)
    rv["month"] = _month_key(rv["month"])
    out = (rv.pivot_table(index=["asset_code", "month"], columns="metric", values="value", aggfunc="sum")
             .reset_index().rename(columns={"executed_revenue_cny": "reviewed_executed_cny",
                                            "total_revenue_cny": "reviewed_total_cny"}))
    for c in ("reviewed_executed_cny", "reviewed_total_cny"):
        if c not in out:
            out[c] = float("nan")
    if actuals is None or actuals.empty:
        a = pd.DataFrame(columns=["asset_code", "month", "actual_arb_cap_cny", "actual_total_cny",
                                  "spot_only_cny"])
    else:
        a = actuals.copy()
        a["month"] = _month_key(a["month"])
        a = a.groupby(["asset_code", "month"], as_index=False)[["arb_cny", "cap_cny", "fee_cny", "other_cny"]].sum()
        a["actual_arb_cap_cny"] = a["arb_cny"] + a["cap_cny"]
        a["actual_total_cny"] = a["actual_arb_cap_cny"] + a["fee_cny"] + a["other_cny"]
        a["spot_only_cny"] = a["arb_cny"]
        a = a[["asset_code", "month", "actual_arb_cap_cny", "actual_total_cny", "spot_only_cny"]]
    out = out.merge(a, on=["asset_code", "month"], how="left")
    for col in ("actual_arb_cap_cny", "actual_total_cny", "spot_only_cny"):
        if col not in out:
            out[col] = float("nan")
    for tag, rev, act in (("exec", "reviewed_executed_cny", "actual_arb_cap_cny"),
                          ("total", "reviewed_total_cny", "actual_total_cny")):
        out[f"diff_{tag}_cny"] = out[rev] - out[act]
        out[f"diff_{tag}_pct"] = [_pct(d, r) for d, r in zip(out[f"diff_{tag}_cny"], out[act])]
        out[f"flag_{tag}"] = out[f"diff_{tag}_pct"].map(lambda p: p is not None and abs(p) > threshold)
    out["flag"] = out["flag_exec"] | out["flag_total"]
    return out[RECON_COLUMNS].sort_values(["asset_code", "month"]).reset_index(drop=True)
