"""Reconciliation of reviewed (复盘) executed revenue against settlement and waterfall 实际."""
from __future__ import annotations

import pandas as pd

THRESHOLD = 0.05
RECON_COLUMNS = ["asset_code", "month", "reviewed_cny", "settlement_cny", "waterfall_cny",
                 "diff_settlement_cny", "diff_settlement_pct", "diff_waterfall_cny",
                 "diff_waterfall_pct", "flag"]


def _month_key(s) -> pd.Series:
    return pd.to_datetime(s.astype(str).str[:7] + "-01").dt.strftime("%Y-%m")


def _pct(diff: float, ref) -> float | None:
    if ref is None or pd.isna(ref) or ref == 0 or pd.isna(diff):
        return None
    return diff / abs(ref)


def reconcile(review: pd.DataFrame, settlement: pd.DataFrame, waterfall: pd.DataFrame,
              threshold: float = THRESHOLD) -> pd.DataFrame:
    """One row per reviewed (asset_code, month) with executed_revenue_cny.

    review: long rows (asset_code, month, metric, value). settlement / waterfall: columns
    (asset_code, month, value) in ¥; missing asset-months stay NaN ("—" in the UI).
    diff = reviewed − reference; pct relative to the reference. flag = any |pct| > threshold.
    """
    if review is None or review.empty:
        return pd.DataFrame(columns=RECON_COLUMNS)
    rv = review[review["metric"] == "executed_revenue_cny"].copy()
    if rv.empty:
        return pd.DataFrame(columns=RECON_COLUMNS)
    rv["month"] = _month_key(rv["month"])
    out = (rv.groupby(["asset_code", "month"], as_index=False)["value"].sum()
             .rename(columns={"value": "reviewed_cny"}))
    for name, ref in (("settlement_cny", settlement), ("waterfall_cny", waterfall)):
        if ref is None or ref.empty:
            out[name] = float("nan")
            continue
        r = ref.copy()
        r["month"] = _month_key(r["month"])
        r = r.groupby(["asset_code", "month"], as_index=False)["value"].sum().rename(columns={"value": name})
        out = out.merge(r, on=["asset_code", "month"], how="left")
    flags = []
    for tag, col in (("settlement", "settlement_cny"), ("waterfall", "waterfall_cny")):
        out[f"diff_{tag}_cny"] = out["reviewed_cny"] - out[col]
        out[f"diff_{tag}_pct"] = [_pct(d, ref) for d, ref in zip(out[f"diff_{tag}_cny"], out[col])]
        flags.append(out[f"diff_{tag}_pct"].map(lambda p: p is not None and abs(p) > threshold))
    out["flag"] = flags[0] | flags[1]
    return out[RECON_COLUMNS].sort_values(["asset_code", "month"]).reset_index(drop=True)
