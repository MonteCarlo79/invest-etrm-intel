"""Pure Trader-agent tool implementation for 复盘 review metrics (read-only, no streamlit)."""
from __future__ import annotations

import logging

import pandas as pd

from libs.asset_risk.review.aliases import asset_code_for
from libs.asset_risk.review.store import load_review
from libs.trading_book.registry import STATIONS

log = logging.getLogger(__name__)

UNIT_NOTE = ("strategy/cleared/executed_revenue_cny = spot + capacity compensation before fees; total_revenue_cny = after 系统运行费/线损/调频分摊; ¥")


def resolve_asset(asset: str | None) -> str | None:
    """Registry asset_code or plant_name (exact, case-insensitive; then unique substring; then the
    station alias map, e.g. 苏右 / 巴盟 / 阿拉善) -> asset_code."""
    a = (asset or "").strip()
    if not a:
        return None
    for s in STATIONS:
        if a.lower() == s.asset_code.lower() or a == s.plant_name:
            return s.asset_code
    hits = [s.asset_code for s in STATIONS if a in s.plant_name or s.plant_name in a]
    if len(hits) == 1:
        return hits[0]
    return asset_code_for(a)


def _month(m):
    return None if not m else pd.Timestamp(m).date().replace(day=1)


def review_metrics_payload(engine, asset: str | None = None, month_from: str | None = None,
                           month_to: str | None = None) -> dict:
    out: dict = {"unit_note": UNIT_NOTE, "rows": []}
    codes = None
    if asset:
        code = resolve_asset(asset)
        if code is None:
            out["note"] = f"Unknown asset '{asset}'. Use a registry name or asset code."
            return out
        codes = [code]
    try:
        df = load_review(engine, asset_codes=codes)
    except Exception:
        log.exception("load_review failed")
        out["note"] = "No 复盘 data yet (review table not available)."
        return out
    try:
        mf, mt = _month(month_from), _month(month_to)
    except Exception:
        out["note"] = "Invalid month; use YYYY-MM or YYYY-MM-DD."
        return out
    if mf is not None:
        df = df[df["month"] >= mf]
    if mt is not None:
        df = df[df["month"] <= mt]
    if df.empty:
        out["note"] = "No 复盘 data for this selection."
        return out
    out["rows"] = [{"asset_code": r.asset_code, "month": r.month.isoformat(), "metric": r.metric,
                    "value": None if pd.isna(r.value) else float(r.value), "unit": r.unit}
                   for r in df.itertuples(index=False)]
    return out
