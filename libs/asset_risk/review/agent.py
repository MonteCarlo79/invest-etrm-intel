"""Pure Trader-agent tool implementation for 复盘 review metrics (read-only, no streamlit)."""
from __future__ import annotations

import pandas as pd

from libs.asset_risk.review.store import load_review
from libs.trading_book.registry import STATIONS

UNIT_NOTE = "revenue = spot + capcomp before fees, ¥"


def resolve_asset(asset: str | None) -> str | None:
    """Registry asset_code or plant_name (exact, case-insensitive; then substring) -> asset_code."""
    a = (asset or "").strip()
    if not a:
        return None
    for s in STATIONS:
        if a.lower() == s.asset_code.lower() or a == s.plant_name:
            return s.asset_code
    hits = [s.asset_code for s in STATIONS if a in s.plant_name or s.plant_name in a]
    return hits[0] if len(hits) == 1 else None


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
