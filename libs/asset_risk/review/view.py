"""Read-only 复盘 reported table: per asset x month strategy/cleared/executed revenue and gaps."""
from __future__ import annotations

import pandas as pd

from libs.asset_risk.review.store import load_review
from libs.trading_book.registry import STATIONS

_NAMES = {s.asset_code: s.plant_name for s in STATIONS}
_COLS = {"strategy_revenue_cny": "strategy", "cleared_revenue_cny": "cleared",
         "executed_revenue_cny": "executed"}


def review_gap_table(df: pd.DataFrame) -> pd.DataFrame:
    """Wide table from long review rows. Gaps are NaN when a leg is absent (never 0)."""
    d = df[df["metric"].isin(_COLS)]
    if d.empty:
        return pd.DataFrame()
    w = d.pivot_table(index=["asset_code", "month"], columns="metric", values="value",
                      aggfunc="first").rename(columns=_COLS).reset_index()
    for c in _COLS.values():
        if c not in w:
            w[c] = float("nan")
    w["cleared−strategy"] = w["cleared"] - w["strategy"]
    w["executed−cleared"] = w["executed"] - w["cleared"]
    w["cleared gap %"] = w["cleared−strategy"] / w["strategy"] * 100
    w["executed gap %"] = w["executed−cleared"] / w["strategy"] * 100
    w.insert(0, "asset", w["asset_code"].map(lambda c: _NAMES.get(c, c)))
    return w.drop(columns="asset_code").sort_values(["asset", "month"]).reset_index(drop=True)


def render_review_section(engine) -> None:
    import streamlit as st
    st.markdown("#### 复盘 reported (read-only)")
    try:
        df = load_review(engine)
    except Exception:
        st.info("No 复盘 data yet")
        return
    tbl = review_gap_table(df)
    if tbl.empty:
        st.info("No 复盘 data yet")
        return
    st.caption("revenue = spot + capcomp before fees, ¥. Gap % is relative to strategy revenue.")
    st.dataframe(tbl.round(1), use_container_width=True, hide_index=True)
