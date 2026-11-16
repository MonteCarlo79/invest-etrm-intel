"""Read-only 复盘 reported section for the P&L Waterfall sub-tab (streamlit renderer)."""
from __future__ import annotations

import streamlit as st

from libs.asset_risk.review.store import load_review
from libs.asset_risk.review.view import review_gap_table


def render_review_section(engine) -> None:
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
