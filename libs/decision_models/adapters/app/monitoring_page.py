"""
libs/decision_models/adapters/app/monitoring_page.py

Streamlit page: BESS asset realization and fragility monitoring.

Pattern A (table-first, no model dispatch):
  Both monitoring services write daily snapshots to the DB.
  This page reads those snapshots directly — no compute on load.

Drop into any Streamlit app:
    from libs.decision_models.adapters.app.monitoring_page import render_monitoring_page
    render_monitoring_page()
"""
from __future__ import annotations

import datetime
from typing import List, Optional

_FRAGILITY_COLORS = {
    "LOW": "green",
    "MEDIUM": "orange",
    "HIGH": "red",
    "CRITICAL": "darkred",
}
_STATUS_COLORS = {
    "NORMAL": "green",
    "WARN": "orange",
    "ALERT": "red",
    "CRITICAL": "darkred",
}


def _load_realization_status(snapshot_date: Optional[datetime.date], lookback_days: int):
    import pandas as pd
    from sqlalchemy import text
    from services.common.db_utils import get_engine

    engine = get_engine()
    if snapshot_date:
        date_clause = "snapshot_date = :snap"
        params = {"snap": snapshot_date, "lookback": lookback_days}
    else:
        date_clause = "snapshot_date = (SELECT MAX(snapshot_date) FROM monitoring.asset_realization_status)"
        params = {"lookback": lookback_days}

    sql = text(f"""
        SELECT asset_code, snapshot_date, lookback_days, days_in_window,
               avg_cleared_actual_pnl, avg_pf_grid_feasible_pnl, realization_ratio,
               avg_grid_restriction_loss, avg_forecast_error_loss, avg_strategy_error_loss,
               avg_nomination_loss, avg_execution_clearing_loss,
               dominant_loss_bucket, status_level, narrative
        FROM monitoring.asset_realization_status
        WHERE {date_clause}
          AND lookback_days = :lookback
        ORDER BY status_level DESC, realization_ratio ASC NULLS LAST
    """)
    try:
        return pd.read_sql(sql, engine, params=params)
    except Exception:
        return pd.DataFrame()


def _load_fragility_status(snapshot_date: Optional[datetime.date]):
    import pandas as pd
    from sqlalchemy import text
    from services.common.db_utils import get_engine

    engine = get_engine()
    if snapshot_date:
        date_clause = "snapshot_date = :snap"
        params = {"snap": snapshot_date}
    else:
        date_clause = "snapshot_date = (SELECT MAX(snapshot_date) FROM monitoring.asset_fragility_status)"
        params = {}

    sql = text(f"""
        SELECT asset_code, snapshot_date,
               composite_score, fragility_level,
               realization_ratio, realization_status_level,
               recent_ratio, prior_ratio, ratio_delta,
               trend_score, realization_score,
               dominant_factor, narrative
        FROM monitoring.asset_fragility_status
        WHERE {date_clause}
        ORDER BY composite_score DESC
    """)
    try:
        return pd.read_sql(sql, engine, params=params)
    except Exception:
        return pd.DataFrame()


def render_monitoring_page() -> None:
    import pandas as pd
    import streamlit as st

    st.header("BESS Asset Monitoring")
    st.caption("Daily realization and fragility status across all assets.")

    # --- Sidebar ---
    with st.sidebar:
        st.subheader("Monitor settings")
        use_custom_date = st.checkbox("Use specific snapshot date", value=False)
        snapshot_date = None
        if use_custom_date:
            snapshot_date = st.date_input(
                "Snapshot date",
                value=datetime.date.today() - datetime.timedelta(days=1),
            )
        lookback_days = st.selectbox("Realization lookback (days)", [7, 14, 30, 60], index=2)

    # --- Load data ---
    with st.spinner("Loading monitoring data…"):
        real_df = _load_realization_status(snapshot_date, lookback_days)
        frag_df = _load_fragility_status(snapshot_date)

    # --- Fragility overview ---
    st.subheader("Fragility overview")
    if frag_df.empty:
        st.info(
            "No fragility data available. "
            "Run `run_realization_monitor.py` then `run_fragility_monitor.py` to populate."
        )
    else:
        snap_label = str(frag_df["snapshot_date"].iloc[0]) if "snapshot_date" in frag_df.columns else "latest"
        st.caption(f"Snapshot: {snap_label}")

        col1, col2, col3, col4 = st.columns(4)
        for level, col in zip(["LOW", "MEDIUM", "HIGH", "CRITICAL"], [col1, col2, col3, col4]):
            count = (frag_df["fragility_level"] == level).sum()
            col.metric(level, count)

        # Fragility table
        display_cols = [
            "asset_code", "fragility_level", "composite_score",
            "realization_ratio", "realization_status_level",
            "ratio_delta", "dominant_factor",
        ]
        available = [c for c in display_cols if c in frag_df.columns]
        st.dataframe(frag_df[available], use_container_width=True, hide_index=True)

    # --- Realization detail ---
    st.subheader("Realization status detail")
    if real_df.empty:
        st.info("No realization data available.")
    else:
        snap_label = str(real_df["snapshot_date"].iloc[0]) if "snapshot_date" in real_df.columns else "latest"
        st.caption(f"Snapshot: {snap_label} | Lookback: {lookback_days}d")

        # Status summary
        status_counts = real_df["status_level"].value_counts()
        cols = st.columns(4)
        for i, level in enumerate(["NORMAL", "WARN", "ALERT", "CRITICAL"]):
            cols[i].metric(level, int(status_counts.get(level, 0)))

        # Table
        display_cols = [
            "asset_code", "status_level", "realization_ratio",
            "days_in_window",
            "avg_cleared_actual_pnl", "avg_pf_grid_feasible_pnl",
            "dominant_loss_bucket",
        ]
        available = [c for c in display_cols if c in real_df.columns]
        st.dataframe(real_df[available], use_container_width=True, hide_index=True)

        # Narratives for non-NORMAL assets
        alerts = real_df[real_df["status_level"] != "NORMAL"]
        if not alerts.empty:
            st.subheader("Alerts and warnings")
            for _, row in alerts.iterrows():
                level = row.get("status_level", "UNKNOWN")
                color = _STATUS_COLORS.get(level, "grey")
                st.markdown(
                    f"**:{color}[{level}]** — {row.get('narrative', '')}",
                    unsafe_allow_html=False,
                )

    # --- Fragility narratives ---
    if not frag_df.empty:
        high_frag = frag_df[frag_df["fragility_level"].isin(["HIGH", "CRITICAL"])]
        if not high_frag.empty:
            st.subheader("High/critical fragility narratives")
            for _, row in high_frag.iterrows():
                level = row.get("fragility_level", "UNKNOWN")
                color = _FRAGILITY_COLORS.get(level, "grey")
                st.markdown(
                    f"**:{color}[{level}]** — {row.get('narrative', '')}",
                    unsafe_allow_html=False,
                )
