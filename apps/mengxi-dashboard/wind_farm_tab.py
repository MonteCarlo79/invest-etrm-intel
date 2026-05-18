# -*- coding: utf-8 -*-
"""
Wind Farm Performance Ranking tab — Mengxi province all-wind-farm view.

Queries md_id_cleared_energy directly (no separate pipeline), classifies
plants via infer_asset_type(), and ranks wind farms by total generation,
revenue, and average clearing price over the selected period.
"""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import text

from inner_mongolia_helpers import infer_asset_type


@st.cache_data(ttl=300, show_spinner=False)
def _load_wind_farm_data(start: str, end: str) -> pd.DataFrame:
    """
    Aggregate md_id_cleared_energy for wind farms over [start, end).
    Returns one row per plant_name with total_gen_mwh, total_revenue, avg_price,
    inferred_capacity_mw (MAX dispatch * 4 for 15-min), days_active.
    """
    import os
    from sqlalchemy import create_engine as _ce
    url = os.environ.get("PGURL") or os.environ.get("DB_DSN")
    engine = _ce(url, pool_pre_ping=True)
    with engine.connect() as conn:
        df = pd.read_sql(
            text("""
                SELECT plant_name, dispatch_unit_name, datetime,
                       cleared_energy_mwh, cleared_price
                FROM marketdata.md_id_cleared_energy
                WHERE datetime >= :start AND datetime < :end
            """),
            conn,
            params={"start": start, "end": end},
        )
    if df.empty:
        return pd.DataFrame()

    # Classify asset type
    df["asset_type"] = df["dispatch_unit_name"].apply(infer_asset_type)
    wind_df = df[df["asset_type"] == "wind"].copy()
    if wind_df.empty:
        return pd.DataFrame()

    # Aggregate (generation = positive energy)
    wind_df["gen_mwh"]     = wind_df["cleared_energy_mwh"].clip(lower=0)
    wind_df["revenue_cny"] = wind_df["gen_mwh"] * wind_df["cleared_price"]

    agg = (
        wind_df.groupby("plant_name").agg(
            dispatch_unit_name=("dispatch_unit_name", "first"),
            total_gen_mwh=("gen_mwh", "sum"),
            total_revenue_cny=("revenue_cny", "sum"),
            max_dispatch_mwh_15min=("cleared_energy_mwh", "max"),
            days_active=("datetime", lambda x: x.dt.date.nunique()),
        )
        .reset_index()
    )

    # 15-min → MW capacity estimate (÷ 4 missing: max_dispatch is already in MWh per 15 min,
    # × 4 gives hourly equivalent MW)
    agg["inferred_capacity_mw"] = (agg["max_dispatch_mwh_15min"] * 4).round(1)
    agg["avg_price_cny_mwh"] = (
        agg["total_revenue_cny"] / agg["total_gen_mwh"].replace(0, float("nan"))
    ).round(2)

    # Scale to practical units
    agg["total_gen_gwh"]        = (agg["total_gen_mwh"] / 1000).round(3)
    agg["total_revenue_万元"]    = (agg["total_revenue_cny"] / 10000).round(2)

    agg = agg.sort_values("total_gen_gwh", ascending=False).reset_index(drop=True)
    agg.index += 1  # 1-based rank

    return agg


def render(engine) -> None:
    """Render the Wind Farm Ranking tab. Call from app.py with the SQLAlchemy engine."""
    st.subheader("Inner Mongolia — Wind Farm Performance Ranking")
    st.caption(
        "Ranks all wind farm plants in `marketdata.md_id_cleared_energy` by generation "
        "volume, revenue, and average clearing price over the selected period."
    )

    # ── Date range ────────────────────────────────────────────────────────────
    today = date.today()
    dc1, dc2, dc3 = st.columns([2, 2, 4])
    start_date = dc1.date_input(
        "Start date", value=today - timedelta(days=30), key="wind_rank_start"
    )
    end_date = dc2.date_input(
        "End date (exclusive)", value=today, key="wind_rank_end"
    )

    if start_date >= end_date:
        st.error("End date must be after start date.")
        return

    start = str(start_date)
    end   = str(end_date)

    with st.spinner("Loading wind farm data…"):
        df = _load_wind_farm_data(start, end)

    if df.empty:
        st.info(
            "No wind farm data found for this period. "
            "Wind farms are identified by keywords in dispatch_unit_name: 风电场, 风储, 风场."
        )
        return

    # ── Summary metrics ───────────────────────────────────────────────────────
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Wind farms", f"{len(df)}")
    m2.metric("Total generation", f"{df['total_gen_gwh'].sum():,.1f} GWh")
    m3.metric("Total revenue", f"¥{df['total_revenue_万元'].sum():,.0f}万")
    m4.metric("Avg clearing price", f"¥{df['avg_price_cny_mwh'].mean():,.1f}/MWh")

    # ── Table ─────────────────────────────────────────────────────────────────
    display_df = df[[
        "plant_name", "dispatch_unit_name",
        "total_gen_gwh", "total_revenue_万元", "avg_price_cny_mwh",
        "inferred_capacity_mw", "days_active",
    ]].copy()
    display_df.columns = [
        "Plant", "Dispatch Unit",
        "Generation (GWh)", "Revenue (万元)", "Avg Price (¥/MWh)",
        "Est. Capacity (MW)", "Active Days",
    ]

    st.dataframe(
        display_df.style.format({
            "Generation (GWh)":    "{:,.3f}",
            "Revenue (万元)":       "{:,.2f}",
            "Avg Price (¥/MWh)":   "{:,.2f}",
            "Est. Capacity (MW)":  "{:,.1f}",
        }),
        use_container_width=True,
        height=min(38 * len(display_df) + 38, 600),
    )

    # ── Bar chart — top 20 by generation ─────────────────────────────────────
    top20 = df.head(20)
    if not top20.empty:
        st.markdown("**Top 20 Wind Farms by Generation**")
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=top20["plant_name"],
            y=top20["total_gen_gwh"],
            name="Generation (GWh)",
            marker_color="#2ca02c",
            hovertemplate="%{x}<br>Generation: %{y:,.3f} GWh<extra></extra>",
        ))
        fig.update_layout(
            height=400,
            margin=dict(l=10, r=10, t=10, b=100),
            xaxis_tickangle=-45,
            yaxis_title="Generation (GWh)",
            plot_bgcolor="white",
            paper_bgcolor="white",
        )
        st.plotly_chart(fig, use_container_width=True, key="wind_rank_bar")

    # ── CSV export ────────────────────────────────────────────────────────────
    st.download_button(
        "Download CSV",
        display_df.to_csv(index=True).encode("utf-8"),
        file_name=f"wind_farm_ranking_{start}_{end}.csv",
        mime="text/csv",
        key="wind_rank_dl",
    )
