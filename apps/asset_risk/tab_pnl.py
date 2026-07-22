"""Tab 3 — Realised P&L: waterfall chart + operational KPIs."""
from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import text


def render_pnl(engine):
    """Render Realised P&L tab."""
    st.subheader("Realised P&L")

    with engine.connect() as conn:
        books = pd.read_sql(text(
            "SELECT b.id, b.name, a.asset_type FROM marketdata.rm_books b "
            "LEFT JOIN marketdata.rm_assets a ON a.id = b.asset_id ORDER BY b.name"
        ), conn)

    if books.empty:
        st.warning("No books found.")
        return

    col1, col2 = st.columns([2, 1])
    with col1:
        book_id = st.selectbox("Book", books["id"].tolist(),
                               format_func=lambda x: books[books["id"] == x]["name"].iloc[0],
                               key="pnl_book")
    with col2:
        date_range = st.date_input("Date Range", value=[], key="pnl_dates")

    asset_type = books[books["id"] == book_id]["asset_type"].iloc[0] or "bess"

    with engine.connect() as conn:
        items_df = pd.read_sql(text("""
            SELECT si.category, SUM(si.amount_cny) as total
            FROM marketdata.rm_settlement_items si
            JOIN marketdata.rm_settlements s ON s.id = si.settlement_id
            WHERE s.book_id = :bid
            GROUP BY si.category
            ORDER BY total DESC
        """), conn, params={"bid": book_id})

    if items_df.empty:
        st.info("No P&L data yet. Upload settlements in Tab 2.")
        return

    # Waterfall chart
    categories = items_df["category"].tolist()
    values = items_df["total"].tolist()
    categories.append("Net P&L")
    values.append(sum(values))
    measures = ["relative"] * (len(categories) - 1) + ["total"]

    fig = go.Figure(go.Waterfall(
        orientation="v", measure=measures, x=categories, y=values,
        connector={"line": {"color": "rgb(63, 63, 63)"}},
        increasing={"marker": {"color": "#2ecc71"}},
        decreasing={"marker": {"color": "#e74c3c"}},
        totals={"marker": {"color": "#3498db"}},
    ))
    fig.update_layout(title=f"P&L Waterfall ({asset_type.upper()})",
                      yaxis_title="CNY", showlegend=False, height=450)
    st.plotly_chart(fig, use_container_width=True)

    # KPIs
    st.subheader("Operational KPIs")
    if asset_type == "bess":
        with engine.connect() as conn:
            ops = pd.read_sql(text("""
                SELECT dispatch_date, charge_mwh, discharge_mwh, cycle_count_day,
                       conversion_ratio, net_margin_cny
                FROM marketdata.rm_dispatch_daily dd
                JOIN marketdata.rm_books b ON b.asset_id = dd.asset_id
                WHERE b.id = :bid ORDER BY dispatch_date DESC LIMIT 30
            """), conn, params={"bid": book_id})
        if not ops.empty:
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total Discharge", f"{ops['discharge_mwh'].sum():,.1f} MWh")
            c2.metric("Total Charge", f"{ops['charge_mwh'].sum():,.1f} MWh")
            c3.metric("Avg Conversion", f"{ops['conversion_ratio'].mean():.2%}")
            c4.metric("Net Margin", f"¥{ops['net_margin_cny'].sum():,.0f}")
            st.dataframe(ops, use_container_width=True, hide_index=True)

    elif asset_type == "wind":
        with engine.connect() as conn:
            snapshots = pd.read_sql(text("""
                SELECT snapshot_date, realized_cny, curtailment_mwh,
                       curtailment_rate_pct, curtailment_opportunity_cost_cny, equivalent_hours
                FROM marketdata.rm_pnl_snapshots WHERE book_id = :bid
                ORDER BY snapshot_date DESC LIMIT 12
            """), conn, params={"bid": book_id})
        if not snapshots.empty:
            c1, c2, c3 = st.columns(3)
            c1.metric("Curtailment Rate", f"{snapshots['curtailment_rate_pct'].iloc[0]:.1%}"
                      if pd.notna(snapshots['curtailment_rate_pct'].iloc[0]) else "N/A")
            c2.metric("Curtailment Cost (YTD)", f"¥{snapshots['curtailment_opportunity_cost_cny'].sum():,.0f}")
            c3.metric("Equiv. Hours (YTD)", f"{snapshots['equivalent_hours'].sum():,.0f} h")

            fig = go.Figure()
            fig.add_trace(go.Scatter(x=snapshots["snapshot_date"], y=snapshots["curtailment_rate_pct"],
                                     mode="lines+markers", name="Curtailment Rate"))
            fig.add_hline(y=0.10, line_dash="dash", line_color="red", annotation_text="10% threshold")
            fig.update_layout(title="Monthly Curtailment Rate", yaxis_title="%", height=300)
            st.plotly_chart(fig, use_container_width=True)
