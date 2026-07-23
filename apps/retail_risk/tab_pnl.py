"""Tab 3 — Realised P&L: retail waterfall, per-customer margins, province/contract-type analysis."""
from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import text


def render_pnl(engine):
    """Render Realised P&L tab."""
    st.subheader("Realised P&L")

    col1, col2 = st.columns([2, 1])
    with col1:
        with engine.connect() as conn:
            customers = pd.read_sql(text("""
                SELECT id, name, province FROM marketdata.rm_customers
                WHERE status = 'active' ORDER BY name
            """), conn)
        if customers.empty:
            st.info("No active customers found.")
            return
        customer_id = st.selectbox(
            "Customer (for waterfall)",
            customers["id"].tolist(),
            format_func=lambda x: customers[customers["id"] == x]["name"].iloc[0],
            key="pnl_customer",
        )
    with col2:
        st.date_input("Date Range", value=[], key="pnl_dates")

    # Waterfall chart for selected customer
    with engine.connect() as conn:
        items_df = pd.read_sql(text("""
            SELECT si.category, SUM(si.amount_cny) AS total
            FROM marketdata.rm_retail_settlement_items si
            JOIN marketdata.rm_retail_settlements s ON s.id = si.settlement_id
            WHERE s.customer_id = :cid
            GROUP BY si.category
            ORDER BY total DESC
        """), conn, params={"cid": customer_id})

    if not items_df.empty:
        _render_waterfall(items_df, title="Retail P&L Waterfall")
    else:
        st.info("No P&L data yet for this customer. Upload settlements in Tab 2.")

    st.divider()

    # Per-customer margin breakdown
    st.subheader("Per-Customer Margin Breakdown")
    with engine.connect() as conn:
        margin_df = pd.read_sql(text("""
            SELECT
                c.name AS customer,
                c.province,
                cc.contract_type,
                SUM(si.amount_cny) FILTER (WHERE si.category = 'retail_revenue') AS revenue_cny,
                SUM(si.amount_cny) FILTER (WHERE si.category = 'energy_procurement') AS procurement_cny,
                SUM(si.amount_cny) FILTER (WHERE si.category = 'transmission_distribution') AS tnd_cny,
                SUM(si.amount_cny) FILTER (WHERE si.category = 'imbalance_penalty') AS penalty_cny,
                SUM(si.amount_cny) AS net_cny,
                SUM(si.volume_mwh) AS volume_mwh
            FROM marketdata.rm_customers c
            JOIN marketdata.rm_retail_settlements s ON s.customer_id = c.id
            JOIN marketdata.rm_retail_settlement_items si ON si.settlement_id = s.id
            LEFT JOIN marketdata.rm_customer_contracts cc
                ON cc.customer_id = c.id AND cc.contract_status = 'active'
            GROUP BY c.id, c.name, c.province, cc.contract_type
            ORDER BY net_cny DESC
        """), conn)

    if not margin_df.empty:
        margin_df["margin_cny_mwh"] = (
            margin_df["net_cny"] / margin_df["volume_mwh"].replace(0, float("nan"))
        ).round(2)
        st.dataframe(margin_df, use_container_width=True, hide_index=True)
        st.download_button("Export CSV", margin_df.to_csv(index=False), "retail_pnl.csv", "text/csv")
    else:
        st.info("No margin data available yet.")

    st.divider()

    # Per-province analysis
    st.subheader("P&L by Province")
    with engine.connect() as conn:
        province_df = pd.read_sql(text("""
            SELECT c.province,
                   SUM(si.amount_cny) FILTER (WHERE si.category = 'retail_revenue') AS revenue_cny,
                   SUM(si.amount_cny) FILTER (WHERE si.category = 'energy_procurement') AS procurement_cny,
                   SUM(si.amount_cny) AS net_cny,
                   COUNT(DISTINCT c.id) AS customer_count
            FROM marketdata.rm_customers c
            JOIN marketdata.rm_retail_settlements s ON s.customer_id = c.id
            JOIN marketdata.rm_retail_settlement_items si ON si.settlement_id = s.id
            GROUP BY c.province
            ORDER BY net_cny DESC
        """), conn)

    if not province_df.empty:
        st.dataframe(province_df, use_container_width=True, hide_index=True)

    # Per-contract-type analysis
    st.subheader("P&L by Contract Type")
    with engine.connect() as conn:
        ctype_df = pd.read_sql(text("""
            SELECT cc.contract_type,
                   SUM(si.amount_cny) FILTER (WHERE si.category = 'retail_revenue') AS revenue_cny,
                   SUM(si.amount_cny) AS net_cny,
                   COUNT(DISTINCT c.id) AS customer_count
            FROM marketdata.rm_customers c
            JOIN marketdata.rm_retail_settlements s ON s.customer_id = c.id
            JOIN marketdata.rm_retail_settlement_items si ON si.settlement_id = s.id
            LEFT JOIN marketdata.rm_customer_contracts cc
                ON cc.customer_id = c.id AND cc.contract_status = 'active'
            WHERE cc.contract_type IS NOT NULL
            GROUP BY cc.contract_type
            ORDER BY net_cny DESC
        """), conn)

    if not ctype_df.empty:
        st.dataframe(ctype_df, use_container_width=True, hide_index=True)
    elif margin_df.empty:
        st.info("No contract-type data available yet.")


def _render_waterfall(items_df: pd.DataFrame, title: str):
    """Render a Plotly waterfall chart from category/total dataframe."""
    categories = items_df["category"].tolist()
    values = items_df["total"].tolist()
    categories.append("Net Margin")
    values.append(sum(values))
    measures = ["relative"] * (len(categories) - 1) + ["total"]

    fig = go.Figure(go.Waterfall(
        orientation="v",
        measure=measures,
        x=categories,
        y=values,
        connector={"line": {"color": "rgb(63, 63, 63)"}},
        increasing={"marker": {"color": "#2ecc71"}},
        decreasing={"marker": {"color": "#e74c3c"}},
        totals={"marker": {"color": "#3498db"}},
    ))
    fig.update_layout(
        title=title,
        yaxis_title="CNY",
        showlegend=False,
        height=450,
    )
    st.plotly_chart(fig, use_container_width=True)
