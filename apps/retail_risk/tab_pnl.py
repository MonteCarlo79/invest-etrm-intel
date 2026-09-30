# apps/retail_risk/tab_pnl.py
"""Realised P&L (Goal 2): 批零价差 cards, bridge waterfall, channel alpha vs spot,
trader/sales attribution, YTD trend. 复盘-aligned."""
from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import text

from services.retail_risk import pnl_bridge as pb


def render_pnl(engine):
    st.subheader("Realised P&L — 批零价差 & Source Breakdown")

    with engine.connect() as conn:
        books = pd.read_sql(text(
            "SELECT id, name FROM marketdata.rm_books WHERE book_type = 'load' ORDER BY name"
        ), conn)
    if books.empty:
        st.info("No load books yet.")
        return

    col1, col2 = st.columns(2)
    with col1:
        book_id = st.selectbox("Book", books["id"].tolist(),
                               format_func=lambda x: books[books["id"] == x]["name"].iloc[0],
                               key="pnl_book")
    with engine.connect() as conn:
        months = pd.read_sql(text("""
            SELECT DISTINCT settlement_month FROM marketdata.rm_settlements
            WHERE book_id = :b ORDER BY settlement_month DESC
        """), conn, params={"b": book_id})
    if months.empty:
        st.info("No settlement data for this book yet.")
        return
    with col2:
        month = st.selectbox("Month", [m for m in months["settlement_month"]], key="pnl_month")

    with engine.connect() as conn:
        bridge = pb.bridge_month(conn, book_id, month)
        alpha = pb.channel_alpha(conn, book_id, month)
        attrib = pb.attribution_month(conn, book_id, month)

    # --- 批零价差 cards
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("零售结算均价", _fmt(bridge.retail_avg_price, " ¥/MWh"))
    c2.metric("批发结算均价", _fmt(bridge.wholesale_avg_cost, " ¥/MWh"))
    c3.metric("批零价差", _fmt(bridge.spread, " ¥/MWh"))
    c4.metric("净毛利", _fmt(bridge.net, " ¥"))

    # --- bridge waterfall
    if bridge.retail_revenue is not None:
        items = [("零售收入", bridge.retail_revenue)]
        items += [("中长期采购", -bridge.channel_costs.get("midlong", 0.0))]
        if bridge.channel_costs.get("green_premium"):
            items.append(("绿电溢价", -bridge.channel_costs["green_premium"]))
        items += [("现货结算", -bridge.spot_cost), ("偏差/考核", -bridge.deviation),
                  ("附加/分摊", -bridge.other)]
        _render_waterfall(pd.DataFrame(items, columns=["category", "total"]),
                          title=f"P&L Bridge — {month}")
    else:
        st.info("Margin N/A for this month — invoice carries no 收益 margin "
                "(山东 7021 is a spot-side bill; the monthly margin PDF is P2). "
                "Cost lines shown below.")

    # --- channel alpha
    st.subheader("Channel Alpha vs Spot (降本/增支)")
    if alpha.empty:
        st.info("No positions or spot data for this month.")
    else:
        st.dataframe(alpha.style.format({
            "volume_mwh": "{:,.1f}", "vwap": "{:,.1f}", "spot_vwap": "{:,.1f}",
            "alpha_cny_mwh": "{:+,.1f}", "alpha_cny": "{:+,.0f}"}),
            use_container_width=True, hide_index=True)

    # --- trader / sales attribution
    st.subheader("Trader / Sales Attribution")
    t = attrib["trader_by_channel"]
    if t.empty or t["mkt_avg"].isna().all():
        st.info("No market benchmark for this month (信息汇总 not ingested or stale).")
    else:
        st.dataframe(t.style.format({"volume_mwh": "{:,.1f}", "vwap": "{:,.1f}",
                                     "mkt_avg": "{:,.1f}", "trader_alpha_cny": "{:+,.0f}"}),
                     use_container_width=True, hide_index=True)
        a1, a2, a3, a4 = st.columns(4)
        a1.metric("Trader alpha", _fmt(attrib["trader_alpha_total"], " ¥", signed=True))
        a2.metric("渠道费用", _fmt(attrib["channel_fee_cny"], " ¥"))
        a3.metric("Sales alpha", _fmt(attrib["sales_alpha_cny"], " ¥", signed=True))
        a4.metric("Identity residual", _fmt(attrib["identity_residual_cny"], " ¥", signed=True))
        st.caption(f"Blended wholesale benchmark: {attrib['blended_benchmark']:.1f} ¥/MWh. "
                   "Trader + Sales = 批零价差 net of 渠道费; residual = spot/deviation not in the pivot."
                   if attrib["blended_benchmark"] else "Blended benchmark N/A")

    # --- YTD trend
    st.subheader("月度盈亏 YTD")
    with engine.connect() as conn:
        snap = pd.read_sql(text("""
            SELECT snapshot_date, realized_cny, bilateral_pnl_cny, spot_pnl_cny,
                   deviation_pnl_cny, other_pnl_cny, unrealized_mtm_cny
            FROM marketdata.rm_pnl_snapshots
            WHERE book_id = :b ORDER BY snapshot_date
        """), conn, params={"b": book_id})
    if not snap.empty:
        st.line_chart(snap.set_index("snapshot_date")[["realized_cny", "unrealized_mtm_cny"]])
    else:
        st.info("No snapshots yet — run the engines (run_backfill or in-app compute).")


def _fmt(v, suffix: str, signed: bool = False) -> str:
    if v is None or pd.isna(v):
        return "N/A"
    return f"{'+' if signed and v >= 0 else ''}{v:,.1f}{suffix}"


def _render_waterfall(items_df: pd.DataFrame, title: str):
    categories = items_df["category"].tolist()
    values = items_df["total"].tolist()
    categories.append("Net Margin")
    values.append(sum(values))
    measures = ["relative"] * (len(categories) - 1) + ["total"]
    fig = go.Figure(go.Waterfall(
        orientation="v", measure=measures, x=categories, y=values,
        connector={"line": {"color": "rgb(63, 63, 63)"}},
        increasing={"marker": {"color": "#2ecc71"}},
        decreasing={"marker": {"color": "#e74c3c"}},
        totals={"marker": {"color": "#3498db"}},
    ))
    fig.update_layout(title=title, yaxis_title="CNY", showlegend=False, height=450)
    st.plotly_chart(fig, use_container_width=True)
