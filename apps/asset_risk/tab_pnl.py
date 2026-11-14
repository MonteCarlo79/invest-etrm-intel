"""Tab 3 — Realised P&L: single-book waterfall + KPIs, and portfolio view."""
from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import text

from libs.asset_risk.settlement.analytics import (  # noqa: F401  (re-exported for tests)
    ASSET_CLASS_OPTIONS, CATEGORY_CN, _REVENUE_CATS, _asset_month_metric, _asset_summary,
    _days_in_months, _load_settlement_items, _portfolio_matrix, book_matches_classes,
)

_CATEGORY_CN = CATEGORY_CN


def render_pnl(engine):
    """Render Realised P&L tab."""
    st.subheader("Realised P&L")

    with engine.connect() as conn:
        books = pd.read_sql(text(
            "SELECT b.id, b.name, b.book_type, a.asset_type, a.province FROM marketdata.rm_books b "
            "LEFT JOIN marketdata.rm_assets a ON a.id = b.asset_id ORDER BY b.name"
        ), conn)

    if books.empty:
        st.warning("No books found.")
        return

    # Province + asset class filters (user request 2026-08-27)
    provinces = sorted(p for p in books["province"].dropna().unique().tolist())
    fcol1, fcol2 = st.columns(2)
    with fcol1:
        sel_provinces = st.multiselect("Province", provinces, default=provinces, key="pnl_prov")
    with fcol2:
        sel_classes = st.multiselect("Asset class", ASSET_CLASS_OPTIONS,
                                     default=ASSET_CLASS_OPTIONS, key="pnl_class")

    all_prov_selected = len(sel_provinces) == len(provinces)
    mask = books.apply(
        lambda r: book_matches_classes(r["asset_type"], r["book_type"], sel_classes)
        and (r["province"] in sel_provinces or (pd.isna(r["province"]) and all_prov_selected)),
        axis=1,
    )
    filtered_books = books[mask]
    if filtered_books.empty:
        st.info("No books match the current filters.")
        return

    # Multi-select over the filtered set: one = single view, many = portfolio.
    # Key encodes the filter state so changing a filter resets to the full filtered set.
    filter_key = f"pnl_books_{len(sel_provinces)}p_{len(sel_classes)}c"
    selected_names = st.multiselect(
        "Book (select one for single view, multiple for portfolio)",
        filtered_books["name"].tolist(), default=filtered_books["name"].tolist(),
        key=filter_key,
    )
    if not selected_names:
        st.info("Select one or more books to see P&L.")
        return
    sel_ids = filtered_books[filtered_books["name"].isin(selected_names)]["id"].tolist()

    if len(sel_ids) > 1:
        _render_portfolio(engine, sel_ids)
        return

    book_id = sel_ids[0]
    asset_type = books[books["id"] == book_id]["asset_type"].iloc[0] or "bess"

    date_range = st.date_input("Date Range", value=[], key="pnl_dates")

    items_df = _load_settlement_items(engine, book_id, date_range)

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


def _render_portfolio(engine, book_ids: list[int] | None = None):
    """Portfolio view: KPI strip, asset×month matrix, waterfall, per-asset bars.

    book_ids: restrict to a subset of books (multi-select); None = all books.
    """
    query = """
        SELECT a.name AS asset, a.capacity_mw, a.bess_duration_h, a.asset_type,
               s.settlement_month, si.category, si.amount_cny, si.volume_mwh
        FROM marketdata.rm_settlement_items si
        JOIN marketdata.rm_settlements s ON s.id = si.settlement_id
        JOIN marketdata.rm_books b ON b.id = s.book_id
        JOIN marketdata.rm_assets a ON a.id = b.asset_id
    """
    params = {}
    if book_ids:
        query += " WHERE b.id = ANY(:ids)"
        params["ids"] = book_ids

    with engine.connect() as conn:
        items = pd.read_sql(text(query), conn, params=params)

    if items.empty:
        st.info("No settlement data yet.")
        return

    items["year"] = pd.to_datetime(items["settlement_month"]).dt.year
    years = sorted(items["year"].unique().tolist())
    sel_years = st.multiselect("Years", years, default=years, key="pnl_portfolio_years")
    items = items[items["year"].isin(sel_years)]
    if items.empty:
        st.info("No data for selected years.")
        return

    summary = _asset_summary(items)

    # --- KPI strip ---
    net = float(items["amount_cny"].sum())
    dis_vol = float(summary["discharge_mwh"].sum())
    chg_vol = float(summary["charge_mwh"].sum())
    arb_income = float(summary["arb_income"].sum())
    atype_map = (items.groupby("asset")["asset_type"].first().to_dict()
                 if "asset_type" in items.columns else {})
    wind_assets = [a for a, t in atype_map.items() if t == "wind"]
    bess_assets = [a for a, t in atype_map.items() if t != "wind"]
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("组合净利润", f"¥{net:,.0f}")
    c2.metric("总放电量", f"{dis_vol:,.0f} MWh")
    c3.metric("总充电量", f"{chg_vol:,.0f} MWh")
    c4.metric("组合套利价差", f"¥{arb_income / dis_vol:,.0f}/MWh" if dis_vol else "N/A")
    if wind_assets and not bess_assets:
        # Wind portfolio: 发电小时数 = total generation / total installed capacity
        cap_total = sum(float(c) for c in items.groupby("asset")["capacity_mw"].first().tolist()
                        if pd.notna(c) and c)
        c5.metric("组合发电小时数", f"{dis_vol / cap_total:,.1f} h" if cap_total else "N/A")
    else:
        # BESS (or mixed): RTE over BESS books only — wind charge is near zero
        rte = summary[summary["asset"].isin(bess_assets)] if bess_assets else summary
        rte_dis, rte_chg = float(rte["discharge_mwh"].sum()), float(rte["charge_mwh"].sum())
        c5.metric("组合转化率", f"{rte_dis / rte_chg:.1%}" if rte_chg else "N/A")

    # --- Asset × month matrix ---
    st.markdown("#### 资产 × 月度净利润")
    mat = _portfolio_matrix(items)
    st.dataframe(mat.style.format("¥{:,.0f}"), use_container_width=True)

    # --- Per-unit metric matrices ---
    for title, metric, fmt in [
        ("资产 × 日均充放次数", "日均充放次数", "{:.2f}"),
        ("资产 × 转化率 (RTE)", "转化率", "{:.1%}"),
        ("资产 × 度电容量补偿 (¥/MWh)", "容量补偿价差", "¥{:,.0f}"),
        ("资产 × 套利度电价差 (¥/MWh)", "套利价差", "¥{:,.0f}"),
    ]:
        m = _asset_month_metric(items, metric)
        if metric == "转化率":
            # Split by asset type: RTE% for BESS books, 发电小时数 for wind books
            b_cols = [c for c in m.columns if c in bess_assets]
            w_cols = [c for c in m.columns if c in wind_assets]
            if b_cols:
                st.markdown("#### 资产 × 转化率 (RTE)")
                st.dataframe(m[b_cols].style.format("{:.1%}", na_rep="—"), use_container_width=True)
            if w_cols:
                st.markdown("#### 资产 × 发电小时数 (h)")
                st.dataframe(m[w_cols].style.format("{:,.1f}", na_rep="—"), use_container_width=True)
            continue
        st.markdown(f"#### {title}")
        st.dataframe(m.style.format(fmt, na_rep="—"), use_container_width=True)

    # --- Portfolio waterfall ---
    st.markdown("#### 组合 P&L 瀑布")
    cat = items.groupby("category")["amount_cny"].sum().sort_values(ascending=False)
    labels = [_CATEGORY_CN.get(c, c) for c in cat.index] + ["净利润"]
    values = cat.tolist() + [float(cat.sum())]
    measures = ["relative"] * len(cat) + ["total"]
    fig = go.Figure(go.Waterfall(
        orientation="v", measure=measures, x=labels, y=values,
        connector={"line": {"color": "rgb(63, 63, 63)"}},
        increasing={"marker": {"color": "#2ecc71"}},
        decreasing={"marker": {"color": "#e74c3c"}},
        totals={"marker": {"color": "#3498db"}},
    ))
    fig.update_layout(yaxis_title="CNY", showlegend=False, height=450)
    st.plotly_chart(fig, use_container_width=True)

    # --- Per-asset comparison bars ---
    st.markdown("#### 分资产对比")
    comp = summary.sort_values("net_profit", ascending=False)
    col_l, col_m, col_r = st.columns(3)
    with col_l:
        fig_n = go.Figure(go.Bar(
            x=comp["asset"], y=comp["net_profit"],
            text=[f"{v/1e6:,.0f}M" for v in comp["net_profit"]],
            textposition="auto", marker_color="#3498db"))
        fig_n.update_layout(title="净利润 by 资产", yaxis_title="CNY", height=350,
                            showlegend=False)
        st.plotly_chart(fig_n, use_container_width=True)
    with col_m:
        comp_s = comp.dropna(subset=["arb_spread"]).sort_values("arb_spread", ascending=False)
        if not comp_s.empty:
            fig_s = go.Figure(go.Bar(
                x=comp_s["asset"], y=comp_s["arb_spread"],
                text=[f"¥{v:.0f}" for v in comp_s["arb_spread"]],
                textposition="auto", marker_color="#2ecc71"))
            fig_s.update_layout(title="套利价差 by 资产", yaxis_title="¥/MWh",
                                height=350, showlegend=False)
            st.plotly_chart(fig_s, use_container_width=True)
    with col_r:
        comp_c = comp.dropna(subset=["cycles_per_day"]).sort_values("cycles_per_day", ascending=False)
        if not comp_c.empty:
            fig_c = go.Figure(go.Bar(
                x=comp_c["asset"], y=comp_c["cycles_per_day"],
                text=[f"{v:.2f}" for v in comp_c["cycles_per_day"]],
                textposition="auto", marker_color="#e67e22"))
            fig_c.update_layout(title="日均充放次数 by 资产", yaxis_title="次/天",
                                height=350, showlegend=False)
            st.plotly_chart(fig_c, use_container_width=True)
