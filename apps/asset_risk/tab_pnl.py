"""Tab 3 — Realised P&L: single-book waterfall + KPIs, and portfolio view."""
from __future__ import annotations

import calendar

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import text

_CATEGORY_CN = {
    "charge_energy": "充电电费",
    "discharge_energy": "放电收入",
    "capacity_compensation": "容量补偿/非市场化",
    "transmission": "输配电费",
    "system_operation": "上网线损费",
    "coal_capacity_charge": "系统运行费",
    "basic_fee": "基本电费/力调",
    "govt_surcharges": "政府基金及附加",
    "frequency": "调频",
    "penalty": "偏差费用",
    "subsidy": "补贴",
    "rebate": "返还",
    "generation_revenue": "发电收入",
    "other": "其他",
}

_REVENUE_CATS = ("discharge_energy", "generation_revenue")


def _portfolio_matrix(items_df: pd.DataFrame) -> pd.DataFrame:
    """Month × asset net-profit matrix with 组合合计 column and 资产合计 row."""
    df = items_df.copy()
    df["month"] = pd.to_datetime(df["settlement_month"]).dt.strftime("%Y-%m")
    mat = df.pivot_table(index="month", columns="asset", values="amount_cny",
                         aggfunc="sum", fill_value=0)
    mat = mat.sort_index()
    mat["组合合计"] = mat.sum(axis=1)
    total_row = mat.sum(axis=0)
    total_row.name = "资产合计"
    return pd.concat([mat, total_row.to_frame().T])


def _days_in_months(month_series) -> int:
    """Total calendar days across the distinct months present in the series."""
    months = pd.to_datetime(month_series).dt.to_period("M").unique()
    return sum(calendar.monthrange(p.year, p.month)[1] for p in months)


def _asset_summary(items_df: pd.DataFrame) -> pd.DataFrame:
    """Per-asset realised P&L summary.

    Returns DataFrame with columns: asset, net_profit, discharge_mwh,
    charge_mwh, arb_income, arb_spread (arb_income / discharge_mwh; None when
    no discharge volume), cycles_per_day (charge_mwh / energy_per_cycle / days;
    None when capacity data or days are unavailable).
    """
    rows = []
    for asset, g in items_df.groupby("asset"):
        dis_vol = float(g.loc[g["category"].isin(_REVENUE_CATS), "volume_mwh"].sum())
        chg_vol = float(g.loc[g["category"] == "charge_energy", "volume_mwh"].sum())
        rev = float(g.loc[g["category"].isin(_REVENUE_CATS), "amount_cny"].sum())
        cost = float(g.loc[g["category"] == "charge_energy", "amount_cny"].sum())
        arb_income = rev + cost

        # Daily average cycles over the months present (needs capacity x duration)
        energy = None
        if "capacity_mw" in g.columns:
            cap = g["capacity_mw"].iloc[0]
            dur = g["bess_duration_h"].iloc[0] if "bess_duration_h" in g.columns else None
            if pd.notna(cap) and cap:
                dur_val = float(dur) if (dur is not None and pd.notna(dur) and dur) else 4.0
                energy = float(cap) * dur_val
        days = _days_in_months(g["settlement_month"])
        cycles = (chg_vol / energy / days) if (energy and days) else None

        rows.append({
            "asset": asset,
            "net_profit": float(g["amount_cny"].sum()),
            "discharge_mwh": dis_vol,
            "charge_mwh": chg_vol,
            "arb_income": arb_income,
            "arb_spread": arb_income / dis_vol if dis_vol else None,
            "cycles_per_day": cycles,
        })
    return pd.DataFrame(rows)


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

    # Multi-select: all books = portfolio; any subset = filtered portfolio;
    # exactly one = single-book view (user request 2026-08-22)
    selected_names = st.multiselect(
        "Book (select one for single view, multiple for portfolio)",
        books["name"].tolist(), default=books["name"].tolist(), key="pnl_books",
    )
    if not selected_names:
        st.info("Select one or more books to see P&L.")
        return
    sel_ids = books[books["name"].isin(selected_names)]["id"].tolist()

    if len(sel_ids) > 1:
        _render_portfolio(engine, sel_ids)
        return

    book_id = sel_ids[0]
    asset_type = books[books["id"] == book_id]["asset_type"].iloc[0] or "bess"

    date_range = st.date_input("Date Range", value=[], key="pnl_dates")

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


def _asset_month_metric(items_df: pd.DataFrame, metric: str) -> pd.DataFrame:
    """Month × asset matrix for a per-unit metric, with per-year YTD rows.

    metric: "容量补偿价差" (capcomp ¥/MWh), "套利价差" (arb ¥/MWh),
            "日均充放次数" (cycles/day via capacity × duration and calendar days),
            "转化率" (round-trip efficiency = discharge / charge).

    YTD rows are volume-weighted aggregates (sum of numerators ÷ sum of
    denominators over the year's months), NOT averages of monthly ratios.
    """
    df = items_df.copy()
    df["month"] = pd.to_datetime(df["settlement_month"]).dt.strftime("%Y-%m")
    rows = []
    ytd = {}  # (asset, year) -> accumulators

    for (asset, month), g in df.groupby(["asset", "month"]):
        dis_vol = float(g.loc[g["category"].isin(_REVENUE_CATS), "volume_mwh"].sum())
        chg_vol = float(g.loc[g["category"] == "charge_energy", "volume_mwh"].sum())
        cap_amt = float(g.loc[g["category"] == "capacity_compensation", "amount_cny"].sum())
        rev = float(g.loc[g["category"].isin(_REVENUE_CATS), "amount_cny"].sum())
        cost = float(g.loc[g["category"] == "charge_energy", "amount_cny"].sum())

        cap = g["capacity_mw"].iloc[0] if "capacity_mw" in g.columns else None
        dur = g["bess_duration_h"].iloc[0] if "bess_duration_h" in g.columns else None
        energy = None
        if pd.notna(cap) and cap:
            energy = float(cap) * (float(dur) if (dur is not None and pd.notna(dur) and dur) else 4.0)
        days = calendar.monthrange(int(month[:4]), int(month[5:7]))[1]

        # Monthly value
        if metric == "容量补偿价差":
            val = cap_amt / dis_vol if dis_vol else None
        elif metric == "套利价差":
            val = (rev + cost) / dis_vol if dis_vol else None
        elif metric == "转化率":
            val = dis_vol / chg_vol if chg_vol else None
        else:  # 日均充放次数
            val = chg_vol / energy / days if (energy and days) else None
        rows.append({"asset": asset, "month": month, "value": val})

        # Accumulate for YTD
        acc = ytd.setdefault((asset, month[:4]), {
            "dis_vol": 0.0, "chg_vol": 0.0, "cap_amt": 0.0,
            "rev": 0.0, "cost": 0.0, "days": 0, "energy": energy,
        })
        acc["dis_vol"] += dis_vol
        acc["chg_vol"] += chg_vol
        acc["cap_amt"] += cap_amt
        acc["rev"] += rev
        acc["cost"] += cost
        acc["days"] += days

    mat = pd.DataFrame(rows).pivot_table(index="month", columns="asset",
                                         values="value", aggfunc="first")
    if mat.empty:
        return mat

    # YTD rows: sum of numerators ÷ sum of denominators
    ytd_rows = {}
    for (asset, year), acc in ytd.items():
        if metric == "容量补偿价差":
            val = acc["cap_amt"] / acc["dis_vol"] if acc["dis_vol"] else None
        elif metric == "套利价差":
            val = (acc["rev"] + acc["cost"]) / acc["dis_vol"] if acc["dis_vol"] else None
        elif metric == "转化率":
            val = acc["dis_vol"] / acc["chg_vol"] if acc["chg_vol"] else None
        else:  # 日均充放次数
            val = (acc["chg_vol"] / acc["energy"] / acc["days"]
                   if (acc["energy"] and acc["days"]) else None)
        if val is not None:
            ytd_rows.setdefault(f"{year} YTD", {})[asset] = val

    # Assemble: each year's months followed by its YTD row
    years = sorted({m[:4] for m in mat.index})
    parts = []
    for year in years:
        months = [m for m in mat.index if m.startswith(year)]
        parts.append(mat.loc[months])
        label = f"{year} YTD"
        if label in ytd_rows and ytd_rows[label]:
            parts.append(pd.DataFrame([ytd_rows[label]], index=[label]))
    return pd.concat(parts)


def _render_portfolio(engine, book_ids: list[int] | None = None):
    """Portfolio view: KPI strip, asset×month matrix, waterfall, per-asset bars.

    book_ids: restrict to a subset of books (multi-select); None = all books.
    """
    query = """
        SELECT a.name AS asset, a.capacity_mw, a.bess_duration_h,
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
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("组合净利润", f"¥{net:,.0f}")
    c2.metric("总放电量", f"{dis_vol:,.0f} MWh")
    c3.metric("总充电量", f"{chg_vol:,.0f} MWh")
    c4.metric("组合套利价差", f"¥{arb_income / dis_vol:,.0f}/MWh" if dis_vol else "N/A")
    c5.metric("组合转化率", f"{dis_vol / chg_vol:.1%}" if chg_vol else "N/A")

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
        st.markdown(f"#### {title}")
        m = _asset_month_metric(items, metric)
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
