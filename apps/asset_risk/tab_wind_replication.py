# -*- coding: utf-8 -*-
"""零碳46 (悦盛昌渠) wind asset views for Tab 7 (Dispatch Diagnostics) and
Tab 8 (收益瀑布 P&L Waterfall).

Wind has no BESS-style 申报→出清 dispatch chain — its diagnostics are the
ID-cleared schedule × RT nodal price, and its waterfall is the monthly
settlement cascade replicated from market data + trades vs book 6 bills:

    现货电能价值 (Σ gen×RT, bottom-up) + 合约差价 (bill-implied)
    + 绿电溢价 (trades) − 储能分摊/调频/其他费用 (bill) = 账单净额

Data: marketdata.wind_dispatch_15min, marketdata.wind_settlement_monthly,
marketdata.rm_settlement_items — built by services/wind_settlement/.
"""
from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import text

PLANT = "悦盛昌渠风光储电站"
ASSET = "新_悦盛昌渠#1期"
BOOK_ID = 6
WIND_LABEL = "零碳46风电(悦盛昌渠)"

_GREEN = "#2ecc71"
_RED = "#e74c3c"
_BLUE = "#3498db"
_GREY = "#95a5a6"


# ── data loaders (cached) ────────────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner=False)
def _load_dispatch(_engine, start: str, end: str) -> pd.DataFrame:
    q = text("""
        SELECT datetime, gen_mw, gen_mwh, rt_price
        FROM marketdata.wind_dispatch_15min
        WHERE plant_name = :plant AND datetime >= :start AND datetime < :end
        ORDER BY datetime
    """)
    return pd.read_sql(q, _engine, params={"plant": PLANT, "start": start, "end": end})


@st.cache_data(ttl=300, show_spinner=False)
def _load_dispatch_range(_engine) -> tuple[pd.Timestamp, pd.Timestamp]:
    with _engine.connect() as conn:
        row = conn.execute(text(
            "SELECT MIN(datetime), MAX(datetime) FROM marketdata.wind_dispatch_15min WHERE plant_name = :p"
        ), {"p": PLANT}).fetchone()
    return row[0], row[1]


@st.cache_data(ttl=300, show_spinner=False)
def _load_monthly(_engine) -> pd.DataFrame:
    q = text("""
        SELECT * FROM marketdata.wind_settlement_monthly
        WHERE asset_name = :asset ORDER BY settle_month
    """)
    return pd.read_sql(q, _engine, params={"asset": ASSET})


@st.cache_data(ttl=300, show_spinner=False)
def _load_bill_items(_engine, month: str) -> pd.DataFrame:
    q = text("""
        SELECT i.category, i.volume_mwh, i.price_cny_kwh, i.amount_cny, i.notes
        FROM marketdata.rm_settlement_items i
        JOIN marketdata.rm_settlements s ON i.settlement_id = s.id
        WHERE s.book_id = :book AND s.settlement_month = :month
    """)
    return pd.read_sql(q, _engine, params={"book": BOOK_ID, "month": f"{month}-01"})


# ── Tab 7 wind mode: Dispatch Diagnostics ────────────────────────────────────

def render_wind_diagnostics(engine) -> None:
    st.markdown("### 零碳46风电（悦盛昌渠）— 日内出清 × 实时节点价")

    try:
        dmin, dmax = _load_dispatch_range(engine)
    except Exception as exc:
        st.error(f"wind_dispatch_15min not ready: {exc}")
        return
    if dmin is None:
        st.warning("No dispatch data — run services/wind_settlement/backfill_dispatch.py")
        return

    c1, c2 = st.columns(2)
    start = c1.date_input("From", value=max(dmin.date(), pd.Timestamp("2026-06-01").date()),
                          min_value=dmin.date(), max_value=dmax.date(), key="wd_diag_start")
    end = c2.date_input("To", value=dmax.date(), min_value=dmin.date(), max_value=dmax.date(),
                        key="wd_diag_end")
    if start > end:
        st.error("From must be ≤ To")
        return

    df = _load_dispatch(engine, str(start), str(end + pd.Timedelta(days=1)))
    if df.empty:
        st.warning("No rows in range")
        return

    gen = df["gen_mwh"].sum()
    priced = df.dropna(subset=["rt_price"])
    cap = (priced["gen_mwh"] * priced["rt_price"]).sum() / priced["gen_mwh"].sum() if priced["gen_mwh"].sum() else None
    rt_avg = priced["rt_price"].mean() if not priced.empty else None
    neg_h = (priced["rt_price"] < 0).sum() / 4.0 if not priced.empty else 0.0

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("上网电量 (proxy)", f"{gen:,.0f} MWh")
    k2.metric("捕获价", f"{cap:,.1f} 元/MWh" if cap is not None else "—")
    k3.metric("RT 均价", f"{rt_avg:,.1f} 元/MWh" if rt_avg is not None else "—")
    k4.metric("捕获率", f"{cap / rt_avg:.1%}" if cap and rt_avg else "—")
    k5.metric("负电价时段", f"{neg_h:,.1f} h")

    daily = df.assign(date=df["datetime"].dt.date).groupby("date").apply(
        lambda g: pd.Series({
            "gen_mwh": g["gen_mwh"].sum(),
            "capture": (g["gen_mwh"] * g["rt_price"]).sum() / g["gen_mwh"].sum()
            if g["gen_mwh"].sum() and g["rt_price"].notna().any() else None,
            "rt_avg": g["rt_price"].mean(),
        }), include_groups=False).reset_index()

    fig = go.Figure()
    fig.add_trace(go.Bar(x=daily["date"], y=daily["gen_mwh"], name="日上网电量 (MWh)",
                         marker_color=_BLUE, yaxis="y"))
    fig.add_trace(go.Scatter(x=daily["date"], y=daily["capture"], name="日捕获价 (元/MWh)",
                             mode="lines", line=dict(color=_GREEN, width=2), yaxis="y2"))
    fig.add_trace(go.Scatter(x=daily["date"], y=daily["rt_avg"], name="RT 均价 (元/MWh)",
                             mode="lines", line=dict(color=_GREY, width=1, dash="dot"), yaxis="y2"))
    fig.update_layout(
        height=380, margin=dict(l=10, r=10, t=30, b=10),
        yaxis=dict(title="MWh", showgrid=False),
        yaxis2=dict(title="元/MWh", overlaying="y", side="right", showgrid=True, gridcolor="#f0f0f0"),
        legend=dict(orientation="h", y=1.12),
        plot_bgcolor="white", paper_bgcolor="white",
    )
    st.plotly_chart(fig, use_container_width=True)

    sel_day = st.date_input("日内明细 (15-min)", value=end, min_value=dmin.date(),
                            max_value=dmax.date(), key="wd_diag_day")
    day = df[df["datetime"].dt.date == sel_day]
    if day.empty:
        st.caption("所选日期无数据")
    else:
        fig2 = go.Figure()
        fig2.add_trace(go.Bar(x=day["datetime"], y=day["gen_mw"], name="出清 (MW)",
                              marker_color=_BLUE, yaxis="y"))
        fig2.add_trace(go.Scatter(x=day["datetime"], y=day["rt_price"], name="RT 节点价 (元/MWh)",
                                  mode="lines", line=dict(color=_RED, width=1.5), yaxis="y2"))
        fig2.update_layout(
            height=340, margin=dict(l=10, r=10, t=30, b=10),
            yaxis=dict(title="MW", showgrid=False),
            yaxis2=dict(title="元/MWh", overlaying="y", side="right", showgrid=True, gridcolor="#f0f0f0"),
            legend=dict(orientation="h", y=1.12),
            plot_bgcolor="white", paper_bgcolor="white",
        )
        st.plotly_chart(fig2, use_container_width=True)

    prof = df.assign(hour=df["datetime"].dt.hour).groupby("hour").agg(
        gen_mwh=("gen_mwh", "mean"), rt=("rt_price", "mean")).reset_index()
    fig3 = go.Figure()
    fig3.add_trace(go.Bar(x=prof["hour"], y=prof["gen_mwh"], name="平均出力 (MWh/15min)",
                          marker_color=_GREEN, yaxis="y"))
    fig3.add_trace(go.Scatter(x=prof["hour"], y=prof["rt"], name="平均 RT 价 (元/MWh)",
                              mode="lines+markers", line=dict(color=_RED, width=2), yaxis="y2"))
    fig3.update_layout(
        height=300, margin=dict(l=10, r=10, t=30, b=10),
        xaxis=dict(title="小时", dtick=1),
        yaxis=dict(title="MWh", showgrid=False),
        yaxis2=dict(title="元/MWh", overlaying="y", side="right", showgrid=True, gridcolor="#f0f0f0"),
        legend=dict(orientation="h", y=1.15),
        plot_bgcolor="white", paper_bgcolor="white",
    )
    st.plotly_chart(fig3, use_container_width=True)


# ── Tab 8 wind mode: 收益瀑布 ────────────────────────────────────────────────

def render_wind_waterfall(engine) -> None:
    from services.wind_settlement.waterfall import settle_price, waterfall_components

    st.markdown("### 零碳46风电（悦盛昌渠）— 结算复制 vs 账单")

    monthly = _load_monthly(engine)
    if monthly.empty:
        st.warning("No replication results — run services/wind_settlement/report.py --persist")
        return

    months = [pd.Timestamp(m).strftime("%Y-%m") for m in monthly["settle_month"]]
    sel = st.selectbox("结算月", months, index=len(months) - 1, key="ww_month")
    row = monthly[monthly["settle_month"].apply(lambda m: pd.Timestamp(m).strftime("%Y-%m")) == sel].iloc[0]
    bill = _load_bill_items(engine, sel)

    cfd_implied = float(row["implied_cfd_cny"]) if pd.notna(row["implied_cfd_cny"]) else 0.0
    cfd_model = row["cfd_sys_cny"] if pd.notna(row.get("cfd_sys_cny")) else row.get("cfd_west_cny")
    comp = waterfall_components(
        bill,
        spot_value=row["spot_value_cny"] or 0.0,
        cfd=cfd_implied,
        green=row["green_cny"] or 0.0,
    )
    replicated_total = sum(v for _, v in comp)
    bill_total = float(row["bill_total_cny"])
    residual = bill_total - replicated_total

    green_acc = (row["green_cny"] / row["bill_green_cny"]) if row["bill_green_cny"] else None
    cfd_acc = (cfd_model / cfd_implied) if (cfd_implied and cfd_model is not None and pd.notna(cfd_model)) else None
    vol_ratio = (row["gen_proxy_mwh"] / row["bill_vol_mwh"]) if row["bill_vol_mwh"] else None

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("账单总额", f"¥{bill_total:,.0f}")
    k2.metric("复制总额", f"¥{replicated_total:,.0f}")
    k3.metric("残差", f"¥{residual:,.0f}", delta=f"{residual / bill_total:.1%}" if bill_total else None,
              delta_color="inverse")
    spot_rows = bill[(bill["category"] == "discharge_energy") & bill["notes"].fillna("").str.contains("现货")]
    k4.metric("账单结算价", f"{settle_price(spot_rows):,.1f} 元/MWh" if not spot_rows.empty else "—")
    k5.metric("复制捕获价", f"{row['capture_price']:,.1f} 元/MWh" if pd.notna(row["capture_price"]) else "—")

    if green_acc and cfd_acc and vol_ratio:
        st.caption(
            f"校验：电量proxy/账单 = **{vol_ratio:.1%}** · 绿电复制准确率 = **{green_acc:.1%}** · "
            f"差价模型/隐含 = **{cfd_acc:.0%}**（账单隐含 ¥{cfd_implied:,.0f} vs 系统参考价模型 ¥{cfd_model:,.0f}）"
        )

    labels = [lbl for lbl, _ in comp] + ["复制净额", "账单净额"]
    values = [v for _, v in comp] + [replicated_total, bill_total]
    measures = ["relative"] * len(comp) + ["total", "total"]

    fig = go.Figure(go.Waterfall(
        orientation="v", measure=measures, x=labels, y=values,
        connector=dict(line=dict(color="rgb(63,63,63)", width=1)),
        decreasing=dict(marker=dict(color=_RED)),
        increasing=dict(marker=dict(color=_GREEN)),
        totals=dict(marker=dict(color=_BLUE)),
        text=[f"¥{v:,.0f}" for v in values], textposition="outside",
        hovertemplate="%{x}<br>¥%{y:,.0f}<extra></extra>",
    ))
    fig.update_layout(
        height=420, margin=dict(l=10, r=10, t=40, b=10),
        yaxis=dict(title="¥", tickformat=",.0f", showgrid=True, gridcolor="#f0f0f0"),
        plot_bgcolor="white", paper_bgcolor="white",
        title=f"{sel} 结算瀑布（现货+绿电按复制值，差价=账单隐含，费用项取账单）",
    )
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("复制明细 vs 账单行", expanded=False):
        c1, c2 = st.columns(2)
        c1.markdown("**复制值**")
        c1.dataframe(pd.DataFrame({
            "项目": ["上网电量 proxy (MWh)", "账单电量 (MWh)", "捕获价 (元/MWh)",
                     "现货价值 (¥)", "合约差价-隐含 (¥)", "合约差价-模型(系统) (¥)",
                     "绿电溢价 (¥)", "账单绿电 (¥)", "合约电量 (MWh)",
                     "参考价-西 (元/MWh)", "参考价-系统 (元/MWh)"],
            "值": [f"{row['gen_proxy_mwh']:,.1f}", f"{row['bill_vol_mwh']:,.1f}",
                   f"{row['capture_price']:,.2f}" if pd.notna(row["capture_price"]) else "—",
                   f"{row['spot_value_cny']:,.0f}", f"{cfd_implied:,.0f}",
                   f"{cfd_model:,.0f}" if cfd_model is not None and pd.notna(cfd_model) else "—",
                   f"{row['green_cny']:,.0f}" if pd.notna(row["green_cny"]) else "—",
                   f"{row['bill_green_cny']:,.0f}" if pd.notna(row["bill_green_cny"]) else "—",
                   f"{row['contract_vol_mwh']:,.1f}",
                   f"{row['ref_price_west']:,.2f}" if pd.notna(row["ref_price_west"]) else "—",
                   f"{row['ref_price_sys']:,.2f}" if pd.notna(row["ref_price_sys"]) else "—"],
        }), hide_index=True, use_container_width=True)
        c2.markdown("**账单行**")
        show = bill.copy()
        show["amount_cny"] = show["amount_cny"].map(lambda v: f"{v:,.2f}" if pd.notna(v) else "—")
        show["volume_mwh"] = show["volume_mwh"].map(lambda v: f"{v:,.1f}" if pd.notna(v) else "—")
        c2.dataframe(show, hide_index=True, use_container_width=True, height=300)

    st.markdown("**逐月组件趋势**")
    trend = monthly.copy()
    trend["month"] = trend["settle_month"].apply(lambda m: pd.Timestamp(m).strftime("%Y-%m"))
    fig2 = go.Figure()
    fig2.add_trace(go.Bar(x=trend["month"], y=trend["spot_value_cny"], name="现货价值", marker_color=_BLUE))
    fig2.add_trace(go.Bar(x=trend["month"], y=trend["implied_cfd_cny"], name="合约差价(隐含)", marker_color=_GREEN))
    fig2.add_trace(go.Bar(x=trend["month"], y=trend["green_cny"], name="绿电溢价", marker_color="#27ae60"))
    fig2.add_trace(go.Bar(x=trend["month"], y=trend["bill_fees_cny"], name="费用(账单)", marker_color=_RED))
    fig2.add_trace(go.Scatter(x=trend["month"], y=trend["bill_total_cny"], name="账单净额",
                              mode="lines+markers", line=dict(color="black", width=2)))
    fig2.update_layout(
        barmode="relative", height=360, margin=dict(l=10, r=10, t=30, b=10),
        yaxis=dict(title="¥", tickformat=",.0f", showgrid=True, gridcolor="#f0f0f0"),
        legend=dict(orientation="h", y=1.12),
        plot_bgcolor="white", paper_bgcolor="white",
    )
    st.plotly_chart(fig2, use_container_width=True)
