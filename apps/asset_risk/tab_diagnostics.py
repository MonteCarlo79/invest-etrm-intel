"""Tab 7 — Dispatch Diagnostics: execution gap (congestion), bid failure,
restrictions & defects. Per-asset monthly volume discrepancies (MWh) and
P&L impacts (¥), computed on the fly from rm_dispatch_chain + md_id_cleared_energy.

Metric definitions (validated 2026-09-01, docs/handoff-2026-08-30):
  exec gap dis = Σ(rt−actual)×0.25×price   where rt_cleared > 0.5, actual not null
  exec gap chg = Σ(actual−rt)×0.25×price   where rt_cleared < −0.5 (reads negative)
  bid fail dis = Σ(nom−da)×0.25×price      where nominated > 0.5
  bid fail chg = Σ(da−nom)×(−0.25)×price   where nominated < −0.5
Price join: plant RT cleared price at interval_start(Beijing) + 15min (period-end
stamp — the AT TIME ZONE pattern from services/arb_match/compute.py; never ::timestamp).
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import bindparam, text

from libs.asset_risk.chain import (  # noqa: F401  (re-exported for tabs/tests)
    _attach_prices, _load_auto_nominations, _load_chain, _load_prices,
    _merge_auto_nominations, _month_token_bounds, _ts_expr,
)
from libs.asset_risk.diagnostics import (  # noqa: F401  (re-exported for tabs/tests)
    AUTO_NOM_CODE, CAPCOMP_RATE, CAPCOMP_RATE_DEFAULT, PLANT_MAP,
    chain_asset_names, load_maps, RESTRICTION_CN, RESTRICTION_COLOR, _coverage, _month_asset_matrix,
    bid_fail_monthly, capacity_loss_monthly, exec_gap_monthly,
    find_defect_events, restriction_monthly,
)


def _load_assets(engine) -> pd.DataFrame:
    """BESS assets with a dispatch chain (registry data_path 'full'; legacy six
    pre-migration) — see diagnostics.chain_asset_names."""
    with engine.connect() as conn:
        df = pd.read_sql(text(
            "SELECT id, name, capacity_mw, bess_duration_h "
            "FROM marketdata.rm_assets WHERE asset_type = 'bess' ORDER BY name"
        ), conn)
    return df[df["name"].isin(chain_asset_names(engine))].reset_index(drop=True)



# ---------------------------------------------------------------------------
# Chart helpers
# ---------------------------------------------------------------------------

def _grouped_bars(monthly: pd.DataFrame, mwh_col: str, cny_col: str, title: str):
    """Two figures: monthly MWh and ¥ grouped bars (放电/充电 traces)."""
    agg = monthly.groupby("month", as_index=False)[[mwh_col, cny_col]].sum().sort_values("month")
    fig_mwh = go.Figure(go.Bar(x=agg["month"], y=agg[mwh_col], marker_color="#3498db",
                               text=[f"{v:,.0f}" for v in agg[mwh_col]], textposition="auto"))
    fig_mwh.update_layout(title=f"{title} — 电量 (MWh)", yaxis_title="MWh",
                          showlegend=False, height=320)
    fig_cny = go.Figure(go.Bar(x=agg["month"], y=agg[cny_col], marker_color="#e67e22",
                               text=[f"{v/1e6:,.1f}M" for v in agg[cny_col]], textposition="auto"))
    fig_cny.update_layout(title=f"{title} — 金额 (¥)", yaxis_title="CNY",
                          showlegend=False, height=320)
    return fig_mwh, fig_cny


def _pct_heatmap(monthly: pd.DataFrame, pct_col: str, title: str):
    """asset × month gap% heatmap (red = worst) — per-asset issue identification."""
    if monthly.empty or monthly[pct_col].isna().all():
        return None
    mat = monthly.pivot_table(index="asset", columns="month", values=pct_col,
                              aggfunc="first")
    fig = go.Figure(go.Heatmap(
        z=mat.values, x=mat.columns.tolist(), y=mat.index.tolist(),
        colorscale="RdYlGn_r", text=[[f"{v:.1f}" if pd.notna(v) else "" for v in row]
                                     for row in mat.values],
        texttemplate="%{text}", colorbar=dict(title="%"),
    ))
    fig.update_layout(title=title, height=260)
    return fig


def _hourly_exec_gap_profile(df: pd.DataFrame):
    """Exec-gap ¥ by hour-of-day — congestion clusters at export-peak hours."""
    dis = df[(df["rt_cleared_mw"] > 0.5) & df["actual_mw"].notna()].copy()
    if dis.empty:
        return None
    dis["hour"] = dis["ts"].dt.hour
    dis["gap_cny"] = ((dis["rt_cleared_mw"] - dis["actual_mw"]) * 0.25
                      * dis["price_cny_mwh"])
    prof = dis.groupby("hour", as_index=False)["gap_cny"].sum()
    fig = go.Figure(go.Bar(x=prof["hour"], y=prof["gap_cny"], marker_color="#e74c3c"))
    fig.update_layout(title="放电执行偏差金额 时段分布 (hour-of-day)",
                      xaxis_title="小时", yaxis_title="CNY", height=300, showlegend=False)
    return fig


# ---------------------------------------------------------------------------
# Render
# ---------------------------------------------------------------------------

def render_diagnostics(engine):
    """Render Dispatch Diagnostics tab."""
    st.subheader("Dispatch Diagnostics 调度执行诊断")

    mode = st.radio("资产类型", ["储能", "零碳46风电(悦盛昌渠)"], horizontal=True, key="dx_mode")
    if mode != "储能":
        from apps.asset_risk.tab_wind_replication import render_wind_diagnostics
        render_wind_diagnostics(engine)
        return

    assets = _load_assets(engine)
    plant_map, nom_map, rate_map = load_maps(engine)
    if assets.empty:
        st.warning("No BESS assets found.")
        return

    names = assets["name"].tolist()
    sel_assets = st.multiselect("资产", names, default=names, key="dx_assets")
    date_range = st.date_input("Date Range", value=[], key="dx_dates")
    start = end = None
    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        start, end = sorted(str(d) for d in date_range)
        end = str(pd.Timestamp(end) + pd.Timedelta(days=1))[:10]  # inclusive → exclusive

    if not sel_assets:
        st.info("请选择至少一个资产。")
        return
    sel_ids = assets[assets["name"].isin(sel_assets)]["id"].tolist()

    chain = _load_chain(engine, sel_ids, start, end,
                        nom_code_map=nom_map, plant_map=plant_map)
    if chain.empty:
        st.info("所选范围无调度链数据。")
        return
    prices = _load_prices(engine, [plant_map[a] for a in sel_assets], start, end)
    df = _attach_prices(chain, prices, {a: plant_map[a] for a in sel_assets})
    df["month"] = df["ts"].dt.strftime("%Y-%m")

    priced_share = df["price_cny_mwh"].notna().mean()
    st.caption(f"价格覆盖率 {priced_share:.1%} — ¥ 影响按已定价区间计算；电量统计不受价格缺失影响")
    with st.expander("数据覆盖 (intervals per asset × month)"):
        cov = _coverage(df)
        st.dataframe(cov, use_container_width=True, hide_index=True)

    # ================= Panel 1: execution gap =================
    st.markdown("#### 1. 执行偏差：实时出清 vs 实际执行（网架拥堵）")
    gap = exec_gap_monthly(df)
    cap1 = capacity_loss_monthly(df, kind="exec_gap", rate_map=rate_map)
    gap = gap.merge(cap1[["asset", "month", "capacity_loss_cny"]],
                    on=["asset", "month"], how="left")
    gap["cost_change_cny"] = -gap["chg_gap_cny"]              # 充电成本变化：+多付 / −少付
    gap["arb_net_cny"] = gap["dis_gap_cny"] - gap["chg_gap_cny"]  # 套利净影响：+净损失

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("放电偏差电量", f"{gap['dis_gap_mwh'].sum():,.0f} MWh", help="正=少放（出清未执行），负=多放")
    c2.metric("充电偏差电量", f"{gap['chg_gap_mwh'].sum():,.0f} MWh", help="正=少充，负=多充")
    c3.metric("套利净影响", f"¥{gap['arb_net_cny'].sum():,.0f}",
              help="放电收入损失 + 充电成本变化（正=净损失）")
    c4.metric("容量补偿影响", f"¥{gap['capacity_loss_cny'].sum():,.0f}",
              help="放电偏差电量 × 容量补偿标准（350元/MWh；锡西二/阿拉善/武川 280元/MWh）")

    m_mwh = gap.groupby("month", as_index=False)[["dis_gap_mwh", "chg_gap_mwh"]].sum().sort_values("month")
    m_cny = gap.groupby("month", as_index=False)[
        ["dis_gap_cny", "cost_change_cny", "arb_net_cny"]].sum().sort_values("month")
    col_l, col_r = st.columns(2)
    with col_l:
        fig = go.Figure([
            go.Bar(name="放电偏差", x=m_mwh["month"], y=m_mwh["dis_gap_mwh"], marker_color="#2ecc71"),
            go.Bar(name="充电偏差", x=m_mwh["month"], y=m_mwh["chg_gap_mwh"], marker_color="#3498db"),
        ])
        fig.update_layout(title="执行偏差电量 by 月（正=少放/少充，负=多放/多充）",
                          yaxis_title="MWh", barmode="group", height=330)
        st.plotly_chart(fig, use_container_width=True)
    with col_r:
        fig = go.Figure([
            go.Bar(name="放电收入损失", x=m_cny["month"], y=m_cny["dis_gap_cny"], marker_color="#e74c3c"),
            go.Bar(name="充电成本变化", x=m_cny["month"], y=m_cny["cost_change_cny"], marker_color="#3498db"),
            go.Bar(name="套利净影响", x=m_cny["month"], y=m_cny["arb_net_cny"], marker_color="#e67e22"),
        ])
        fig.update_layout(title="套利影响 by 月（放电收入 − 充电成本；正=净损失）",
                          yaxis_title="CNY", barmode="group", height=330)
        st.plotly_chart(fig, use_container_width=True)

    m_cap = gap.groupby("month", as_index=False)["capacity_loss_cny"].sum().sort_values("month")
    fig = go.Figure(go.Bar(x=m_cap["month"], y=m_cap["capacity_loss_cny"], marker_color="#9b59b6",
                           text=[f"{v/1e6:,.2f}M" for v in m_cap["capacity_loss_cny"]],
                           textposition="auto"))
    fig.update_layout(title="容量补偿影响 by 月（放电偏差电量 × 350元/MWh；锡西二/阿拉善/武川 280元/MWh）",
                      yaxis_title="CNY", showlegend=False, height=300)
    st.plotly_chart(fig, use_container_width=True)

    prof = _hourly_exec_gap_profile(df)
    if prof is not None:
        st.plotly_chart(prof, use_container_width=True)
    hm = _pct_heatmap(gap, "dis_gap_pct", "放电执行偏差率 % (资产 × 月)")
    if hm is not None:
        st.plotly_chart(hm, use_container_width=True)
    col_l, col_r = st.columns(2)
    with col_l:
        st.markdown("**套利净影响 (¥)**")
        st.dataframe(_month_asset_matrix(gap, "arb_net_cny").style.format("¥{:,.0f}"),
                     use_container_width=True)
    with col_r:
        st.markdown("**容量补偿影响 (¥)**")
        st.dataframe(_month_asset_matrix(gap, "capacity_loss_cny").style.format("¥{:,.0f}"),
                     use_container_width=True)

    st.divider()

    # ================= Panel 2: bid failure =================
    st.markdown("#### 2. 申报失败：申报 vs 日前出清")
    fail = bid_fail_monthly(df)
    cap2 = capacity_loss_monthly(df, kind="bid_fail", rate_map=rate_map)
    fail = fail.merge(cap2[["asset", "month", "capacity_loss_cny"]],
                      on=["asset", "month"], how="left")
    fail["chg_shortfall_mwh"] = -fail["chg_fail_mwh"]      # 充电未中标电量：正=少中标，负=多中标
    fail["arb_net_cny"] = fail["dis_fail_cny"] + fail["chg_fail_cny"]  # 套利净影响：+净损失
    # fail["chg_fail_cny"]: negative = 少中标（少付成本/avoided），positive = 多中标（多付成本）

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("放电未中标电量", f"{fail['dis_fail_mwh'].sum():,.0f} MWh")
    c2.metric("充电未中标电量", f"{fail['chg_shortfall_mwh'].sum():,.0f} MWh",
              help="正=少中标，负=多中标")
    c3.metric("套利净影响", f"¥{fail['arb_net_cny'].sum():,.0f}",
              help="放电收入损失 + 充电成本变化（正=净损失）")
    c4.metric("容量补偿影响", f"¥{fail['capacity_loss_cny'].sum():,.0f}",
              help="放电未中标电量 × 容量补偿标准（350元/MWh；锡西二/阿拉善/武川 280元/MWh）")

    f_mwh = fail.groupby("month", as_index=False)[["dis_fail_mwh", "chg_shortfall_mwh"]].sum().sort_values("month")
    f_cny = fail.groupby("month", as_index=False)[
        ["dis_fail_cny", "chg_fail_cny", "arb_net_cny"]].sum().sort_values("month")
    col_l, col_r = st.columns(2)
    with col_l:
        fig = go.Figure([
            go.Bar(name="放电未中标", x=f_mwh["month"], y=f_mwh["dis_fail_mwh"], marker_color="#e74c3c"),
            go.Bar(name="充电未中标", x=f_mwh["month"], y=f_mwh["chg_shortfall_mwh"], marker_color="#3498db"),
        ])
        fig.update_layout(title="申报失败电量 by 月（正=少中标，负=多中标）",
                          yaxis_title="MWh", barmode="group", height=330)
        st.plotly_chart(fig, use_container_width=True)
    with col_r:
        fig = go.Figure([
            go.Bar(name="放电收入损失", x=f_cny["month"], y=f_cny["dis_fail_cny"], marker_color="#e74c3c"),
            go.Bar(name="充电成本变化", x=f_cny["month"], y=f_cny["chg_fail_cny"], marker_color="#3498db"),
            go.Bar(name="套利净影响", x=f_cny["month"], y=f_cny["arb_net_cny"], marker_color="#e67e22"),
        ])
        fig.update_layout(title="套利影响 by 月（放电收入 − 充电成本；正=净损失）",
                          yaxis_title="CNY", barmode="group", height=330)
        st.plotly_chart(fig, use_container_width=True)

    f_cap = fail.groupby("month", as_index=False)["capacity_loss_cny"].sum().sort_values("month")
    fig = go.Figure(go.Bar(x=f_cap["month"], y=f_cap["capacity_loss_cny"], marker_color="#9b59b6",
                           text=[f"{v/1e6:,.2f}M" for v in f_cap["capacity_loss_cny"]],
                           textposition="auto"))
    fig.update_layout(title="容量补偿影响 by 月（放电未中标电量 × 350元/MWh；锡西二/阿拉善/武川 280元/MWh）",
                      yaxis_title="CNY", showlegend=False, height=300)
    st.plotly_chart(fig, use_container_width=True)

    hm2 = _pct_heatmap(fail, "dis_fail_pct", "放电申报失败率 % (资产 × 月)")
    if hm2 is not None:
        st.plotly_chart(hm2, use_container_width=True)
    col_l, col_r = st.columns(2)
    with col_l:
        st.markdown("**套利净影响 (¥)**")
        st.dataframe(_month_asset_matrix(fail, "arb_net_cny").style.format("¥{:,.0f}"),
                     use_container_width=True)
    with col_r:
        st.markdown("**容量补偿影响 (¥)**")
        st.dataframe(_month_asset_matrix(fail, "capacity_loss_cny").style.format("¥{:,.0f}"),
                     use_container_width=True)

    st.divider()

    # ================= Panel 3: restrictions & defects =================
    st.markdown("#### 3. 系统限制与设备缺陷")
    st.markdown("**3a 调度限制窗口（申报表时间格颜色）**")
    rest = restriction_monthly(df, rate_map=rate_map)
    n_flag = rest["charge_only_intervals"].sum() + rest["discharge_only_intervals"].sum()
    gap_vol = rest["gap_dis_mwh"].sum() + rest["gap_chg_mwh"].sum()
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("受限区间数", f"{n_flag:,}")
    c2.metric("受限占比", f"{n_flag / max(rest['total_intervals'].sum(), 1):.1%}")
    c3.metric("受限窗口计划电量", f"{rest['moved_mwh'].sum():,.0f} MWh",
              help="受限窗口内的计划（出清）电量 Σ|rt|×0.25 — 非偏差，仅度量受限规模")
    c4.metric("窗口内执行偏差电量", f"{gap_vol:,.0f} MWh",
              help="受限窗口内 实时出清 vs 实际执行 的偏差（正=少放/少充，负=多放/多充）")
    c5.metric("窗口内执行偏差金额", f"¥{rest['gap_cny'].sum():,.0f}",
              help="仅套利影响（按实时价格加权）；容量补偿影响见下行")
    c6, c7 = st.columns(2)
    c6.metric("窗口内容量补偿影响", f"¥{rest['capacity_loss_cny'].sum():,.0f}",
              help="窗口内放电偏差电量 × 容量补偿标准（350元/MWh；锡西二/阿拉善/武川 280元/MWh）"
                   "— 为 Panel 1 容量补偿影响的受限窗口子集")

    by_month_type = rest.groupby("month")[["charge_only_intervals",
                                           "discharge_only_intervals"]].sum()
    fig = go.Figure([
        go.Bar(name=RESTRICTION_CN["charge_only"], x=by_month_type.index,
               y=by_month_type["charge_only_intervals"],
               marker_color=RESTRICTION_COLOR["charge_only"]),
        go.Bar(name=RESTRICTION_CN["discharge_only"], x=by_month_type.index,
               y=by_month_type["discharge_only_intervals"],
               marker_color=RESTRICTION_COLOR["discharge_only"]),
    ])
    fig.update_layout(title="受限区间数 by 月 × 类型", yaxis_title="区间数",
                      barmode="stack", height=320)
    st.plotly_chart(fig, use_container_width=True)
    col_l, col_r = st.columns(2)
    with col_l:
        st.dataframe(_month_asset_matrix(rest, "restricted_share").style.format("{:.1%}"),
                     use_container_width=True)
    with col_r:
        st.dataframe(_month_asset_matrix(rest, "gap_cny").style.format("¥{:,.0f}"),
                     use_container_width=True)

    st.markdown("**3b 设备缺陷（非响应事件：出清≠0 而实际≈0，≥1 小时持续）**")
    events = find_defect_events(df)
    c1, c2, c3 = st.columns(3)
    c1.metric("事件数", f"{len(events):,}")
    c2.metric("损失电量", f"{events['lost_mwh'].sum():,.1f} MWh" if not events.empty else "0 MWh")
    c3.metric("损失金额", f"¥{events['lost_cny'].sum():,.0f}" if not events.empty else "¥0")
    if events.empty:
        st.info("所选范围内未发现非响应事件。")
    else:
        ev = events.copy()
        ev["month"] = ev["start_ts"].dt.strftime("%Y-%m")
        monthly_lost = ev.groupby("month", as_index=False)["lost_cny"].sum()
        fig = go.Figure(go.Bar(x=monthly_lost["month"], y=monthly_lost["lost_cny"],
                               marker_color="#e74c3c"))
        fig.update_layout(title="非响应事件损失金额 by 月", yaxis_title="CNY",
                          showlegend=False, height=300)
        st.plotly_chart(fig, use_container_width=True)
        show = ev.sort_values("lost_cny", ascending=False).head(50).copy()
        show["start_ts"] = show["start_ts"].dt.strftime("%Y-%m-%d %H:%M")
        show["end_ts"] = show["end_ts"].dt.strftime("%Y-%m-%d %H:%M")
        show = show.rename(columns={
            "asset": "资产", "start_ts": "开始", "end_ts": "结束",
            "intervals": "时长(区间)", "rt_avg_mw": "平均出清MW",
            "lost_mwh": "损失电量MWh", "lost_cny": "损失金额¥",
        })
        st.dataframe(show.style.format({"平均出清MW": "{:.1f}", "损失电量MWh": "{:.2f}",
                                        "损失金额¥": "¥{:,.0f}"}),
                     use_container_width=True, hide_index=True)
