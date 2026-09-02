"""Tab 8 — 收益瀑布 P&L Waterfall: 投资标准 → 申报 → 出清 → 实际,
arbitrage + capacity subsidy stacked per stage, deviations between stages
per 复盘 definitions (assets/operating/复盘/储能/蒙西/).

Stage definitions:
  投资标准  — LP perfect-foresight dispatch with standard params (year-table SOH/RTE
              by asset age, DOD 90%, capacity from rm_assets), ramp 3.3%/min, at
              actual 15-min RT nodal prices; capacity subsidy internalised in LP objective.
  申报      — nominated_mw × RT price (arbitrage) + nominated discharge × capcomp rate.
  出清      — rt_cleared_mw × RT price + rt discharge × rate.
  实际      — settlement bills: 现货收益 (discharge+charge) + 容量补偿 − 系统运行费 − 线损费用
              (复盘 slide-3 formula); fees exist only in this leg.

Deviation definitions （复盘 slides 7-18):
  投资标准→申报 = 策略与预测偏差; 申报→出清 = 出清校核; 出清→实际 = 执行偏差与费用.
"""
from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import bindparam, text

from apps.asset_risk.standard_params import params_for_asset
from apps.asset_risk.tab_diagnostics import (
    CAPCOMP_RATE,
    CAPCOMP_RATE_DEFAULT,
    PLANT_MAP,
    _attach_prices,
    _load_chain,
    _load_prices,
    _month_asset_matrix,
)

_COMPONENT_COLORS = {"arb_cny": "#3498db", "cap_cny": "#9b59b6",
                     "fee_cny": "#e74c3c", "other_cny": "#95a5a6"}
_COMPONENT_CN = {"arb_cny": "套利", "cap_cny": "容量补偿", "fee_cny": "系统运行费+线损", "other_cny": "其他费用"}
_STAGES = ["投资标准", "Δ策略与预测", "申报", "Δ出清校核", "出清", "Δ执行与费用", "实际"]


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

def _load_assets(engine) -> pd.DataFrame:
    with engine.connect() as conn:
        df = pd.read_sql(text(
            "SELECT id, name, capacity_mw, bess_duration_h, commission_date "
            "FROM marketdata.rm_assets WHERE asset_type = 'bess' ORDER BY name"
        ), conn)
    return df[df["name"].isin(PLANT_MAP.keys())].reset_index(drop=True)


def _load_settlement_actuals(engine, asset_ids: list[int],
                             start: str | None, end: str | None) -> pd.DataFrame:
    """Per (asset, month) from settlement bills: arb/cap/fee/other (signed amounts)."""
    sql = """
        SELECT a.id AS asset_id, a.name AS asset, s.settlement_month AS month,
               si.category, si.amount_cny
        FROM marketdata.rm_settlement_items si
        JOIN marketdata.rm_settlements s ON s.id = si.settlement_id
        JOIN marketdata.rm_books b ON b.id = s.book_id
        JOIN marketdata.rm_assets a ON a.id = b.asset_id
        WHERE a.id IN :ids
    """
    params: dict = {"ids": asset_ids}
    if start:
        sql += " AND s.settlement_month >= :start"
        params["start"] = start
    if end:
        sql += " AND s.settlement_month < :end"
        params["end"] = end
    stmt = text(sql).bindparams(bindparam("ids", expanding=True))
    with engine.connect() as conn:
        df = pd.read_sql(stmt, conn, params=params)
    if df.empty:
        return pd.DataFrame(columns=["asset", "month", "arb_cny", "cap_cny", "fee_cny", "other_cny"])
    df["month"] = df["month"].astype(str).str[:7]
    rows = []
    for (asset, month), g in df.groupby(["asset", "month"]):
        amt = g.groupby("category")["amount_cny"].sum()
        fee = float(amt.get("coal_capacity_charge", 0.0) + amt.get("system_operation", 0.0))
        cap = float(amt.get("capacity_compensation", 0.0))
        arb = float(amt.get("discharge_energy", 0.0) + amt.get("generation_revenue", 0.0)
                    + amt.get("charge_energy", 0.0))
        other = float(amt.drop(index=[c for c in amt.index
                                      if c in ("discharge_energy", "generation_revenue",
                                               "charge_energy", "capacity_compensation",
                                               "coal_capacity_charge", "system_operation")]).sum())
        rows.append({"asset": asset, "month": month, "arb_cny": arb, "cap_cny": cap,
                     "fee_cny": fee, "other_cny": other})
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Stage math (pure)
# ---------------------------------------------------------------------------

def stage_arbitrage(df: pd.DataFrame, mw_col: str) -> pd.DataFrame:
    """Per (asset, month): discharge/charge volumes and arbitrage ¥ for a MW column.

    df columns: asset, month, <mw_col> (MW, + = discharge), price_cny_mwh.
    arb_cny = Σ mw × 0.25 × price (NaN price skipped; MWh counted regardless).
    """
    rows = []
    for (asset, month), g in df.groupby(["asset", "month"]):
        mw = g[mw_col].astype(float)
        dis_mwh = float((mw.clip(lower=0) * 0.25).sum())
        chg_mwh = float((-mw.clip(upper=0) * 0.25).sum())
        arb = float((mw * 0.25 * g["price_cny_mwh"]).sum())
        rows.append({"asset": asset, "month": month, "dis_mwh": dis_mwh,
                     "chg_mwh": chg_mwh, "arb_cny": arb})
    return pd.DataFrame(rows)


def stage_capacity(dis_mwh: pd.Series, assets: pd.Series) -> pd.Series:
    """discharge MWh × capacity-subsidy rate per asset (350 default / 280 named)."""
    return pd.Series(
        [float(d) * CAPCOMP_RATE.get(a, CAPCOMP_RATE_DEFAULT) for d, a in zip(dis_mwh, assets)],
        index=dis_mwh.index,
    )


@st.cache_data(show_spinner=False)
def benchmark_leg(_engine, asset_id: int, month: str, capacity_mw: float,
                  duration_h: float, commission_iso: str | None,
                  capcomp_rate: float, plant: str) -> dict:
    """Investment-standard leg: LP perfect-foresight with standard params for one
    asset-month. Cached on (asset_id, month, ...) — historical inputs immutable.

    LP input prices are shifted −15min so each interval-start index maps to the
    period-END price stamp (same alignment as arb_match).
    """
    from services.bess_map.optimisation_engine import compute_dispatch_from_15min_prices  # lazy (pulp)

    month_start = pd.Timestamp(month + "-01")
    month_end = (month_start + pd.offsets.MonthBegin(1)).strftime("%Y-%m-%d")
    params = params_for_asset(capacity_mw, duration_h, commission_iso, month_start)

    prices = _load_prices(_engine, [plant], month_start.strftime("%Y-%m-%d"), month_end)
    if prices.empty:
        return {"arb_cny": 0.0, "dis_mwh": 0.0, "chg_mwh": 0.0, "cap_cny": 0.0,
                "days_solved": 0, "days_skipped": 0, "age": params["age"], "note": "no prices"}
    s = pd.Series({pd.Timestamp(t) - pd.Timedelta(minutes=15): float(v)
                   for t, v in zip(prices["datetime"], prices["cleared_price"]) if pd.notna(v)})
    s = s[~s.index.duplicated(keep="last")].sort_index()
    s.index = pd.DatetimeIndex(s.index)

    dispatch_df, profit_s = compute_dispatch_from_15min_prices(
        s, power_mw=capacity_mw, duration_h=params["duration_h"] * params["dod"] * params["soh"],
        roundtrip_eff=params["roundtrip_eff"], window_days=1,
        ramp_rate_pct_per_min=3.3,
        # capacity subsidy NOT internalised in the objective: it makes the MILP
        # ~25-500x slower (every discharge marginally profitable explodes the binary
        # search) while changing discharge volume by only ~0.2% (measured 2026-06,
        # 悦杭独贵: 903.1 vs 904.6 MWh). Subsidy is added outside as dis_mwh x rate.
    )
    dis_mwh = float((dispatch_df["discharge_mw"] * 0.25).sum()) if not dispatch_df.empty else 0.0
    chg_mwh = float((dispatch_df["charge_mw"] * 0.25).sum()) if not dispatch_df.empty else 0.0
    arb = float(profit_s.sum()) if len(profit_s) else 0.0
    days_in_month = (month_start + pd.offsets.MonthEnd(0)).day
    return {"arb_cny": arb, "dis_mwh": dis_mwh, "chg_mwh": chg_mwh,
            "cap_cny": dis_mwh * capcomp_rate,
            "days_solved": int(len(profit_s)), "days_skipped": days_in_month - int(len(profit_s)),
            "age": params["age"], "note": ""}


# ---------------------------------------------------------------------------
# Waterfall assembly (pure)
# ---------------------------------------------------------------------------

def build_waterfall(bench: pd.DataFrame, nominated: pd.DataFrame,
                    cleared: pd.DataFrame, actual: pd.DataFrame) -> pd.DataFrame:
    """Long frame: stage × component → cny, with bridge deviations.

    Stages: 投资标准, Δ策略与预测 (=申报−标准), 申报, Δ出清校核 (=出清−申报),
    出清, Δ执行与费用 (=实际−出清), 实际. Components: arb_cny, cap_cny, fee_cny, other_cny
    (fee/other are zero except in the 实际 leg and its incoming deviation).
    Bridge identity per component: std + d1 = nom; nom + d2 = clr; clr + d3 = act.
    """
    comps = ["arb_cny", "cap_cny", "fee_cny", "other_cny"]
    rows = []

    def _tot(df, comp):
        return float(df[comp].sum()) if comp in df.columns and not df.empty else 0.0

    totals = {
        "投资标准": {c: _tot(bench, c) for c in comps},
        "申报": {c: _tot(nominated, c) for c in comps},
        "出清": {c: _tot(cleared, c) for c in comps},
        "实际": {c: _tot(actual, c) for c in comps},
    }
    for stage in ("投资标准", "申报", "出清"):
        for c in comps:
            totals[stage].setdefault(c, 0.0)
    for stage, prev, delta_name in [("申报", "投资标准", "Δ策略与预测"),
                                    ("出清", "申报", "Δ出清校核"),
                                    ("实际", "出清", "Δ执行与费用")]:
        for c in comps:
            rows.append({"stage": delta_name, "component": c,
                         "cny": totals[stage][c] - totals[prev][c]})
    for stage in ("投资标准", "申报", "出清", "实际"):
        for c in comps:
            rows.append({"stage": stage, "component": c, "cny": totals[stage][c]})
    order = {s: i for i, s in enumerate(_STAGES)}
    out = pd.DataFrame(rows)
    out["stage_order"] = out["stage"].map(order)
    return out.sort_values(["stage_order", "component"]).reset_index(drop=True)


def waterfall_figure(wf: pd.DataFrame) -> go.Figure:
    """Stacked-bar waterfall: stage columns stacked from 0 (positive up, negative down),
    deviation columns as floating bridges (base+value) landing on next stage total."""
    comps = ["arb_cny", "cap_cny", "fee_cny", "other_cny"]
    totals = {s: {c: float(wf[(wf["stage"] == s) & (wf["component"] == c)]["cny"].sum())
                  for c in comps} for s in ("投资标准", "申报", "出清", "实际")}
    stage_total = {s: sum(totals[s].values()) for s in totals}

    traces = {c: {"base": [], "y": [], "text": []} for c in comps}
    x = list(_STAGES)

    def stage_bar(stage):
        running = 0.0
        for c in comps:
            v = totals[stage][c]
            if v >= 0:
                traces[c]["base"].append(0.0 if running == 0 else None)
                traces[c]["base"][-1] = running if running else 0.0
                traces[c]["y"].append(v)
                running += v
            else:
                traces[c]["base"].append(running + v if running else v)
                traces[c]["y"].append(-v)
            label = f"¥{v / 1e6:,.2f}M" if v else ""
            traces[c]["text"].append(label)

    def delta_bar(stage, prev_stage):
        running = stage_total[prev_stage]
        next_stage = {"Δ策略与预测": "申报", "Δ出清校核": "出清", "Δ执行与费用": "实际"}[stage]
        for c in comps:
            d = float(wf[(wf["stage"] == stage) & (wf["component"] == c)]["cny"].sum())
            if d >= 0:
                traces[c]["base"].append(running)
                traces[c]["y"].append(d)
            else:
                traces[c]["base"].append(running + d)
                traces[c]["y"].append(-d)
            running += d
            traces[c]["text"].append(f"{d / 1e6:+,.2f}M" if d else "")
        assert abs(running - stage_total[next_stage]) < 1.0, (
            f"bridge {stage}: lands {running:.0f} != {stage_total[next_stage]:.0f}")

    for stage in x:
        if stage in totals:
            stage_bar(stage)
        else:
            prev = {"Δ策略与预测": "投资标准", "Δ出清校核": "申报", "Δ执行与费用": "出清"}[stage]
            delta_bar(stage, prev)

    fig = go.Figure()
    for c in comps:
        fig.add_trace(go.Bar(
            x=x, y=traces[c]["y"], base=traces[c]["base"],
            name=_COMPONENT_CN[c], marker_color=_COMPONENT_COLORS[c],
            text=traces[c]["text"], textposition="outside",
            opacity=0.95,
        ))
    fig.update_layout(
        title="收益瀑布：投资标准 → 申报 → 出清 → 实际（套利 + 容量补偿 stacked；费用仅在实际段）",
        yaxis_title="CNY", barmode="overlay", height=480,
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    return fig


# ---------------------------------------------------------------------------
# Render
# ---------------------------------------------------------------------------

_DEFINITIONS_MD = """
**偏差定义（来源：蒙西储能 2026 上半年复盘，slides 7–18）**

| 区间 | 名称 | 主要成因 |
|---|---|---|
| 投资标准 → 申报 | **策略与预测偏差** | 价格预测误差、留电/SOC 策略选择、交易员经验差异 |
| 申报 → 出清 | **出清校核** | 日前校核（bid failure）、SOC 校核（"充满还在充"）、申报充电量校核、爬坡/红绿灯约束 |
| 出清 → 实际 | **执行偏差与费用** | 断面阻塞（呼包东苏右/四子王放电校核 15–20%）、调度曲线核减、涉网试验（杭锦旗 AGC 退出）、设备瓶颈（乌拉特 95MW 限功率 + 充放间隔静置 1h）、红绿灯限制窗口；以及仅存在于结算单的费用项（系统运行费、上网线损费） |

实际收益 = 现货收益 + 容量补偿 − 系统运行费 − 线损费用（复盘 slide 3）。
容量补贴标准：350 元/MWh（锡西二/阿拉善/武川 280 元/MWh）；复盘显示容量补偿约占总收益 88.5%（slide 6）。
"""

_CAVEATS_MD = """
**口径与假设**

- 投资标准：LP 完美预见调度（15 分钟 RT 节点电价，纯现货套利目标），参数取标准表（容量=资产配置容量，DOD 90%，SOH/RTE 按投运年限），爬坡 3.3%/min；容量补偿按放电量 × 标准单价另计。补贴不内化于 LP 目标函数的实测效果（2026-06 悦杭独贵）：放电 13.7 GWh/月 ≈ 1.5 次/日（与实际电站节奏一致；内化则为 21.5 GWh ≈ 2.4 次/日，超出实际运营水平），且求解快 60 倍。
- 申报/出清两腿按 15 分钟节点电价计价；账单充电按小时均价结算（已确认的结算规则），与 15 分钟口径存在 ±1–4% 基差，归入"出清→实际"偏差。
- 费用项（系统运行费、线损、其他费用）仅存在于实际段（结算单），前两腿为零 — 这是 Δ执行与费用 的组成部分，不是错误。
- commission_date 缺失的资产按 Year 0（最新参数）处理，并在下方列出警告。
- LP 窗口 7 天、SOC 窗口起点为 0；价格或调度链缺失的日期跳过（见数据覆盖）。
"""


def render_waterfall(engine):
    """Render 收益瀑布 P&L Waterfall tab."""
    st.subheader("收益瀑布 P&L Waterfall")

    assets = _load_assets(engine)
    if assets.empty:
        st.warning("No BESS assets found.")
        return

    chain = _load_chain(engine, assets["id"].tolist(), None, None)
    if chain.empty:
        st.info("No dispatch-chain data.")
        return
    chain["month"] = chain["ts"].dt.strftime("%Y-%m")

    months = sorted(chain["month"].unique().tolist())
    c1, c2 = st.columns([1, 2])
    with c1:
        sel_asset = st.selectbox("资产", ["全部资产"] + assets["name"].tolist(), key="wf_asset")
    with c2:
        sel_months = st.multiselect("月份", months, default=months[-1:] if months else [],
                                    key="wf_months")
    if not sel_months:
        st.info("请选择月份。")
        return

    df = chain[chain["month"].isin(sel_months)].copy()
    prices = _load_prices(engine, [PLANT_MAP[a] for a in assets["name"]], None, None)
    df = _attach_prices(df, prices, PLANT_MAP)

    if sel_asset != "全部资产":
        df = df[df["asset"] == sel_asset]
        assets = assets[assets["name"] == sel_asset]

    # --- legs ---
    nominated = stage_arbitrage(df, "nominated_mw")
    nominated["cap_cny"] = stage_capacity(nominated["dis_mwh"], nominated["asset"])
    cleared = stage_arbitrage(df, "rt_cleared_mw")
    cleared["cap_cny"] = stage_capacity(cleared["dis_mwh"], cleared["asset"])

    actual = _load_settlement_actuals(engine, assets["id"].tolist(),
                                      min(sel_months), None)
    actual = actual[actual["month"].isin(sel_months)] if not actual.empty else actual

    bench_rows = []
    missing_commission = []
    for _, a in assets.iterrows():
        commission_iso = None
        if pd.notna(a.get("commission_date")):
            commission_iso = str(a["commission_date"])[:10]
        else:
            missing_commission.append(a["name"])
        for month in sel_months:
            rate = CAPCOMP_RATE.get(a["name"], CAPCOMP_RATE_DEFAULT)
            r = benchmark_leg(engine, int(a["id"]), month, float(a["capacity_mw"]),
                              float(a["bess_duration_h"]), commission_iso, rate,
                              PLANT_MAP[a["name"]])
            bench_rows.append({"asset": a["name"], "month": month, **r})
    bench = pd.DataFrame(bench_rows) if bench_rows else pd.DataFrame(
        columns=["asset", "month", "arb_cny", "dis_mwh", "cap_cny"])
    if missing_commission:
        st.warning(f"以下资产 commission_date 缺失，按 Year 0 参数计算：{'、'.join(missing_commission)}")

    actual_has_bills = not actual.empty and len(actual) > 0
    if not actual_has_bills:
        actual = pd.DataFrame([{"asset": n, "month": m, "arb_cny": 0.0, "cap_cny": 0.0,
                                "fee_cny": 0.0, "other_cny": 0.0}
                               for n in assets["name"] for m in sel_months])
        st.caption("所选月份无结算单 — 实际段显示为 0（账单尚未发布；通常次月中旬可得）。")

    wf = build_waterfall(bench, nominated, cleared, actual)

    # --- KPI strip ---
    def _tot(stage, comp):
        return float(wf[(wf["stage"] == stage) & (wf["component"] == comp)]["cny"].sum())

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("投资标准", f"¥{sum(_tot('投资标准', c) for c in ('arb_cny', 'cap_cny')):,.0f}")
    k2.metric("实际收益（账单）",
              f"¥{sum(_tot('实际', c) for c in ('arb_cny', 'cap_cny', 'fee_cny', 'other_cny')):,.0f}",
              help="现货收益 + 容量补偿 − 系统运行费 − 线损费用（复盘 slide 3 口径）")
    k3.metric("实际套利", f"¥{_tot('实际', 'arb_cny'):,.0f}")
    k4.metric("实际容量补偿", f"¥{_tot('实际', 'cap_cny'):,.0f}")

    st.plotly_chart(waterfall_figure(wf), use_container_width=True)

    with st.expander("偏差定义（复盘）"):
        st.markdown(_DEFINITIONS_MD)
    with st.expander("口径与假设 / 数据覆盖"):
        st.markdown(_CAVEATS_MD)
        cov = (df.groupby(["asset", "month"]).agg(intervals=("ts", "count"),
               priced=("price_cny_mwh", lambda s: f"{s.notna().mean():.0%}")).reset_index())
        st.dataframe(cov, use_container_width=True, hide_index=True)

    # --- stage × month matrix ---
    st.markdown("**各段收益 by 资产 × 月 (¥)**")
    mat_src = pd.concat([
        bench.assign(stage="投资标准")[["asset", "month", "stage", "arb_cny", "cap_cny"]],
        nominated.assign(stage="申报")[["asset", "month", "stage", "arb_cny", "cap_cny"]],
        cleared.assign(stage="出清")[["asset", "month", "stage", "arb_cny", "cap_cny"]],
        actual.assign(stage="实际")[["asset", "month", "stage", "arb_cny", "cap_cny"]],
    ], ignore_index=True)
    mat_src["total_cny"] = mat_src["arb_cny"] + mat_src["cap_cny"]
    mat = mat_src.pivot_table(index="month", columns="stage", values="total_cny",
                              aggfunc="sum", fill_value=0)
    st.dataframe(mat.style.format("¥{:,.0f}"), use_container_width=True)
