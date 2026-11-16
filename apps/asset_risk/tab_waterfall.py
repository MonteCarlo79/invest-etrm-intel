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
from sqlalchemy import text

from libs.asset_risk import waterfall as _wf
from libs.asset_risk.legs_store import ensure_table, get_or_compute, legs_inputs_key
from libs.asset_risk.waterfall import (
    CLEARED_VOLUME_TOLERANCE,
    _COMPONENT_CN,
    _COMPONENT_COLORS,
    _STAGES,
    _STAGES_IC,
    _load_nodal_prices_filled,
    _load_settlement_actuals,
    attribution_rows,
    build_waterfall,
    guard_cleared_leg,
    ic_leg,
    ic_passthrough,
    ic_scale_to_bench,
    stage_arbitrage,
    stage_capacity,
)
from libs.asset_risk.diagnostics import (
    CAPCOMP_RATE_DEFAULT,
    _month_asset_matrix,
    chain_asset_names,
    load_maps,
    exec_gap_monthly,
)
from libs.asset_risk import ic_case
from libs.asset_risk.review import store as review_store
from libs.asset_risk.review.overlay import reviewed_keys, waterfall_recon
from libs.asset_risk.chain import _attach_prices, _check_sign, _load_chain, _load_prices



# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

def _load_assets(engine) -> pd.DataFrame:
    with engine.connect() as conn:
        df = pd.read_sql(text(
            "SELECT id, name, capacity_mw, bess_duration_h, commission_date "
            "FROM marketdata.rm_assets WHERE asset_type = 'bess' ORDER BY name"
        ), conn)
    return df[df["name"].isin(chain_asset_names(engine))].reset_index(drop=True)



def stage_waterfall_figure(wf: pd.DataFrame) -> go.Figure:
    """Classic waterfall on stage TOTALS (arb+cap+fees aggregated per stage).

    plotly's go.Waterfall computes the bridge geometry itself — no manual base
    math, no overlapping segments. Stage bars absolute, delta bars relative.
    """
    stage_totals = {}
    for stage in ("投决预期", "投资标准", "申报", "出清", "实际"):
        stage_totals[stage] = float(wf[wf["stage"] == stage]["cny"].sum())
    d1 = stage_totals["申报"] - stage_totals["投资标准"]
    d2 = stage_totals["出清"] - stage_totals["申报"]
    d3 = stage_totals["实际"] - stage_totals["出清"]

    x = ["投资标准", "Δ策略与预测", "申报", "Δ出清校核", "出清", "Δ执行与费用", "实际"]
    measures = ["absolute", "relative", "absolute", "relative", "absolute", "relative", "absolute"]
    y = [stage_totals["投资标准"], d1, stage_totals["申报"], d2,
         stage_totals["出清"], d3, stage_totals["实际"]]
    texts = [f"<b>¥{stage_totals['投资标准'] / 1e6:,.2f}M</b>",
             f"{d1 / 1e6:+,.2f}M",
             f"<b>¥{stage_totals['申报'] / 1e6:,.2f}M</b>",
             f"{d2 / 1e6:+,.2f}M",
             f"<b>¥{stage_totals['出清'] / 1e6:,.2f}M</b>",
             f"{d3 / 1e6:+,.2f}M",
             f"<b>¥{stage_totals['实际'] / 1e6:,.2f}M</b>"]

    if (wf["stage"] == "投决预期").any():
        d0 = stage_totals["投资标准"] - stage_totals["投决预期"]
        x = ["投决预期", "市场偏差"] + x
        measures = ["absolute", "relative"] + measures
        y = [stage_totals["投决预期"], d0] + y
        texts = [f"<b>¥{stage_totals['投决预期'] / 1e6:,.2f}M</b>", f"{d0 / 1e6:+,.2f}M"] + texts

    fig = go.Figure(go.Waterfall(
        x=x, measure=measures, y=y, text=texts, textposition="outside",
        connector={"line": {"color": "rgb(120, 120, 120)", "dash": "dot"}},
        increasing={"marker": {"color": "#2ecc71"}},
        decreasing={"marker": {"color": "#e74c3c"}},
        totals={"marker": {"color": "#3498db"}},
    ))
    fig.update_layout(
        title="收益瀑布（合计口径：套利 + 容量补偿 + 费用）",
        yaxis_title="CNY", height=440, showlegend=False,
    )
    return fig


def deviation_breakdown_figure(wf: pd.DataFrame) -> go.Figure:
    """Deviation decomposition: per delta column, its components as ADJACENT
    grouped bars (no overlap) — makes compensation structure explicit, e.g.
    套利 −1.40M next to 容量补偿 +1.72M."""
    delta_stages = ["Δ策略与预测", "Δ出清校核", "Δ执行与费用"]
    if (wf["stage"] == "市场偏差").any():
        delta_stages = ["市场偏差"] + delta_stages
    comps = ["arb_cny", "cap_cny", "fee_cny", "other_cny"]
    fig = go.Figure()
    for c in comps:
        vals = []
        texts = []
        for s in delta_stages:
            v = float(wf[(wf["stage"] == s) & (wf["component"] == c)]["cny"].sum())
            vals.append(v)
            texts.append(f"{v / 1e6:+,.2f}M" if abs(v) > 1e4 else "")
        fig.add_trace(go.Bar(
            name=_COMPONENT_CN[c], x=delta_stages, y=vals,
            marker_color=_COMPONENT_COLORS[c], text=texts, textposition="outside",
        ))
    fig.update_layout(
        title="偏差分解（按成因并排展示，正=增益，负=损失）",
        yaxis_title="CNY", barmode="group", height=340,
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    return fig
def _review_overlay(engine, fig, assets, sel_months, nom_map, nominated, cleared, actual):
    """Add the 复盘 marker trace to fig; return (recon_df, included asset-months) or None.

    Any failure (e.g. review table not migrated) degrades silently to no overlay.
    """
    try:
        codes = [nom_map[n] for n in assets["name"] if nom_map.get(n)]
        if not codes:
            return None
        rv = review_store.load_review(engine, asset_codes=codes,
                                      months=[f"{m}-01" for m in sel_months])
        if rv.empty:
            return None
        parts = []
        for stage, leg in (("申报", nominated), ("出清", cleared), ("实际", actual)):
            if leg is None or leg.empty:
                continue
            parts.append(leg[["asset", "month", "arb_cny", "cap_cny"]].assign(stage=stage))
        if not parts:
            return None
        stg = pd.concat(parts, ignore_index=True)
        stg["asset_code"] = stg["asset"].map(nom_map)
        recon_df = waterfall_recon(stg.dropna(subset=["asset_code"]), rv)
        if recon_df.empty:
            return None
        fig.add_trace(go.Scatter(
            x=recon_df["stage"], y=recon_df["reviewed_cny"], mode="markers", name="复盘",
            marker={"symbol": "diamond", "size": 13, "color": "#f39c12",
                    "line": {"width": 1, "color": "#333"}}))
        fig.update_layout(showlegend=True)
        code_name = {v: k for k, v in nom_map.items()}
        return recon_df, sorted((code_name.get(c, c), m) for c, m in reviewed_keys(rv))
    except Exception:
        return None


@st.cache_resource(show_spinner=False)
def _ensure_legs_table(_engine):
    ensure_table(_engine)
    return True


@st.cache_data(ttl=600, show_spinner=False)
def _cached_chain(_engine, ids: tuple[int, ...]) -> pd.DataFrame:
    """Full-history chain for the waterfall's month list (sign guard deferred to
    the displayed subset)."""
    plant_map, nom_map, _ = load_maps(_engine)
    return _load_chain(_engine, list(ids), None, None, nom_code_map=nom_map,
                       plant_map=plant_map, check_sign=False)


# The current (incomplete) month is never persisted by get_or_compute — these
# short-TTL wrappers stop the default view re-solving its LPs on every rerun.
@st.cache_data(ttl=3600, show_spinner=False)
def _cached_benchmark_leg(_engine, *args, **kwargs):
    return _wf.benchmark_leg(_engine, *args, **kwargs)


@st.cache_data(ttl=3600, show_spinner=False)
def _cached_forecast_leg(_engine, *args, **kwargs):
    return _wf.forecast_optimal_leg(_engine, *args, **kwargs)



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

- 投资标准：LP 完美预见调度（15 分钟 RT 节点电价，纯现货套利目标），参数取标准表（容量=资产配置容量，DOD 90%，SOH/RTE 按投运年限），蒙西调度约束：爬坡 3.3%/min + 每日最多 1.5 次循环（2026-09 用户确认）；容量补偿按放电量 × 标准单价另计。补贴不内化于 LP 目标函数的实测效果（2026-06 悦杭独贵）：放电 13.7 GWh/月 ≈ 1.5 次/日（与实际电站节奏一致；内化则为 21.5 GWh ≈ 2.4 次/日，超出实际运营水平），且求解快 60 倍。
- 申报/出清两腿按 15 分钟节点电价计价；账单充电按小时均价结算（已确认的结算规则），与 15 分钟口径存在 ±1–4% 基差，归入"出清→实际"偏差。
- 费用项（系统运行费、线损、其他费用）仅存在于实际段（结算单），前两腿为零 — 这是 Δ执行与费用 的组成部分，不是错误。
- commission_date 缺失的资产按 Year 0（最新参数）处理，并在下方列出警告。
- 偏差归因的预测最优采用 ols_rt_time_v1 小时级预测调度、按实际小时均价结算（沿用 strategy_comparison 口径；与 15 分钟口径存在基差）。
- LP 窗口 1 天、SOC 窗口起点为 0；价格或调度链缺失的日期跳过（见数据覆盖）。引擎跳过任何含空槽的日期；当月出清电价（md_id_cleared_energy.cleared_price）无完整日时，投资标准回退至节点填充电价（md_mengxi_nodal_price_96_filled.avg_node_price，同一节点市场价、完整 96 槽），并在页面标注。
- 申报腿数据来源：2026-09 起为企业微信自动抓取的调度计划表（marketdata.bess_nomination_96，每日两次自动更新）；此前为人工上传的调度计划表（rm_dispatch_chain，止于 2026-08-30）。重叠时段以自动抓取为准；9 月以后的出清腿由交易所出清电量（md_id_cleared_energy）补齐。
- 出清腿校验：出清放电量与账单放电量偏差 >30% 的 (资产, 月) 判为出清数据不可信，该腿按账单口径降级展示并在页面标注（2026-11-05 用户确认阈值；触发例：查干哈达 2026-09 出清 294k MWh vs 账单 107k MWh）。
"""


def render_ic_upload(engine):
    """Expander: IC assumptions workbook -> parse -> preview + notes -> save."""
    with st.expander("投决边界上传 (IC assumptions)"):
        up = st.file_uploader("投决边界 xlsx（内蒙古储能项目经评边界值 / 投委会预审会）",
                              type=["xlsx"], key="wf_ic_upload")
        if up is None:
            return
        try:
            ic_df, notes = ic_case.parse_ic_workbook(up, up.name)
        except Exception as e:  # unreadable/corrupt workbook: report, never crash the tab
            st.error(f"无法解析 {up.name}: {e}")
            return
        for n in notes:
            st.caption(n)
        if ic_df.empty:
            st.warning("未解析到任何投决边界（见上方说明）。")
            return
        prev = (ic_df.groupby("asset_code")
                .agg(case_label=("case_label", "first"), capacity_mw=("capacity_mw", "first"),
                     energy_mwh=("energy_mwh", "first"), ic_cod=("ic_cod", "first"),
                     y1_cycles=("cycles_per_year", "first"), y1_charge=("charge_price", "first"),
                     y1_discharge=("discharge_price", "first"), grid_fee=("grid_fee", "first"))
                .reset_index())
        st.dataframe(prev, use_container_width=True, hide_index=True)
        st.caption("保存将按 asset_code 覆盖已存储的投决边界（价格/费用单位 ¥/MWh）。")
        if st.button("保存", key="wf_ic_save"):
            ic_case.ensure_table(engine)
            ic_case.save_ic(engine, ic_df)
            st.success(f"已保存 {ic_df['asset_code'].nunique()} 个资产的投决边界。")


def _bench_note_captions(bench_rows: list[dict]) -> tuple[str | None, str | None]:
    """(filled-price fallback caption, no-price ¥0 caption) from benchmark notes."""
    filled = sorted({b["asset"] for b in bench_rows if b.get("note") == "节点填充电价"})
    zero = sorted((b["asset"], b["month"]) for b in bench_rows
                  if b.get("note") in ("no prices", "no complete price day"))
    filled_cap = ("投资标准价格回退：以下资产当月出清电价无完整日（缺口 ~32%），"
                  f"投资标准按节点填充电价计算：{'、'.join(filled)}") if filled else None
    zero_cap = ("投资标准无可用电价，按 ¥0 显示："
                + "、".join(f"{a} {m}" for a, m in zero)) if zero else None
    return filled_cap, zero_cap


def render_waterfall(engine):
    """Render 收益瀑布 P&L Waterfall tab."""
    st.subheader("收益瀑布 P&L Waterfall")

    from apps.asset_risk.tab_wind_replication import WIND_LABEL
    mode = st.radio("资产类型", ["储能", WIND_LABEL], horizontal=True, key="wf_mode")
    if mode != "储能":
        from apps.asset_risk.tab_wind_replication import render_wind_waterfall
        render_wind_waterfall(engine)
        return

    assets = _load_assets(engine)
    if assets.empty:
        st.warning("No BESS assets found.")
        return
    render_ic_upload(engine)

    plant_map, nom_map, rate_map = load_maps(engine)
    chain = _cached_chain(engine, tuple(sorted(int(i) for i in assets["id"]))).copy()
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
    _ensure_legs_table(engine)
    force_legs = st.button("重算所选月份", key="wf_recompute",
                           help="忽略已存储的投资标准/预测最优腿，对所选资产×月份重新求解并覆盖")
    if force_legs:
        _cached_benchmark_leg.clear()
        _cached_forecast_leg.clear()

    df = chain[chain["month"].isin(sel_months)].copy()
    month_end = (pd.Timestamp(max(sel_months) + "-01") + pd.offsets.MonthBegin(1)).strftime("%Y-%m-%d")
    prices = _load_prices(engine, [plant_map[a] for a in assets["name"]],
                          min(sel_months), month_end)
    df = _attach_prices(df, prices, {a: plant_map[a] for a in assets["name"]})

    if sel_asset != "全部资产":
        df = df[df["asset"] == sel_asset]
        assets = assets[assets["name"] == sel_asset]

    if df.empty:
        # 查干哈达 2025-09: month exists in the chain via other assets — after the
        # asset filter nothing remains and stage_arbitrage returned a columnless
        # frame → KeyError: 'dis_mwh' on the cap line.
        st.info("所选资产在所选月份没有调度链数据（该月链条由其他资产贡献）。")
        return
    try:
        _check_sign(df)
    except ValueError as e:
        st.error(str(e))
        st.stop()

    # --- legs ---
    nominated = stage_arbitrage(df, "nominated_mw")
    nominated["cap_cny"] = stage_capacity(nominated["dis_mwh"], nominated["asset"], rate_map)
    cleared = stage_arbitrage(df, "rt_cleared_mw")
    cleared["cap_cny"] = stage_capacity(cleared["dis_mwh"], cleared["asset"], rate_map)

    actual = _load_settlement_actuals(engine, assets["id"].tolist(),
                                      min(sel_months), None)
    actual = actual[actual["month"].isin(sel_months)] if not actual.empty else actual

    # Unit assertion: cleared > 3× billed discharge means a unit error (e.g. a
    # stray ×4 on interval-average MW), not a feed gap — stop, don't downgrade.
    if not cleared.empty and not actual.empty and "dis_mwh" in actual.columns:
        billed = actual.set_index(["asset", "month"])["dis_mwh"]
        unit_bad = []
        for _, row in cleared.iterrows():
            b = billed.get((row["asset"], row["month"]))
            if b is not None and pd.notna(b) and b > 0 and row["dis_mwh"] / b > 3:
                unit_bad.append(f"{row['asset']} {row['month']} ({row['dis_mwh'] / b:.1f}×)")
        if unit_bad:
            st.error("出清放电量超过账单放电量 3 倍（疑似单位错误，页面停止）: " + "、".join(unit_bad))
            st.stop()

    cleared, cleared_untrusted = guard_cleared_leg(cleared, actual)
    if cleared_untrusted:
        st.warning("出清数据不可信（出清电量与账单放电量偏差 >30%，出清腿已按账单口径降级展示）: "
                   + "、".join(f"{a} {m}" for a, m in cleared_untrusted))

    bench_rows = []
    missing_commission = []
    work = []
    for _, a in assets.iterrows():
        commission_iso = None
        if pd.notna(a.get("commission_date")):
            commission_iso = str(a["commission_date"])[:10]
        else:
            missing_commission.append(a["name"])
        for month in sel_months:
            work.append((a, month, commission_iso))

    def _solve(item):
        a, month, commission_iso = item
        rate = rate_map.get(a["name"], CAPCOMP_RATE_DEFAULT)
        aid = int(a["id"])
        ikey = legs_inputs_key(a["capacity_mw"], a["bess_duration_h"], commission_iso, rate)
        # 重算 bypasses the TTL cache so it really re-solves.
        bench_fn = _wf.benchmark_leg if force_legs else _cached_benchmark_leg
        fc_fn = _wf.forecast_optimal_leg if force_legs else _cached_forecast_leg
        r = get_or_compute(
            engine, "benchmark", aid, month,
            lambda: bench_fn(engine, aid, month, float(a["capacity_mw"]),
                                      float(a["bess_duration_h"]), commission_iso, rate,
                                      plant_map[a["name"]], asset_name=a["name"],
                                      asset_code=nom_map.get(a["name"])),
            force=force_legs, inputs_key=ikey)
        fr = get_or_compute(
            engine, "forecast", aid, month,
            lambda: fc_fn(engine, aid, month, float(a["capacity_mw"]),
                                             float(a["bess_duration_h"]), commission_iso,
                                             plant_map[a["name"]]),
            force=force_legs, inputs_key=ikey)
        return {"asset": a["name"], "month": month, **r, "fc_arb_cny": fr.get("arb_cny") or 0.0}

    if len(work) > 1:
        status = st.status(f"正在计算投资标准基准 (LP ×{len(work)})…", expanded=False)
    from concurrent.futures import ThreadPoolExecutor, as_completed
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(_solve, item): item for item in work}
        done = 0
        for fut in as_completed(futures):
            bench_rows.append(fut.result())
            done += 1
            if len(work) > 1:
                status.update(label=f"正在计算投资标准基准 (LP {done}/{len(work)})…")
    if len(work) > 1:
        status.update(label="投资标准基准计算完成", state="complete", expanded=False)
    bench = pd.DataFrame(bench_rows) if bench_rows else pd.DataFrame(
        columns=["asset", "month", "arb_cny", "dis_mwh", "cap_cny"])
    if missing_commission:
        st.warning(f"以下资产 commission_date 缺失，按 Year 0 参数计算：{'、'.join(missing_commission)}")
    for cap in _bench_note_captions(bench_rows):
        if cap:
            st.caption(cap)

    missing_bills = _wf.missing_bill_keys(actual, assets["name"].tolist(), sel_months)
    if missing_bills:
        st.warning(_wf.MISSING_BILLS_NOTE.format(
            keys="、".join(f"{a} {m}" for a, m in missing_bills)))
    actual_has_bills = not actual.empty and len(actual) > 0
    if not actual_has_bills:
        actual = pd.DataFrame([{"asset": n, "month": m, "arb_cny": 0.0, "cap_cny": 0.0,
                                "fee_cny": 0.0, "other_cny": 0.0}
                               for n in assets["name"] for m in sel_months])
        st.caption("所选月份无结算单 — 实际段显示为 0（账单尚未发布；通常次月中旬可得）。")
    elif missing_bills:
        # Partial selection: chart only asset-months that have bills.
        missing_set = set(missing_bills)
        billed_keys = [(n, m) for n in assets["name"] for m in sel_months
                       if (n, m) not in missing_set]
        st.info("以下资产×月份无结算单，已从瀑布图中排除："
                + "、".join(f"{a} {m}" for a, m in missing_bills))
        bench = _wf.restrict_to_keys(bench, billed_keys)
        nominated = _wf.restrict_to_keys(nominated, billed_keys)
        cleared = _wf.restrict_to_keys(cleared, billed_keys)
        actual = _wf.restrict_to_keys(actual, billed_keys)
        df = _wf.restrict_to_keys(df, billed_keys)
        bench_rows = [r for r in bench_rows if (r["asset"], r["month"]) in set(billed_keys)]

    # --- 投决预期 (IC) stage. Other legs are never restricted: asset-months without
    # an IC case enter this stage at their own 投资标准 value (市场偏差 = 0).
    ic_leg_df = None
    try:
        ic_all = ic_case.load_ic(engine)
    except Exception:
        ic_all = pd.DataFrame(columns=ic_case.IC_COLUMNS)   # table not created yet
    if not ic_all.empty and not bench.empty:
        keys = sorted(set(zip(bench["asset"], bench["month"])))
        commission = {r["name"]: r.get("commission_date") for _, r in assets.iterrows()}
        leg, no_ic = ic_leg(ic_all, keys, nom_map, commission)
        if leg.empty:
            st.caption("所选资产×月份均无投决预期数据，瀑布不含投决预期段。")
        else:
            if no_ic:
                st.caption("无投决边界，按投资标准计入: " + "、".join(f"{a} {m}" for a, m in no_ic))
            leg, scaled = ic_scale_to_bench(leg, bench)
            if scaled:
                st.caption("投决预期按投资标准已求解天数折算: " + "、".join(f"{a} {m}" for a, m in scaled))
            ic_leg_df = ic_passthrough(leg, no_ic, bench)

    wf = build_waterfall(bench, nominated, cleared, actual, ic=ic_leg_df)

    # --- KPI strip ---
    def _tot(stage, comp):
        return float(wf[(wf["stage"] == stage) & (wf["component"] == comp)]["cny"].sum())

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("投资标准", f"¥{sum(_tot('投资标准', c) for c in ('arb_cny', 'cap_cny')):,.0f}")
    _nobill_help = "部分资产×月份尚无结算单"
    k2.metric("实际收益（账单）",
              "—" if missing_bills else
              f"¥{sum(_tot('实际', c) for c in ('arb_cny', 'cap_cny', 'fee_cny', 'other_cny')):,.0f}",
              help=_nobill_help if missing_bills else
              "现货收益 + 容量补偿 − 系统运行费 − 线损费用（复盘 slide 3 口径）")
    k3.metric("实际套利", "—" if missing_bills else f"¥{_tot('实际', 'arb_cny'):,.0f}",
              help=_nobill_help if missing_bills else None)
    k4.metric("实际容量补偿", "—" if missing_bills else f"¥{_tot('实际', 'cap_cny'):,.0f}",
              help=_nobill_help if missing_bills else None)

    fig_wf = stage_waterfall_figure(wf)
    recon = _review_overlay(engine, fig_wf, assets, sel_months, nom_map, nominated, cleared, actual)
    st.plotly_chart(fig_wf, use_container_width=True)
    if recon is not None:
        recon_df, included = recon
        st.markdown("**复盘对照（申报/出清/实际：计算套利+容量补偿 vs 复盘收入，均不含费用）**")
        st.dataframe(recon_df.style.format({"computed_cny": "¥{:,.0f}", "reviewed_cny": "¥{:,.0f}",
                                            "diff_cny": "¥{:+,.0f}", "diff_pct": "{:+.1%}"}, na_rep="—"),
                     use_container_width=True, hide_index=True)
        st.caption("复盘口径=现货+容量补偿（费用前）。仅汇总有复盘数据的资产×月份："
                   + "、".join(f"{a} {m}" for a, m in included))
    st.plotly_chart(deviation_breakdown_figure(wf), use_container_width=True)

    # --- 偏差归因 (loss breakdown by cause) ---
    st.markdown("#### 偏差归因（按成因分解损失）")
    attr = attribution_rows(df, wf, bench, nominated, cleared, actual, bench_rows)
    st.dataframe(attr[["偏差区间", "成因", "电量_fmt", "金额_fmt"]]
                 .rename(columns={"电量_fmt": "电量 (MWh)", "金额_fmt": "金额"}),
                 use_container_width=True, hide_index=True)
    st.caption("各段行合计 ≡ 上方瀑布对应段（正=增益，负=损失）：套利成因=预测误差+申报策略；"
               "容量补偿行 = Δ放电量 × 补偿单价；系统运行费/线损/其他费用仅存在于结算单（实际段）。"
               "成因列为链数据全缺时显示 无数据（9 月起自动申报链只补齐出清电量）。"
               "预测最优基于 ols_rt_time_v1（小时级，与实际价差结算；详见口径与假设）。")

    with st.expander("偏差定义（复盘）"):
        st.markdown(_DEFINITIONS_MD)
    with st.expander("口径与假设 / 数据覆盖"):
        st.markdown(_CAVEATS_MD)
        cov = (df.groupby(["asset", "month"]).agg(intervals=("ts", "count"),
               priced=("price_cny_mwh", lambda s: f"{s.notna().mean():.0%}")).reset_index())
        st.dataframe(cov, use_container_width=True, hide_index=True)

    # --- stage × month matrix ---
    st.markdown("**各段收益 by 资产 × 月 (¥)**")
    mat_src = pd.concat(([ic_leg_df.assign(stage="投决预期")[["asset", "month", "stage", "arb_cny", "cap_cny", "fee_cny"]]]
                         if ic_leg_df is not None else []) + [
        bench.assign(stage="投资标准")[["asset", "month", "stage", "arb_cny", "cap_cny"]],
        nominated.assign(stage="申报")[["asset", "month", "stage", "arb_cny", "cap_cny"]],
        cleared.assign(stage="出清")[["asset", "month", "stage", "arb_cny", "cap_cny"]],
        actual.assign(stage="实际")[["asset", "month", "stage", "arb_cny", "cap_cny"]],
    ], ignore_index=True)
    mat_src["total_cny"] = mat_src["arb_cny"] + mat_src["cap_cny"]
    if "fee_cny" in mat_src.columns:   # only the IC leg carries fee_cny (grid fee, negative)
        mat_src["total_cny"] += mat_src["fee_cny"].fillna(0.0)
    mat = mat_src.pivot_table(index="month", columns="stage", values="total_cny",
                              aggfunc="sum", fill_value=0)
    st.dataframe(mat.style.format("¥{:,.0f}"), use_container_width=True)
