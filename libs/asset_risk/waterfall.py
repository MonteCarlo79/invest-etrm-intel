"""Waterfall stage math and LP legs (moved verbatim from apps/asset_risk/tab_waterfall.py).

Pure logic (UI-free). Stage definitions: 投资标准 -> 申报 -> 出清 -> 实际.
"""
from __future__ import annotations

import pandas as pd
from sqlalchemy import bindparam, text

from libs.asset_risk.standard_params import params_for_asset
from libs.asset_risk.chain import _attach_prices, _load_chain, _load_prices
from libs.asset_risk.diagnostics import (
    AUTO_NOM_CODE,
    CAPCOMP_RATE,
    CAPCOMP_RATE_DEFAULT,
    PLANT_MAP,
    _month_asset_matrix,
    exec_gap_monthly,
)

_COMPONENT_COLORS = {"arb_cny": "#3498db", "cap_cny": "#9b59b6",
                     "fee_cny": "#e74c3c", "other_cny": "#95a5a6"}
_COMPONENT_CN = {"arb_cny": "套利", "cap_cny": "容量补偿", "fee_cny": "系统运行费+线损", "other_cny": "其他费用"}
_STAGES = ["投资标准", "Δ策略与预测", "申报", "Δ出清校核", "出清", "Δ执行与费用", "实际"]

# 出清腿降级阈值 (user-set 2026-11-05): 查干哈达 2026-09 cleared 294k MWh vs
# billed 107k MWh (2.76×; synthetic ±834 MWh/15min rows) — a broken 出清 feed
# shows as a huge Δ出清校核 that Δ执行与费用 mirrors back by construction.
CLEARED_VOLUME_TOLERANCE = 0.30


def _load_settlement_actuals(engine, asset_ids: list[int],
                             start: str | None, end: str | None) -> pd.DataFrame:
    """Per (asset, month) from settlement bills: arb/cap/fee/other (signed amounts).

    start/end accept ISO dates or 'YYYY-MM' month tokens (normalized via
    _month_token_bounds from tab_diagnostics)."""
    from libs.asset_risk.chain import _month_token_bounds
    start, end = _month_token_bounds(start, end)
    sql = """
        SELECT a.id AS asset_id, a.name AS asset, s.settlement_month AS month,
               si.category, si.amount_cny, si.volume_mwh
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
        return pd.DataFrame(columns=["asset", "month", "arb_cny", "cap_cny", "fee_cny",
                                     "other_cny", "dis_mwh", "chg_mwh"])
    df["month"] = df["month"].astype(str).str[:7]
    rows = []
    for (asset, month), g in df.groupby(["asset", "month"]):
        amt = g.groupby("category")["amount_cny"].sum()
        vol = g.groupby("category")["volume_mwh"].sum()
        fee = float(amt.get("coal_capacity_charge", 0.0) + amt.get("system_operation", 0.0))
        cap = float(amt.get("capacity_compensation", 0.0))
        arb = float(amt.get("discharge_energy", 0.0) + amt.get("generation_revenue", 0.0)
                    + amt.get("charge_energy", 0.0))
        other = float(amt.drop(index=[c for c in amt.index
                                      if c in ("discharge_energy", "generation_revenue",
                                               "charge_energy", "capacity_compensation",
                                               "coal_capacity_charge", "system_operation")]).sum())
        rows.append({"asset": asset, "month": month, "arb_cny": arb, "cap_cny": cap,
                     "fee_cny": fee, "other_cny": other,
                     "dis_mwh": float(vol.get("discharge_energy", 0.0)
                                      + vol.get("generation_revenue", 0.0)),
                     "chg_mwh": float(abs(vol.get("charge_energy", 0.0)))})
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Stage math (pure)
# ---------------------------------------------------------------------------

def stage_arbitrage(df: pd.DataFrame, mw_col: str) -> pd.DataFrame:
    """Per (asset, month): discharge/charge volumes and arbitrage ¥ for a MW column.

    df columns: asset, month, <mw_col> (MW, + = discharge), price_cny_mwh.
    arb_cny = Σ mw × 0.25 × price (NaN price skipped; MWh counted regardless).
    Empty input returns a columned empty frame — a columnless one crashes
    callers on df["dis_mwh"] (查干哈达 2025-09: month existed via other assets,
    nothing left after the asset filter → KeyError on the cap line).
    """
    rows = []
    for (asset, month), g in df.groupby(["asset", "month"]):
        mw = g[mw_col].astype(float)
        dis_mwh = float((mw.clip(lower=0) * 0.25).sum())
        chg_mwh = float((-mw.clip(upper=0) * 0.25).sum())
        arb = float((mw * 0.25 * g["price_cny_mwh"]).sum())
        rows.append({"asset": asset, "month": month, "dis_mwh": dis_mwh,
                     "chg_mwh": chg_mwh, "arb_cny": arb})
    if not rows:
        return pd.DataFrame(columns=["asset", "month", "dis_mwh", "chg_mwh", "arb_cny"])
    return pd.DataFrame(rows)


def stage_capacity(dis_mwh: pd.Series, assets: pd.Series) -> pd.Series:
    """discharge MWh × capacity-subsidy rate per asset (350 default / 280 named)."""
    return pd.Series(
        [float(d) * CAPCOMP_RATE.get(a, CAPCOMP_RATE_DEFAULT) for d, a in zip(dis_mwh, assets)],
        index=dis_mwh.index,
    )


def guard_cleared_leg(cleared: pd.DataFrame, actual: pd.DataFrame,
                      tolerance: float = CLEARED_VOLUME_TOLERANCE) -> tuple[pd.DataFrame, list]:
    """Downgrade 出清 legs whose discharge volume diverges from billed > tolerance.

    The 出清 leg feeds from exchange cleared energy (auto-extension after the
    manual chain ingest stopped 2026-08-30). When that feed breaks, the
    waterfall shows a phantom Δ出清校核 that Δ执行与费用 mirrors back by
    construction — pure noise. Affected legs are replaced with billed values
    (arb/cap/volumes) and returned in the untrusted list for the page banner.
    Months with no billed discharge volume (b ≤ 0 / absent) are left as-is —
    no ground truth to check against.
    """
    if cleared.empty or actual.empty or "dis_mwh" not in actual.columns:
        return cleared, []
    billed = actual.set_index(["asset", "month"])[["dis_mwh", "chg_mwh", "arb_cny", "cap_cny"]]
    out = cleared.copy()
    untrusted: list[tuple[str, str]] = []
    for idx, row in out.iterrows():
        key = (row["asset"], row["month"])
        if key not in billed.index:
            continue
        b = billed.loc[key]
        if not b["dis_mwh"] or b["dis_mwh"] <= 0:
            continue
        if abs(row["dis_mwh"] - b["dis_mwh"]) / b["dis_mwh"] > tolerance:
            untrusted.append(key)
            out.loc[idx, ["dis_mwh", "chg_mwh", "arb_cny", "cap_cny"]] = \
                [b["dis_mwh"], b["chg_mwh"], b["arb_cny"], b["cap_cny"]]
    return out, untrusted


def _load_nodal_prices_filled(engine, asset_name: str, start: str, end: str) -> pd.Series:
    """Filled nodal RT price (md_mengxi_nodal_price_96_filled.avg_node_price)
    for the station's own registry node — the same market price, complete
    96-slot days. metric_time stamps interval START (no −15min shift, unlike
    md_id_cleared_energy.cleared_price which stamps period end). Empty series
    when the asset has no registry mapping/node."""
    from libs.trading_book.registry import get_station

    code = AUTO_NOM_CODE.get(asset_name)
    if not code:
        return pd.Series(dtype=float)
    station = get_station(code)
    if not station.price_nodes:
        return pd.Series(dtype=float)
    df = pd.read_sql(text("""
        SELECT metric_time, avg_node_price
        FROM marketdata.md_mengxi_nodal_price_96_filled
        WHERE node_name = :node AND metric_time >= :s AND metric_time < :e
        ORDER BY metric_time
    """), engine, params={"node": station.price_nodes[0], "s": start, "e": end})
    if df.empty:
        return pd.Series(dtype=float)
    s = pd.Series({pd.Timestamp(t): float(v)
                   for t, v in zip(df["metric_time"], df["avg_node_price"])
                   if pd.notna(v)})
    return s[~s.index.duplicated(keep="last")].sort_index()


def benchmark_leg(engine, asset_id: int, month: str, capacity_mw: float,
                  duration_h: float, commission_iso: str | None,
                  capcomp_rate: float, plant: str,
                  asset_name: str = "") -> dict:
    """Investment-standard leg: LP perfect-foresight with standard params for one
    asset-month. Cached on (asset_id, month, ...) — historical inputs immutable.

    LP input prices are shifted −15min so each interval-start index maps to the
    period-END price stamp (same alignment as arb_match).
    """
    from services.bess_map.optimisation_engine import compute_dispatch_from_15min_prices  # lazy (pulp)

    month_start = pd.Timestamp(month + "-01")
    month_end = (month_start + pd.offsets.MonthBegin(1)).strftime("%Y-%m-%d")
    params = params_for_asset(capacity_mw, duration_h, commission_iso, month_start)

    prices = _load_prices(engine, [plant], month_start.strftime("%Y-%m-%d"), month_end)
    if prices.empty:
        return {"arb_cny": 0.0, "dis_mwh": 0.0, "chg_mwh": 0.0, "cap_cny": 0.0,
                "days_solved": 0, "days_skipped": 0, "age": params["age"], "note": "no prices"}
    s = pd.Series({pd.Timestamp(t) - pd.Timedelta(minutes=15): float(v)
                   for t, v in zip(prices["datetime"], prices["cleared_price"]) if pd.notna(v)})
    s = s[~s.index.duplicated(keep="last")].sort_index()
    s.index = pd.DatetimeIndex(s.index)

    def _run(series):
        return compute_dispatch_from_15min_prices(
            series, power_mw=capacity_mw,
            duration_h=params["duration_h"] * params["dod"] * params["soh"],
            roundtrip_eff=params["roundtrip_eff"], window_days=1,
            ramp_rate_pct_per_min=3.3, max_cycles_per_day=1.5,
            # 蒙西 operator rules: ramp 3.3%/min + max 1.5 cycles/day per asset (user 2026-09).
            # capacity subsidy NOT internalised in the objective: it makes the MILP
            # ~25-500x slower (every discharge marginally profitable explodes the binary
            # search) while changing discharge volume by only ~0.2% (measured 2026-06,
            # 悦杭独贵: 903.1 vs 904.6 MWh). Subsidy is added outside as dis_mwh x rate.
        )

    dispatch_df, profit_s = _run(s)
    price_note = ""
    if len(profit_s) == 0:
        # Cleared-price feed has no COMPLETE 96-slot day this month (the engine
        # skips any day with a NaN slot; 2026-10: ~32% nulls). Fall back to the
        # filled nodal price — the same market price, complete days.
        s2 = _load_nodal_prices_filled(engine, asset_name,
                                       month_start.strftime("%Y-%m-%d"), month_end)
        if not s2.empty:
            dispatch_df, profit_s = _run(s2)
            price_note = "节点填充电价"
    if len(profit_s) == 0:
        return {"arb_cny": 0.0, "dis_mwh": 0.0, "chg_mwh": 0.0, "cap_cny": 0.0,
                "days_solved": 0,
                "days_skipped": int(s.groupby(s.index.date).ngroups) if not s.empty else 0,
                "age": params["age"], "note": "no complete price day"}
    dis_mwh = float((dispatch_df["discharge_mw"] * 0.25).sum()) if not dispatch_df.empty else 0.0
    chg_mwh = float((dispatch_df["charge_mw"] * 0.25).sum()) if not dispatch_df.empty else 0.0
    arb = float(profit_s.sum()) if len(profit_s) else 0.0
    days_in_month = (month_start + pd.offsets.MonthEnd(0)).day
    return {"arb_cny": arb, "dis_mwh": dis_mwh, "chg_mwh": chg_mwh,
            "cap_cny": dis_mwh * capcomp_rate,
            "days_solved": int(len(profit_s)), "days_skipped": days_in_month - int(len(profit_s)),
            "age": params["age"], "note": price_note}


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




# ---------------------------------------------------------------------------
# Deviation attribution (偏差归因): pin each waterfall deviation to named
# causes with MWh + ¥. Decompositions sum exactly to their waterfall column.
# ---------------------------------------------------------------------------

def forecast_optimal_leg(engine, asset_id: int, month: str, capacity_mw: float,
                         duration_h: float, commission_iso: str | None,
                         plant: str) -> dict:
    """Forecast-optimal dispatch for one asset-month: ols_rt_time_v1 forecast
    (hourly, on the asset's plant RT series) → LP → settled at ACTUAL hourly
    mean prices. Mirrors strategy_comparison Skill-3 convention (hourly
    granularity, province-model caveat)."""
    from services.bess_map.forecast_engine import build_forecast
    from services.bess_map.optimisation_engine import compute_dispatch_from_hourly_prices

    params = params_for_asset(capacity_mw, duration_h, commission_iso,
                              pd.Timestamp(month + "-01"))
    month_start = pd.Timestamp(month + "-01")
    month_end = (month_start + pd.offsets.MonthBegin(1)).strftime("%Y-%m-%d")

    prices = _load_prices(engine, [plant], None, month_end)
    if prices.empty:
        return {"arb_cny": 0.0, "note": "no prices"}
    s = pd.Series({pd.Timestamp(t): float(v) for t, v in zip(prices["datetime"], prices["cleared_price"])
                   if pd.notna(v)}).sort_index()
    hourly = s.resample("h").mean().dropna()
    hourly_df = pd.DataFrame({"rt_price": hourly})

    rt_pred = build_forecast(hourly_df, model="ols_rt_time_v1")
    target = rt_pred[(rt_pred.index >= month_start) & (rt_pred.index < pd.Timestamp(month_end))].dropna()
    if target.empty:
        return {"arb_cny": 0.0, "note": "no forecast"}

    dispatch_df, _ = compute_dispatch_from_hourly_prices(
        target, power_mw=capacity_mw,
        duration_h=params["duration_h"] * params["dod"] * params["soh"],
        roundtrip_eff=params["roundtrip_eff"], window_days=1,
        max_cycles_per_day=1.5,
    )
    if dispatch_df.empty:
        return {"arb_cny": 0.0, "note": "lp empty"}
    actual_hourly = hourly[target.index[0]:]
    grid = dispatch_df["dispatch_grid_mw"].reindex(actual_hourly.index).fillna(0.0)
    arb = float((grid * actual_hourly).sum())
    return {"arb_cny": arb, "note": ""}


def attribution_rows(df: pd.DataFrame, wf: pd.DataFrame,
                     bench: pd.DataFrame, nominated: pd.DataFrame,
                     cleared: pd.DataFrame, actual: pd.DataFrame,
                     forecast_rows: list[dict]) -> pd.DataFrame:
    """Per-cause attribution for the three waterfall deviations.

    Every row's ¥ follows the waterfall sign convention (+ = gain, − = loss),
    and each segment's rows sum EXACTLY to its waterfall segment total:
    cause rows decompose the arb component, component rows (容量补偿/费用) come
    straight from the wf long frame, residual rows absorb valuation basis gaps.
    Cause rows whose gating chain columns are all-NaN (auto-nomination
    extension era carries rt_cleared only) show 无数据 instead of a fake ¥0.

    df: priced chain frame. wf: build_waterfall output. bench/nominated/
    cleared/actual: the stage legs (cleared already guard-downgraded).
    forecast_rows: forecast_optimal_leg results per (asset, month).
    """
    rows = []

    def _wf_tot(stage: str, comp: str) -> float:
        return float(wf[(wf["stage"] == stage) & (wf["component"] == comp)]["cny"].sum())

    def _data_ok(*cols: str) -> bool:
        return all(c in df.columns and df[c].notna().any() for c in cols)

    def _diff_sum(col_a: str, col_b: str) -> tuple[float, float] | tuple[None, None]:
        """Σ(a − b) volumes (|.|×0.25) and ¥ (signed ×0.25×price), or Nones."""
        if not _data_ok(col_a, col_b):
            return None, None
        total_mwh, total_cny = 0.0, 0.0
        for (asset, month), g in df.groupby(["asset", "month"]):
            v = g[col_a] - g[col_b]
            total_mwh += float((v.abs() * 0.25).sum())
            total_cny += float((v * 0.25 * g["price_cny_mwh"]).sum())
        return total_mwh, total_cny

    # --- Δ策略与预测: 预测误差 + 申报策略 + 容量补偿 ---
    seg = "Δ策略与预测"
    std_arb = float(bench["arb_cny"].sum()) if not bench.empty else 0.0
    std_dis = float(bench["dis_mwh"].sum()) if not bench.empty else 0.0
    # forecast rows may be plain ({arb_cny}) or merged benchmark rows ({..., fc_arb_cny})
    fc_arb = float(sum(r.get("fc_arb_cny", r.get("arb_cny", 0.0)) for r in forecast_rows))
    nom_arb = float(nominated["arb_cny"].sum())
    nom_dis = float(nominated["dis_mwh"].sum())
    rows.append({"偏差区间": seg, "成因": "预测误差（预测最优 − 投资标准）",
                 "mwh": None, "cny": fc_arb - std_arb})
    rows.append({"偏差区间": seg, "成因": "申报策略选择（申报 − 预测最优）",
                 "mwh": None, "cny": nom_arb - fc_arb})
    rows.append({"偏差区间": seg, "成因": "容量补偿（Δ放电量 × 补偿单价）",
                 "mwh": nom_dis - std_dis, "cny": _wf_tot(seg, "cap_cny")})

    # --- Δ出清校核: 日前未中标 + 实时校核 + SOC校核 + 容量补偿 + 残差 ---
    seg = "Δ出清校核"
    clr_dis = float(cleared["dis_mwh"].sum()) if not cleared.empty else 0.0
    mwh, cny = _diff_sum("da_cleared_mw", "nominated_mw")
    rows.append({"偏差区间": seg, "成因": "日前未中标（日前出清 − 申报）",
                 "mwh": mwh, "cny": cny})
    mwh2, cny2 = _diff_sum("rt_cleared_mw", "da_cleared_mw")
    rows.append({"偏差区间": seg, "成因": "实时校核（实时出清 − 日前出清）",
                 "mwh": mwh2, "cny": cny2})
    if _data_ok("soc_pct", "rt_cleared_mw"):
        soc_mwh, soc_cny = 0.0, 0.0
        for (asset, month), g in df.groupby(["asset", "month"]):
            m = g[(g["soc_pct"] >= 95.0) & (g["rt_cleared_mw"] < -0.5)]
            soc_mwh += float((-m["rt_cleared_mw"] * 0.25).sum())
            soc_cny += float((m["rt_cleared_mw"] * 0.25 * m["price_cny_mwh"]).sum())
    else:
        soc_mwh, soc_cny = None, None
    rows.append({"偏差区间": seg, "成因": "　其中：SOC校核（soc≥95% 仍出清充电）",
                 "mwh": soc_mwh, "cny": soc_cny})
    rows.append({"偏差区间": seg, "成因": "容量补偿（Δ放电量 × 补偿单价）",
                 "mwh": clr_dis - nom_dis, "cny": _wf_tot(seg, "cap_cny")})
    arb_resid = _wf_tot(seg, "arb_cny") - (cny or 0.0) - (cny2 or 0.0)
    rows.append({"偏差区间": seg, "成因": "残差（价格基差/出清降级调整）",
                 "mwh": None, "cny": arb_resid})

    # --- Δ执行与费用: 红绿灯 + 非响应 + 调度残差 + 容量补偿 + 费用 + 基差残差 ---
    seg = "Δ执行与费用"
    from libs.asset_risk.diagnostics import restriction_monthly, find_defect_events
    act_dis = float(actual["dis_mwh"].sum()) if not actual.empty and "dis_mwh" in actual.columns else 0.0
    if _data_ok("restriction", "rt_cleared_mw", "actual_mw"):
        rest = restriction_monthly(df)
        rw_cny = float(rest["gap_cny"].sum())
        rw_mwh = float(rest["gap_dis_mwh"].sum() + abs(rest["gap_chg_mwh"].sum()))
    else:
        rw_cny, rw_mwh = None, None
    if _data_ok("actual_mw", "rt_cleared_mw"):
        events = find_defect_events(df)
        def_cny = float(events["lost_cny"].sum()) if not events.empty else 0.0
        def_mwh = float(events["lost_mwh"].abs().sum()) if not events.empty else 0.0
        gap = exec_gap_monthly(df)
        total_exec_cny = float((gap["dis_gap_cny"] + gap["chg_gap_cny"]).sum())
        total_exec_mwh = float((gap["dis_gap_mwh"].abs() + gap["chg_gap_mwh"].abs()).sum())
    else:
        def_cny = def_mwh = total_exec_cny = total_exec_mwh = None
    rows.append({"偏差区间": seg, "成因": "红绿灯限制窗口（仅充电/仅放电时段）",
                 "mwh": rw_mwh, "cny": (-rw_cny if rw_cny is not None else None)})
    rows.append({"偏差区间": seg, "成因": "设备非响应事件（出清≠0 实际≈0）",
                 "mwh": def_mwh, "cny": (-def_cny if def_cny is not None else None)})
    res_mwh = (total_exec_mwh - (rw_mwh or 0.0) - (def_mwh or 0.0)) if total_exec_mwh is not None else None
    res_cny = (total_exec_cny - (rw_cny or 0.0) - (def_cny or 0.0)) if total_exec_cny is not None else None
    rows.append({"偏差区间": seg, "成因": "阻塞/调度核减/涉网试验（调度残差）",
                 "mwh": res_mwh, "cny": (-res_cny if res_cny is not None else None)})
    rows.append({"偏差区间": seg, "成因": "容量补偿（Δ放电量 × 补偿单价）",
                 "mwh": act_dis - clr_dis, "cny": _wf_tot(seg, "cap_cny")})
    rows.append({"偏差区间": seg, "成因": "系统运行费+线损（仅存在于结算单）",
                 "mwh": None, "cny": _wf_tot(seg, "fee_cny")})
    rows.append({"偏差区间": seg, "成因": "其他费用（仅存在于结算单）",
                 "mwh": None, "cny": _wf_tot(seg, "other_cny")})
    basis_resid = _wf_tot(seg, "arb_cny") + (total_exec_cny or 0.0)
    rows.append({"偏差区间": seg, "成因": "结算口径/价格基差（残差）",
                 "mwh": None, "cny": basis_resid})

    out = pd.DataFrame(rows)
    out["金额_fmt"] = out["cny"].map(lambda v: "无数据" if pd.isna(v) else f"¥{v:,.0f}")
    out["电量_fmt"] = out["mwh"].map(lambda v: "—" if pd.isna(v) else f"{v:,.0f}")
    return out[["偏差区间", "成因", "电量_fmt", "金额_fmt", "mwh", "cny"]]
