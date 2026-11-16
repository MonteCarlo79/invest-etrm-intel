"""Waterfall stage math and LP legs (moved verbatim from apps/asset_risk/tab_waterfall.py).

Pure logic (UI-free). Stage definitions: 投资标准 -> 申报 -> 出清 -> 实际.
"""
from __future__ import annotations

import pandas as pd
from sqlalchemy import bindparam, text

from libs.asset_risk.standard_params import params_for_asset
from libs.asset_risk.chain import MD_ROW_TO_SLOT, _attach_prices, _load_chain, _load_prices
from libs.asset_risk.diagnostics import (
    AUTO_NOM_CODE,
    CAPCOMP_RATE_DEFAULT,
    _month_asset_matrix,
    exec_gap_monthly,
)

_COMPONENT_COLORS = {"arb_cny": "#3498db", "cap_cny": "#9b59b6",
                     "fee_cny": "#e74c3c", "other_cny": "#95a5a6"}
_COMPONENT_CN = {"arb_cny": "套利", "cap_cny": "容量补偿", "fee_cny": "系统运行费+线损", "other_cny": "其他费用"}
_STAGES = ["投资标准", "Δ策略与预测", "申报", "Δ出清校核", "出清", "Δ执行与费用", "实际"]
# With an investment-committee case: 投决预期 → 市场偏差 → 投资标准 → ...
_STAGES_IC = ["投决预期", "市场偏差"] + _STAGES

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

def missing_bill_keys(actual, assets, months):
    """(asset, month) pairs with no settlement-bill row — bills usually land
    mid next month, so these must not be read as zero revenue."""
    have = set(zip(actual["asset"], actual["month"])) if not actual.empty else set()
    return [(a, m) for a in assets for m in months if (a, m) not in have]


def restrict_to_keys(df, keys):
    """Rows of df whose (asset, month) is in keys (pure; empty-safe)."""
    keys = set(keys)
    if df.empty or not keys:
        return df.iloc[0:0]
    mask = [(a, m) in keys for a, m in zip(df["asset"], df["month"])]
    return df[mask]


MISSING_BILLS_NOTE = ("以下资产×月份尚无结算单（通常次月中旬发布）：{keys}；"
                      "实际段与Δ执行与费用不可用，不代表收入为零。")

# Δ执行与费用 attribution rows whose value depends on the settlement bills.
BILL_DEPENDENT_CAUSES = ("容量补偿", "系统运行费+线损", "其他费用", "结算口径/价格基差")


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


def stage_capacity(dis_mwh: pd.Series, assets: pd.Series,
                   rate_map: dict[str, float]) -> pd.Series:
    """discharge MWh × capacity-subsidy rate per asset (rate_map from
    diagnostics.load_maps; assets absent from it take the 350 default)."""
    return pd.Series(
        [float(d) * rate_map.get(a, CAPCOMP_RATE_DEFAULT) for d, a in zip(dis_mwh, assets)],
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


def _load_nodal_prices_filled(engine, asset_name: str, start: str, end: str,
                              asset_code: str | None = None) -> pd.Series:
    """Filled nodal RT price (md_mengxi_nodal_price_96_filled.avg_node_price)
    for the station's own registry node — the same market price, complete
    96-slot days. metric_time stamps interval START and is used as-is (no
    MD_ROW_TO_SLOT shift — that applies only to md_id_cleared_energy rows). Empty series
    when the asset has no registry mapping/node."""
    from libs.trading_book.registry import get_station

    code = asset_code or AUTO_NOM_CODE.get(asset_name)
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


def _load_nodal_rt_prices(engine, asset_name: str, start: str, end: str,
                          asset_code: str | None = None) -> pd.Series:
    """Nodal RT price (md_rt_nodal_price.node_price) for the station's registry node,
    indexed in the chain slot frame (row datetime + MD_ROW_TO_SLOT — the same shift as
    md_id_cleared_energy, verified r=0.997 / MAE ¥1.1). `start`/`end` bound the SLOT
    window; the query is widened by the shift. Empty series when no registry node."""
    from libs.trading_book.registry import get_station

    code = asset_code or AUTO_NOM_CODE.get(asset_name)
    if not code:
        return pd.Series(dtype=float)
    station = get_station(code)
    if not station.price_nodes:
        return pd.Series(dtype=float)
    s0 = pd.Timestamp(start) - MD_ROW_TO_SLOT
    e0 = pd.Timestamp(end) - MD_ROW_TO_SLOT
    df = pd.read_sql(text("""
        SELECT datetime, node_price
        FROM marketdata.md_rt_nodal_price
        WHERE node_name = :node AND datetime >= :s AND datetime < :e
        ORDER BY datetime
    """), engine, params={"node": station.price_nodes[0], "s": str(s0), "e": str(e0)})
    if df.empty:
        return pd.Series(dtype=float)
    s = pd.Series({pd.Timestamp(t) + MD_ROW_TO_SLOT: float(v)
                   for t, v in zip(df["datetime"], df["node_price"]) if pd.notna(v)},
                  dtype=float)
    return s[~s.index.duplicated(keep="last")].sort_index()


PRICE_SOURCE_LABELS = ("出清价", "节点RT价", "节点填充价")


def merge_price_sources(cleared: pd.Series, rt: pd.Series, filled: pd.Series
                        ) -> tuple[pd.Series, str]:
    """Slot-wise merge in priority order cleared > node RT > filled (combine_first, so each
    source fills only slots missing from the previous ones). Returns (series, note) where
    note names the sources that contributed at least one slot joined by '+'; '' when
    cleared alone supplied every slot (or nothing was supplied at all)."""
    out = pd.Series(dtype=float)
    used = []
    for label, src in zip(PRICE_SOURCE_LABELS, (cleared, rt, filled)):
        if src is None or len(src) == 0:
            continue
        merged = out.combine_first(src) if len(out) else src.copy()
        if len(merged) > len(out):
            used.append(label)
        out = merged
    out = out.sort_index()
    note = "" if used in ([], [PRICE_SOURCE_LABELS[0]]) else "+".join(used)
    return out, note


def _complete_days_only(s: pd.Series) -> bool:
    """True when the series is non-empty and every date present has all 96 slots."""
    return len(s) > 0 and bool((s.groupby(s.index.date).size() >= 96).all())


def benchmark_leg(engine, asset_id: int, month: str, capacity_mw: float,
                  duration_h: float, commission_iso: str | None,
                  capcomp_rate: float, plant: str,
                  asset_name: str = "", asset_code: str | None = None) -> dict:
    """Investment-standard leg: LP perfect-foresight with standard params for one
    asset-month. Cached on (asset_id, month, ...) — historical inputs immutable.

    LP input prices are indexed at md datetime + MD_ROW_TO_SLOT (the chain slot
    frame, owner decision 2026-11-15), so LP day boundaries follow chain days;
    rows mapping outside the month are dropped.
    """
    from services.bess_map.optimisation_engine import compute_dispatch_from_15min_prices  # lazy (pulp)

    month_start = pd.Timestamp(month + "-01")
    month_end = (month_start + pd.offsets.MonthBegin(1)).strftime("%Y-%m-%d")
    params = params_for_asset(capacity_mw, duration_h, commission_iso, month_start)

    prices = _load_prices(engine, [plant], month_start.strftime("%Y-%m-%d"), month_end)
    if prices.empty:
        return {"arb_cny": 0.0, "dis_mwh": 0.0, "chg_mwh": 0.0, "cap_cny": 0.0,
                "days_solved": 0, "days_skipped": 0, "age": params["age"], "note": "no prices"}
    s = pd.Series({pd.Timestamp(t) + MD_ROW_TO_SLOT: float(v)
                   for t, v in zip(prices["datetime"], prices["cleared_price"]) if pd.notna(v)},
                  dtype=float)
    s = s[~s.index.duplicated(keep="last")].sort_index()
    s.index = pd.DatetimeIndex(s.index)
    s = s[(s.index >= month_start) & (s.index < pd.Timestamp(month_end))]

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

    price_note = ""
    if not _complete_days_only(s):
        # Cleared feed has gaps (2026-09..11: 38-49% null). Fill slot-wise from the node
        # RT price, then the filled table; the engine skips any day with a NaN slot.
        m0 = month_start.strftime("%Y-%m-%d")
        rt = _load_nodal_rt_prices(engine, asset_name, m0, month_end, asset_code=asset_code)
        filled = _load_nodal_prices_filled(engine, asset_name, m0, month_end,
                                           asset_code=asset_code)
        s, price_note = merge_price_sources(s, rt, filled)
        s = s[(s.index >= month_start) & (s.index < pd.Timestamp(month_end))]
    dispatch_df, profit_s = _run(s)
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

def ic_leg(ic: pd.DataFrame, keys, name_to_code: dict, commission: dict
           ) -> tuple[pd.DataFrame, list[tuple[str, str]]]:
    """投决预期 leg for (asset name, 'YYYY-MM') keys.

    Returns (leg, no_ic_keys). leg columns: asset, month, arb_cny, cap_cny, fee_cny
    (fee_cny = −grid fee, the same sign as the 实际 fee leg, so stacking is a plain
    sum), dis_mwh, chg_mwh. Keys without IC rows, without a usable cod, or before
    cod are NOT zero-filled — they are returned in no_ic_keys.
    commission: asset name → rm_assets.commission_date (date/Timestamp/str/None).
    """
    from libs.asset_risk.ic_case import ic_month_value

    rows, missing = [], []
    for name, month in keys:
        cod = commission.get(name)
        try:
            cod = None if cod is None or pd.isna(cod) else pd.Timestamp(cod).date()
        except (ValueError, TypeError):
            cod = None
        code = name_to_code.get(name)
        v = ic_month_value(ic, code, pd.Timestamp(month + "-01").date(), cod) if code else None
        if v is None:
            missing.append((name, month))
            continue
        rows.append({"asset": name, "month": month, "arb_cny": v["arbitrage_cny"],
                     "cap_cny": v["capcomp_cny"] + v["capacity_price_cny"], "fee_cny": -v["grid_fee_cny"],
                     "dis_mwh": v["discharge_mwh"], "chg_mwh": v["charge_mwh"], "ic_days": v["days"]})
    cols = ["asset", "month", "arb_cny", "cap_cny", "fee_cny", "dis_mwh", "chg_mwh", "ic_days"]
    return pd.DataFrame(rows, columns=cols), missing


def chain_coverage_days(chain: pd.DataFrame, commission: dict | None = None
                        ) -> dict[tuple[str, str], int]:
    """(asset, month) -> number of days with >=1 non-null nominated_mw slot (申报 data),
    counting only days on/after the asset's cod (commission: asset name -> date-like/None;
    missing/unparseable cod = no cut). Day = calendar date of the chain slot ts."""
    if chain is None or chain.empty or "nominated_mw" not in chain:
        return {}
    d = chain[chain["nominated_mw"].notna()]
    if d.empty:
        return {}
    d = pd.DataFrame({"asset": d["asset"].values, "month": d["month"].values,
                      "day": pd.to_datetime(d["ts"]).dt.normalize().values})
    out = {}
    for (asset, month), g in d.groupby(["asset", "month"]):
        days = pd.DatetimeIndex(g["day"].unique())
        cod = (commission or {}).get(asset)
        try:
            cod = None if cod is None or pd.isna(cod) else pd.Timestamp(cod).normalize()
        except (ValueError, TypeError):
            cod = None
        if cod is not None:
            days = days[days >= cod]
        out[(asset, month)] = int(len(days))
    return out


def ic_scale_to_chain(leg: pd.DataFrame, chain_days: dict[tuple[str, str], int]
                      ) -> tuple[pd.DataFrame, list[tuple[str, str]]]:
    """Scale each IC asset-month (arb/cap/fee) by chain-coverage days / ic_days, where
    chain-coverage days = days with 申报 data on/after cod (see chain_coverage_days) and
    ic_days = days in the month on/after cod. Independent of the benchmark solve, so a
    0-day benchmark no longer zeroes the IC leg.

    The factor is capped at 1. Asset-months without chain coverage info are left
    unscaled. Returns (leg, scaled keys) where scaled keys are those with factor < 1.
    """
    if leg is None or leg.empty or not chain_days or "ic_days" not in leg:
        return leg, []
    out = leg.copy()
    scaled = []
    for i, r in out.iterrows():
        n = chain_days.get((r["asset"], r["month"]))
        if n is None or pd.isna(r["ic_days"]) or not r["ic_days"]:
            continue
        f = min(1.0, float(n) / float(r["ic_days"]))
        if f < 1.0:
            for c in ("arb_cny", "cap_cny", "fee_cny"):
                out.at[i, c] = r[c] * f
            scaled.append((r["asset"], r["month"]))
    return out, scaled


def bench_coverage_gaps(bench: pd.DataFrame, chain_days: dict[tuple[str, str], int],
                        threshold: float = 0.9) -> list[tuple[str, str, int, int]]:
    """Asset-months where bench days_solved < threshold x chain days with 申报 data.
    Returns [(asset, month, solved, chain_days)] sorted; months without chain data
    (0 days) or without days_solved are not flagged."""
    if bench is None or bench.empty or "days_solved" not in bench or not chain_days:
        return []
    solved = bench.groupby(["asset", "month"])["days_solved"].sum(min_count=1)
    gaps = []
    for (asset, month), n in solved.items():
        y = chain_days.get((asset, month))
        if not y or pd.isna(n):
            continue
        if float(n) < threshold * y:
            gaps.append((asset, month, int(n), int(y)))
    return sorted(gaps)


def ic_passthrough(leg: pd.DataFrame, missing, bench: pd.DataFrame) -> pd.DataFrame:
    """Add keys without an IC case to the 投决预期 leg at their own 投资标准 value
    (arb/cap from bench, fee 0), so their 市场偏差 is 0 and the other legs never
    change. Keys absent from bench are dropped."""
    if not missing or bench.empty:
        return leg
    miss = set(missing)
    pt = bench[[(a, m) in miss for a, m in zip(bench["asset"], bench["month"])]]
    pt = pt.groupby(["asset", "month"], as_index=False)[["arb_cny", "cap_cny"]].sum()
    pt["fee_cny"] = 0.0
    return pd.concat([leg, pt], ignore_index=True)


def build_waterfall(bench: pd.DataFrame, nominated: pd.DataFrame,
                    cleared: pd.DataFrame, actual: pd.DataFrame,
                    ic: pd.DataFrame | None = None) -> pd.DataFrame:
    """Long frame: stage × component → cny, with bridge deviations.

    Stages: 投资标准, Δ策略与预测 (=申报−标准), 申报, Δ出清校核 (=出清−申报),
    出清, Δ执行与费用 (=实际−出清), 实际. Components: arb_cny, cap_cny, fee_cny, other_cny
    (fee/other are zero except in the 实际 leg and its incoming deviation).
    Optional ic (ic_leg output, same asset-months as the other legs) prepends 投决预期
    and 市场偏差 (=投资标准−投决预期); fee_cny there is the IC grid fee. Without ic the
    output is unchanged.
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
    transitions = [("申报", "投资标准", "Δ策略与预测"),
                   ("出清", "申报", "Δ出清校核"),
                   ("实际", "出清", "Δ执行与费用")]
    stages = ("投资标准", "申报", "出清", "实际")
    stage_list = _STAGES
    if ic is not None and not ic.empty:
        totals["投决预期"] = {c: _tot(ic, c) for c in comps}
        transitions.insert(0, ("投资标准", "投决预期", "市场偏差"))
        stages = ("投决预期",) + stages
        stage_list = _STAGES_IC
    for stage, prev, delta_name in transitions:
        for c in comps:
            rows.append({"stage": delta_name, "component": c,
                         "cny": totals[stage][c] - totals[prev][c]})
    for stage in stages:
        for c in comps:
            rows.append({"stage": stage, "component": c, "cny": totals[stage][c]})
    order = {s: i for i, s in enumerate(stage_list)}
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
    # index in the chain slot frame (md datetime + MD_ROW_TO_SLOT) before hourly mean
    s = pd.Series({pd.Timestamp(t) + MD_ROW_TO_SLOT: float(v)
                   for t, v in zip(prices["datetime"], prices["cleared_price"])
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
