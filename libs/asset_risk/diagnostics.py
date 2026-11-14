"""Dispatch-diagnostics constants and pure helpers (moved from apps/asset_risk/tab_diagnostics.py)."""
from __future__ import annotations

import numpy as np
import pandas as pd

from libs.asset_risk.registry import (
    asset_identity, capcomp_rate_map, has_asset_code, nom_code_map, plant_map,
)
from libs.trading_book.registry import get_station

# Legacy literal maps — read only as the fallback when rm_assets has no asset_code
# column yet (pre-migration); render paths get their maps from load_maps().
# rm_assets.name → md_id_cleared_energy.plant_name (validated map)
PLANT_MAP = {
    "悦杭独贵": "悦杭独贵储能电站",
    "景怡查干哈达": "景怡查干哈达储能电站",
    "景蓝乌尔图": "景蓝乌尔图储能电站",
    "裕昭沙子坝": "裕昭沙子坝储能电站",
    "远景乌拉特": "远景乌拉特储能电站",
    "四子王旗": "景通四益堂储能电站",
}

# rm_assets.name → trading-book registry asset_code — the auto-scraped
# nomination source (marketdata.bess_nomination_96, WeCom 调度计划表
# pipeline, services/wecom_nominations + services/mengxi_nodal/
# nomination_curves.py). The manual dispatch-chain ingest stopped
# 2026-08-30; this table is current.
AUTO_NOM_CODE = {
    "悦杭独贵": "hangjinqi",
    "景怡查干哈达": "bameng",
    "景蓝乌尔图": "suyou",
    "裕昭沙子坝": "gushanliang",
    "远景乌拉特": "wulate",
    "四子王旗": "siziwangqi",
}

RESTRICTION_CN = {"charge_only": "仅可充电", "discharge_only": "仅可放电"}
RESTRICTION_COLOR = {"charge_only": "#e67e22", "discharge_only": "#e74c3c"}  # Excel cell colours

# 容量补偿标准 (¥/MWh, per user 2026-09-01): default 350; 锡西二/阿拉善/武川 280.
CAPCOMP_RATE: dict[str, float] = {"锡西二": 280.0, "阿拉善": 280.0, "武川": 280.0}
CAPCOMP_RATE_DEFAULT = 350.0


def load_maps(engine) -> tuple[dict[str, str], dict[str, str], dict[str, float]]:
    """(plant_map_first_name, nom_code_map, capcomp_rate_map) keyed by rm_assets.name,
    derived from the registry identity frame. Multi-account stations (武川) map to
    their FIRST plant name here (prices); cross-account cleared sums are Task 11.
    Pre-migration (no asset_code column) → the legacy literal dicts."""
    if not has_asset_code(engine):
        return dict(PLANT_MAP), dict(AUTO_NOM_CODE), dict(CAPCOMP_RATE)
    ident = asset_identity(engine)
    pm = {n: names[0] for n, names in plant_map(ident).items() if names}
    return pm, nom_code_map(ident), capcomp_rate_map(ident)


def chain_asset_names(engine) -> set[str]:
    """rm_assets names eligible for the dispatch-chain tabs: registry station with
    data_path 'full' and at least one cleared plant name (keeps settlement_only
    乌海/零碳46 out). Pre-migration → the legacy six (PLANT_MAP keys)."""
    if not has_asset_code(engine):
        return set(PLANT_MAP)
    ident = asset_identity(engine)
    return {r.name for r in ident.itertuples()
            if r.asset_code and r.plant_names and get_station(r.asset_code).data_path == "full"}

def exec_gap_monthly(df: pd.DataFrame) -> pd.DataFrame:
    """Per (asset, month): exec gap volumes, ¥ impacts, cleared volumes, gap %.

    dis: rt>0.5 & actual notna. chg: rt<−0.5 & actual notna (gap reads negative).
    """
    rows = []
    for (asset, month), g in df.groupby(["asset", "month"]):
        out = {"asset": asset, "month": month}
        dis = g[(g["rt_cleared_mw"] > 0.5) & g["actual_mw"].notna()]
        chg = g[(g["rt_cleared_mw"] < -0.5) & g["actual_mw"].notna()]
        out["dis_gap_mwh"] = float(((dis["rt_cleared_mw"] - dis["actual_mw"]) * 0.25).sum())
        out["dis_gap_cny"] = float(((dis["rt_cleared_mw"] - dis["actual_mw"]) * 0.25
                                    * dis["price_cny_mwh"]).sum())
        out["dis_cleared_mwh"] = float((dis["rt_cleared_mw"] * 0.25).sum())
        out["dis_gap_pct"] = (100 * out["dis_gap_mwh"] / out["dis_cleared_mwh"]
                              if out["dis_cleared_mwh"] else None)
        out["chg_gap_mwh"] = float(((chg["actual_mw"] - chg["rt_cleared_mw"]) * 0.25).sum())
        out["chg_gap_cny"] = float(((chg["actual_mw"] - chg["rt_cleared_mw"]) * 0.25
                                    * chg["price_cny_mwh"]).sum())
        out["chg_cleared_mwh"] = float((-chg["rt_cleared_mw"] * 0.25).sum())
        out["chg_gap_pct"] = (100 * out["chg_gap_mwh"] / out["chg_cleared_mwh"]
                              if out["chg_cleared_mwh"] else None)
        rows.append(out)
    return pd.DataFrame(rows)


def bid_fail_monthly(df: pd.DataFrame) -> pd.DataFrame:
    """Per (asset, month): bid failure volumes, ¥ impacts, nominated volumes, fail %.

    dis: nom>0.5 → Σ(nom−da)×0.25. chg: nom<−0.5 → Σ(da−nom)×(−0.25) (negative).
    """
    rows = []
    for (asset, month), g in df.groupby(["asset", "month"]):
        out = {"asset": asset, "month": month}
        dis = g[g["nominated_mw"] > 0.5]
        chg = g[g["nominated_mw"] < -0.5]
        out["dis_fail_mwh"] = float(((dis["nominated_mw"] - dis["da_cleared_mw"]) * 0.25).sum())
        out["dis_fail_cny"] = float(((dis["nominated_mw"] - dis["da_cleared_mw"]) * 0.25
                                     * dis["price_cny_mwh"]).sum())
        out["dis_nom_mwh"] = float((dis["nominated_mw"] * 0.25).sum())
        out["dis_fail_pct"] = (100 * out["dis_fail_mwh"] / out["dis_nom_mwh"]
                               if out["dis_nom_mwh"] else None)
        out["chg_fail_mwh"] = float(((chg["da_cleared_mw"] - chg["nominated_mw"]) * -0.25).sum())
        out["chg_fail_cny"] = float(((chg["da_cleared_mw"] - chg["nominated_mw"]) * -0.25
                                     * chg["price_cny_mwh"]).sum())
        out["chg_nom_mwh"] = float((-chg["nominated_mw"] * 0.25).sum())
        out["chg_fail_pct"] = (100 * out["chg_fail_mwh"] / out["chg_nom_mwh"]
                               if out["chg_nom_mwh"] else None)
        rows.append(out)
    return pd.DataFrame(rows)


def capacity_loss_monthly(df: pd.DataFrame, kind: str = "exec_gap",
                          rate_map: dict[str, float] | None = None,
                          default_rate: float = CAPCOMP_RATE_DEFAULT) -> pd.DataFrame:
    """Per (asset, month): capacity subsidy foregone on discharge volume shortfall.

    kind="exec_gap":  vol = Σ(rt−actual)×0.25 where rt>0.5, actual notna (panel 1)
    kind="bid_fail":  vol = Σ(nom−da)×0.25   where nom>0.5           (panel 2)
    capacity_loss_cny = vol × rate (350 ¥/MWh default; 280 for 锡西二/阿拉善/武川,
    per user 2026-09-01). Positive = subsidy foregone.
    """
    rates = dict(CAPCOMP_RATE if rate_map is None else rate_map)
    rows = []
    for (asset, month), g in df.groupby(["asset", "month"]):
        if kind == "bid_fail":
            m = g[g["nominated_mw"] > 0.5]
            vol = float(((m["nominated_mw"] - m["da_cleared_mw"]) * 0.25).sum())
        else:
            m = g[(g["rt_cleared_mw"] > 0.5) & g["actual_mw"].notna()]
            vol = float(((m["rt_cleared_mw"] - m["actual_mw"]) * 0.25).sum())
        rate = rates.get(asset, default_rate)
        rows.append({
            "asset": asset, "month": month,
            "dis_shortfall_mwh": vol,
            "capcomp_rate": rate,
            "capacity_loss_cny": vol * rate,
        })
    return pd.DataFrame(rows)


def restriction_monthly(df: pd.DataFrame,
                        rate_map: dict[str, float] | None = None) -> pd.DataFrame:
    """Per (asset, month): restriction counts, share, scheduled volume/¥ in windows,
    and the exec-gap deviation (MWh + ¥) WITHIN flagged windows.

    moved_mwh/cny = Σ|rt|×0.25 / Σ rt×0.25×price over flagged intervals — scheduled
    energy in restricted windows (context size, NOT a deviation).
    gap_mwh/gap_cny = exec-gap deviation inside flagged windows (same masks/formulas
    as exec_gap_monthly) — the deviation quantity.
    """
    rates = CAPCOMP_RATE if rate_map is None else rate_map
    rows = []
    for (asset, month), g in df.groupby(["asset", "month"]):
        out = {"asset": asset, "month": month}
        flagged = g[g["restriction"].notna()]
        out["charge_only_intervals"] = int((flagged["restriction"] == "charge_only").sum())
        out["discharge_only_intervals"] = int((flagged["restriction"] == "discharge_only").sum())
        out["total_intervals"] = int(len(g))
        out["restricted_share"] = (len(flagged) / len(g)) if len(g) else None
        out["moved_mwh"] = float((flagged["rt_cleared_mw"].abs() * 0.25).sum())
        out["moved_cny"] = float((flagged["rt_cleared_mw"] * 0.25
                                  * flagged["price_cny_mwh"]).sum())
        dis = flagged[(flagged["rt_cleared_mw"] > 0.5) & flagged["actual_mw"].notna()]
        chg = flagged[(flagged["rt_cleared_mw"] < -0.5) & flagged["actual_mw"].notna()]
        out["gap_dis_mwh"] = float(((dis["rt_cleared_mw"] - dis["actual_mw"]) * 0.25).sum())
        out["gap_chg_mwh"] = float(((chg["actual_mw"] - chg["rt_cleared_mw"]) * 0.25).sum())
        out["gap_cny"] = float(((dis["rt_cleared_mw"] - dis["actual_mw"]) * 0.25
                                * dis["price_cny_mwh"]).sum()
                               + ((chg["actual_mw"] - chg["rt_cleared_mw"]) * 0.25
                                  * chg["price_cny_mwh"]).sum())
        # In-window capacity subsidy foregone (subset of panel-1 容量补偿影响):
        # discharge exec-gap volume in flagged windows × rate (350 default / 280 named)
        out["capacity_loss_cny"] = out["gap_dis_mwh"] * rates.get(asset, CAPCOMP_RATE_DEFAULT)
        rows.append(out)
    return pd.DataFrame(rows)


def find_defect_events(df: pd.DataFrame, min_run: int = 4, deadband: float = 0.5,
                       thr_frac: float = 0.25) -> pd.DataFrame:
    """Non-response events: runs of >=min_run consecutive 15-min intervals (per asset)
    with |actual| <= deadband and |rt_cleared| > thr_frac × capacity_mw.

    lost = exec-gap formula over the run (strict subset of panel-1 gap ¥).
    """
    events = []
    for asset, g in df.sort_values("ts").groupby("asset"):
        cap = float(g["capacity_mw"].iloc[0]) if g["capacity_mw"].notna().any() else 0.0
        thr = thr_frac * cap
        g = g.sort_values("ts").reset_index(drop=True)
        flagged = (g["actual_mw"].abs() <= deadband) & (g["rt_cleared_mw"].abs() > thr)
        ts_gap = g["ts"].diff() != pd.Timedelta(minutes=15)
        run_id = (flagged.ne(flagged.shift()) | ts_gap).cumsum()
        g["_run"] = run_id
        for _, run in g[flagged].groupby("_run"):
            if len(run) < min_run:
                continue
            lost_mwh = float(((run["rt_cleared_mw"] - run["actual_mw"]) * 0.25).sum())
            events.append({
                "asset": asset,
                "start_ts": run["ts"].iloc[0],
                "end_ts": run["ts"].iloc[-1],
                "intervals": int(len(run)),
                "rt_avg_mw": float(run["rt_cleared_mw"].mean()),
                "lost_mwh": lost_mwh,
                "lost_cny": float(((run["rt_cleared_mw"] - run["actual_mw"]) * 0.25
                                   * run["price_cny_mwh"]).sum()),
            })
    return pd.DataFrame(events)


def _coverage(df: pd.DataFrame) -> pd.DataFrame:
    """Per (asset, month): intervals, with actual, priced — data-holes transparency."""
    return (df.groupby(["asset", "month"])
              .agg(intervals=("ts", "count"),
                   with_actual=("actual_mw", lambda s: int(s.notna().sum())),
                   priced=("price_cny_mwh", lambda s: int(s.notna().sum())))
              .reset_index())


def _month_asset_matrix(monthly: pd.DataFrame, value_col: str) -> pd.DataFrame:
    """month × asset pivot + 组合合计 column + 资产合计 row (tab_pnl pattern)."""
    if monthly.empty:
        return pd.DataFrame()
    mat = monthly.pivot_table(index="month", columns="asset", values=value_col,
                              aggfunc="sum", fill_value=0)
    mat = mat.sort_index()
    mat["组合合计"] = mat.sum(axis=1)
    total_row = mat.sum(axis=0)
    total_row.name = "资产合计"
    return pd.concat([mat, total_row.to_frame().T])
