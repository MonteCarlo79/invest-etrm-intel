# services/nodal_agents/writer.py
"""Daily orchestrator for the L3 asset agents (spec SS5).

run_day(conn, target_date):
    1. load active assets (nodal_asset_registry), grid price level
       (nodal_fc_grid_daily via nodal_forecast.db — LingFeng RT fallback
       inside), per-node intraday shapes (md_mengxi_nodal_price_96, 30d mean
       curve normalized to mean 1), substation caps (nodal_node_registry
       n1_firm_mva), zone behavior history (md_id_cleared_energy)
    2. curves_fn: nodal level = grid_price_hat - sensitivity x aggregate BESS
       dispatch (bidding-space adjustment, linear v1); curve = level x shape
    3. recursion.converge -> recursive optimal strategies
    4. upsert nodal_strategy_daily (curve_json, assumptions_json, iterations,
       convergence_delta_mwh, model_version)

`data` (optional) bypasses ALL DB reads for synthetic tests/offline runs:
    {"assets": [...], "grid_price_hat": float, "shapes": {node: (96,)},
     "caps": {substation: mw}, "zone_hist": {zone: DataFrame},
     "zones_map": {plant: (96,) int}}
Writes (the upsert) always go through conn.

v1 limitations (documented, not wired): zone_hist DB loading (non-registry
BESS fleet separation needs storage-name heuristics — Task 2's extract runs
per-registry only), traffic-light zone learning from asset-risk history
(zones default green), substation cap = max n1_firm_mva per substation until
the reviewed node map lands.
"""
from __future__ import annotations

import json
from datetime import date, timedelta

import numpy as np
import pandas as pd

from services.nodal_forecast import db as fc_db
from services.nodal_agents import behavior, recursion

MODEL_VERSION = "nodal_agent_v1"
BESS_SENSITIVITY = 0.02  # CNY/MWh price drop per MW of aggregate BESS discharge
SHAPE_LOOKBACK_DAYS = 30
_ONES = np.ones(96, dtype=float)

_STRATEGY_UPSERT = """INSERT INTO marketdata.nodal_strategy_daily
    (plant_name, target_date, curve_json, assumptions_json,
     iterations, convergence_delta_mwh, model_version)
    VALUES (%(plant_name)s, %(target_date)s, %(curve_json)s,
            %(assumptions_json)s, %(iterations)s,
            %(convergence_delta_mwh)s, %(model_version)s)
    ON CONFLICT (plant_name, target_date, model_version) DO UPDATE SET
        curve_json = EXCLUDED.curve_json,
        assumptions_json = EXCLUDED.assumptions_json,
        iterations = EXCLUDED.iterations,
        convergence_delta_mwh = EXCLUDED.convergence_delta_mwh,
        created_at = NOW()"""


# ---------------------------------------------------------------- DB loaders

def _load_assets(conn) -> list[dict]:
    cur = conn.cursor()
    cur.execute(
        """SELECT plant_name, node, substation, capacity_mw, duration_h,
                  rte_pct, zone, settle_node
           FROM marketdata.nodal_asset_registry
           WHERE active IS TRUE ORDER BY plant_name""")
    cols = ["plant_name", "node", "substation", "capacity_mw", "duration_h",
            "rte_pct", "zone", "settle_node"]
    out = []
    for row in cur.fetchall():
        a = dict(zip(cols, row))
        if pd.notna(a["capacity_mw"]) and pd.notna(a["duration_h"]) and pd.notna(a["rte_pct"]):
            out.append(a)
    return out


_GRID_PROVINCE = "蒙西"  # matches fc_db.get_grid_forecast's default


def _load_grid_price_hat(conn, target_date: date,
                         model_version: str = MODEL_VERSION) -> float:
    # Version-filtered: L1 may write several model_version rows for one
    # target_date — never average across versions. Take the run's version;
    # if absent, the version with the latest fc_date for this target_date.
    cur = conn.cursor()
    cur.execute(
        """SELECT price_hat, model_version, fc_date
           FROM marketdata.nodal_fc_grid_daily
           WHERE province = %s AND target_date = %s AND price_hat IS NOT NULL""",
        (_GRID_PROVINCE, target_date))
    rows = [(float(p), mv, fc) for p, mv, fc in cur.fetchall()]
    if rows:
        mine = [p for p, mv, _ in rows if mv == model_version]
        if mine:
            return float(np.mean(mine))
        latest_fc = max(fc for _, _, fc in rows)
        return float(np.mean([p for p, _, fc in rows if fc == latest_fc]))
    # Table entirely empty for the date -> existing LingFeng RT fallback.
    df = fc_db.get_grid_forecast(conn, target_date)
    vals = df["price_hat"] if not df.empty else pd.Series(dtype=float)
    vals = vals[pd.notna(vals)]
    if vals.empty:
        raise RuntimeError(f"no grid forecast for {target_date} "
                           "(nodal_fc_grid_daily empty AND LingFeng RT fallback empty)")
    return float(vals.astype(float).mean())


def _load_shapes(conn, target_date: date) -> dict:
    cur = conn.cursor()
    cur.execute(
        """SELECT node_name, time_order_96, avg_node_price
           FROM marketdata.md_mengxi_nodal_price_96
           WHERE metric_time::date >= %s AND metric_time::date < %s""",
        (target_date - timedelta(days=SHAPE_LOOKBACK_DAYS), target_date))
    rows = cur.fetchall()
    if not rows:
        return {}
    df = pd.DataFrame(rows, columns=["node", "interval", "price"])
    df = df[pd.notna(df["price"])]
    if df.empty:
        return {}
    mat = df.pivot_table(index="node", columns="interval", values="price",
                         aggfunc="mean").reindex(columns=range(96))
    shapes = {}
    for node, row in mat.iterrows():
        v = row.to_numpy(dtype=float)
        mean = np.nanmean(v)
        if np.isfinite(mean) and abs(mean) > 1e-9:
            shapes[node] = np.nan_to_num(v / mean, nan=1.0)
    return shapes


def _load_caps(conn) -> dict:
    cur = conn.cursor()
    cur.execute(
        """SELECT substation, n1_firm_mva FROM marketdata.nodal_node_registry
           WHERE substation IS NOT NULL AND n1_firm_mva IS NOT NULL""")
    caps = {}
    for sub, mva in cur.fetchall():
        if pd.notna(mva):
            caps[sub] = max(caps.get(sub, 0.0), float(mva))
    return caps


# ------------------------------------------------------------------ run_day

def run_day(conn, target_date: date, model_version: str = MODEL_VERSION,
            data: dict | None = None, max_iter: int = 3,
            tol_mwh_pct: float = 2.0,
            bess_sensitivity: float = BESS_SENSITIVITY) -> dict:
    # Asset list first: a day with zero active assets is a clean no-op and
    # must not die on an empty forecast table (DB mode).
    if data is not None:
        src = data
    else:
        src = {"assets": _load_assets(conn)}
    assets = list(src.get("assets") or [])
    if not assets:
        return {"target_date": target_date.isoformat(), "plants": 0,
                "upserted": 0, "iterations": 0, "convergence_delta_mwh": 0.0,
                "model_version": model_version}
    if data is None:
        src = {**src,
               "grid_price_hat": _load_grid_price_hat(conn, target_date,
                                                      model_version),
               "shapes": _load_shapes(conn, target_date),
               "caps": _load_caps(conn),
               "zone_hist": {},
               "zones_map": {}}
    grid_price_hat = float(src["grid_price_hat"])
    shapes = src.get("shapes") or {}
    caps = src.get("caps") or {}
    zones_map = src.get("zones_map") or {}

    # Zone behavior forecast of the non-agent BESS fleet (empty when no hist)
    zone_hist = src.get("zone_hist") or {}
    zone_fc = {z: behavior.forecast_zone_bess(z, target_date, h)
               for z, h in zone_hist.items()}
    fleet_baseline = np.zeros(96, dtype=float)
    for fc in zone_fc.values():
        fleet_baseline = fleet_baseline + fc

    def curves_fn(iter_no, dispatch):
        agg = fleet_baseline.copy()
        if dispatch:
            for c in dispatch.values():
                agg = agg + np.nan_to_num(c, nan=0.0)
        level = grid_price_hat - bess_sensitivity * agg
        return {a["plant_name"]: level * shapes.get(a.get("node"), _ONES)
                for a in assets}

    enriched = []
    for a in assets:
        enriched.append({**a,
                         "cap_mw": caps.get(a.get("substation")),
                         "zones": zones_map.get(a["plant_name"],
                                                np.zeros(96, dtype=int)),
                         "others_baseline": zone_fc.get(a.get("zone"),
                                                        np.zeros(96))})

    conv = recursion.converge(enriched, curves_fn, max_iter=max_iter,
                              tol_mwh_pct=tol_mwh_pct)

    rows = []
    for a in assets:
        plant = a["plant_name"]
        strat = conv["strategies"][plant]
        asm = {**strat["assumptions"],
               "grid_price_hat": grid_price_hat,
               "bess_sensitivity": bess_sensitivity,
               "model_version": model_version}
        rows.append({
            "plant_name": plant,
            "target_date": target_date,
            "curve_json": json.dumps([round(float(x), 4) for x in strat["curve"]]),
            "assumptions_json": json.dumps(asm, ensure_ascii=False),
            "iterations": int(conv["iterations"]),
            "convergence_delta_mwh": float(conv["convergence_delta_mwh"]),
            "model_version": model_version,
        })
    cur = conn.cursor()
    cur.executemany(_STRATEGY_UPSERT, rows)
    conn.commit()

    return {"target_date": target_date.isoformat(), "plants": len(assets),
            "upserted": len(rows), "iterations": conv["iterations"],
            "convergence_delta_mwh": float(conv["convergence_delta_mwh"]),
            "model_version": model_version}
