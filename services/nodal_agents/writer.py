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
from services.bess_map.strategy_experiments import (assign_status,
                                                    compute_window_metrics)

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
    # time_order_96 is 1-based (slots 1..96) — reindex to exactly those
    # columns so index i of the shape vector holds slot i+1 (review 2026-09-22:
    # range(96) dropped slot 96 and shifted every node's shape one interval).
    mat = df.pivot_table(index="node", columns="interval", values="price",
                         aggfunc="mean").reindex(columns=range(1, 97))
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


# ------------------------------------------- promote loop registration (Task 6)
#
# register_strategies scores converged strategies against ACTUAL RT prices and
# the perfect-foresight benchmark, then upserts one row per plant into
# marketdata.strategy_experiments (scope 'nodal_agent') so the promote loop
# can track strategy versions per plant. Pure window math is delegated to
# services.bess_map.strategy_experiments (compute_window_metrics /
# assign_status); only the capture-frame assembly is nodal-specific.

_EXPERIMENT_SCOPE = "nodal_agent"
_PF_POWER_MW = 100.0     # reports.nodal_pf_node_daily stored config:
_PF_DURATION_H = 2.0     # 100 MW / 2h / 85% (scaled to the asset by power)
_PF_RTE = 0.85
_PF_NOTE = "theoretical scaled from PF 100MW/2h/85% config"

_REGISTER_STRATEGIES_SQL = """SELECT DISTINCT ON (plant_name, target_date)
       plant_name, target_date, curve_json, assumptions_json, model_version
    FROM marketdata.nodal_strategy_daily
    WHERE target_date >= %s AND target_date <= %s
    ORDER BY plant_name, target_date, created_at DESC, model_version DESC"""

# fengxing_node_name lives on nodal_node_registry (keyed by node), not on the
# asset registry — resolve it via LEFT JOIN on the plant's node.
_REGISTER_REGISTRY_SQL = """SELECT a.plant_name, a.node, a.settle_node,
       n.fengxing_node_name
    FROM marketdata.nodal_asset_registry a
    LEFT JOIN marketdata.nodal_node_registry n ON n.name = a.node
    WHERE a.active IS TRUE"""

_REGISTER_PRICES_SQL = """SELECT node_name, metric_time::date, time_order_96,
       avg_node_price
    FROM marketdata.md_mengxi_nodal_price_96
    WHERE metric_time::date >= %s AND metric_time::date <= %s"""

_REGISTER_PF_SQL = """SELECT data_date, node_name, revenue_cny
    FROM reports.nodal_pf_node_daily
    WHERE province = '蒙西'
      AND power_mw = 100.0 AND duration_h = 2.0 AND rte_pct = 85.0
      AND data_date >= %s AND data_date <= %s"""

_REGISTER_PREV_SQL = """SELECT model, province, window_end, mean_capture_rate,
       status
    FROM marketdata.strategy_experiments
    WHERE scope = %s AND window_days = %s AND window_end < %s"""

_EXPERIMENT_UPSERT = """INSERT INTO marketdata.strategy_experiments
    (scope, model, province, duration_h, power_mw, roundtrip_eff,
     window_days, window_end, days, mean_capture_rate,
     mean_realized_per_mwh, mean_theoretical_per_mwh,
     delta_vs_champion, status, note)
    VALUES ('nodal_agent', %(model)s, %(province)s, %(duration_h)s,
            %(power_mw)s, %(roundtrip_eff)s, %(window_days)s, %(window_end)s,
            %(days)s, %(mean_capture_rate)s, %(mean_realized_per_mwh)s,
            %(mean_theoretical_per_mwh)s, %(delta_vs_champion)s, %(status)s,
            %(note)s)
    ON CONFLICT (scope, model, province, duration_h, power_mw, roundtrip_eff,
                 window_days, window_end)
    DO UPDATE SET days = EXCLUDED.days,
        mean_capture_rate = EXCLUDED.mean_capture_rate,
        mean_realized_per_mwh = EXCLUDED.mean_realized_per_mwh,
        mean_theoretical_per_mwh = EXCLUDED.mean_theoretical_per_mwh,
        delta_vs_champion = EXCLUDED.delta_vs_champion,
        status = EXCLUDED.status,
        note = EXCLUDED.note,
        evaluated_at = NOW()"""


def _f(v):
    return None if v is None or pd.isna(v) else float(v)


def _price_node(plant: str, registry: dict, assumptions: dict):
    reg = registry.get(plant) or {}
    return (reg.get("fengxing_node_name") or assumptions.get("settle_node")
            or assumptions.get("node"))


def register_strategies(conn, target_date: date, window_days: int = 30) -> int:
    """Evaluate converged strategies over [target_date - window_days + 1,
    target_date] against actual RT prices and upsert one strategy_experiments
    row per plant (scope 'nodal_agent', model 'nodal_agent_<model_version>',
    province column = plant name).

    Realized CNY/day = sum_t curve_t x price_t x 0.25 (curve MW net,
    15-min intervals). Per-MWh normalization basis = power_mw x duration_h.
    Theoretical = reports.nodal_pf_node_daily revenue at the stored
    100MW/2h/85% config, scaled by asset power_mw / 100 (note appended when
    duration/rte differ from the PF config). Plants/days lacking a converged
    strategy, actual prices, or PF theoretical are skipped; returns 0 with no
    writes when nothing scores."""
    start = target_date - timedelta(days=window_days - 1)
    cur = conn.cursor()

    cur.execute(_REGISTER_STRATEGIES_SQL, (start, target_date))
    strategies = cur.fetchall()
    if not strategies:
        return 0

    cur.execute(_REGISTER_REGISTRY_SQL)
    registry = {r[0]: {"node": r[1], "settle_node": r[2],
                       "fengxing_node_name": r[3]} for r in cur.fetchall()}

    cur.execute(_REGISTER_PRICES_SQL, (start, target_date))
    slots: dict = {}
    for node, d, slot, px in cur.fetchall():
        if px is not None:
            slots.setdefault((node, d), {})[int(slot)] = float(px)
    price_vec = {}
    for key, per_slot in slots.items():
        v = np.full(96, np.nan)
        for slot, px in per_slot.items():
            if 1 <= slot <= 96:
                v[slot - 1] = px
        if np.isfinite(v).all():  # partial days misalign revenue — skip
            price_vec[key] = v
    if not price_vec:
        return 0

    cur.execute(_REGISTER_PF_SQL, (start, target_date))
    pf = {(node, d): float(rev) for d, node, rev in cur.fetchall()
          if rev is not None}
    if not pf:
        return 0

    capture_rows = []
    plant_params = {}
    for plant, d, curve_json, asm_json, mv in strategies:
        try:
            asm = json.loads(asm_json) if asm_json else {}
            curve = np.asarray(json.loads(curve_json), dtype=float)
        except (TypeError, ValueError):
            continue
        if curve.shape != (96,) or not np.isfinite(curve).all():
            continue
        power = float(asm.get("capacity_mw") or 0.0)
        duration_h = float(asm.get("duration_h") or 0.0)
        rte_pct = float(asm.get("rte_pct") or 0.0)
        if power <= 0 or duration_h <= 0 or rte_pct <= 0:
            continue
        node = _price_node(plant, registry, asm)
        px = price_vec.get((node, d))
        theoretical_cfg = pf.get((node, d))
        if px is None or theoretical_cfg is None:
            continue
        realized = float((curve * px).sum() * 0.25)
        theoretical = theoretical_cfg * (power / _PF_POWER_MW)
        basis = power * duration_h
        rte = rte_pct / 100.0
        # model_version is already namespaced in prod (MODEL_VERSION =
        # "nodal_agent_v1") — prefix only bare versions ("v1") so the
        # persisted key never doubles up (nodal_agent_nodal_agent_v1).
        model = mv if mv.startswith(_EXPERIMENT_SCOPE) else f"{_EXPERIMENT_SCOPE}_{mv}"
        capture_rows.append({
            "model": model,
            "province": plant,
            "date": pd.Timestamp(d),
            "capture_rate": (realized / theoretical
                             if theoretical > 0 else np.nan),
            "realized_profit_per_mwh_day": realized / basis,
            "theoretical_profit_per_mwh_day": theoretical / basis,
        })
        note = (_PF_NOTE if (duration_h != _PF_DURATION_H or rte != _PF_RTE)
                else None)
        plant_params[(model, plant)] = {"duration_h": duration_h,
                                        "power_mw": power,
                                        "roundtrip_eff": rte, "note": note}
    if not capture_rows:
        return 0

    metrics = compute_window_metrics(pd.DataFrame(capture_rows), window_days,
                                     target_date)
    if metrics.empty:
        return 0

    cur.execute(_REGISTER_PREV_SQL, (_EXPERIMENT_SCOPE, window_days,
                                     target_date))
    prev = pd.DataFrame(cur.fetchall(),
                        columns=["model", "province", "window_end",
                                 "mean_capture_rate", "status"])
    scored = assign_status(metrics, prev)

    n = 0
    for _, row in scored.iterrows():
        params = plant_params.get((row["model"], row["province"]))
        if params is None:
            continue
        cur.execute(_EXPERIMENT_UPSERT, {
            "model": row["model"], "province": row["province"],
            "duration_h": params["duration_h"], "power_mw": params["power_mw"],
            "roundtrip_eff": params["roundtrip_eff"],
            "window_days": window_days, "window_end": target_date,
            "days": int(row["days"]),
            "mean_capture_rate": _f(row["mean_capture_rate"]),
            "mean_realized_per_mwh": _f(row["mean_realized_per_mwh"]),
            "mean_theoretical_per_mwh": _f(row["mean_theoretical_per_mwh"]),
            "delta_vs_champion": _f(row["delta_vs_champion"]),
            "status": row["status"], "note": params["note"],
        })
        n += 1
    conn.commit()
    return n
