# scripts/nodal_l2_backtest.py
"""Offline L2 nodal-model backtest gate (Task 8 step 2).

Consumes the daily extract produced by the ECS run-task probe (RDS is
unreachable from dev machines) and scores the L2 price-formation model
against the grid-only baseline per zone.

Extract CSV columns (one row per node per day):
    node_name, d, nodal_price, grid_price, headroom, renewable

    nodal_price  — actual day-avg RT nodal price (md_mengxi_nodal_price_96)
    grid_price   — actual day-avg provincial RT (spot_prices_hourly, 蒙西)
    headroom     — day-avg bidding_space_d1_mw (spot_fundamentals_hourly;
                   D-1 published value, so a legitimate forecast feature)
    renewable    — day-avg renewable_d1_mw (same source; province-level
                   proxy for substation renewable — v1 approximation,
                   substation-level split is future work)

Feature honesty: headroom/renewable are D-1 published forecasts. grid_price
is the SAME-DAY ACTUAL (perfect-foresight level shared by both arms) — the
gate therefore isolates the delta model's value, not the grid forecast's;
when L1's nodal_fc_grid_daily is populated this script should switch
grid_hat to the L1 forecast.

Clusters = portfolio asset zones from services.mengxi_nodal.zones
(CURRENT_ASSETS: own_node + parent_nodes per asset). congested =
headroom < 0 (binding substation proxy).

Usage:
    python scripts/nodal_l2_backtest.py /path/to/extract.csv [--holdout 21]

Gate (printed, and exit 1 on any FAIL): for every zone with >= MIN_TRAIN
train rows, nodal MAE <= grid-only baseline MAE.
"""
from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.mengxi_nodal.zones import CURRENT_ASSETS, ZONES
from services.nodal_forecast import backtest, price_formation as pf

MIN_TRAIN = 30  # rows per zone before the gate applies (brief: ≥30 days)


def _zone_map() -> dict[str, str]:
    """Fengxing node name -> zone label (asset's substation zone)."""
    asset_zone = {}
    for z in ZONES:
        for code in z.get("our_assets", []):
            asset_zone[code] = z["zone"]
    node_zone = {}
    for a in CURRENT_ASSETS:
        zone = asset_zone.get(a["asset_code"])
        if zone is None:
            continue
        for n in [a.get("own_node"), *(a.get("parent_nodes") or [])]:
            if n:
                node_zone[n] = zone
    return node_zone


def build_hist(df: pd.DataFrame) -> pd.DataFrame:
    """Attach zone + congested flag; restrict to zone-mapped nodes."""
    node_zone = _zone_map()
    df = df[df["node_name"].isin(node_zone)].copy()
    if df.empty:
        return df
    df["zone"] = df["node_name"].map(node_zone)
    df["congested"] = df["headroom"] < 0.0
    return df.rename(columns={"nodal_price": "nodal_price"})


def run_gate(df: pd.DataFrame, holdout_days: int) -> tuple[pd.DataFrame, bool]:
    df = df.dropna(subset=["nodal_price", "grid_price", "headroom", "renewable"])
    if df.empty:
        return pd.DataFrame(), True
    df["d"] = pd.to_datetime(df["d"]).dt.date
    all_days = sorted(df["d"].unique())
    if len(all_days) <= holdout_days + 10:
        raise SystemExit(f"only {len(all_days)} days in extract — need > "
                         f"{holdout_days + 10} for a meaningful train/holdout split")
    cutoff = all_days[-holdout_days]
    results = []
    ok_all = True
    for zone, zdf in df.groupby("zone"):
        train = zdf[zdf["d"] < cutoff]
        holdout = sorted(d for d in zdf["d"].unique() if d >= cutoff)
        if len(train) < MIN_TRAIN or not holdout:
            results.append({"zone": zone, "train_rows": len(train),
                            "holdout_days": len(holdout),
                            "nodal_mae": None, "baseline_mae": None,
                            "gate": "SKIP (<30 train rows)"})
            continue
        hist = train.rename(columns={"d": "d"})[["d", "node_name", "grid_price",
                                                 "headroom", "renewable",
                                                 "congested", "nodal_price"]]
        hist = hist.rename(columns={"node_name": "node",
                                    "renewable": "substation_renewable"})
        model = pf.fit_cluster_model(hist)
        day_feats = (zdf[["d", "headroom", "renewable", "congested"]]
                     .drop_duplicates("d").set_index("d"))

        def model_fn(day: date, _model=model, _zdf=zdf, _feats=day_feats,
                     _zone=zone) -> pd.DataFrame:
            rows = _zdf[_zdf["d"] == day]
            if rows.empty or day not in _feats.index:
                return pd.DataFrame()
            f = _feats.loc[day]
            features = {"headroom": float(f["headroom"]),
                        "substation_renewable": float(f["renewable"]),
                        "congested": bool(f["congested"])}
            out = []
            for r in rows.itertuples():
                m = pf.resolve_model(_model, r.node_name)
                nodal_hat = float(pf.nodal_curve(float(r.grid_price),
                                                 features, m)[0])
                out.append({"cluster": _zone, "nodal_hat": nodal_hat,
                            "grid_hat": float(r.grid_price),
                            "actual": float(r.nodal_price)})
            return pd.DataFrame(out)

        scores = backtest.nodal_mae(model_fn, holdout)
        nodal = scores.get(zone)
        base = scores.get(zone + backtest.BASELINE_SUFFIX)
        ok = nodal is not None and base is not None and nodal <= base
        ok_all = ok_all and ok
        results.append({"zone": zone, "train_rows": len(train),
                        "holdout_days": len(holdout),
                        "nodal_mae": None if nodal is None else round(nodal, 2),
                        "baseline_mae": None if base is None else round(base, 2),
                        "gate": "PASS" if ok else "FAIL"})
    return pd.DataFrame(results), ok_all


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("csv", help="probe extract CSV (node_name,d,nodal_price,grid_price,headroom,renewable)")
    ap.add_argument("--holdout", type=int, default=21)
    args = ap.parse_args()
    df = pd.read_csv(args.csv)
    hist = build_hist(df)
    if hist.empty:
        print("no rows matched the portfolio zone node map — check extract")
        return 1
    table, ok = run_gate(hist, args.holdout)
    print(f"\nL2 nodal backtest gate — holdout last {args.holdout} days, "
          f"zones from mengxi_nodal.zones, grid_hat = same-day actual "
          f"(perfect-foresight level; gates the delta model)\n")
    print(table.to_string(index=False))
    print(f"\nOverall gate: {'PASS' if ok else 'FAIL'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
