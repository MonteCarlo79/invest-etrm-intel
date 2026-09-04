"""Daily nodal PF batch — computes perfect-foresight BESS revenue for every
node in a province for each date and upserts into reports.nodal_pf_node_daily.

Lives in services/ so it ships inside the mengxi-dashboard image (scripts/ is
not copied). Run as:  python -m services.bess_map.nodal_pf_daily [--date D]
[--start S --end E] [--province P] [--dry-run]

Config fixed: 100 MW / 2h / 85% RTE (matches services.mengxi_nodal.pf_results.CONFIG).
Price source: 蒙西 reads the EnOS-backed view (md_mengxi_nodal_price_96);
other provinces read the Fengxing base table filtered by market_name.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

try:
    from dotenv import load_dotenv
    load_dotenv(_REPO / "config" / ".env", override=False)
except ImportError:
    pass

import pandas as pd
from sqlalchemy import create_engine, text

from services.bess_map.optimisation_engine import compute_dispatch_from_15min_prices

POWER_MW, DURATION_H, RTE_PCT = 100.0, 2.0, 85.0
_MIN_NODES = 100  # days with fewer stored rows are considered incomplete

_UPSERT = text("""
    INSERT INTO reports.nodal_pf_node_daily
        (data_date, province, node_name, power_mw, duration_h, rte_pct,
         revenue_cny, charge_mwh, discharge_mwh)
    VALUES (:data_date, :province, :node_name, :power_mw, :duration_h, :rte_pct,
            :revenue_cny, :charge_mwh, :discharge_mwh)
    ON CONFLICT (data_date, province, node_name, power_mw, duration_h, rte_pct)
    DO UPDATE SET revenue_cny  = EXCLUDED.revenue_cny,
                  charge_mwh   = EXCLUDED.charge_mwh,
                  discharge_mwh = EXCLUDED.discharge_mwh,
                  computed_at  = NOW()
""")


def _engine():
    url = os.environ.get("PGURL") or os.environ.get("DB_DSN")
    if not url:
        raise SystemExit("PGURL/DB_DSN not set")
    return create_engine(url, pool_pre_ping=True)


def _price_sql(province: str) -> text:
    if province == "蒙西":
        return text("""
            SELECT node_name, metric_time, time_order_96, avg_node_price
            FROM marketdata.md_mengxi_nodal_price_96
            WHERE metric_time >= :s AND metric_time < :e
            ORDER BY node_name, time_order_96
        """)
    return text("""
        SELECT node_name, metric_time, time_order_96, avg_node_price
        FROM marketdata.md_shanxi_nodal_price_96
        WHERE market_name = :prov AND metric_time >= :s AND metric_time < :e
        ORDER BY node_name, time_order_96
    """)


def _fetch_prices(engine, province: str, day: date) -> pd.DataFrame:
    # Explicit CST bounds: python dates would be coerced at midnight in the DB
    # session TZ (UTC), shifting the window 8h and dropping slots 1-32.
    s = f"{day} 00:00:00+08"
    e = f"{day + timedelta(days=1)} 00:00:00+08"
    params = {"s": s, "e": e}
    if province != "蒙西":
        params["prov"] = province
    return pd.read_sql(_price_sql(province), engine, params=params)


def _stored_count(engine, province: str, day: date) -> int:
    with engine.connect() as conn:
        return conn.execute(text("""
            SELECT COUNT(*) FROM reports.nodal_pf_node_daily
            WHERE data_date = :d AND province = :p
              AND power_mw = :pw AND duration_h = :dur AND rte_pct = :rte
        """), {"d": day, "p": province, "pw": POWER_MW, "dur": DURATION_H, "rte": RTE_PCT}).scalar()


def run_day(engine, province: str, day: date, dry_run: bool = False) -> tuple[int, str]:
    df = _fetch_prices(engine, province, day)
    if df.empty:
        return 0, "no price data"
    nodes = df["node_name"].unique()
    if dry_run:
        return len(nodes), f"DRY-RUN {len(nodes)} nodes"

    day_start = pd.Timestamp(day.year, day.month, day.day)
    rows = []
    for node in nodes:
        g = df[df["node_name"] == node].sort_values("time_order_96")
        if len(g) < 96:
            continue
        series = pd.Series(
            g["avg_node_price"].astype(float).values,
            index=day_start + pd.to_timedelta((g["time_order_96"].values - 1) * 15, unit="min"),
        )
        try:
            disp, profit = compute_dispatch_from_15min_prices(
                series, power_mw=POWER_MW, duration_h=DURATION_H,
                roundtrip_eff=RTE_PCT / 100.0,
            )
            rows.append({
                "data_date": day, "province": province, "node_name": node,
                "power_mw": POWER_MW, "duration_h": DURATION_H, "rte_pct": RTE_PCT,
                "revenue_cny": float(profit.sum()),
                "charge_mwh": float(disp["charge_mw"].sum() * 0.25),
                "discharge_mwh": float(disp["discharge_mw"].sum() * 0.25),
            })
        except Exception:
            continue

    if not rows:
        return 0, "no results"
    with engine.begin() as conn:
        for i in range(0, len(rows), 500):
            conn.execute(_UPSERT, rows[i : i + 500])
    return len(rows), f"{len(rows):,} rows"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    ap.add_argument("--start", default=None)
    ap.add_argument("--end", default=None)
    ap.add_argument("--province", default="蒙西")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if args.date:
        start = end = date.fromisoformat(args.date)
    else:
        end = date.fromisoformat(args.end) if args.end else date.today() - timedelta(days=1)
        start = date.fromisoformat(args.start) if args.start else end

    engine = _engine()
    total_t0 = time.time()
    d = start
    while d <= end:
        n_stored = _stored_count(engine, args.province, d)
        if n_stored >= _MIN_NODES and not args.dry_run:
            print(f"{d}: skip (already {n_stored} rows)", flush=True)
        else:
            t0 = time.time()
            n, msg = run_day(engine, args.province, d, dry_run=args.dry_run)
            print(f"{d}: {msg}  ({time.time()-t0:.0f}s)", flush=True)
        d += timedelta(days=1)
    print(f"DONE in {time.time()-total_t0:.0f}s", flush=True)


if __name__ == "__main__":
    main()
