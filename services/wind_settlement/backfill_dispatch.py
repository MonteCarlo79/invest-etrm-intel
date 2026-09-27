# -*- coding: utf-8 -*-
"""Backfill marketdata.wind_dispatch_15min for 悦盛昌渠.

Pulls md_id_cleared_energy + md_rt_nodal_price once each for the full range,
joins locally, upserts. One-off slow queries on the big tables; the extract
table keeps the dashboard tabs fast.

Usage:
    python -m services.wind_settlement.backfill_dispatch --create --start 2026-01-01 --end 2026-08-01
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

PLANT = "悦盛昌渠风光储电站"
NODE = "内蒙.悦盛昌渠风光储电站/220kV.1M"
DDL = Path("db/ddl/marketdata/wind_dispatch_15min.sql")


def _load_env() -> None:
    env = Path("config/.env")
    if env.exists():
        for line in env.read_text().splitlines():
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


def backfill(engine, start: str, end: str) -> int:
    print("pulling md_id_cleared_energy …")
    energy = pd.read_sql(
        text("""
            SELECT datetime, cleared_energy_mwh, data_date
            FROM marketdata.md_id_cleared_energy
            WHERE plant_name = :plant AND datetime >= :start AND datetime < :end
        """),
        engine, params={"plant": PLANT, "start": start, "end": end},
    )
    print(f"  {len(energy)} rows")
    print("pulling md_rt_nodal_price …")
    price = pd.read_sql(
        text("""
            SELECT datetime, node_price
            FROM marketdata.md_rt_nodal_price
            WHERE node_name = :node AND datetime >= :start AND datetime < :end
        """),
        engine, params={"node": NODE, "start": start, "end": end},
    )
    print(f"  {len(price)} rows")

    df = energy.merge(price, on="datetime", how="left")
    df["gen_mwh"] = df["cleared_energy_mwh"].clip(lower=0) * 0.25
    df = df.rename(columns={"cleared_energy_mwh": "gen_mw", "node_price": "rt_price"})
    df["plant_name"] = PLANT
    df["node_name"] = NODE

    rows = df[["plant_name", "node_name", "datetime", "gen_mw", "gen_mwh", "rt_price", "data_date"]]

    # COPY via staging table — one round trip (plain executemany would take
    # ~20k round trips over the cross-Pacific link)
    import csv
    import io

    buf = io.StringIO()
    w = csv.writer(buf)
    for r in rows.itertuples(index=False):
        w.writerow(["" if pd.isna(v) else v for v in r])
    buf.seek(0)

    print("inserting …", flush=True)
    raw = engine.raw_connection()
    try:
        with raw.cursor() as cur:
            cur.execute("CREATE TEMP TABLE _wind_dispatch_stage (LIKE marketdata.wind_dispatch_15min) ON COMMIT DROP")
            cur.copy_expert(
                "COPY _wind_dispatch_stage (plant_name, node_name, datetime, gen_mw, gen_mwh, rt_price, data_date) "
                "FROM STDIN WITH (FORMAT csv)",
                buf,
            )
            cur.execute("""
                INSERT INTO marketdata.wind_dispatch_15min
                    (plant_name, node_name, datetime, gen_mw, gen_mwh, rt_price, data_date)
                SELECT plant_name, node_name, datetime, gen_mw, gen_mwh, rt_price, data_date
                FROM _wind_dispatch_stage
                ON CONFLICT (plant_name, datetime) DO UPDATE SET
                    gen_mw = EXCLUDED.gen_mw, gen_mwh = EXCLUDED.gen_mwh,
                    rt_price = EXCLUDED.rt_price, data_date = EXCLUDED.data_date
            """)
        raw.commit()
    finally:
        raw.close()
    return len(rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--create", action="store_true")
    ap.add_argument("--start", default="2026-01-01")
    ap.add_argument("--end", default="2026-08-01")
    args = ap.parse_args()

    _load_env()
    engine = create_engine(os.environ["PGURL"], connect_args={"connect_timeout": 10})

    if args.create:
        with engine.begin() as conn:
            for stmt in DDL.read_text().split(";"):
                if stmt.strip():
                    conn.execute(text(stmt))
        print("DDL applied")

    n = backfill(engine, args.start, args.end)
    print(f"backfilled {n} dispatch rows")


if __name__ == "__main__":
    main()
