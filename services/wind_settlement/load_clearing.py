# -*- coding: utf-8 -*-
"""Load the 日清算 workbook into marketdata.wind_daily_clearing (COPY staging).

Usage:
    python -m services.wind_settlement.load_clearing [--create]
"""
from __future__ import annotations

import argparse
import csv
import io
import os
from pathlib import Path

from sqlalchemy import create_engine, text

from services.wind_settlement.clearing import parse_clearing_file

CLEARING_FILE = Path(
    "data/raw/零碳46交易数据&月度复盘/零碳46交易数据（2026年1~7月）/日清算202601~202608.xlsx"
)
DDL = Path("db/ddl/marketdata/wind_daily_clearing.sql")

_COLS = ["datetime", "metered_mwh", "energy_price", "energy_fee",
         "rt_cleared_mw", "rt_nodal_price", "contract_mwh", "contract_price",
         "ic_da_mw", "ic_da_price", "ic_id_mw", "ic_id_price",
         "curve_min", "curve_mean", "source_file"]


def _load_env() -> None:
    env = Path("config/.env")
    if env.exists():
        for line in env.read_text().splitlines():
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


def load(engine, path: Path = CLEARING_FILE) -> int:
    df = parse_clearing_file(path)
    buf = io.StringIO()
    w = csv.writer(buf)
    for r in df.itertuples(index=False):
        rec = dict(zip(df.columns, r))
        w.writerow(["" if rec[c] is None or (isinstance(rec[c], float) and rec[c] != rec[c]) else rec[c]
                    for c in _COLS])
    buf.seek(0)
    raw = engine.raw_connection()
    try:
        with raw.cursor() as cur:
            cur.execute("CREATE TEMP TABLE _wind_clearing_stage (LIKE marketdata.wind_daily_clearing) ON COMMIT DROP")
            cur.copy_expert(
                f"COPY _wind_clearing_stage ({', '.join(_COLS)}) FROM STDIN WITH (FORMAT csv)", buf)
            cur.execute(f"""
                INSERT INTO marketdata.wind_daily_clearing ({', '.join(_COLS)})
                SELECT {', '.join(_COLS)} FROM _wind_clearing_stage
                ON CONFLICT (datetime) DO NOTHING
            """)
        raw.commit()
    finally:
        raw.close()
    return len(df)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--create", action="store_true")
    args = ap.parse_args()
    _load_env()
    engine = create_engine(os.environ["PGURL"], connect_args={"connect_timeout": 10})
    if args.create:
        with engine.begin() as conn:
            for stmt in DDL.read_text().split(";"):
                if stmt.strip():
                    conn.execute(text(stmt))
        print("DDL applied")
    n = load(engine)
    print(f"loaded {n} clearing rows")


if __name__ == "__main__":
    main()
