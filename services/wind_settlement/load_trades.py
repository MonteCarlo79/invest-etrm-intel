# -*- coding: utf-8 -*-
"""Load 零碳46 trade confirmations into marketdata.wind_trades.

Usage:
    python -m services.wind_settlement.load_trades            # all files
    python -m services.wind_settlement.load_trades --create   # apply DDL first

Idempotent: rows carry a natural-key unique index; reloads use ON CONFLICT
DO NOTHING.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

from sqlalchemy import create_engine, text

from services.wind_settlement.trades import parse_trades_file

ASSET = "新_悦盛昌渠#1期"
TRADES_DIR = Path("data/raw/零碳46交易数据&月度复盘/零碳46交易数据（2026年1~7月）")
DDL = Path("db/ddl/marketdata/wind_trades.sql")


def _load_env() -> None:
    env = Path("config/.env")
    if env.exists():
        for line in env.read_text().splitlines():
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


def _copy_rows(engine, rows: list[dict]) -> None:
    """Bulk-load via COPY (one round trip) — plain executemany INSERTs take
    ~40k round trips over the cross-Pacific link."""
    import csv
    import io

    cols = ["asset", "month", "channel", "row_no", "trade_type", "mode", "period",
            "energy_kind", "consumer_unit", "generator_unit", "volume_mwh",
            "energy_price", "energy_fee", "env_value", "all_in_price", "source_file"]
    buf = io.StringIO()
    w = csv.writer(buf)
    for r in rows:
        w.writerow(["" if r[c] is None else r[c] for c in cols])
    buf.seek(0)
    raw = engine.raw_connection()
    try:
        with raw.cursor() as cur:
            cur.execute("CREATE TEMP TABLE _wind_trades_stage (LIKE marketdata.wind_trades INCLUDING DEFAULTS) ON COMMIT DROP")
            cur.copy_expert(
                "COPY _wind_trades_stage (asset_name, delivery_month, channel, row_no, trade_type, mode, period, "
                "energy_kind, consumer_unit, generator_unit, volume_mwh, energy_price, energy_fee, "
                "env_value, all_in_price, source_file) FROM STDIN WITH (FORMAT csv)",
                buf,
            )
            cur.execute("""
                INSERT INTO marketdata.wind_trades (
                    asset_name, delivery_month, channel, row_no, trade_type, mode, period,
                    energy_kind, consumer_unit, generator_unit, volume_mwh,
                    energy_price, energy_fee, env_value, all_in_price, source_file
                )
                SELECT asset_name, delivery_month, channel, row_no, trade_type, mode, period,
                       energy_kind, consumer_unit, generator_unit, volume_mwh,
                       energy_price, energy_fee, env_value, all_in_price, source_file
                FROM _wind_trades_stage
                ON CONFLICT (source_file, row_no)
                DO NOTHING
            """)
        raw.commit()
    finally:
        raw.close()


def load_all(engine, trades_dir: Path = TRADES_DIR) -> int:
    files = sorted(
        list((trades_dir / "2.跨省成交单分月总表").glob("*.xlsx"))
        + list((trades_dir / "3.省内成交单分月总表").glob("*.xlsx"))
    )
    rows = []
    for f in files:
        df = parse_trades_file(f)
        rows.extend(
            {
                "asset": ASSET,
                "month": f"{r.delivery_month}-01",
                "channel": r.channel,
                "row_no": int(float(r.row_no)),
                "trade_type": r.trade_type,
                "mode": r.mode,
                "period": r.period,
                "energy_kind": r.energy_kind,
                "consumer_unit": r.consumer_unit,
                "generator_unit": r.generator_unit,
                "volume_mwh": r.volume_mwh,
                "energy_price": r.energy_price,
                "energy_fee": r.energy_fee,
                "env_value": r.env_value,
                "all_in_price": r.all_in_price,
                "source_file": r.source_file,
            }
            for r in df.itertuples()
        )
        print(f"  {f.name}: {len(df)} rows", flush=True)
    _copy_rows(engine, rows)
    return len(rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--create", action="store_true", help="apply DDL before loading")
    args = ap.parse_args()

    _load_env()
    engine = create_engine(os.environ["PGURL"], connect_args={"connect_timeout": 10})

    if args.create:
        with engine.begin() as conn:
            for stmt in DDL.read_text().split(";"):
                if stmt.strip():
                    conn.execute(text(stmt))
        print("DDL applied")

    n = load_all(engine)
    print(f"loaded {n} trade rows")


if __name__ == "__main__":
    main()
