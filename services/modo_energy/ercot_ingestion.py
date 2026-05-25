"""ERCOT data ingestion from Modo Energy API → intl_market.ercot_* tables.

Usage:
    python -m services.modo_energy.ercot_ingestion --start 2024-01-01 --end 2026-05-10
"""
import argparse
import os
import sys
from datetime import date, timedelta

import pandas as pd
from sqlalchemy import text as sql_text

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from services.common.db_utils import get_engine
from services.modo_energy.client import ModoClient

_DDL = {
    "intl_market.ercot_bess_assets": """
        CREATE TABLE IF NOT EXISTS intl_market.ercot_bess_assets (
            asset TEXT NOT NULL, history_table TEXT NOT NULL,
            date_from DATE, date_to DATE, value TEXT,
            UNIQUE (asset, history_table, date_from)
        )
    """,
    "intl_market.ercot_bess_daily_index": """
        CREATE TABLE IF NOT EXISTS intl_market.ercot_bess_daily_index (
            settlement_date DATE NOT NULL, market TEXT NOT NULL,
            revenue_permw NUMERIC, revenue_permwh NUMERIC, duration NUMERIC,
            PRIMARY KEY (settlement_date, market)
        )
    """,
    "intl_market.ercot_bess_monthly_index": """
        CREATE TABLE IF NOT EXISTS intl_market.ercot_bess_monthly_index (
            year_month TEXT NOT NULL, market TEXT NOT NULL,
            revenue_permw NUMERIC, revenue_permwh NUMERIC, duration NUMERIC,
            PRIMARY KEY (year_month, market)
        )
    """,
    "intl_market.ercot_bess_leaderboard": """
        CREATE TABLE IF NOT EXISTS intl_market.ercot_bess_leaderboard (
            asset TEXT NOT NULL, settlement_date DATE NOT NULL, market TEXT NOT NULL,
            revenue NUMERIC, rated_power NUMERIC, energy_capacity NUMERIC,
            PRIMARY KEY (asset, settlement_date, market)
        )
    """,
    "intl_market.ercot_spot_price": """
        CREATE TABLE IF NOT EXISTS intl_market.ercot_spot_price (
            settlement_date DATE NOT NULL, settlement_period INT NOT NULL,
            region TEXT NOT NULL DEFAULT 'ERCOT', spot_price NUMERIC,
            PRIMARY KEY (settlement_date, settlement_period, region)
        )
    """,
    "intl_market.ercot_ancillary_results": """
        CREATE TABLE IF NOT EXISTS intl_market.ercot_ancillary_results (
            settlement_date DATE NOT NULL, service TEXT NOT NULL,
            region TEXT NOT NULL DEFAULT 'ERCOT',
            clearing_price NUMERIC, volume_mw NUMERIC,
            PRIMARY KEY (settlement_date, service, region)
        )
    """,
    "intl_market.ercot_ingestion_log": """
        CREATE TABLE IF NOT EXISTS intl_market.ercot_ingestion_log (
            id SERIAL PRIMARY KEY, run_at TIMESTAMPTZ DEFAULT NOW(),
            trigger TEXT NOT NULL, date_from DATE, date_to DATE,
            status TEXT NOT NULL, rows_ingested JSONB,
            error_msg TEXT, duration_seconds NUMERIC
        )
    """,
}


def _ensure_tables(engine):
    with engine.begin() as conn:
        for _, ddl in _DDL.items():
            conn.execute(sql_text(ddl))


def _upsert(engine, table, df, conflict_cols, batch_size=2000):
    from psycopg2.extras import execute_values
    if df.empty:
        return 0
    df = df.where(df.notna(), other=None).drop_duplicates(subset=conflict_cols, keep="last")
    cols = list(df.columns)
    update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c not in conflict_cols)
    sql = (
        f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s "
        f"ON CONFLICT ({', '.join(conflict_cols)}) DO UPDATE SET {update_set}"
    )
    rows = [tuple(r[c] for c in cols) for r in df.to_dict(orient="records")]
    for attempt in range(5):
        try:
            with engine.begin() as conn:
                with conn.connection.cursor() as cur:
                    execute_values(cur, sql, rows, page_size=batch_size)
            return len(rows)
        except Exception:
            if attempt == 4:
                raise
            import time; engine.dispose(); time.sleep(30 * (attempt + 1))
    return 0


def _chunk_dates(start, end, days=90):
    cur = start
    while cur <= end:
        yield cur, min(cur + timedelta(days=days - 1), end)
        cur += timedelta(days=days)


def _try_get(client, path, params=None):
    try:
        return client.get(path, params)
    except Exception as exc:
        if any(c in str(exc) for c in ["404", "403", "400"]):
            print(f"  [ercot] endpoint {path} not available: {exc}")
            return None
        raise


def run_ingestion(start, end, only=None):
    engine = get_engine()
    _ensure_tables(engine)
    client = ModoClient()
    results = {}

    def ingest_assets():
        records = _try_get(client, "/ercot/modo/asset/database")
        if not records:
            return 0
        rows = [{"asset": r.get("asset_name", r.get("asset", "")), "history_table": ht,
                 "date_from": r.get("date_from"), "date_to": r.get("date_to"), "value": str(r[ht])}
                for r in records for ht in ("rated_power", "energy_capacity", "owner", "operator")
                if r.get(ht) is not None]
        return _upsert(engine, "intl_market.ercot_bess_assets",
                       pd.DataFrame(rows) if rows else pd.DataFrame(),
                       ["asset", "history_table", "date_from"])

    def ingest_leaderboard():
        total = 0
        for d_from, d_to in _chunk_dates(start, end):
            records = _try_get(client, "/ercot/modo/benchmarking/leaderboard-live",
                               {"date_from": d_from.isoformat(), "date_to": d_to.isoformat()})
            if not records:
                continue
            rows = [{"asset": r.get("asset_name", r.get("asset", "")),
                     "settlement_date": r.get("date", r.get("settlement_date")),
                     "market": r.get("market", "total"), "revenue": r.get("revenue"),
                     "rated_power": r.get("rated_power_mw"), "energy_capacity": r.get("energy_capacity_mwh")}
                    for r in records]
            total += _upsert(engine, "intl_market.ercot_bess_leaderboard", pd.DataFrame(rows),
                             ["asset", "settlement_date", "market"])
        return total

    def ingest_daily_index():
        total = 0
        for d_from, d_to in _chunk_dates(start, end):
            records = _try_get(client, "/ercot/modo/benchmarking/daily-index-live",
                               {"date_from": d_from.isoformat(), "date_to": d_to.isoformat()})
            if not records:
                continue
            rows = [{"settlement_date": r.get("date", r.get("settlement_date")),
                     "market": r.get("market", "total"), "revenue_permw": r.get("revenue_per_mw"),
                     "revenue_permwh": r.get("revenue_per_mwh"), "duration": r.get("duration")}
                    for r in records]
            total += _upsert(engine, "intl_market.ercot_bess_daily_index", pd.DataFrame(rows),
                             ["settlement_date", "market"])
        return total

    def ingest_spot_price():
        total = 0
        for d_from, d_to in _chunk_dates(start, end):
            records = _try_get(client, "/ercot/modo/markets/lmp-live",
                               {"date_from": d_from.isoformat(), "date_to": d_to.isoformat()})
            if not records:
                continue
            rows = [{"settlement_date": r.get("date", r.get("settlement_date")),
                     "settlement_period": r.get("settlement_period", r.get("period", 1)),
                     "region": r.get("region", "ERCOT"), "spot_price": r.get("lmp", r.get("price"))}
                    for r in records]
            total += _upsert(engine, "intl_market.ercot_spot_price", pd.DataFrame(rows),
                             ["settlement_date", "settlement_period", "region"])
        return total

    for key, fn in [("assets", ingest_assets), ("leaderboard", ingest_leaderboard),
                    ("daily_index", ingest_daily_index), ("spot_price", ingest_spot_price)]:
        if only and key not in only:
            continue
        print(f"  [ercot/{key}]…", end="", flush=True)
        try:
            n = fn(); results[key] = n; print(f" {n} rows")
        except Exception as exc:
            results[key] = 0; print(f" ERROR: {exc}")
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default=(date.today() - timedelta(days=1)).isoformat())
    parser.add_argument("--end",   default=(date.today() - timedelta(days=1)).isoformat())
    parser.add_argument("--only")
    args = parser.parse_args()
    print(run_ingestion(date.fromisoformat(args.start), date.fromisoformat(args.end),
                        only=args.only.split(",") if args.only else None))
