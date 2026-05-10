"""GB data ingestion from Modo Energy API → intl_market schema.

Usage (backfill):
    python -m services.modo_energy.gb_ingestion --start 2024-01-01 --end 2026-05-10

Endpoints covered:
    1. /gb/modo/asset/database          → intl_market.gb_bess_assets
    2. /gb/modo/benchmarking/daily-index-live  → intl_market.gb_bess_daily_index
    3. /gb/modo/benchmarking/monthly-index-live → intl_market.gb_bess_monthly_index
    4. /gb/modo/benchmarking/leaderboard-live  → intl_market.gb_bess_leaderboard
    5. /gb/modo/markets/system-price-live      → intl_market.gb_system_price
    6. /gb/modo/markets/niv-live               → intl_market.gb_niv
    7. /gb/epex/day-ahead/hh                   → intl_market.gb_epex_da_hh
    8. /gb/national-grid/dx/results-summary    → intl_market.gb_dx_results
"""
import argparse
import os
import sys
from datetime import date, datetime, timedelta

import pandas as pd
from sqlalchemy import text as sql_text

# Allow running as a script from repo root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from services.common.db_utils import get_engine
from services.modo_energy.client import ModoClient

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _upsert(engine, table: str, df: pd.DataFrame, conflict_cols: list[str],
            batch_size: int = 500):
    """Bulk upsert via INSERT … ON CONFLICT DO UPDATE, in batches to avoid SSL timeouts."""
    if df.empty:
        return 0
    # Replace NaN/NaT with None so psycopg2 maps them to NULL
    df = df.where(df.notna(), other=None)
    cols = list(df.columns)
    placeholders = ", ".join(f":{c}" for c in cols)
    update_set = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in cols if c not in conflict_cols
    )
    conflict = ", ".join(conflict_cols)
    sql = f"""
        INSERT INTO {table} ({", ".join(cols)})
        VALUES ({placeholders})
        ON CONFLICT ({conflict}) DO UPDATE SET {update_set}
    """
    records = df.to_dict(orient="records")
    total = 0
    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        with engine.begin() as conn:
            conn.execute(sql_text(sql), batch)
        total += len(batch)
    return total


def _chunk_dates(start: date, end: date, days: int = 90):
    """Yield (from, to) date pairs in chunks to avoid huge API responses."""
    cur = start
    while cur <= end:
        chunk_end = min(cur + timedelta(days=days - 1), end)
        yield cur, chunk_end
        cur = chunk_end + timedelta(days=1)


# ---------------------------------------------------------------------------
# Per-endpoint ingestion functions
# ---------------------------------------------------------------------------

def ingest_assets(client: ModoClient, engine) -> int:
    records = client.get("/gb/modo/asset/database")
    if not records:
        return 0
    df = pd.DataFrame(records)
    # Normalise date columns
    for col in ["valid_from", "valid_to", "commissioning_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
    df["ingested_at"] = datetime.utcnow()
    return _upsert(engine, "intl_market.gb_bess_assets", df, ["asset", "valid_from", "history_table"])


def ingest_daily_index(client: ModoClient, engine, start: date, end: date) -> int:
    total = 0
    for d_from, d_to in _chunk_dates(start, end):
        records = client.get("/gb/modo/benchmarking/daily-index-live", {
            "settlement_date_from": d_from.isoformat(),
            "settlement_date_to": d_to.isoformat(),
        })
        if not records:
            continue
        df = pd.DataFrame(records)
        df["settlement_date"] = pd.to_datetime(df["settlement_date"]).dt.date
        df["ingested_at"] = datetime.utcnow()
        total += _upsert(engine, "intl_market.gb_bess_daily_index", df,
                         ["settlement_date", "duration", "market"])
    return total


def ingest_monthly_index(client: ModoClient, engine, start: date, end: date) -> int:
    month_from = start.strftime("%Y-%m")
    month_to = end.strftime("%Y-%m")
    records = client.get("/gb/modo/benchmarking/monthly-index-live", {
        "month_from": month_from,
        "month_to": month_to,
    })
    if not records:
        return 0
    df = pd.DataFrame(records)
    df["month"] = pd.to_datetime(df["month"]).dt.date
    df["ingested_at"] = datetime.utcnow()
    return _upsert(engine, "intl_market.gb_bess_monthly_index", df,
                   ["month", "duration", "market"])


def ingest_leaderboard(client: ModoClient, engine, start: date, end: date) -> int:
    total = 0
    for d_from, d_to in _chunk_dates(start, end, days=14):
        records = client.get("/gb/modo/benchmarking/leaderboard-live", {
            "settlement_date_from": d_from.isoformat(),
            "settlement_date_to": d_to.isoformat(),
        })
        if not records:
            continue
        df = pd.DataFrame(records)
        df["settlement_date"] = pd.to_datetime(df["settlement_date"]).dt.date
        df = df.drop(columns=["id"], errors="ignore")
        df["ingested_at"] = datetime.utcnow()
        total += _upsert(engine, "intl_market.gb_bess_leaderboard", df,
                         ["settlement_date", "settlement_period", "asset", "market"])
    return total


def ingest_system_price(client: ModoClient, engine, start: date, end: date) -> int:
    total = 0
    for d_from, d_to in _chunk_dates(start, end):
        records = client.get("/gb/modo/markets/system-price-live", {
            "date_from": d_from.isoformat(),
            "date_to": d_to.isoformat(),
        })
        if not records:
            continue
        df = pd.DataFrame(records)
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df["ingested_at"] = datetime.utcnow()
        total += _upsert(engine, "intl_market.gb_system_price", df,
                         ["date", "settlement_period"])
    return total


def ingest_niv(client: ModoClient, engine, start: date, end: date) -> int:
    total = 0
    for d_from, d_to in _chunk_dates(start, end):
        records = client.get("/gb/modo/markets/niv-live", {
            "date_from": d_from.isoformat(),
            "date_to": d_to.isoformat(),
        })
        if not records:
            continue
        df = pd.DataFrame(records)
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df["ingested_at"] = datetime.utcnow()
        total += _upsert(engine, "intl_market.gb_niv", df,
                         ["date", "settlement_period"])
    return total


def ingest_epex_da_hh(client: ModoClient, engine, start: date, end: date) -> int:
    total = 0
    for d_from, d_to in _chunk_dates(start, end):
        records = client.get("/gb/epex/day-ahead/hh", {
            "date_from": d_from.isoformat(),
            "date_to": d_to.isoformat(),
        })
        if not records:
            continue
        df = pd.DataFrame(records)
        df["delivery_date"] = pd.to_datetime(df["delivery_date"]).dt.date
        df["start_time"] = pd.to_datetime(df["start_time"], utc=True, errors="coerce")
        df = df.drop(columns=["country"], errors="ignore")
        df["ingested_at"] = datetime.utcnow()
        total += _upsert(engine, "intl_market.gb_epex_da_hh", df,
                         ["delivery_date", "settlement_period"])
    return total


def ingest_dx_results(client: ModoClient, engine, start: date, end: date) -> int:
    total = 0
    for d_from, d_to in _chunk_dates(start, end):
        records = client.get("/gb/national-grid/dx/results-summary", {
            "date_from": d_from.isoformat(),
            "date_to": d_to.isoformat(),
        })
        if not records:
            continue
        df = pd.DataFrame(records)
        df["efa_date"] = pd.to_datetime(df["efa_date"]).dt.date
        df["delivery_start"] = pd.to_datetime(df["delivery_start"], utc=True, errors="coerce")
        df["delivery_end"] = pd.to_datetime(df["delivery_end"], utc=True, errors="coerce")
        df["ingested_at"] = datetime.utcnow()
        total += _upsert(engine, "intl_market.gb_dx_results", df,
                         ["efa_date", "efa", "service", "auction_id"])
    return total


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_gb_backfill(start: date, end: date, verbose: bool = True):
    client = ModoClient()
    engine = get_engine()

    steps = [
        ("Assets (static)",    lambda: ingest_assets(client, engine)),
        ("Daily BESS index",   lambda: ingest_daily_index(client, engine, start, end)),
        ("Monthly BESS index", lambda: ingest_monthly_index(client, engine, start, end)),
        ("Leaderboard",        lambda: ingest_leaderboard(client, engine, start, end)),
        ("System price",       lambda: ingest_system_price(client, engine, start, end)),
        ("NIV",                lambda: ingest_niv(client, engine, start, end)),
        ("EPEX DA HH",         lambda: ingest_epex_da_hh(client, engine, start, end)),
        ("DX results",         lambda: ingest_dx_results(client, engine, start, end)),
    ]

    for label, fn in steps:
        if verbose:
            print(f"  {label} ... ", end="", flush=True)
        n = fn()
        if verbose:
            print(f"{n} rows")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv(
        os.path.join(os.path.dirname(__file__), "..", "..", "config", ".env"),
        override=False,
    )

    parser = argparse.ArgumentParser(description="Backfill GB market data from Modo Energy")
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end",   required=True, help="End date YYYY-MM-DD")
    args = parser.parse_args()

    start_date = date.fromisoformat(args.start)
    end_date   = date.fromisoformat(args.end)

    print(f"Backfilling GB data {start_date} → {end_date}")
    run_gb_backfill(start_date, end_date)
    print("Done.")
