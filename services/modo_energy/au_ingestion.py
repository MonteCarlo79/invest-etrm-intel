"""AU (NEM) data ingestion from Modo Energy API → intl_market.au_* tables.

Mirrors gb_ingestion.py but uses /au/modo/... endpoint paths.
Falls back gracefully if Modo API does not yet expose AU endpoints.

Usage (backfill):
    python -m services.modo_energy.au_ingestion --start 2024-01-01 --end 2026-05-10
"""
import argparse
import os
import sys
from datetime import date, datetime, timedelta

import pandas as pd
from sqlalchemy import text as sql_text

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from services.common.db_utils import get_engine
from services.modo_energy.client import ModoClient


# ---------------------------------------------------------------------------
# Table DDL (auto-created on first run)
# ---------------------------------------------------------------------------

_DDL = {
    "intl_market.au_bess_assets": """
        CREATE TABLE IF NOT EXISTS intl_market.au_bess_assets (
            asset          TEXT NOT NULL,
            history_table  TEXT NOT NULL,
            date_from      DATE,
            date_to        DATE,
            value          TEXT,
            UNIQUE (asset, history_table, date_from)
        )
    """,
    "intl_market.au_bess_daily_index": """
        CREATE TABLE IF NOT EXISTS intl_market.au_bess_daily_index (
            settlement_date    DATE NOT NULL,
            market             TEXT NOT NULL,
            revenue_permw      NUMERIC,
            revenue_permwh     NUMERIC,
            duration           NUMERIC,
            PRIMARY KEY (settlement_date, market)
        )
    """,
    "intl_market.au_bess_monthly_index": """
        CREATE TABLE IF NOT EXISTS intl_market.au_bess_monthly_index (
            year_month   TEXT NOT NULL,
            market       TEXT NOT NULL,
            revenue_permw  NUMERIC,
            revenue_permwh NUMERIC,
            duration       NUMERIC,
            PRIMARY KEY (year_month, market)
        )
    """,
    "intl_market.au_bess_leaderboard": """
        CREATE TABLE IF NOT EXISTS intl_market.au_bess_leaderboard (
            asset            TEXT NOT NULL,
            settlement_date  DATE NOT NULL,
            market           TEXT NOT NULL,
            revenue          NUMERIC,
            rated_power      NUMERIC,
            energy_capacity  NUMERIC,
            PRIMARY KEY (asset, settlement_date, market)
        )
    """,
    "intl_market.au_spot_price": """
        CREATE TABLE IF NOT EXISTS intl_market.au_spot_price (
            settlement_date    DATE NOT NULL,
            settlement_period  INT  NOT NULL,
            region             TEXT NOT NULL DEFAULT 'NEM',
            spot_price         NUMERIC,
            PRIMARY KEY (settlement_date, settlement_period, region)
        )
    """,
    "intl_market.au_ancillary_results": """
        CREATE TABLE IF NOT EXISTS intl_market.au_ancillary_results (
            settlement_date  DATE NOT NULL,
            service          TEXT NOT NULL,
            region           TEXT NOT NULL DEFAULT 'NEM',
            clearing_price   NUMERIC,
            volume_mw        NUMERIC,
            PRIMARY KEY (settlement_date, service, region)
        )
    """,
    "intl_market.au_ingestion_log": """
        CREATE TABLE IF NOT EXISTS intl_market.au_ingestion_log (
            id               SERIAL PRIMARY KEY,
            run_at           TIMESTAMPTZ DEFAULT NOW(),
            trigger          TEXT NOT NULL,
            date_from        DATE,
            date_to          DATE,
            status           TEXT NOT NULL,
            rows_ingested    JSONB,
            error_msg        TEXT,
            duration_seconds NUMERIC
        )
    """,
}


def _ensure_tables(engine) -> None:
    with engine.begin() as conn:
        for table, ddl in _DDL.items():
            conn.execute(sql_text(ddl))


# ---------------------------------------------------------------------------
# Upsert helper (identical to gb_ingestion._upsert)
# ---------------------------------------------------------------------------

def _upsert(engine, table: str, df: pd.DataFrame, conflict_cols: list[str], batch_size: int = 2000):
    from psycopg2.extras import execute_values
    if df.empty:
        return 0
    df = df.where(df.notna(), other=None)
    df = df.drop_duplicates(subset=conflict_cols, keep="last")
    cols = list(df.columns)
    update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c not in conflict_cols)
    conflict = ", ".join(conflict_cols)
    col_list = ", ".join(cols)
    sql = (
        f"INSERT INTO {table} ({col_list}) VALUES %s "
        f"ON CONFLICT ({conflict}) DO UPDATE SET {update_set}"
    )
    rows = [tuple(row[c] for c in cols) for row in df.to_dict(orient="records")]
    for attempt in range(5):
        try:
            with engine.begin() as conn:
                raw = conn.connection
                with raw.cursor() as cur:
                    execute_values(cur, sql, rows, page_size=batch_size)
            return len(rows)
        except Exception:
            if attempt == 4:
                raise
            import time
            engine.dispose()
            time.sleep(30 * (attempt + 1))
    return 0


def _chunk_dates(start: date, end: date, days: int = 90):
    cur = start
    while cur <= end:
        yield cur, min(cur + timedelta(days=days - 1), end)
        cur += timedelta(days=days)


# ---------------------------------------------------------------------------
# Ingest functions (try AU endpoints, log 404 gracefully)
# ---------------------------------------------------------------------------

def _try_get(client: ModoClient, path: str, params: dict | None = None) -> list[dict] | None:
    """Try a Modo API endpoint; return None if 404/403 (endpoint not available)."""
    try:
        return client.get(path, params)
    except Exception as exc:
        if "404" in str(exc) or "403" in str(exc) or "400" in str(exc):
            print(f"  [au] endpoint {path} not available: {exc}")
            return None
        raise


def ingest_assets(engine, client: ModoClient) -> int:
    records = _try_get(client, "/au/modo/asset/database")
    if not records:
        return 0
    rows = []
    for rec in records:
        for ht in ("rated_power", "energy_capacity", "owner", "operator"):
            v = rec.get(ht)
            if v is not None:
                rows.append({
                    "asset": rec.get("asset_name", rec.get("asset", "")),
                    "history_table": ht,
                    "date_from": rec.get("date_from"),
                    "date_to": rec.get("date_to"),
                    "value": str(v),
                })
    if not rows:
        return 0
    df = pd.DataFrame(rows)
    return _upsert(engine, "intl_market.au_bess_assets", df, ["asset", "history_table", "date_from"])


def ingest_leaderboard(engine, client: ModoClient, start: date, end: date) -> int:
    total = 0
    for d_from, d_to in _chunk_dates(start, end):
        records = _try_get(client, "/au/modo/benchmarking/leaderboard-live",
                           {"date_from": d_from.isoformat(), "date_to": d_to.isoformat()})
        if not records:
            continue
        rows = []
        for rec in records:
            rows.append({
                "asset": rec.get("asset_name", rec.get("asset", "")),
                "settlement_date": rec.get("date", rec.get("settlement_date")),
                "market": rec.get("market", "total"),
                "revenue": rec.get("revenue", rec.get("revenue_gbp")),
                "rated_power": rec.get("rated_power_mw", rec.get("rated_power")),
                "energy_capacity": rec.get("energy_capacity_mwh", rec.get("energy_capacity")),
            })
        if rows:
            df = pd.DataFrame(rows)
            total += _upsert(engine, "intl_market.au_bess_leaderboard", df,
                             ["asset", "settlement_date", "market"])
    return total


def ingest_daily_index(engine, client: ModoClient, start: date, end: date) -> int:
    total = 0
    for d_from, d_to in _chunk_dates(start, end):
        records = _try_get(client, "/au/modo/benchmarking/daily-index-live",
                           {"date_from": d_from.isoformat(), "date_to": d_to.isoformat()})
        if not records:
            continue
        rows = []
        for rec in records:
            rows.append({
                "settlement_date": rec.get("date", rec.get("settlement_date")),
                "market": rec.get("market", "total"),
                "revenue_permw": rec.get("revenue_per_mw", rec.get("revenue_permw")),
                "revenue_permwh": rec.get("revenue_per_mwh", rec.get("revenue_permwh")),
                "duration": rec.get("duration"),
            })
        if rows:
            df = pd.DataFrame(rows)
            total += _upsert(engine, "intl_market.au_bess_daily_index", df,
                             ["settlement_date", "market"])
    return total


def ingest_spot_price(engine, client: ModoClient, start: date, end: date) -> int:
    total = 0
    for d_from, d_to in _chunk_dates(start, end):
        records = _try_get(client, "/au/modo/markets/spot-price-live",
                           {"date_from": d_from.isoformat(), "date_to": d_to.isoformat()})
        if not records:
            continue
        rows = []
        for rec in records:
            rows.append({
                "settlement_date": rec.get("date", rec.get("settlement_date")),
                "settlement_period": rec.get("settlement_period", rec.get("period", 1)),
                "region": rec.get("region", "NEM"),
                "spot_price": rec.get("spot_price", rec.get("price")),
            })
        if rows:
            df = pd.DataFrame(rows)
            total += _upsert(engine, "intl_market.au_spot_price", df,
                             ["settlement_date", "settlement_period", "region"])
    return total


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_ingestion(start: date, end: date, only: list[str] | None = None) -> dict[str, int]:
    engine = get_engine()
    _ensure_tables(engine)
    client = ModoClient()
    results: dict[str, int] = {}

    tasks = [
        ("assets",      lambda: ingest_assets(engine, client)),
        ("leaderboard", lambda: ingest_leaderboard(engine, client, start, end)),
        ("daily_index", lambda: ingest_daily_index(engine, client, start, end)),
        ("spot_price",  lambda: ingest_spot_price(engine, client, start, end)),
    ]
    for key, fn in tasks:
        if only and key not in only:
            continue
        print(f"  [au/{key}]…", end="", flush=True)
        try:
            n = fn()
            results[key] = n
            print(f" {n} rows")
        except Exception as exc:
            results[key] = 0
            print(f" ERROR: {exc}")

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default=(date.today() - timedelta(days=1)).isoformat())
    parser.add_argument("--end",   default=(date.today() - timedelta(days=1)).isoformat())
    parser.add_argument("--only")
    args = parser.parse_args()
    only = args.only.split(",") if args.only else None
    print(run_ingestion(
        date.fromisoformat(args.start),
        date.fromisoformat(args.end),
        only=only,
    ))
