# -*- coding: utf-8 -*-
"""
Created on Tue Mar 24 12:47:32 2026

@author: dipeng.chen
"""

# services/trading/bess/mengxi/run_pnl_refresh.py
from __future__ import annotations

import os
import sys

# Ensure repo root is on sys.path when run as a script
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, "..", "..", "..", ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

# Set DB_DSN from PGURL if only the latter is provided
_url = os.environ.get("PGURL") or os.environ.get("DB_DSN")
if _url:
    os.environ.setdefault("DB_DSN", _url)
    os.environ.setdefault("PGURL", _url)

import logging
from datetime import date, datetime, timedelta
from typing import Dict

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from apps.trading.bess.mengxi.pnl_attribution.calc import (
    ASSET_ALIAS_MAP,
    SCENARIOS,
    DEFAULT_COMPENSATION_YUAN_PER_MWH,
    build_daily_attribution_row,
    build_daily_scenario_rows,
)

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

DB_URL = os.getenv("DB_DSN") or os.getenv("PGURL")
if not DB_URL:
    raise ValueError("Missing DB_DSN / PGURL")

ENGINE = create_engine(DB_URL, pool_pre_ping=True, pool_recycle=300)
DEFAULT_COMPENSATION_YUAN_PER_MWH = float(
    os.getenv("DEFAULT_COMPENSATION_YUAN_PER_MWH", "350"))
REFRESH_LOOKBACK_DAYS = int(os.getenv("PNL_REFRESH_LOOKBACK_DAYS", "7"))


def ensure_report_tables(engine: Engine) -> None:
    ddl = """
    CREATE SCHEMA IF NOT EXISTS reports;
    CREATE SCHEMA IF NOT EXISTS core;

    CREATE TABLE IF NOT EXISTS reports.bess_asset_daily_scenario_pnl (
        trade_date date NOT NULL,
        asset_code text NOT NULL,
        scenario_name text NOT NULL,
        scenario_available boolean NOT NULL,
        compensation_yuan_per_mwh numeric,
        market_revenue_yuan numeric,
        subsidy_revenue_yuan numeric,
        total_revenue_yuan numeric,
        discharge_mwh numeric,
        charge_mwh numeric,
        avg_daily_cycles numeric,
        created_at timestamptz NOT NULL DEFAULT now(),
        updated_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (trade_date, asset_code, scenario_name)
    );
    CREATE TABLE IF NOT EXISTS core.asset_monthly_compensation (
        asset_code text NOT NULL,
        effective_month date NOT NULL,
        compensation_yuan_per_mwh numeric NOT NULL,
        source_system text,
        notes text,
        active_flag boolean NOT NULL DEFAULT TRUE,
        created_at timestamptz NOT NULL DEFAULT now(),
        updated_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (asset_code, effective_month)
    );

    CREATE TABLE IF NOT EXISTS reports.bess_asset_daily_attribution (
        trade_date date NOT NULL,
        asset_code text NOT NULL,
        pf_unrestricted_pnl numeric,
        pf_grid_feasible_pnl numeric,
        cleared_actual_pnl numeric,
        nominated_pnl numeric,
        tt_forecast_optimal_pnl numeric,
        tt_strategy_pnl numeric,
        grid_restriction_loss numeric,
        forecast_error_loss numeric,
        strategy_error_loss numeric,
        nomination_loss numeric,
        execution_clearing_loss numeric,
        realisation_gap_vs_pf numeric,
        realisation_gap_vs_pf_grid numeric,
        created_at timestamptz NOT NULL DEFAULT now(),
        updated_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (trade_date, asset_code)
    );
    """
    with engine.begin() as con:
        for stmt in ddl.split(";"):
            sql = stmt.strip()
            if sql:
                con.execute(text(sql))

def fetch_asset_monthly_compensation(engine: Engine) -> pd.DataFrame:
    sql = text("""
        SELECT
            asset_code,
            effective_month,
            compensation_yuan_per_mwh,
            source_system,
            notes
        FROM core.asset_monthly_compensation
        WHERE active_flag = TRUE
    """)
    try:
        return pd.read_sql(sql, engine)
    
    except Exception:
        return pd.DataFrame(
            columns=[
                "asset_code",
                "effective_month",
                "compensation_yuan_per_mwh",
                "source_system",
                "notes",
            ]
        )
def fetch_scenario_availability(engine: Engine) -> pd.DataFrame:
    sql = text("""
        SELECT asset_code, scenario_name, available_flag
        FROM core.asset_scenario_availability
        WHERE active_flag = TRUE
    """)
    try:
        return pd.read_sql(sql, engine)
    except Exception:
        rows = []
        from apps.trading.bess.mengxi.pnl_attribution.calc import scenario_availability_df
        return scenario_availability_df()


def fetch_trade_dates(engine: Engine, lookback_days: int) -> list[date]:
    end_dt = date.today()
    start_dt = end_dt - timedelta(days=lookback_days)
    return [start_dt + timedelta(days=i) for i in range((end_dt - start_dt).days + 1)]


def load_actual_price(engine: Engine, asset_code: str, trade_date: date) -> pd.DataFrame:
    """
    v1 assumption:
    - canonical view exists and resolves whichever source is authoritative.
    """
    sql = text("""
        SELECT time, price
        FROM canon.nodal_rt_price_15min
        WHERE asset_code = :asset_code
          AND time >= :start_ts
          AND time < :end_ts
        ORDER BY time
    """)
    start_ts = pd.Timestamp(trade_date)
    end_ts = start_ts + pd.Timedelta(days=1)
    return pd.read_sql(sql, engine, params={"asset_code": asset_code, "start_ts": start_ts, "end_ts": end_ts})


def load_dispatch_scenario(engine: Engine, asset_code: str, scenario_name: str, trade_date: date) -> pd.DataFrame:
    sql = text("""
        SELECT time, dispatch_mw
        FROM canon.scenario_dispatch_15min
        WHERE asset_code = :asset_code
          AND scenario_name = :scenario_name
          AND time >= :start_ts
          AND time < :end_ts
        ORDER BY time
    """)
    start_ts = pd.Timestamp(trade_date)
    end_ts = start_ts + pd.Timedelta(days=1)
    return pd.read_sql(
        sql,
        engine,
        params={
            "asset_code": asset_code,
            "scenario_name": scenario_name,
            "start_ts": start_ts,
            "end_ts": end_ts,
        },
    )


def build_availability_map(df: pd.DataFrame, asset_code: str) -> Dict[str, bool]:
    out = {s: False for s in SCENARIOS}
    hit = df[df["asset_code"] == asset_code]
    for _, row in hit.iterrows():
        out[str(row["scenario_name"])] = bool(row["available_flag"])
    return out


def upsert_df(engine: Engine, table_name: str, df: pd.DataFrame, pk_cols: list[str]) -> None:
    if df.empty:
        return

    stage_name = f"_tmp_{table_name.replace('.', '_')}_{int(pd.Timestamp.now('UTC').timestamp())}"
    schema_name, bare_name = table_name.split(".", 1)

    with engine.begin() as con:
        df.to_sql(stage_name, con=con, schema=schema_name, if_exists="replace", index=False)

        cols = list(df.columns)
        insert_cols = ", ".join(cols)
        select_cols = ", ".join(cols)
        update_cols = ", ".join([f"{c}=EXCLUDED.{c}" for c in cols if c not in pk_cols])

        con.execute(
            text(f"""
                INSERT INTO {table_name} ({insert_cols})
                SELECT {select_cols}
                FROM {schema_name}."{stage_name}"
                ON CONFLICT ({", ".join(pk_cols)})
                DO UPDATE SET
                    {update_cols},
                    updated_at = now()
            """)
        )
        con.execute(text(f'DROP TABLE IF EXISTS {schema_name}."{stage_name}"'))


def main() -> None:
    import argparse
    from datetime import date as _date

    p = argparse.ArgumentParser(description="Mengxi BESS P&L refresh")
    p.add_argument("--start-date", default=None, help="ISO date (default: today - LOOKBACK_DAYS)")
    p.add_argument("--end-date",   default=None, help="ISO date (default: today)")
    args = p.parse_args()

    if args.start_date or args.end_date:
        today = _date.today()
        start = _date.fromisoformat(args.start_date) if args.start_date else today - timedelta(days=REFRESH_LOOKBACK_DAYS)
        end   = _date.fromisoformat(args.end_date)   if args.end_date   else today
        dates = [start + timedelta(days=i) for i in range((end - start).days + 1)]
        logger.info("Date range from args: %s → %s (%d days)", start, end, len(dates))
    else:
        dates = fetch_trade_dates(ENGINE, REFRESH_LOOKBACK_DAYS)

    logger.info("Starting Mengxi P&L refresh")
    ensure_report_tables(ENGINE)

    availability_df = fetch_scenario_availability(ENGINE)
    compensation_df = fetch_asset_monthly_compensation(ENGINE)
    scenario_rows_all = []
    attribution_rows_all = []

    for trade_date in dates:
        for asset_code in ASSET_ALIAS_MAP.keys():
            availability_map = build_availability_map(availability_df, asset_code)
            if not any(availability_map.values()):
                continue

            actual_price_df = load_actual_price(ENGINE, asset_code, trade_date)
            if actual_price_df.empty:
                logger.warning("No actual price found for asset=%s trade_date=%s", asset_code, trade_date)
                continue

            scenario_dispatch_map = {}
            for scenario_name, available_flag in availability_map.items():
                if not available_flag:
                    continue
                scenario_dispatch_map[scenario_name] = load_dispatch_scenario(
                    ENGINE, asset_code, scenario_name, trade_date
                )

            scenario_rows = build_daily_scenario_rows(
                trade_date=pd.Timestamp(trade_date),
                asset_code=asset_code,
                actual_price_df=actual_price_df,
                scenario_dispatch_map=scenario_dispatch_map,
                availability_map=availability_map,
                compensation_df=compensation_df,
                default_compensation_yuan_per_mwh=DEFAULT_COMPENSATION_YUAN_PER_MWH,
            )
            attribution_row = build_daily_attribution_row(scenario_rows)

            scenario_rows_all.append(scenario_rows)
            attribution_rows_all.append(attribution_row)

    if scenario_rows_all:
        scenario_df = pd.concat(scenario_rows_all, ignore_index=True)
        upsert_df(
            ENGINE,
            "reports.bess_asset_daily_scenario_pnl",
            scenario_df,
            pk_cols=["trade_date", "asset_code", "scenario_name"],
        )

    if attribution_rows_all:
        attribution_df = pd.concat(attribution_rows_all, ignore_index=True)
        upsert_df(
            ENGINE,
            "reports.bess_asset_daily_attribution",
            attribution_df,
            pk_cols=["trade_date", "asset_code"],
        )

    logger.info("Mengxi P&L refresh completed")


if __name__ == "__main__":
    main()