# -*- coding: utf-8 -*-
"""
services/trading/bess/mengxi/run_compensation_refresh.py

Calculate and refresh monthly BESS compensation rates from extracted data.

Formula:
    compensation_yuan_per_mwh = compensation_yuan / discharge_mwh

Data sources:
    - staging.mengxi_compensation_extracted: compensation amounts from 储能容量补偿费用统计表
    - staging.mengxi_settlement_extracted: discharge MWh from 上网结算单

Output:
    - core.asset_monthly_compensation: upsert calculated rates

Author: Matrix Agent
Created: 2026-03-26
"""
from __future__ import annotations

import argparse
import logging
import os
from datetime import date
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

DB_DSN = os.getenv("DB_DSN") or os.getenv("PGURL")
DEFAULT_RATE = 350.0  # Fallback rate yuan/MWh


def ensure_tables(engine: Engine) -> None:
    """Ensure required tables exist."""
    ddl = """
    CREATE SCHEMA IF NOT EXISTS staging;
    CREATE SCHEMA IF NOT EXISTS core;
    
    CREATE TABLE IF NOT EXISTS core.asset_monthly_compensation (
        asset_code              text        NOT NULL,
        effective_month         date        NOT NULL,
        compensation_yuan_per_mwh numeric   NOT NULL,
        source_system           text,
        notes                   text,
        active_flag             boolean     NOT NULL DEFAULT TRUE,
        created_at              timestamptz NOT NULL DEFAULT now(),
        updated_at              timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (asset_code, effective_month)
    );
    """
    with engine.begin() as conn:
        for stmt in ddl.split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(text(stmt))


def load_compensation_extracted(engine: Engine) -> pd.DataFrame:
    """Load extracted compensation data."""
    sql = text("""
        SELECT 
            asset_code,
            settlement_month,
            compensation_yuan,
            source_filename,
            parse_confidence
        FROM staging.mengxi_compensation_extracted
        WHERE asset_code IS NOT NULL
          AND parse_confidence != 'pending'
    """)
    try:
        return pd.read_sql(sql, engine)
    except Exception as e:
        logger.warning("Could not load compensation data: %s", e)
        return pd.DataFrame()


def load_settlement_extracted(engine: Engine) -> pd.DataFrame:
    """Load extracted settlement (discharge MWh) data."""
    sql = text("""
        SELECT 
            asset_code,
            settlement_month,
            discharge_mwh,
            source_filename,
            parse_confidence
        FROM staging.mengxi_settlement_extracted
        WHERE asset_code IS NOT NULL
          AND discharge_mwh IS NOT NULL
          AND parse_confidence != 'pending'
    """)
    try:
        return pd.read_sql(sql, engine)
    except Exception as e:
        logger.warning("Could not load settlement data: %s", e)
        return pd.DataFrame()


def calculate_compensation_rates(
    compensation_df: pd.DataFrame,
    settlement_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Calculate compensation rates by joining compensation and settlement data.
    
    Returns DataFrame with columns:
        - asset_code
        - effective_month
        - compensation_yuan_per_mwh
        - source_system
        - notes
    """
    if compensation_df.empty:
        logger.warning("No compensation data available")
        return pd.DataFrame()
    
    if settlement_df.empty:
        logger.warning("No settlement (discharge MWh) data available")
        # Can still create records with NULL rate if we have compensation but no MWh
        return pd.DataFrame()
    
    # Join on asset_code and settlement_month
    merged = compensation_df.merge(
        settlement_df,
        on=["asset_code", "settlement_month"],
        how="inner",
        suffixes=("_comp", "_sett"),
    )
    
    if merged.empty:
        logger.warning("No matching records between compensation and settlement data")
        return pd.DataFrame()
    
    # Calculate rate
    merged["compensation_yuan_per_mwh"] = (
        merged["compensation_yuan"] / merged["discharge_mwh"]
    ).round(2)
    
    # Build output
    result = merged[["asset_code", "settlement_month"]].copy()
    result["effective_month"] = merged["settlement_month"]
    result["compensation_yuan_per_mwh"] = merged["compensation_yuan_per_mwh"]
    result["source_system"] = "extracted_settlement"
    result["notes"] = merged.apply(
        lambda r: f"comp={r['compensation_yuan']:.2f}, mwh={r['discharge_mwh']:.2f}",
        axis=1,
    )
    
    return result[["asset_code", "effective_month", "compensation_yuan_per_mwh", 
                   "source_system", "notes"]]


def upsert_compensation_rates(engine: Engine, rates_df: pd.DataFrame) -> int:
    """Upsert calculated rates into core.asset_monthly_compensation."""
    if rates_df.empty:
        return 0
    
    count = 0
    with engine.begin() as conn:
        for _, row in rates_df.iterrows():
            sql = text("""
                INSERT INTO core.asset_monthly_compensation (
                    asset_code, effective_month, compensation_yuan_per_mwh,
                    source_system, notes, active_flag
                ) VALUES (
                    :asset_code, :effective_month, :compensation_yuan_per_mwh,
                    :source_system, :notes, TRUE
                )
                ON CONFLICT (asset_code, effective_month) DO UPDATE SET
                    compensation_yuan_per_mwh = EXCLUDED.compensation_yuan_per_mwh,
                    source_system = EXCLUDED.source_system,
                    notes = EXCLUDED.notes,
                    updated_at = now()
            """)
            conn.execute(sql, {
                "asset_code": row["asset_code"],
                "effective_month": row["effective_month"],
                "compensation_yuan_per_mwh": row["compensation_yuan_per_mwh"],
                "source_system": row["source_system"],
                "notes": row["notes"],
            })
            count += 1
    
    return count


def main():
    parser = argparse.ArgumentParser(description="Refresh Mengxi compensation rates")
    parser.add_argument("--dry-run", action="store_true", help="Calculate but don't upsert")
    args = parser.parse_args()

    if not DB_DSN:
        raise RuntimeError("DB_DSN or PGURL environment variable required")

    engine = create_engine(DB_DSN, pool_pre_ping=True)
    ensure_tables(engine)

    # Load extracted data
    comp_df = load_compensation_extracted(engine)
    sett_df = load_settlement_extracted(engine)

    logger.info("Loaded %d compensation records, %d settlement records",
                len(comp_df), len(sett_df))

    # Calculate rates
    rates_df = calculate_compensation_rates(comp_df, sett_df)
    
    if rates_df.empty:
        logger.warning("No rates calculated - check data availability")
        print("No rates calculated. Ensure both compensation and settlement data are available.")
        return

    logger.info("Calculated %d compensation rates", len(rates_df))
    print(rates_df.to_string(index=False))

    if args.dry_run:
        print("\n[DRY RUN] No changes made to database")
        return

    # Upsert to DB
    count = upsert_compensation_rates(engine, rates_df)
    logger.info("Upserted %d compensation rates", count)
    print(f"\nUpserted {count} compensation rates to core.asset_monthly_compensation")


if __name__ == "__main__":
    main()
