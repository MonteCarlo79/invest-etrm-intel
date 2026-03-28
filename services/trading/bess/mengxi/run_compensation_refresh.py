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

    CREATE TABLE IF NOT EXISTS staging.mengxi_compensation_coverage_status (
        asset_code text NOT NULL,
        effective_month date NOT NULL,
        discharge_known boolean NOT NULL,
        compensation_known boolean NOT NULL,
        blocked_missing_compensation boolean NOT NULL,
        notes text,
        updated_at timestamptz NOT NULL DEFAULT now(),
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


def build_coverage_status(
    compensation_df: pd.DataFrame,
    settlement_df: pd.DataFrame,
) -> pd.DataFrame:
    """Build coverage status rows for discharge-known / compensation-missing visibility."""
    sett = pd.DataFrame(columns=["asset_code", "effective_month"]) if settlement_df.empty else settlement_df[
        ["asset_code", "settlement_month"]
    ].rename(columns={"settlement_month": "effective_month"}).drop_duplicates()
    comp = pd.DataFrame(columns=["asset_code", "effective_month"]) if compensation_df.empty else compensation_df[
        ["asset_code", "settlement_month"]
    ].rename(columns={"settlement_month": "effective_month"}).drop_duplicates()

    merged = sett.merge(comp, on=["asset_code", "effective_month"], how="outer", indicator=True)
    if merged.empty:
        return pd.DataFrame(
            columns=[
                "asset_code", "effective_month", "discharge_known", "compensation_known",
                "blocked_missing_compensation", "notes",
            ]
        )

    merged["discharge_known"] = merged["_merge"].isin(["left_only", "both"])
    merged["compensation_known"] = merged["_merge"].isin(["right_only", "both"])
    merged["blocked_missing_compensation"] = merged["discharge_known"] & (~merged["compensation_known"])
    merged["notes"] = merged["blocked_missing_compensation"].map(
        lambda x: "discharge extracted but compensation missing" if x else None
    )
    return merged[[
        "asset_code", "effective_month", "discharge_known", "compensation_known",
        "blocked_missing_compensation", "notes",
    ]]


def upsert_coverage_status(engine: Engine, coverage_df: pd.DataFrame) -> int:
    if coverage_df.empty:
        return 0
    count = 0
    with engine.begin() as conn:
        for _, row in coverage_df.iterrows():
            conn.execute(text("""
                INSERT INTO staging.mengxi_compensation_coverage_status (
                    asset_code, effective_month, discharge_known, compensation_known,
                    blocked_missing_compensation, notes, updated_at
                ) VALUES (
                    :asset_code, :effective_month, :discharge_known, :compensation_known,
                    :blocked_missing_compensation, :notes, now()
                )
                ON CONFLICT (asset_code, effective_month) DO UPDATE SET
                    discharge_known = EXCLUDED.discharge_known,
                    compensation_known = EXCLUDED.compensation_known,
                    blocked_missing_compensation = EXCLUDED.blocked_missing_compensation,
                    notes = EXCLUDED.notes,
                    updated_at = now()
            """), {
                "asset_code": row["asset_code"],
                "effective_month": row["effective_month"],
                "discharge_known": bool(row["discharge_known"]),
                "compensation_known": bool(row["compensation_known"]),
                "blocked_missing_compensation": bool(row["blocked_missing_compensation"]),
                "notes": row["notes"],
            })
            count += 1
    return count


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
    coverage_df = build_coverage_status(comp_df, sett_df)
    blocked_count = int(coverage_df["blocked_missing_compensation"].sum()) if not coverage_df.empty else 0
    logger.info("Coverage status rows=%d blocked_missing_compensation=%d", len(coverage_df), blocked_count)
    
    if rates_df.empty:
        logger.warning("No rates calculated - check data availability")
        print("No rates calculated. Ensure both compensation and settlement data are available.")
        return

    logger.info("Calculated %d compensation rates", len(rates_df))
    print(rates_df.to_string(index=False))

    if args.dry_run:
        if not coverage_df.empty:
            print("\nCoverage status snapshot:")
            print(coverage_df.sort_values(["asset_code", "effective_month"]).to_string(index=False))
        print("\n[DRY RUN] No changes made to database")
        return

    coverage_count = upsert_coverage_status(engine, coverage_df)
    logger.info("Upserted %d coverage status rows", coverage_count)

    # Upsert to DB
    count = upsert_compensation_rates(engine, rates_df)
    logger.info("Upserted %d compensation rates", count)
    print(f"\nUpserted {count} compensation rates to core.asset_monthly_compensation")


if __name__ == "__main__":
    main()
