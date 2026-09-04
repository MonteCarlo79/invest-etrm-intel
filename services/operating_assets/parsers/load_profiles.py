"""Customer load profile ingestion.

Two formats:
- Format A (Shandong-style): daily .xls, rows=customers, columns=H1..H24
- Format B (Jiangsu-style): daily CSV, 96×15-min columns, aggregated to hourly

Both write to rm_customer_profiles (customer_id, profile_date, hour, load_mwh).
"""
from __future__ import annotations

import pandas as pd
import numpy as np
from typing import Any
from shared.agents.db import get_conn


def ingest_shandong_daily(file_path: str, profile_date: str, batch_id: str) -> dict:
    """Ingest Shandong-style daily .xls (rows=customers, cols=H1..H24).

    Args:
        file_path: Path to .xls file
        profile_date: Date string (YYYY-MM-DD)
        batch_id: Upload batch ID

    Returns:
        Dict with rows_written, errors, customers_matched, customers_missing
    """
    df = pd.read_excel(file_path)
    rows_written = 0
    errors = []
    customers_matched = 0
    customers_missing = []

    with get_conn() as conn:
        with conn.cursor() as cur:
            for _, row in df.iterrows():
                # First column is customer name/ID
                customer_name = str(row.iloc[0]).strip()
                if not customer_name or customer_name == "nan":
                    continue

                # Look up customer
                cur.execute(
                    "SELECT id FROM marketdata.rm_customers WHERE name = %s",
                    (customer_name,)
                )
                result = cur.fetchone()
                if not result:
                    customers_missing.append(customer_name)
                    continue

                customer_id = result[0]
                customers_matched += 1

                # Columns 1-24 are hours (H1=hour 0, H24=hour 23)
                for hour in range(24):
                    col_idx = hour + 1
                    if col_idx >= len(row):
                        break
                    load_val = row.iloc[col_idx]
                    if pd.isna(load_val):
                        continue
                    load_mwh = float(load_val) / 1000.0  # kWh → MWh if needed

                    cur.execute("""
                        INSERT INTO marketdata.rm_customer_profiles
                            (customer_id, profile_date, hour, load_mwh, upload_batch_id)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (customer_id, profile_date, hour) DO UPDATE SET
                            load_mwh = EXCLUDED.load_mwh,
                            upload_batch_id = EXCLUDED.upload_batch_id
                    """, (customer_id, profile_date, hour, load_mwh, batch_id))
                    rows_written += 1

        conn.commit()

    return {
        "rows_written": rows_written,
        "errors": errors,
        "customers_matched": customers_matched,
        "customers_missing": customers_missing,
    }


def ingest_jiangsu_csv(file_path: str, batch_id: str) -> dict:
    """Ingest Jiangsu-style CSV (96×15-min intervals, aggregated to hourly).

    File format: 日期, 户号, 用户名称, 售电公司名称, 00:15, 00:30, ..., 24:00
    Values in kWh. Aggregated: 4×15-min → 1 hour, sum.

    Args:
        file_path: Path to CSV file
        batch_id: Upload batch ID

    Returns:
        Dict with rows_written, errors, customers_matched, customers_missing
    """
    df = pd.read_csv(file_path, encoding="utf-8-sig")
    rows_written = 0
    errors = []
    customers_matched = 0
    customers_missing = []

    # Identify time columns (00:15, 00:30, ..., 24:00)
    time_cols = [c for c in df.columns if ":" in str(c)]
    if len(time_cols) != 96:
        errors.append(f"Expected 96 time columns, found {len(time_cols)}")
        return {"rows_written": 0, "errors": errors, "customers_matched": 0, "customers_missing": []}

    with get_conn() as conn:
        with conn.cursor() as cur:
            for _, row in df.iterrows():
                profile_date = str(row.get("日期", "")).strip()
                customer_name = str(row.get("用户名称", "")).strip()
                meter_id = str(row.get("户号", "")).strip()

                if not customer_name or customer_name == "nan":
                    continue

                # Look up customer by name or meter_id
                cur.execute(
                    "SELECT id FROM marketdata.rm_customers WHERE name = %s",
                    (customer_name,)
                )
                result = cur.fetchone()
                if not result:
                    customers_missing.append(customer_name)
                    continue

                customer_id = result[0]
                customers_matched += 1

                # Aggregate 4×15-min → hourly (sum kWh → MWh)
                for hour in range(24):
                    start_idx = hour * 4
                    interval_cols = time_cols[start_idx:start_idx + 4]
                    values = [float(row[c]) if pd.notna(row[c]) else 0.0 for c in interval_cols]
                    hourly_kwh = sum(values)
                    load_mwh = hourly_kwh / 1000.0  # kWh → MWh

                    if load_mwh == 0:
                        continue

                    cur.execute("""
                        INSERT INTO marketdata.rm_customer_profiles
                            (customer_id, profile_date, hour, load_mwh, upload_batch_id)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (customer_id, profile_date, hour) DO UPDATE SET
                            load_mwh = EXCLUDED.load_mwh,
                            upload_batch_id = EXCLUDED.upload_batch_id
                    """, (customer_id, profile_date, hour, load_mwh, batch_id))
                    rows_written += 1

        conn.commit()

    return {
        "rows_written": rows_written,
        "errors": errors,
        "customers_matched": customers_matched,
        "customers_missing": customers_missing,
    }
