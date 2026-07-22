"""Process manually uploaded forward curve CSV files.

Expected CSV columns: delivery_date, province, product, price_cny_mwh
Optional: delivery_hour (if hourly granularity)
"""
from __future__ import annotations

import pandas as pd
from shared.agents.db import get_conn


def validate_curve_csv(df: pd.DataFrame) -> list[str]:
    """Validate uploaded curve CSV. Returns list of error messages (empty = valid)."""
    errors = []
    required = {"delivery_date", "province", "product", "price_cny_mwh"}
    missing = required - set(df.columns)
    if missing:
        errors.append(f"Missing columns: {missing}")
    if df.empty:
        errors.append("File is empty")
    if "price_cny_mwh" in df.columns and (df["price_cny_mwh"] <= 0).any():
        errors.append("price_cny_mwh must be positive")
    return errors


def upload_manual_curve(df: pd.DataFrame, curve_date: str | None = None) -> int:
    """Write validated curve DataFrame to rm_forward_curves.

    Args:
        df: DataFrame with columns: delivery_date, province, product, price_cny_mwh,
            optional: delivery_hour
        curve_date: Date the curve was generated (defaults to today)

    Returns:
        Number of rows written.
    """
    if curve_date is None:
        curve_date = pd.Timestamp.now().strftime("%Y-%m-%d")

    rows_written = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for _, row in df.iterrows():
                hour = row.get("delivery_hour") if "delivery_hour" in df.columns else None
                cur.execute("""
                    INSERT INTO marketdata.rm_forward_curves
                        (province, product, curve_date, delivery_date, delivery_hour, price_cny_kwh, source)
                    VALUES (%s, %s, %s, %s, %s, %s, 'manual')
                    ON CONFLICT (province, product, curve_date, delivery_date, delivery_hour, source)
                    DO UPDATE SET price_cny_kwh = EXCLUDED.price_cny_kwh, uploaded_at = NOW()
                """, (
                    row["province"], row["product"], curve_date,
                    row["delivery_date"], hour,
                    float(row["price_cny_mwh"]) / 1000.0,
                ))
                rows_written += 1
        conn.commit()

    return rows_written
