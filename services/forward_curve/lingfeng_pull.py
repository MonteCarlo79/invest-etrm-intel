"""Pull near-term forward price forecasts from LingFeng pipeline.

Uses the existing services/lingfeng/collector.py to download data,
then writes parsed prices to rm_forward_curves.
"""
from __future__ import annotations

import os
import pandas as pd
from shared.agents.db import get_conn


def pull_lingfeng_curves(
    province: str,
    product: str = "spot",
    start_date: str | None = None,
    end_date: str | None = None,
) -> int:
    """Pull curves from LingFeng and write to rm_forward_curves.

    Args:
        province: Province code (e.g. 'inner_mongolia_mengxi')
        product: Product type (e.g. 'spot', 'da', 'rt')
        start_date: Start date (YYYY-MM-DD), defaults to today
        end_date: End date (YYYY-MM-DD), defaults to +30 days

    Returns:
        Number of rows inserted/updated.
    """
    from services.lingfeng.collector import collect

    if start_date is None:
        start_date = pd.Timestamp.now().strftime("%Y-%m-%d")
    if end_date is None:
        end_date = (pd.Timestamp.now() + pd.Timedelta(days=30)).strftime("%Y-%m-%d")

    username = os.environ.get("LINGFENG_USERNAME", "")
    password = os.environ.get("LINGFENG_PASSWORD", "")
    download_dir = os.environ.get("LINGFENG_DOWNLOAD_DIR", "/tmp/lingfeng")

    path = collect(username, password, "mengxi", "price_forecast", start_date, end_date, download_dir)

    df = pd.read_excel(path)
    curve_date = pd.Timestamp.now().date()
    rows_written = 0

    with get_conn() as conn:
        with conn.cursor() as cur:
            for _, row in df.iterrows():
                cur.execute("""
                    INSERT INTO marketdata.rm_forward_curves
                        (province, product, curve_date, delivery_date, delivery_hour, price_cny_kwh, source)
                    VALUES (%s, %s, %s, %s, %s, %s, 'lingfeng')
                    ON CONFLICT (province, product, curve_date, delivery_date, delivery_hour, source)
                    DO UPDATE SET price_cny_kwh = EXCLUDED.price_cny_kwh, uploaded_at = NOW()
                """, (
                    province, product, curve_date,
                    row.get("delivery_date"), row.get("hour"),
                    float(row.get("price", 0)) / 1000.0,
                ))
                rows_written += 1
        conn.commit()

    return rows_written
