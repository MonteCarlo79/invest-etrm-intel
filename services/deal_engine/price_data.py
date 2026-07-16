"""services/deal_engine/price_data.py — Fetch historical hourly prices from DB."""
from __future__ import annotations
from typing import Optional
import pandas as pd
from services.common.db_utils import get_engine


def fetch_price_history(
    province: str,
    start_date: str,
    end_date: str,
    price_col: str = "da_price",   # "da_price" or "rt_price"
) -> list[float]:
    """
    Fetch hourly price series from marketdata.spot_prices_hourly.

    Returns flat list of yuan/MWh values ordered by datetime.
    Missing hours are forward-filled. Raises ValueError if fewer than 168 hours returned.

    price_col: "da_price" uses day-ahead clearing price (default).
               "rt_price" uses real-time clearing price.
    """
    engine = get_engine()
    sql = f"""
        SELECT datetime, {price_col} AS price
        FROM marketdata.spot_prices_hourly
        WHERE province = :province
          AND datetime >= :start_date
          AND datetime <  :end_date
        ORDER BY datetime
    """
    df = pd.read_sql(sql, engine, params={"province": province, "start_date": start_date, "end_date": end_date})
    if df.empty:
        raise ValueError(f"No price data for province={province!r} between {start_date} and {end_date}")
    if len(df) < 168:
        raise ValueError(f"Insufficient data: only {len(df)} hours returned (need >= 168)")

    # Forward-fill gaps
    df["price"] = df["price"].ffill().bfill()
    # Convert yuan/kWh -> yuan/MWh if values look like kWh scale (< 5)
    if df["price"].median() < 5.0:
        df["price"] = df["price"] * 1000.0

    return df["price"].tolist()
