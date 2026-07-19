"""services/deal_engine/price_data.py — Fetch historical hourly prices from DB."""
from __future__ import annotations
from typing import Optional
import pandas as pd
from sqlalchemy import text
from services.common.db_utils import get_engine

import calendar


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
    sql = text(f"""
        SELECT datetime, {price_col} AS price
        FROM marketdata.spot_prices_hourly
        WHERE province = :province
          AND datetime >= :start_date
          AND datetime <  :end_date
        ORDER BY datetime
    """)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"province": province, "start_date": start_date, "end_date": end_date})
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


def fetch_price_wind_correlation() -> pd.DataFrame:
    """
    Compute Pearson correlation between monthly avg spot price and monthly
    wind capacity factor, per province.

    Data sources:
      - staging.exchange_excel_metrics: wind_generation_gwh, wind_capacity_mw
      - marketdata.spot_prices_hourly: da_price (monthly average)

    Returns DataFrame with columns:
      province, n_months, correlation, interpretation
    Provinces with < 6 overlapping months are excluded.
    """
    engine = get_engine()

    with engine.connect() as conn:
        # Monthly wind capacity factor
        wind_sql = text("""
            SELECT
                province,
                DATE_TRUNC('month', report_month)::date AS month,
                wind_generation_gwh,
                wind_capacity_mw,
                EXTRACT(YEAR FROM report_month)  AS yr,
                EXTRACT(MONTH FROM report_month) AS mo
            FROM staging.exchange_excel_metrics
            WHERE wind_generation_gwh IS NOT NULL
              AND wind_capacity_mw    IS NOT NULL
              AND wind_capacity_mw    > 0
        """)
        wind_df = pd.read_sql(wind_sql, conn)

        if wind_df.empty:
            return pd.DataFrame(columns=["province", "n_months", "correlation", "interpretation"])

        # Monthly average DA price per province
        price_sql = text("""
            SELECT
                province,
                DATE_TRUNC('month', datetime)::date AS month,
                AVG(da_price) AS avg_price
            FROM marketdata.spot_prices_hourly
            WHERE da_price IS NOT NULL
            GROUP BY province, DATE_TRUNC('month', datetime)
        """)
        price_df = pd.read_sql(price_sql, conn)

    if wind_df.empty:
        return pd.DataFrame(columns=["province", "n_months", "correlation", "interpretation"])

    # Hours in each calendar month
    def _hours(row):
        return calendar.monthrange(int(row["yr"]), int(row["mo"]))[1] * 24

    wind_df["hours"] = wind_df.apply(_hours, axis=1)
    wind_df["cf"] = wind_df["wind_generation_gwh"] * 1000.0 / (wind_df["wind_capacity_mw"] * wind_df["hours"])
    wind_df["cf"] = wind_df["cf"].clip(0.0, 1.0)

    if price_df.empty:
        return pd.DataFrame(columns=["province", "n_months", "correlation", "interpretation"])

    # Convert yuan/kWh -> yuan/MWh if needed
    if price_df["avg_price"].median() < 5.0:
        price_df["avg_price"] = price_df["avg_price"] * 1000.0

    # Join on province + month
    merged = wind_df[["province", "month", "cf"]].merge(
        price_df[["province", "month", "avg_price"]],
        on=["province", "month"],
        how="inner",
    )

    rows = []
    for province, grp in merged.groupby("province"):
        if len(grp) < 6:
            continue
        corr = grp["cf"].corr(grp["avg_price"])
        if pd.isna(corr):
            continue
        if corr < -0.3:
            interp = "Strong negative (cannibalization)"
        elif corr < -0.1:
            interp = "Mild negative"
        elif corr < 0.1:
            interp = "Uncorrelated"
        elif corr < 0.3:
            interp = "Mild positive"
        else:
            interp = "Strong positive"
        rows.append({"province": province, "n_months": len(grp), "correlation": round(corr, 3), "interpretation": interp})

    result = pd.DataFrame(rows).sort_values("correlation")
    return result
