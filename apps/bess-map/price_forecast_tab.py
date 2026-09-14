"""Price Forecast tab — DB loaders (top) + Streamlit UI (bottom, Tasks 9-11).

Only this module touches the DB for the price-forecast feature. All loaders
take a SQLAlchemy engine; Streamlit caching is applied in the UI layer.
"""
from __future__ import annotations

import json
from datetime import date
import pandas as pd


def load_confirmed_fuel_fleet(eng, province: str):
    df = pd.read_sql(
        """SELECT coal_price_yuan_t, gas_price_yuan_m3, fleet_segments, effective_date
           FROM marketdata.province_fuel_fleet
           WHERE province = %(p)s AND status = 'confirmed'
           ORDER BY effective_date DESC, ingested_at DESC LIMIT 1""",
        eng, params={"p": province})
    if df.empty:
        return None
    r = df.iloc[0]
    segs = r["fleet_segments"]
    if isinstance(segs, str):
        segs = json.loads(segs)
    return {"coal_price_yuan_t": r["coal_price_yuan_t"],
            "gas_price_yuan_m3": r["gas_price_yuan_m3"],
            "fleet_segments": segs,
            "effective_date": r["effective_date"]}


def load_import_blocks(eng, recv_province: str) -> list[dict]:
    trades = pd.read_sql(
        """SELECT DISTINCT ON (channel_1) channel_1, land_price
           FROM staging.interconnector_trades
           WHERE recv_province = %(p)s AND channel_1 IS NOT NULL AND land_price IS NOT NULL
           ORDER BY channel_1, month_start DESC""",
        eng, params={"p": recv_province})
    if trades.empty:
        return []
    channels = pd.read_sql("SELECT name, gw FROM staging.interconnector_channels", eng)
    cap = dict(zip(channels["name"], channels["gw"]))
    blocks = []
    for _, r in trades.iterrows():
        gw = cap.get(r["channel_1"])
        blocks.append({"label": r["channel_1"],
                       "price_yuan_mwh": float(r["land_price"]),   # already ¥/MWh
                       "capacity_mw": float(gw) * 1000.0 if gw else 1000.0})
    return blocks


def load_fundamentals_d1(eng, province: str, start: date, end: date) -> pd.DataFrame:
    df = pd.read_sql(
        """SELECT datetime,
                  COALESCE(load_d1_mw, load_mw)                       AS load_d1_mw,
                  COALESCE(renewable_d1_mw, renewable_total_mw)       AS renewable_total_d1_mw,
                  COALESCE(bidding_space_d1_mw, bidding_space_mw)     AS bidding_space_d1_mw,
                  COALESCE(wind_d1_mw, wind_mw)                       AS wind_d1_mw,
                  COALESCE(solar_d1_mw, solar_mw)                     AS solar_d1_mw,
                  COALESCE(net_export_d1_mw, net_export_mw)           AS net_export_d1_mw
           FROM marketdata.spot_fundamentals_hourly
           WHERE province = %(p)s AND datetime BETWEEN %(s)s AND %(e)s
           ORDER BY datetime""",
        eng, params={"p": province, "s": start, "e": end})
    if df.empty:
        return df
    df["datetime"] = pd.to_datetime(df["datetime"])
    return df.set_index("datetime")


def load_rt_prices(eng, province: str, start: date, end: date) -> pd.DataFrame:
    df = pd.read_sql(
        """SELECT datetime, rt_price FROM marketdata.spot_prices_hourly
           WHERE province = %(p)s AND datetime BETWEEN %(s)s AND %(e)s
           ORDER BY datetime""",
        eng, params={"p": province, "s": start, "e": end})
    if df.empty:
        return df
    df["datetime"] = pd.to_datetime(df["datetime"])
    return df.set_index("datetime")


def compute_net_import_share(fund_df: pd.DataFrame) -> pd.Series:
    share = -fund_df["net_export_d1_mw"] / fund_df["load_d1_mw"]
    return share.clip(-1, 1).fillna(0.0)


def load_landing_price_latest(eng, recv_province: str):
    df = pd.read_sql(
        """SELECT SUM(vol_post_mwh * land_price) / NULLIF(SUM(vol_post_mwh), 0) AS wavg
           FROM staging.interconnector_trades
           WHERE recv_province = %(p)s AND land_price IS NOT NULL
             AND month_start = (SELECT MAX(month_start) FROM staging.interconnector_trades
                                WHERE recv_province = %(p)s)""",
        eng, params={"p": recv_province})
    if df.empty or df.iloc[0]["wavg"] is None:
        return None
    return float(df.iloc[0]["wavg"])   # land_price already stored in ¥/MWh
