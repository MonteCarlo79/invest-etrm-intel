"""Adapters: trade datasets -> traded_points input frame. Pure: no DB, no UI."""
from __future__ import annotations

import pandas as pd

from libs.positions.conventions import canonical_province, tou_of_hour

TRADE_COLUMNS = ["province", "delivery_month", "tou_block", "price_cny_mwh", "volume_mwh"]
_CHANNELS = ("annual", "monthly_auction", "monthly_listed", "intramonth_match")


def _empty() -> pd.DataFrame:
    return pd.DataFrame(columns=TRADE_COLUMNS)


def trades_from_position_volumes(pv: pd.DataFrame, province: str) -> pd.DataFrame:
    """One row per (month, block, channel). Every hour counts toward `base`;
    peak/offpeak hours additionally count toward their own block."""
    prov = canonical_province(province)
    if pv is None or pv.empty:
        return _empty()
    d = pv.dropna(subset=["delivery_date", "hour"])
    months = pd.to_datetime(d["delivery_date"]).dt.to_period("M").dt.to_timestamp().dt.date
    blocks = d["hour"].astype(int).map(tou_of_hour)
    frames = []
    for ch in _CHANNELS:
        p, v = f"{ch}_price_cny_mwh", f"{ch}_volume_mwh"
        if p not in d.columns or v not in d.columns:
            continue
        base = pd.DataFrame({"delivery_month": months, "block": blocks,
                             "price_cny_mwh": d[p], "volume_mwh": d[v]})
        base = base.dropna(subset=["price_cny_mwh", "volume_mwh"])
        base = base[base["volume_mwh"] > 0]
        if base.empty:
            continue
        own = base[base["block"] != "flat"]
        frames.append(base.assign(tou_block="base"))
        frames.append(own.assign(tou_block=own["block"]))
    if not frames:
        return _empty()
    df = pd.concat(frames, ignore_index=True)
    df["_pv"] = df["price_cny_mwh"] * df["volume_mwh"]
    g = df.groupby(["delivery_month", "tou_block"], as_index=False)[["_pv", "volume_mwh"]].sum()
    g["price_cny_mwh"] = g["_pv"] / g["volume_mwh"]
    g["province"] = prov
    return g[TRADE_COLUMNS]


def trades_from_wind_trades(wt: pd.DataFrame, province: str = "蒙西") -> pd.DataFrame:
    if wt is None or wt.empty:
        return _empty()
    d = wt.dropna(subset=["delivery_month", "energy_price", "volume_mwh"])
    d = d[(d["energy_kind"] == "正常") & (d["channel"] == "省内") & (d["volume_mwh"] > 0)]
    if d.empty:
        return _empty()
    return pd.DataFrame({"province": canonical_province(province),
                         "delivery_month": d["delivery_month"].values,
                         "tou_block": "base",
                         "price_cny_mwh": d["energy_price"].values,
                         "volume_mwh": d["volume_mwh"].values})[TRADE_COLUMNS]
