"""Forward reference curve frame and sources. Pure: no DB, no UI."""
from __future__ import annotations

from datetime import date

import pandas as pd

from libs.positions.conventions import OFFPEAK_HOURS, PEAK_HOURS, TOU_BLOCKS

CURVE_COLUMNS = ["province", "delivery_month", "tou_block", "price_cny_mwh", "source"]
_PRIORITY = {"manual": 0, "traded": 1, "spot_seasonal": 2}
_BLOCK_HOURS = {"base": None, "peak": PEAK_HOURS, "offpeak": OFFPEAK_HOURS}


def _empty() -> pd.DataFrame:
    return pd.DataFrame(columns=CURVE_COLUMNS)


def spot_seasonal(hourly: pd.DataFrame, province: str, months: list[date],
                  lookback_years: int = 2) -> pd.DataFrame:
    """Mean hourly price per block over the same calendar month of the prior
    `lookback_years` years (strictly before the target year), history permitting."""
    dt = pd.to_datetime(hourly["datetime"])
    df = pd.DataFrame({"year": dt.dt.year, "month": dt.dt.month,
                       "hour": dt.dt.hour, "price": hourly["price"]}).dropna(subset=["price"])
    rows = []
    for m in months:
        win = df[(df.month == m.month) & (df.year < m.year)
                 & (df.year >= m.year - lookback_years)]
        if win.empty:
            continue
        for b in TOU_BLOCKS:
            hrs = _BLOCK_HOURS[b]
            sel = win if hrs is None else win[win.hour.isin(hrs)]
            if sel.empty:
                continue
            rows.append([province, m, b, float(sel.price.mean()), "spot_seasonal"])
    return pd.DataFrame(rows, columns=CURVE_COLUMNS) if rows else _empty()


def traded_points(trades: pd.DataFrame) -> pd.DataFrame:
    t = trades.dropna(subset=["volume_mwh", "price_cny_mwh"])
    t = t[t["volume_mwh"] != 0].copy()
    if t.empty:
        return _empty()
    t["_pv"] = t["price_cny_mwh"] * t["volume_mwh"]
    g = t.groupby(["province", "delivery_month", "tou_block"], as_index=False)[
        ["_pv", "volume_mwh"]].sum()
    g["price_cny_mwh"] = g["_pv"] / g["volume_mwh"]
    g["source"] = "traded"
    return g[CURVE_COLUMNS].reset_index(drop=True)


def reference_curve(*sources: pd.DataFrame) -> pd.DataFrame:
    frames = [s[CURVE_COLUMNS] for s in sources if s is not None and not s.empty]
    if not frames:
        return _empty()
    df = pd.concat(frames, ignore_index=True)
    df["_p"] = df["source"].map(_PRIORITY).fillna(len(_PRIORITY))
    df = df.sort_values("_p", kind="stable").drop_duplicates(
        ["province", "delivery_month", "tou_block"], keep="first")
    return df.drop(columns="_p").sort_values(
        ["province", "delivery_month", "tou_block"]).reset_index(drop=True)


def curve_lookup(curve: pd.DataFrame, province: str, month: date,
                 tou_block: str = "base") -> float | None:
    if curve is None or curve.empty:
        return None
    sub = curve[(curve.province == province) & (curve.delivery_month == month)]
    for b in (tou_block, "base"):
        hit = sub[sub.tou_block == b]
        if not hit.empty:
            return float(hit.price_cny_mwh.iloc[0])
    return None
