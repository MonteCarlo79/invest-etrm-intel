# -*- coding: utf-8 -*-
"""
Revenue-weighted availability (Ardian Opta lesson) for IM BESS assets.

Flat delivery ratio treats all 15-min intervals equally; revenue-weighted
availability (RWA) weights shortfalls by the value of the interval in which
they occur:

    foregone_value(t) = shortfall_mwh(t) × value(t)
    value(t)          = price(t)            for discharge-nominated intervals (price > 0)
                      = −price(t)           for charge-nominated intervals (price < 0)
                      = 0                   otherwise (shortfall has no revenue cost)

    flat_delivery  = Σ min(|actual|, |nominated|) / Σ |nominated|
    RWA            = 1 − Σ foregone_value / Σ (|nominated| × value)

Sign convention in ops_bess_dispatch_15min: nominated/actual MW, positive =
discharge, negative = charge.
"""
from __future__ import annotations

import logging
from typing import Optional

import numpy as np
import pandas as pd
from sqlalchemy import text as sql_text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

CALLED_EPS_MW = 0.1     # |nominated| above this = asset is being dispatched
HOURS_PER_INTERVAL = 0.25


# ── Pure compute (testable without DB) ───────────────────────────────────────

def compute_intervals(df: pd.DataFrame) -> pd.DataFrame:
    """Annotate dispatch rows with shortfall, delivered, value, foregone.

    Input columns: nominated_dispatch_mw, actual_dispatch_mw, nodal_price_excel.
    Returns a copy with: called, shortfall_mw, delivered_mw, value_per_mwh,
    foregone_cny.
    """
    out = df.copy()
    nom = out["nominated_dispatch_mw"].astype(float).fillna(0.0)
    act = out["actual_dispatch_mw"].astype(float).fillna(0.0)
    price = out["nodal_price_excel"].astype(float).fillna(0.0)

    out["called"] = nom.abs() > CALLED_EPS_MW
    out["shortfall_mw"] = (nom.abs() - act.abs()).clip(lower=0.0)
    out["delivered_mw"] = np.minimum(act.abs(), nom.abs())
    # Only count delivery for called intervals; idle intervals carry no foregone.
    out.loc[~out["called"], ["shortfall_mw", "delivered_mw"]] = 0.0

    out["value_per_mwh"] = np.where(
        nom > 0, price.clip(lower=0.0), np.where(nom < 0, (-price).clip(lower=0.0), 0.0)
    )
    out["foregone_cny"] = out["shortfall_mw"] * HOURS_PER_INTERVAL * out["value_per_mwh"]
    return out


def summarize(df: pd.DataFrame, group_cols=("asset_code",)) -> pd.DataFrame:
    """Aggregate annotated intervals into per-group availability metrics."""
    if df.empty:
        return pd.DataFrame(columns=list(group_cols) + [
            "called_mwh", "delivered_mwh", "flat_delivery", "value_called_cny",
            "foregone_cny", "rwa", "intervals_called",
        ])
    w = df.copy()
    w["called_energy"] = w["nominated_dispatch_mw"].abs() * HOURS_PER_INTERVAL
    w["delivered_energy"] = w["delivered_mw"] * HOURS_PER_INTERVAL
    w["value_called_cny"] = (w["nominated_dispatch_mw"].abs()
                             * w["value_per_mwh"] * HOURS_PER_INTERVAL)
    g = w.groupby(list(group_cols), as_index=False).agg(
        called_mwh=("called_energy", "sum"),
        delivered_mwh=("delivered_energy", "sum"),
        foregone_cny=("foregone_cny", "sum"),
        value_called_cny=("value_called_cny", "sum"),
        intervals_called=("called", "sum"),
    )
    g["flat_delivery"] = g["delivered_mwh"] / g["called_mwh"].replace(0, np.nan)
    g["rwa"] = 1.0 - g["foregone_cny"] / g["value_called_cny"].replace(0, np.nan)
    return g


def worst_intervals(df: pd.DataFrame, top_n: int = 5) -> pd.DataFrame:
    """Top foregone-value intervals per asset."""
    cols = ["asset_code", "interval_start", "nominated_dispatch_mw",
            "actual_dispatch_mw", "nodal_price_excel", "foregone_cny"]
    if df.empty:
        return pd.DataFrame(columns=cols)
    return (df.sort_values("foregone_cny", ascending=False)
              .groupby("asset_code", as_index=False)
              .head(top_n)[cols]
              .reset_index(drop=True))


def om_window_table(df: pd.DataFrame) -> pd.DataFrame:
    """Hour-of-day maintenance-window economics per asset.

    window_score = avg |nominated| MW × avg value_per_mwh — the expected
    foregone revenue if the asset were taken offline for that hour. Lowest
    scores are the cheapest O&M windows.
    """
    if df.empty:
        return pd.DataFrame(columns=["asset_code", "hour", "avg_price", "avg_nominated_mw",
                                     "avg_value_per_mwh", "window_score", "n_intervals"])
    w = df.copy()
    w["hour"] = pd.to_datetime(w["interval_start"]).dt.hour
    g = w.groupby(["asset_code", "hour"], as_index=False).agg(
        avg_price=("nodal_price_excel", "mean"),
        avg_nominated_mw=("nominated_dispatch_mw", lambda s: float(s.abs().mean())),
        avg_value_per_mwh=("value_per_mwh", "mean"),
        n_intervals=("hour", "size"),
    )
    g["window_score"] = g["avg_nominated_mw"] * g["avg_value_per_mwh"]
    return g.sort_values(["asset_code", "window_score"]).reset_index(drop=True)


# ── DB layer ─────────────────────────────────────────────────────────────────

def fetch_dispatch(engine: Engine, asset_codes: list[str],
                   start=None, end=None) -> pd.DataFrame:
    sql = """
        SELECT asset_code, interval_start, nominated_dispatch_mw,
               actual_dispatch_mw, nodal_price_excel
        FROM marketdata.ops_bess_dispatch_15min
        WHERE asset_code = ANY(:codes)
    """
    params = {"codes": asset_codes}
    if start is not None:
        sql += " AND data_date >= :start"
        params["start"] = start
    if end is not None:
        sql += " AND data_date <= :end"
        params["end"] = end
    return pd.read_sql(sql_text(sql), engine, params=params)


def data_coverage(engine: Engine, asset_codes: list[str]) -> pd.DataFrame:
    return pd.read_sql(sql_text("""
        SELECT asset_code, COUNT(*) AS rows, MIN(data_date) AS first_date,
               MAX(data_date) AS last_date, COUNT(DISTINCT data_date) AS days
        FROM marketdata.ops_bess_dispatch_15min
        WHERE asset_code = ANY(:codes)
        GROUP BY asset_code
    """), engine, params={"codes": asset_codes})
