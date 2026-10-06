# -*- coding: utf-8 -*-
"""
Quantile forecast engine — probabilistic bands on top of point forecasts.

Keeps any existing point-forecast model (default ``ols_rt_time_v1``) as the
backbone and adds empirical residual quantiles per hour-of-day, estimated on a
rolling window of out-of-sample residuals (the base model is already
walk-forward, so residuals are honest out-of-sample errors).

    band_q(t) = point_forecast(t) + Q_q(residuals | hour-of-day(t), past W days)

Stored per (province, datetime, model) in
``marketdata.spot_prices_hourly_rt_forecast_quantile``.
"""
from __future__ import annotations

import logging
from typing import Optional

import numpy as np
import pandas as pd
from sqlalchemy import text as sql_text
from sqlalchemy.engine import Engine

from .forecast_engine import build_forecast

logger = logging.getLogger(__name__)

QUANTILES = (0.10, 0.25, 0.50, 0.75, 0.90)
Q_COLS = ["q10", "q25", "q50", "q75", "q90"]
QUANTILE_MODEL = "ols_rt_time_q_v1"

_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.spot_prices_hourly_rt_forecast_quantile (
    province  TEXT NOT NULL,
    datetime  TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    model     TEXT NOT NULL,
    q10 DOUBLE PRECISION, q25 DOUBLE PRECISION, q50 DOUBLE PRECISION,
    q75 DOUBLE PRECISION, q90 DOUBLE PRECISION,
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
    PRIMARY KEY (province, datetime, model)
);
"""


def ensure_table(engine: Engine, schema: str = "marketdata") -> None:
    with engine.begin() as conn:
        conn.execute(sql_text(_DDL.format(schema=schema)))


# ── Pure logic (testable without DB) ─────────────────────────────────────────

def compute_bands(hourly: pd.DataFrame, model: str = "ols_rt_time_v1",
                  resid_window_days: int = 30,
                  min_train_days: int = 7,
                  lookback_days: int = 60,
                  min_resid_samples: int = 5) -> pd.DataFrame:
    """Return a DataFrame indexed like the point forecast with q10..q90 columns.

    hourly: DataFrame with DatetimeIndex and 'rt_price' column.
    Band = point forecast + per-hour-of-day residual quantile over the past
    `resid_window_days` calendar days. Falls back to global (hour-agnostic)
    residual quantiles when an hour has < min_resid_samples.
    """
    pred = build_forecast(hourly, model, min_train_days=min_train_days,
                          lookback_days=lookback_days)
    if pred.empty:
        return pd.DataFrame(columns=["rt_pred"] + Q_COLS)

    df = hourly[["rt_price"]].copy()
    df["rt_pred"] = pred
    df = df.dropna(subset=["rt_pred"])
    df["resid"] = df["rt_price"] - df["rt_pred"]
    df["hour"] = df.index.hour
    df["date"] = df.index.date

    global_q = {q: float(np.nanquantile(df["resid"].to_numpy(), q))
                for q in QUANTILES}

    # Per (date, hour): residual quantiles over the previous resid_window_days
    band_q: dict = {}
    for hour, grp in df.groupby("hour"):
        grp = grp.sort_values("datetime" if "datetime" in grp.columns else "date")
        daily = grp.groupby("date")["resid"].mean()
        dates = list(daily.index)
        for i, d in enumerate(dates):
            lo = pd.Timestamp(d) - pd.Timedelta(days=resid_window_days)
            hist = grp[(grp["date"] < d) & (grp["date"] >= lo.date())]["resid"]
            if len(hist) < min_resid_samples:
                band_q[(d, hour)] = global_q
            else:
                band_q[(d, hour)] = {
                    q: float(np.nanquantile(hist.to_numpy(), q))
                    for q in QUANTILES
                }

    out = df[["rt_pred"]].copy()
    for q, col in zip(QUANTILES, Q_COLS):
        offsets = [band_q[(r.date, r.hour)][q] for r in df.itertuples()]
        out[col] = df["rt_pred"].to_numpy() + np.array(offsets, dtype=float)
    return out


def empirical_coverage(bands: pd.DataFrame, actual: pd.Series) -> dict:
    """Share of actual prices inside [q25,q75] and [q10,q90]."""
    joined = bands.join(actual.rename("rt_price"), how="inner").dropna(
        subset=["rt_price"])
    if joined.empty:
        return {"n": 0, "cov_25_75": None, "cov_10_90": None}
    inside_mid = ((joined["rt_price"] >= joined["q25"])
                  & (joined["rt_price"] <= joined["q75"])).mean()
    inside_wide = ((joined["rt_price"] >= joined["q10"])
                   & (joined["rt_price"] <= joined["q90"])).mean()
    return {"n": int(len(joined)), "cov_25_75": float(inside_mid),
            "cov_10_90": float(inside_wide)}


# ── DB layer ─────────────────────────────────────────────────────────────────

def upsert_bands(engine: Engine, schema: str, province: str, model: str,
                 bands: pd.DataFrame) -> int:
    rows = [
        (province, idx.to_pydatetime(), model,
         float(r.q10), float(r.q25), float(r.q50), float(r.q75), float(r.q90))
        for idx, r in bands.iterrows()
    ]
    if not rows:
        return 0
    with engine.begin() as conn:
        conn.execute(sql_text(f"""
            INSERT INTO {schema}.spot_prices_hourly_rt_forecast_quantile
                (province, datetime, model, q10, q25, q50, q75, q90)
            VALUES (:p, :dt, :m, :q10, :q25, :q50, :q75, :q90)
            ON CONFLICT (province, datetime, model) DO UPDATE SET
                q10 = EXCLUDED.q10, q25 = EXCLUDED.q25, q50 = EXCLUDED.q50,
                q75 = EXCLUDED.q75, q90 = EXCLUDED.q90, updated_at = NOW()
        """), [
            {"p": p, "dt": dt, "m": m, "q10": q10, "q25": q25, "q50": q50,
             "q75": q75, "q90": q90}
            for p, dt, m, q10, q25, q50, q75, q90 in rows
        ])
    return len(rows)


def run_for_province(engine: Engine, schema: str, province: str,
                     base_model: str = "ols_rt_time_v1",
                     resid_window_days: int = 30,
                     lookback_days: int = 60) -> dict:
    """Compute + store quantile bands for one province. Returns coverage stats."""
    from .run_capture_pipeline import fetch_hourly_prices  # reuse fetcher

    ensure_table(engine, schema)
    hourly = fetch_hourly_prices(engine, schema, province)
    if hourly.empty:
        return {"province": province, "rows": 0, "note": "no hourly prices"}

    bands = compute_bands(hourly, model=base_model,
                          resid_window_days=resid_window_days,
                          lookback_days=lookback_days)
    n = upsert_bands(engine, schema, province, QUANTILE_MODEL, bands)

    # Coverage on the most recent resid_window_days of the banded series
    if not bands.empty:
        recent_end = bands.index.max()
        recent_start = recent_end - pd.Timedelta(days=resid_window_days)
        recent = bands[bands.index >= recent_start]
        cov = empirical_coverage(recent, hourly["rt_price"])
    else:
        cov = {"n": 0, "cov_25_75": None, "cov_10_90": None}
    cov.update({"province": province, "rows": n})
    logger.info("quantile bands %s: %d rows, coverage %s", province, n, cov)
    return cov
