"""
services/bess_map/forecast_engine.py

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PROVINCE-LEVEL HOURLY RT PRICE FORECAST ENGINE — SOURCE OF TRUTH
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Pure computation module — no DB, no I/O.
Extracted from services/bess_map/run_capture_pipeline.py.

Available models
----------------
naive_da          : RT prediction = DA price (identity mapping).
                    Requires only da_price column. No training.

ols_da_time_v1    : OLS regression with features [1, da_price, sin(2πh/24), cos(2πh/24)].
                    Rolling lookback: fit on prior N days each day.
                    Falls back to naive_da if insufficient training data.

Usage
-----
Direct (pipeline scripts):

    from services.bess_map.forecast_engine import build_forecast
    import pandas as pd

    hourly = pd.DataFrame(...)  # DatetimeIndex, columns: rt_price, da_price
    rt_pred = build_forecast(hourly, model="ols_da_time_v1", min_train_days=7, lookback_days=60)
    # rt_pred: pd.Series with same DatetimeIndex

Via shared decision model library (apps / agents):

    import libs.decision_models.price_forecast_dayahead
    from libs.decision_models.runners.local import run

    result = run("price_forecast_dayahead", {
        "hourly_prices": [...],   # list of {datetime, rt_price, da_price} dicts
        "target_date":  "2026-04-15",
        "model":        "ols_da_time_v1",
    })

Assumptions
-----------
- Province-level (NOT nodal/asset-level)
- Hourly granularity (24 intervals per day)
- Deterministic given the same inputs — no stochastic elements
- Rolling, in-sample OLS; no pretrained artifact on disk
- No confidence intervals
- No external market features beyond DA price and hour-of-day
"""
from __future__ import annotations

from typing import List, Optional

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Feature engineering
# ---------------------------------------------------------------------------

def _design_matrix(hours: np.ndarray, da: np.ndarray) -> np.ndarray:
    """
    Build OLS feature matrix: [intercept, da_price, sin(2πh/24), cos(2πh/24)].

    Parameters
    ----------
    hours : (N,) array of hour-of-day integers (0–23)
    da    : (N,) array of DA prices (Yuan/MWh)

    Returns
    -------
    (N, 4) float ndarray
    """
    h = hours.astype(float)
    return np.column_stack([
        np.ones_like(h),
        da.astype(float),
        np.sin(2 * np.pi * h / 24.0),
        np.cos(2 * np.pi * h / 24.0),
    ])


# ---------------------------------------------------------------------------
# Forecast models
# ---------------------------------------------------------------------------

def forecast_naive_da(hourly: pd.DataFrame) -> pd.Series:
    """
    Naive day-ahead forecast: rt_pred = da_price.

    Parameters
    ----------
    hourly : pd.DataFrame with DatetimeIndex and a 'da_price' column

    Returns
    -------
    pd.Series named 'rt_pred' with the same index
    """
    s = hourly["da_price"].copy()
    s.name = "rt_pred"
    return s


def forecast_ols_da_time_v1(
    hourly: pd.DataFrame,
    min_train_days: int = 7,
    lookback_days: int = 60,
) -> pd.Series:
    """
    Rolling OLS forecast of hourly RT price using DA price + time-of-day features.

    For each day D in `hourly`, trains OLS on the window [D - lookback_days, D)
    using features [1, da_price, sin(2πh/24), cos(2πh/24)], then predicts RT
    for day D. Falls back to naive_da when training history is shorter than
    min_train_days.

    Parameters
    ----------
    hourly         : pd.DataFrame with DatetimeIndex, columns 'rt_price' and 'da_price'
    min_train_days : minimum complete training days required before using OLS
    lookback_days  : rolling training window width in days

    Returns
    -------
    pd.Series named 'rt_pred' with the same DatetimeIndex as hourly
    """
    if hourly.empty:
        return pd.Series(dtype=float, name="rt_pred")

    df = hourly[["rt_price", "da_price"]].copy()
    df["hour"] = df.index.hour
    df["date"] = df.index.date

    dates = pd.Index(pd.to_datetime(df["date"]).unique()).sort_values()
    preds: List[pd.DataFrame] = []

    for d in dates:
        day = d.date()
        day_df = df.loc[df["date"] == day]
        if day_df.empty:
            continue

        train_end = pd.Timestamp(day)
        train_start = train_end - pd.Timedelta(days=lookback_days)
        train_df = df.loc[
            (df.index < train_end) & (df.index >= train_start)
        ].dropna(subset=["rt_price", "da_price"])

        if train_df["date"].nunique() < min_train_days:
            # Fall back: use naive DA
            pred = day_df["da_price"].copy()
            pred.name = "rt_pred"
            preds.append(pred.to_frame())
            continue

        X = _design_matrix(
            train_df["hour"].to_numpy(),
            train_df["da_price"].to_numpy(),
        )
        y = train_df["rt_price"].to_numpy(dtype=float)
        beta, *_ = np.linalg.lstsq(X, y, rcond=None)

        Xp = _design_matrix(
            day_df["hour"].to_numpy(),
            day_df["da_price"].to_numpy(),
        )
        yhat = Xp @ beta
        preds.append(pd.Series(yhat, index=day_df.index, name="rt_pred").to_frame())

    if not preds:
        return pd.Series(dtype=float, name="rt_pred")

    out = pd.concat(preds).sort_index()
    out = out[~out.index.duplicated(keep="last")]
    return out["rt_pred"].astype(float)


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

SUPPORTED_MODELS = ("naive_da", "ols_da_time_v1")


def build_forecast(
    hourly: pd.DataFrame,
    model: str,
    min_train_days: int = 7,
    lookback_days: int = 60,
) -> pd.Series:
    """
    Dispatch to the named forecast model.

    Parameters
    ----------
    hourly         : pd.DataFrame with DatetimeIndex, columns 'rt_price' and 'da_price'
    model          : model name — one of ``SUPPORTED_MODELS``
    min_train_days : passed to ols_da_time_v1
    lookback_days  : passed to ols_da_time_v1

    Returns
    -------
    pd.Series named 'rt_pred'
    """
    model = model.lower().strip()
    if model == "naive_da":
        return forecast_naive_da(hourly)
    if model == "ols_da_time_v1":
        return forecast_ols_da_time_v1(
            hourly,
            min_train_days=min_train_days,
            lookback_days=lookback_days,
        )
    raise ValueError(
        f"Unknown model: {model!r}. Supported: {SUPPORTED_MODELS}"
    )
