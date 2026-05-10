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

import datetime
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
# RT-only forecast models (no DA price required)
# For markets without a day-ahead auction (e.g. Inner Mongolia Mengxi grid)
# ---------------------------------------------------------------------------

def _lag_rt_forecast(
    hourly: pd.DataFrame,
    lag_days: int,
) -> pd.Series:
    """
    Naive lag forecast: RT prediction for day D hour H = RT price of (D - lag_days) hour H.

    Falls back to the rolling 7-day mean for that hour when the lag day is absent.
    """
    df = hourly[["rt_price"]].copy()
    df["hour"] = df.index.hour
    df["date"] = df.index.date
    dates = sorted(df["date"].unique())

    preds: List[pd.DataFrame] = []
    for d in dates:
        day_df = df[df["date"] == d]
        lag_day = d - datetime.timedelta(days=lag_days)
        lag_df = df[df["date"] == lag_day]

        if not lag_df.empty:
            hour_map = lag_df.set_index(lag_df.index.hour)["rt_price"].to_dict()
            pred_vals = day_df.index.map(lambda ts: hour_map.get(ts.hour, float("nan")))
        else:
            # Fallback: rolling mean for each hour over all available history before d
            hist = df[df["date"] < d]
            if hist.empty:
                pred_vals = [float("nan")] * len(day_df)
            else:
                mean_by_hour = hist.groupby("hour")["rt_price"].mean().to_dict()
                pred_vals = day_df.index.map(lambda ts: mean_by_hour.get(ts.hour, float("nan")))

        preds.append(pd.Series(pred_vals, index=day_df.index, name="rt_pred").to_frame())

    if not preds:
        return pd.Series(dtype=float, name="rt_pred")
    out = pd.concat(preds).sort_index()
    return out["rt_pred"].astype(float)


def forecast_naive_rt_lag1(hourly: pd.DataFrame) -> pd.Series:
    """RT prediction = same hour yesterday."""
    return _lag_rt_forecast(hourly, lag_days=1)


def forecast_naive_rt_lag7(hourly: pd.DataFrame) -> pd.Series:
    """RT prediction = same hour 7 days ago (captures weekly seasonality)."""
    return _lag_rt_forecast(hourly, lag_days=7)


def forecast_ols_rt_time_v1(
    hourly: pd.DataFrame,
    min_train_days: int = 7,
    lookback_days: int = 60,
) -> pd.Series:
    """
    Rolling OLS forecast using only historical RT prices and time-of-day features.

    Features: [1, sin(2πh/24), cos(2πh/24), lag7_mean_rt]
      where lag7_mean_rt is the rolling 7-day average RT for the same hour.

    Falls back to naive_rt_lag1 when fewer than min_train_days of history are available.
    """
    if hourly.empty:
        return pd.Series(dtype=float, name="rt_pred")

    df = hourly[["rt_price"]].copy()
    df["hour"] = df.index.hour
    df["date"] = df.index.date

    # Precompute hourly 7-day rolling mean for each (date, hour) as an extra feature
    hourly_pivot = (
        df.dropna(subset=["rt_price"])
        .groupby(["date", "hour"])["rt_price"].mean().reset_index()
    )
    hourly_pivot["date"] = pd.to_datetime(hourly_pivot["date"])
    # For each (date, hour), rolling mean of rt_price over the previous 7 calendar days
    lag7_records = {}
    for _, grp in hourly_pivot.groupby("hour"):
        grp = grp.sort_values("date")
        grp["lag7_mean"] = grp["rt_price"].shift(1).rolling(7, min_periods=1).mean()
        for _, row in grp.iterrows():
            lag7_records[(row["date"].date(), int(row["hour"]))] = row["lag7_mean"]

    dates = pd.Index(pd.to_datetime(df["date"]).unique()).sort_values()
    preds: List[pd.DataFrame] = []

    for d in dates:
        day = d.date()
        day_df = df[df["date"] == day]

        train_end = pd.Timestamp(day)
        train_start = train_end - pd.Timedelta(days=lookback_days)
        train_df = df.loc[
            (df.index < train_end) & (df.index >= train_start)
        ].dropna(subset=["rt_price"])

        if train_df["date"].nunique() < min_train_days:
            # Fall back to lag-1
            pred = _lag_rt_forecast(hourly, lag_days=1)
            preds.append(pred[pred.index.date == day].to_frame())
            continue

        h_train = train_df["hour"].to_numpy(dtype=float)
        y_train = train_df["rt_price"].to_numpy(dtype=float)
        lag7_train = np.array([
            lag7_records.get((r["date"], int(r["hour"])), np.nan)
            for _, r in train_df.iterrows()
        ])
        # Drop rows where lag7 is nan
        valid = ~np.isnan(lag7_train)
        if valid.sum() < min_train_days * 4:
            pred = _lag_rt_forecast(hourly, lag_days=1)
            preds.append(pred[pred.index.date == day].to_frame())
            continue

        X_train = np.column_stack([
            np.ones(valid.sum()),
            np.sin(2 * np.pi * h_train[valid] / 24.0),
            np.cos(2 * np.pi * h_train[valid] / 24.0),
            lag7_train[valid],
        ])
        beta, *_ = np.linalg.lstsq(X_train, y_train[valid], rcond=None)

        h_pred = day_df["hour"].to_numpy(dtype=float)
        lag7_pred = np.array([
            lag7_records.get((day, int(h)), np.nan) for h in h_pred
        ])
        lag7_pred = np.where(np.isnan(lag7_pred), np.nanmean(lag7_train[valid]), lag7_pred)
        X_pred = np.column_stack([
            np.ones(len(h_pred)),
            np.sin(2 * np.pi * h_pred / 24.0),
            np.cos(2 * np.pi * h_pred / 24.0),
            lag7_pred,
        ])
        yhat = X_pred @ beta
        preds.append(pd.Series(yhat, index=day_df.index, name="rt_pred").to_frame())

    if not preds:
        return pd.Series(dtype=float, name="rt_pred")
    out = pd.concat(preds).sort_index()
    out = out[~out.index.duplicated(keep="last")]
    return out["rt_pred"].astype(float)


def forecast_naive_rt_ar17(
    hourly: pd.DataFrame,
    min_train_days: int = 8,
    lookback_days: int = 60,
) -> pd.Series:
    """
    Rolling OLS forecast combining lag-1 and lag-7 same-hour RT prices with time-of-day.

    Features: [1, sin(2πh/24), cos(2πh/24), lag1_rt_h, lag7_rt_h]
      where lag1_rt_h = RT price same hour yesterday
            lag7_rt_h = RT price same hour 7 days ago

    Falls back to naive_rt_lag1 when fewer than min_train_days of history available
    or when both lag values are unavailable for a prediction day.

    Parameters
    ----------
    hourly         : pd.DataFrame with DatetimeIndex and 'rt_price' column
    min_train_days : minimum days of history required before using OLS (default 8, needs lag-7)
    lookback_days  : rolling training window width in days
    """
    if hourly.empty:
        return pd.Series(dtype=float, name="rt_pred")

    df = hourly[["rt_price"]].copy()
    df["hour"] = df.index.hour
    df["date"] = df.index.date

    # Build (date, hour) -> rt_price lookup for fast lag access
    ph_lookup: dict = {}
    for ts, row in df.iterrows():
        if not pd.isna(row["rt_price"]):
            ph_lookup[(ts.date(), ts.hour)] = float(row["rt_price"])

    # Precompute per-hour global mean for fallback when lag values are missing
    hour_means = df.dropna(subset=["rt_price"]).groupby("hour")["rt_price"].mean().to_dict()

    dates = sorted(df["date"].unique())
    preds: List[pd.DataFrame] = []

    for d in dates:
        day_df = df[df["date"] == d]
        d_lag1 = d - datetime.timedelta(days=1)
        d_lag7 = d - datetime.timedelta(days=7)

        train_end = pd.Timestamp(d)
        train_start = train_end - pd.Timedelta(days=lookback_days)
        train_df = df.loc[
            (df.index < train_end) & (df.index >= train_start)
        ].dropna(subset=["rt_price"])

        if train_df["date"].nunique() < min_train_days:
            pred = _lag_rt_forecast(hourly, lag_days=1)
            day_pred = pred[pred.index.date == d]
            if not day_pred.empty:
                preds.append(day_pred.to_frame())
            continue

        # Build training matrix with lag-1 and lag-7 features per row
        X_rows: list = []
        y_rows: list = []
        for ts, row in train_df.iterrows():
            tdate, thour = ts.date(), ts.hour
            l1 = ph_lookup.get((tdate - datetime.timedelta(days=1), thour), np.nan)
            l7 = ph_lookup.get((tdate - datetime.timedelta(days=7), thour), np.nan)
            if np.isnan(l1) or np.isnan(l7):
                continue
            h = float(thour)
            X_rows.append([1.0, np.sin(2 * np.pi * h / 24.0), np.cos(2 * np.pi * h / 24.0), l1, l7])
            y_rows.append(float(row["rt_price"]))

        if len(y_rows) < min_train_days * 4:
            pred = _lag_rt_forecast(hourly, lag_days=1)
            day_pred = pred[pred.index.date == d]
            if not day_pred.empty:
                preds.append(day_pred.to_frame())
            continue

        beta, *_ = np.linalg.lstsq(np.array(X_rows), np.array(y_rows), rcond=None)

        # Predict for each hour of the day
        X_pred = []
        for ts in day_df.index:
            h = float(ts.hour)
            l1 = ph_lookup.get((d_lag1, ts.hour), hour_means.get(ts.hour, 0.0))
            l7 = ph_lookup.get((d_lag7, ts.hour), hour_means.get(ts.hour, 0.0))
            X_pred.append([1.0, np.sin(2 * np.pi * h / 24.0), np.cos(2 * np.pi * h / 24.0), l1, l7])

        yhat = np.array(X_pred) @ beta
        preds.append(pd.Series(yhat, index=day_df.index, name="rt_pred").to_frame())

    if not preds:
        return pd.Series(dtype=float, name="rt_pred")
    out = pd.concat(preds).sort_index()
    out = out[~out.index.duplicated(keep="last")]
    return out["rt_pred"].astype(float)


def forecast_ols_fundamentals_v1(
    hourly: pd.DataFrame,
    min_train_days: int = 7,
    lookback_days: int = 60,
) -> pd.Series:
    """
    Rolling OLS forecast using historical RT prices, time-of-day, and D-1 fundamentals.

    Features: [1, sin(2πh/24), cos(2πh/24), lag7_mean_rt, bidding_space_d1_mw, load_d1_mw, renewable_d1_mw]

    The caller is responsible for joining the fundamentals columns into `hourly` before
    calling this function (via a LEFT JOIN on spot_fundamentals_hourly).  If the columns
    are absent or the day has no fundamentals rows, falls back to naive_rt_lag1 for that day.

    Parameters
    ----------
    hourly         : pd.DataFrame with DatetimeIndex and columns:
                       rt_price  (required)
                       bidding_space_d1_mw, load_d1_mw, renewable_d1_mw  (optional — from spot_fundamentals_hourly)
    min_train_days : minimum complete training days before using OLS (falls back to naive_rt_lag1)
    lookback_days  : rolling training window width in days
    """
    _FUND_COLS = ["bidding_space_d1_mw", "load_d1_mw", "renewable_d1_mw"]
    has_fund = all(c in hourly.columns for c in _FUND_COLS)

    if hourly.empty:
        return pd.Series(dtype=float, name="rt_pred")

    df = hourly[["rt_price"]].copy()
    if has_fund:
        for c in _FUND_COLS:
            df[c] = hourly[c].values
    df["hour"] = df.index.hour
    df["date"] = df.index.date

    # Precompute per-(date, hour) 7-day rolling mean of rt_price
    hourly_pivot = (
        df.dropna(subset=["rt_price"])
        .groupby(["date", "hour"])["rt_price"].mean().reset_index()
    )
    hourly_pivot["date"] = pd.to_datetime(hourly_pivot["date"])
    lag7_records: dict = {}
    for _, grp in hourly_pivot.groupby("hour"):
        grp = grp.sort_values("date")
        grp["lag7_mean"] = grp["rt_price"].shift(1).rolling(7, min_periods=1).mean()
        for _, row in grp.iterrows():
            lag7_records[(row["date"].date(), int(row["hour"]))] = row["lag7_mean"]

    dates = pd.Index(pd.to_datetime(df["date"]).unique()).sort_values()
    preds: List[pd.DataFrame] = []

    for d in dates:
        day = d.date()
        day_df = df[df["date"] == day]

        train_end = pd.Timestamp(day)
        train_start = train_end - pd.Timedelta(days=lookback_days)
        train_df = df.loc[
            (df.index < train_end) & (df.index >= train_start)
        ].dropna(subset=["rt_price"])

        day_has_fund = has_fund and day_df[_FUND_COLS].notna().any(axis=None)

        if train_df["date"].nunique() < min_train_days or not day_has_fund:
            pred = _lag_rt_forecast(hourly, lag_days=1)
            day_pred = pred[pred.index.date == day]
            if not day_pred.empty:
                preds.append(day_pred.to_frame())
            continue

        h_train = train_df["hour"].to_numpy(dtype=float)
        y_train = train_df["rt_price"].to_numpy(dtype=float)
        lag7_train = np.array([
            lag7_records.get((r["date"], int(r["hour"])), np.nan)
            for _, r in train_df.iterrows()
        ])
        fund_train = train_df[_FUND_COLS].fillna(method="ffill").fillna(method="bfill").to_numpy(dtype=float)

        valid = ~np.isnan(lag7_train) & ~np.any(np.isnan(fund_train), axis=1)
        if valid.sum() < min_train_days * 4:
            pred = _lag_rt_forecast(hourly, lag_days=1)
            day_pred = pred[pred.index.date == day]
            if not day_pred.empty:
                preds.append(day_pred.to_frame())
            continue

        X_train = np.column_stack([
            np.ones(valid.sum()),
            np.sin(2 * np.pi * h_train[valid] / 24.0),
            np.cos(2 * np.pi * h_train[valid] / 24.0),
            lag7_train[valid],
            fund_train[valid],
        ])
        beta, *_ = np.linalg.lstsq(X_train, y_train[valid], rcond=None)

        h_pred = day_df["hour"].to_numpy(dtype=float)
        lag7_pred = np.array([
            lag7_records.get((day, int(h)), np.nan) for h in h_pred
        ])
        lag7_pred = np.where(np.isnan(lag7_pred), np.nanmean(lag7_train[valid]), lag7_pred)

        fund_pred = day_df[_FUND_COLS].fillna(method="ffill").fillna(method="bfill").to_numpy(dtype=float)
        for col_i in range(fund_pred.shape[1]):
            col_mean = np.nanmean(fund_train[valid, col_i]) if valid.sum() > 0 else 0.0
            fund_pred[:, col_i] = np.where(np.isnan(fund_pred[:, col_i]), col_mean, fund_pred[:, col_i])

        X_pred = np.column_stack([
            np.ones(len(h_pred)),
            np.sin(2 * np.pi * h_pred / 24.0),
            np.cos(2 * np.pi * h_pred / 24.0),
            lag7_pred,
            fund_pred,
        ])
        yhat = X_pred @ beta
        preds.append(pd.Series(yhat, index=day_df.index, name="rt_pred").to_frame())

    if not preds:
        return pd.Series(dtype=float, name="rt_pred")
    out = pd.concat(preds).sort_index()
    out = out[~out.index.duplicated(keep="last")]
    return out["rt_pred"].astype(float)


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

SUPPORTED_MODELS = (
    # DA-based (legacy — require da_price column)
    "naive_da",
    "ols_da_time_v1",
    # RT-only (no DA prices required — default for Inner Mongolia Mengxi)
    "naive_rt_lag1",
    "naive_rt_lag7",
    "naive_rt_ar17",       # OLS combining lag-1 and lag-7 same-hour prices
    "ols_rt_time_v1",
    # RT + D-1 fundamentals (bidding_space_d1_mw, load_d1_mw, renewable_d1_mw)
    "ols_fundamentals_v1",
)

RT_ONLY_MODELS = frozenset([
    "naive_rt_lag1", "naive_rt_lag7", "naive_rt_ar17",
    "ols_rt_time_v1", "ols_fundamentals_v1",
])


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
    hourly         : pd.DataFrame with DatetimeIndex and 'rt_price' column.
                     RT-only models require only 'rt_price'.
                     DA-based models also require 'da_price'.
                     ols_fundamentals_v1 also requires D-1 fundamentals columns joined in by caller.
    model          : model name — one of ``SUPPORTED_MODELS``
    min_train_days : passed to OLS-based models
    lookback_days  : passed to OLS-based models

    Returns
    -------
    pd.Series named 'rt_pred'
    """
    model = model.lower().strip()
    if model == "naive_da":
        return forecast_naive_da(hourly)
    if model == "ols_da_time_v1":
        return forecast_ols_da_time_v1(hourly, min_train_days=min_train_days, lookback_days=lookback_days)
    if model == "naive_rt_lag1":
        return forecast_naive_rt_lag1(hourly)
    if model == "naive_rt_lag7":
        return forecast_naive_rt_lag7(hourly)
    if model == "naive_rt_ar17":
        return forecast_naive_rt_ar17(hourly, min_train_days=min_train_days, lookback_days=lookback_days)
    if model == "ols_rt_time_v1":
        return forecast_ols_rt_time_v1(hourly, min_train_days=min_train_days, lookback_days=lookback_days)
    if model == "ols_fundamentals_v1":
        return forecast_ols_fundamentals_v1(hourly, min_train_days=min_train_days, lookback_days=lookback_days)
    raise ValueError(
        f"Unknown model: {model!r}. Supported: {SUPPORTED_MODELS}"
    )
