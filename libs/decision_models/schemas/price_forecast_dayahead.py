"""
libs/decision_models/schemas/price_forecast_dayahead.py

Input/output contracts for the price_forecast_dayahead model.

Scope
-----
Province-level hourly RT price forecast.
    - Granularity : hourly (24 intervals per day)
    - NOT nodal / asset-level
    - NOT 15-min

The model takes a window of historical hourly prices (RT + DA) and a target
date, and returns 24 hourly RT price predictions for that date.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class HourlyPriceRecord:
    """
    One hour of observed prices used as training / prediction input.

    datetime  : ISO8601 string, e.g. '2026-04-15T08:00:00'
    rt_price  : actual RT settlement price (Yuan/MWh). May be None/NaN for the
                target date (only DA price is available in advance).
    da_price  : day-ahead clearing price (Yuan/MWh). Required for all hours.
    """
    datetime: str
    da_price: float
    rt_price: Optional[float] = None


@dataclass
class PriceForecastInput:
    """
    Input for province-level day-ahead hourly RT price forecast.

    hourly_prices  : list of HourlyPriceRecord dicts covering at least the
                     target_date hours plus sufficient lookback history.
                     - Must include all 24 hours of target_date with da_price.
                     - For ols_da_time_v1: include at least min_train_days ×24
                       hours of prior history with both rt_price and da_price.
    target_date    : ISO date string (e.g. '2026-04-15'). Must be a date
                     present in hourly_prices.
    model          : forecast model to use:
                       'ols_da_time_v1' (default) — rolling OLS
                       'naive_da'                 — RT = DA identity
    min_train_days : minimum prior complete days required before OLS is used
                     (falls back to naive_da if fewer). Default: 7.
    lookback_days  : rolling training window width in days. Default: 60.
    """
    hourly_prices: List[dict]   # list of HourlyPriceRecord dicts
    target_date: str
    model: str = "ols_da_time_v1"
    min_train_days: int = 7
    lookback_days: int = 60


@dataclass
class PriceForecastOutput:
    """
    Output: 24 hourly RT price predictions for target_date.

    target_date  : the date that was forecast
    model        : model name used
    datetimes    : list of 24 ISO8601 timestamps (one per hour of target_date)
    rt_pred      : list of 24 predicted RT prices (Yuan/MWh)
    model_used   : 'ols' if OLS was fitted, 'naive_da' if fell back due to
                   insufficient training data
    """
    target_date: str
    model: str
    datetimes: List[str]    # 24 ISO strings
    rt_pred: List[float]    # 24 predictions
    model_used: str         # 'ols' or 'naive_da'
