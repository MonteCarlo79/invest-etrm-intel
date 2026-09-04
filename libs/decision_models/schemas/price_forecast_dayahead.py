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
                target date.
    da_price  : day-ahead clearing price (Yuan/MWh). Required only for DA-based
                models (ols_da_time_v1, naive_da). Optional for RT-only models
                (ols_rt_time_v1, naive_rt_lag1, naive_rt_lag7).
    """
    datetime: str
    rt_price: Optional[float] = None
    da_price: Optional[float] = None


@dataclass
class PriceForecastInput:
    """
    Input for province-level day-ahead hourly RT price forecast.

    hourly_prices  : list of HourlyPriceRecord dicts covering at least the
                     target_date hours plus sufficient lookback history.
                     RT-only models (ols_rt_time_v1, naive_rt_lag1, naive_rt_lag7):
                       - da_price is not required; only rt_price history needed.
                     DA-based models (ols_da_time_v1, naive_da):
                       - Must include all 24 hours of target_date with da_price.
                       - Include at least min_train_days ×24 hours of prior
                         history with both rt_price and da_price.
    target_date    : ISO date string (e.g. '2026-04-15'). Must be a date
                     present in hourly_prices.
    model          : forecast model to use:
                       'ols_rt_time_v1' (default) — rolling OLS on RT history + time-of-day
                       'naive_rt_lag1'             — yesterday same hour
                       'naive_rt_lag7'             — 7 days ago same hour
                       'ols_da_time_v1'            — rolling OLS with DA price (requires DA market)
                       'naive_da'                  — RT = DA identity (requires DA market)
    min_train_days : minimum prior complete days required before OLS is used. Default: 7.
    lookback_days  : rolling training window width in days. Default: 60.
    """
    hourly_prices: List[dict]   # list of HourlyPriceRecord dicts
    target_date: str
    model: str = "ols_rt_time_v1"
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
    model_used   : actual model applied — 'ols' (OLS fitted), 'naive_da',
                   'naive_rt_lag1', or 'naive_rt_lag7' (fallback for RT-only OLS)
    """
    target_date: str
    model: str
    datetimes: List[str]    # 24 ISO strings
    rt_pred: List[float]    # 24 predictions
    model_used: str         # 'ols' or 'naive_da'
