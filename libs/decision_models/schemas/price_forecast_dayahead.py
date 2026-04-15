"""
libs/decision_models/schemas/price_forecast_dayahead.py

Input/output contracts for the price_forecast_dayahead model.
Placeholder — to be populated when the forecasting module is built.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import List, Optional


@dataclass
class PriceForecastInput:
    """
    Input for day-ahead nodal price forecast.

    asset_code:     stable internal asset code
    forecast_date:  the date to forecast prices for
    feature_window_days: how many historical days to use as features
    """
    asset_code: str
    forecast_date: date
    feature_window_days: int = 30


@dataclass
class PriceForecastOutput:
    """
    Output: 96-interval (15-min) day-ahead price forecast.

    forecast_prices: 96 price values in Yuan/MWh
    confidence_lower: optional lower bound
    confidence_upper: optional upper bound
    model_version_used: which trained model artefact was used
    """
    asset_code: str
    forecast_date: date
    forecast_prices: List[float]
    confidence_lower: Optional[List[float]] = None
    confidence_upper: Optional[List[float]] = None
    model_version_used: Optional[str] = None
