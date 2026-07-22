"""Value at Risk computation.

Two methods:
- Historical simulation: reprice positions using historical price scenarios
- Parametric delta-normal: VaR = delta * sigma * z * sqrt(t)
"""
from __future__ import annotations

import numpy as np
import pandas as pd

Z_SCORES = {0.95: 1.6449, 0.99: 2.3263}


def historical_var(
    price_returns: pd.Series,
    delta_mwh: float,
    confidence: float = 0.95,
) -> float:
    """Compute VaR using historical simulation.

    Args:
        price_returns: Series of historical daily price changes (CNY/MWh)
        delta_mwh: Net MWh exposure (positive = long)
        confidence: Confidence level (0.95 or 0.99)

    Returns:
        VaR as a positive number representing potential loss (CNY).
    """
    scenarios = delta_mwh * price_returns.values
    percentile = (1 - confidence) * 100
    var_value = -np.percentile(scenarios, percentile)
    return float(max(var_value, 0.0))


def parametric_var(
    delta_mwh: float,
    sigma_price: float,
    confidence: float = 0.95,
    horizon_days: int = 1,
) -> float:
    """Compute VaR using parametric delta-normal method.

    Args:
        delta_mwh: Net MWh exposure (positive = long)
        sigma_price: Daily price volatility (CNY/MWh)
        confidence: Confidence level (0.95 or 0.99)
        horizon_days: VaR horizon in days

    Returns:
        VaR as a positive number representing potential loss (CNY).
    """
    z = Z_SCORES.get(confidence, 1.6449)
    var_value = abs(delta_mwh) * sigma_price * z * np.sqrt(horizon_days)
    return float(var_value)
