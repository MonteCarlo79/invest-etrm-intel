"""Tests for libs/risk/var.py"""
import pytest
import numpy as np
import pandas as pd
from libs.risk.var import historical_var, parametric_var


def test_historical_var_95():
    """Historical VaR at 95% = 5th percentile of P&L scenarios."""
    np.random.seed(42)
    price_history = pd.Series(np.random.normal(0, 10, 252))
    delta_mwh = 100.0
    result = historical_var(price_history, delta_mwh, confidence=0.95)
    assert result > 0
    expected = -delta_mwh * np.percentile(price_history, 5)
    assert result == pytest.approx(expected, rel=0.01)


def test_parametric_var_95():
    """Parametric VaR = delta * sigma * z * sqrt(t)."""
    delta_mwh = 100.0
    sigma = 15.0
    result = parametric_var(delta_mwh, sigma, confidence=0.95, horizon_days=1)
    expected = 100.0 * 15.0 * 1.6449
    assert result == pytest.approx(expected, rel=0.01)


def test_parametric_var_10day():
    """10-day VaR uses sqrt(10) scaling."""
    delta_mwh = 100.0
    sigma = 15.0
    result = parametric_var(delta_mwh, sigma, confidence=0.95, horizon_days=10)
    expected = 100.0 * 15.0 * 1.6449 * np.sqrt(10)
    assert result == pytest.approx(expected, rel=0.01)
