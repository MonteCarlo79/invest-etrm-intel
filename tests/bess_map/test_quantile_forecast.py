"""Tests for the quantile forecast engine (compute_bands / empirical_coverage)."""
import numpy as np
import pandas as pd
import pytest

from services.bess_map import quantile_forecast as qf


def _series(days=70, seed=7):
    idx = pd.date_range("2026-06-01", periods=days * 24, freq="h")
    rng = np.random.default_rng(seed)
    hour = idx.hour
    price = (300 + 80 * np.sin(2 * np.pi * (hour - 7) / 24)
             + 40 * np.sin(2 * np.pi * np.arange(len(idx)) / (24 * 7))
             + rng.normal(0, 25, len(idx)))
    return pd.DataFrame({"rt_price": price}, index=idx)


def test_band_ordering_and_columns():
    bands = qf.compute_bands(_series(), resid_window_days=30)
    assert not bands.empty
    for col in qf.Q_COLS:
        assert col in bands.columns
    arr = bands[qf.Q_COLS].to_numpy()
    assert (np.diff(arr, axis=1) >= -1e-9).all(), "quantiles must be non-decreasing"


def test_q50_near_point_forecast():
    bands = qf.compute_bands(_series(), resid_window_days=30)
    dev = (bands["q50"] - bands["rt_pred"]).abs()
    assert dev.max() < 200  # median residual offset stays bounded on smooth series


def test_empty_input_returns_empty():
    bands = qf.compute_bands(pd.DataFrame({"rt_price": []},
                                          index=pd.DatetimeIndex([])))
    assert bands.empty


def test_coverage_on_synthetic_noise():
    hourly = _series(days=100)
    bands = qf.compute_bands(hourly, resid_window_days=30)
    cov = qf.empirical_coverage(bands, hourly["rt_price"])
    assert cov["n"] > 1000
    # Wide band should cover materially more than the mid band
    assert cov["cov_10_90"] > cov["cov_25_75"]
    # Sanity bounds — not degenerate (not 0%, not 100%)
    assert 0.2 < cov["cov_25_75"] < 0.95
    assert 0.5 < cov["cov_10_90"] <= 1.0
