"""Hybrid price forecast = structural level (merit-order) + statistical shape (PCA)."""
from __future__ import annotations

import numpy as np
import pandas as pd


def combine(level, shape, clip_lo, clip_hi):
    level = np.asarray(level, dtype=float)
    shape = np.asarray(shape, dtype=float)
    return np.clip(level + shape, clip_lo, clip_hi)


def clip_bounds(prices: pd.Series) -> tuple[float, float]:
    s = pd.to_numeric(prices, errors="coerce").dropna()
    if s.empty:
        return (0.0, 1500.0)
    return (float(s.quantile(0.001)), float(s.quantile(0.999)))
