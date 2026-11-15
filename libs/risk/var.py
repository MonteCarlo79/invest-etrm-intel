"""Value at Risk computation.

Two methods:
- Historical simulation: reprice positions using historical price scenarios
- Parametric delta-normal: VaR = delta * sigma * z * sqrt(t)
"""
from __future__ import annotations

from statistics import NormalDist

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


def monthly_price_scenarios(
    hourly: pd.DataFrame,
    window_days: int = 30,
    lookback_days: int = 252,
) -> pd.Series:
    """Daily change of the trailing-`window_days` mean price (CNY/MWh).

    v1 proxy for forward-month moves. Input: hourly frame with `datetime`,
    `price`. Output: Series indexed by date, last `lookback_days` values.
    Units: CNY/MWh per day.
    """
    df = pd.DataFrame({"datetime": pd.to_datetime(hourly["datetime"]),
                       "price": pd.to_numeric(hourly["price"], errors="coerce")})
    df = df.dropna(subset=["price"])
    if df.empty:
        return pd.Series(dtype=float)
    daily = df.groupby(df["datetime"].dt.normalize())["price"].mean().sort_index()
    rolled = daily.rolling(window_days, min_periods=window_days).mean()
    return rolled.diff().dropna().iloc[-lookback_days:]


def portfolio_var(
    net_delta_mwh_by_province: dict[str, float],
    scenarios: dict[str, pd.Series],
    confidence: float = 0.95,
    horizon_days: int = 1,
) -> dict:
    """Multi-province VaR over aligned (inner-joined) daily scenario series.

    historical: −quantile(1−confidence) of Σ delta × Δprice, × sqrt(horizon), floored at 0.
    parametric: z × sqrt(dᵀΣd) × sqrt(horizon), Σ from the aligned series.
    Fewer than 20 aligned scenarios → both None. Zero-delta provinces are
    excluded before the join; non-zero-delta provinces without a scenario
    series are reported in `provinces_missing` / `delta_missing_mwh`.
    Units: deltas in MWh, scenarios in CNY/MWh, VaR in CNY.
    """
    deltas = net_delta_mwh_by_province
    missing = sorted(p for p, d in deltas.items() if d != 0 and p not in scenarios)
    extra = {"provinces_missing": missing,
             "delta_missing_mwh": {p: float(deltas[p]) for p in missing}}
    provs = [p for p, d in deltas.items() if d != 0 and p in scenarios]
    if not provs:
        return {"historical": None, "parametric": None, "n_scenarios": 0, **extra}
    aligned = pd.concat({p: scenarios[p] for p in provs}, axis=1, join="inner").dropna()
    n = int(len(aligned))
    if n < 20:
        return {"historical": None, "parametric": None, "n_scenarios": n, **extra}
    d = np.array([float(net_delta_mwh_by_province[p]) for p in provs])
    x = aligned[provs].to_numpy(dtype=float)
    sqrt_h = np.sqrt(horizon_days)

    pnl = x @ d
    hist = max(-float(np.quantile(pnl, 1 - confidence)) * sqrt_h, 0.0)

    cov = np.atleast_2d(np.cov(x, rowvar=False))
    z = NormalDist().inv_cdf(confidence)  # any confidence, not just 0.95 / 0.99 (M4)
    para = float(z * np.sqrt(max(float(d @ cov @ d), 0.0)) * sqrt_h)
    return {"historical": float(hist), "parametric": para, "n_scenarios": n, **extra}
