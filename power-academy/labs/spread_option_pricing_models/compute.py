"""Worked example for spread_option_pricing_models (anchor lab).

Margrabe closed form, Kirk approximation, Monte Carlo cross-check.
Forwards-based (non-storability -> no cost-of-carry). Deterministic seed=11.
"""

import math

import numpy as np
from scipy.stats import norm

F1, F2 = 80.0, 60.0       # forward power / fuel+carbon leg, EUR/MWh
S1, S2, RHO = 0.50, 0.30, 0.40
T, DF = 0.25, 0.99
K = 10.0                  # VOM-type strike, EUR/MWh


def spread_vol(s1: float, s2: float, rho: float) -> float:
    return math.sqrt(s1 ** 2 + s2 ** 2 - 2 * rho * s1 * s2)


def margrabe(f1, f2, s1, s2, rho, t, df):
    """Value of max(F1 - F2, 0) at t."""
    sig = spread_vol(s1, s2, rho)
    d1 = (math.log(f1 / f2) + 0.5 * sig ** 2 * t) / (sig * math.sqrt(t))
    d2 = d1 - sig * math.sqrt(t)
    v = df * (f1 * norm.cdf(d1) - f2 * norm.cdf(d2))
    return v, d1, d2


def kirk(f1, f2, k, s1, s2, rho, t, df):
    """Approximate value of max(F1 - F2 - K, 0)."""
    s2t = s2 * f2 / (f2 + k)
    return margrabe(f1, f2 + k, s1, s2t, rho, t, df)


def margrabe_deltas(f1, f2, s1, s2, rho, t, df):
    _, d1, d2 = margrabe(f1, f2, s1, s2, rho, t, df)
    return df * norm.cdf(d1), -df * norm.cdf(d2)


def mc_spread(f1, f2, k, s1, s2, rho, t, df, n=200_000, seed=11):
    """MC value of max(F1 - F2 - K, 0) with correlated lognormal legs."""
    rng = np.random.default_rng(seed)
    z1 = rng.standard_normal(n)
    z2 = rho * z1 + math.sqrt(1 - rho ** 2) * rng.standard_normal(n)
    x1 = f1 * np.exp(-0.5 * s1 ** 2 * t + s1 * math.sqrt(t) * z1)
    x2 = f2 * np.exp(-0.5 * s2 ** 2 * t + s2 * math.sqrt(t) * z2)
    return df * np.maximum(x1 - x2 - k, 0.0).mean()


if __name__ == "__main__":
    v, d1, d2 = margrabe(F1, F2, S1, S2, RHO, T, DF)
    vk, _, _ = kirk(F1, F2, K, S1, S2, RHO, T, DF)
    mc0 = mc_spread(F1, F2, 0.0, S1, S2, RHO, T, DF)
    mck = mc_spread(F1, F2, K, S1, S2, RHO, T, DF)
    print(f"spread vol      = {spread_vol(S1, S2, RHO):.3f}")
    print(f"Margrabe        = {v:.2f} EUR/MWh (d1={d1:.3f}, d2={d2:.3f})")
    print(f"MC (K=0)        = {mc0:.2f}  ({(mc0 / v - 1) * 100:+.2f}% vs Margrabe)")
    print(f"Kirk (K=10)     = {vk:.2f} EUR/MWh")
    print(f"MC (K=10)       = {mck:.2f}  ({(mck / vk - 1) * 100:+.2f}% vs Kirk)")
    print(f"deltas          = {margrabe_deltas(F1, F2, S1, S2, RHO, T, DF)}")
