"""Worked example for power_price_spike_models_and_option_valuation.

Pareto (power-law) spike tails: inverse-CDF sampler, Hill estimator,
closed-form spike call, and the Gaussian-jump underpricing comparison.
Deterministic seed=3; no external data. Units: EUR/MWh.
"""

import math

import numpy as np
from scipy.stats import norm

U_THR, ALPHA = 100.0, 3.0   # threshold and true tail exponent
N = 400
SEED = 3


def pareto_sample(n: int = N, u: float = U_THR, alpha: float = ALPHA,
                  seed: int = SEED) -> np.ndarray:
    rng = np.random.default_rng(seed)
    return u * rng.random(n) ** (-1.0 / alpha)


def hill_estimate(samples: np.ndarray, u: float = U_THR) -> float:
    """Hill estimator of the Pareto exponent from exceedances over u."""
    exc = samples[samples >= u]
    return 1.0 / np.log(exc / u).mean()


def pareto_call(u: float, alpha: float, k: float) -> float:
    """E[max(X - K, 0)] for X ~ Pareto(u, alpha), K >= u, alpha > 1."""
    if k < u:
        raise ValueError("closed form requires K >= u")
    return u ** alpha / ((alpha - 1.0) * k ** (alpha - 1.0))


def pareto_moments(u: float, alpha: float):
    """Mean and std of Pareto(u, alpha) (requires alpha > 2 for var)."""
    mean = u * alpha / (alpha - 1.0)
    var = u * u * alpha / ((alpha - 1.0) ** 2 * (alpha - 2.0))
    return mean, math.sqrt(var)


def gaussian_call_matched(u: float, alpha: float, k: float) -> float:
    """E[max(G - K, 0)] for Gaussian with the Pareto's mean and std."""
    mu, sd = pareto_moments(u, alpha)
    z = (k - mu) / sd
    return sd * norm.pdf(z) - (k - mu) * norm.cdf(-z)


if __name__ == "__main__":
    s = pareto_sample()
    a_hat = hill_estimate(s)
    print(f"Hill estimate of alpha: {a_hat:.2f} (true {ALPHA})")
    for K in (150, 300, 400):
        p = pareto_call(U_THR, ALPHA, K)
        g = gaussian_call_matched(U_THR, ALPHA, K)
        print(f"K={K}: Pareto E[max(X-K,0)] = {p:6.2f}   "
              f"Gaussian-matched = {g:6.2f}   ratio = {p / g:6.1f}x")
