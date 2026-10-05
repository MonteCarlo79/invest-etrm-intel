"""Worked example for gradual_linear_and_benchmark_hedging_strategies.

Synthetic year, 12 delivery months, 2000 simulated years (seed 17).
Q=100 MWh/month, K=50, monthly OU price (theta=55, kappa=0.3, sigma=6).
Strategies: unhedged / gradual linear (12 tranches) / benchmark 70%.
Metric: PaR95 of annual earnings. Deterministic.
"""

import numpy as np

Q, K = 100.0, 50.0
THETA, KAPPA, SIGMA = 55.0, 0.3, 6.0
N_YEARS, SEED = 2000, 17
BENCH_H = 0.70


def price_years(n=N_YEARS, seed=SEED):
    """(n, 13) monthly OU prices including the sale point before month 1."""
    rng = np.random.default_rng(seed)
    p = np.empty((n, 13))
    p[:, 0] = THETA
    z = rng.standard_normal((n, 12))
    for m in range(12):
        p[:, m + 1] = p[:, m] + KAPPA * (THETA - p[:, m]) + SIGMA * z[:, m]
    return p


def par95(x):
    return float(x.mean() - np.quantile(x, 0.05))


def earnings_unhedged(p):
    return (Q * (p[:, 1:] - K)).sum(axis=1)


def earnings_linear(p):
    """Each month's volume sold in 12 equal tranches at the 12 sale prices.

    Entry for delivery month m: prices p_0..p_11 (tranches before delivery).
    Earnings per month: Q * (mean of entry prices - K).
    """
    # tranche j of month m sold at p_j for j=0..m (deliveries average all prior sale points)
    entry_avg = np.stack([p[:, :m + 1].mean(axis=1) for m in range(12)], axis=1)
    return (Q * (entry_avg - K)).sum(axis=1)


def earnings_benchmark(p, h=BENCH_H):
    """h of each month's volume sold one month ahead, rest at spot."""
    locked = h * (p[:, :-1] - K)
    floating = (1 - h) * (p[:, 1:] - K)
    return (Q * (locked + floating)).sum(axis=1)


if __name__ == "__main__":
    p = price_years()
    u, l, b = earnings_unhedged(p), earnings_linear(p), earnings_benchmark(p)
    for name, e in (("unhedged", u), ("linear", l), ("benchmark70", b)):
        print(f"{name:11s}: E = {e.mean():8.0f}  PaR95 = {par95(e):7.0f}")
