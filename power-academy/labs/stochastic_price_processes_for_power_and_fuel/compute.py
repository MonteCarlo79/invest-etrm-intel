"""Worked example for stochastic_price_processes_for_power_and_fuel.

Mean-reverting jump-diffusion (OU in log-price + compound Poisson jumps),
hourly steps, one year. Deterministic via seed=7; no external data.
"""

import numpy as np

KAPPA = 52.0        # mean-reversion speed per year (half-life ~4.8 days)
SIGMA = 0.9         # log-price volatility per sqrt(year)
THETA = np.log(60.0)  # mean log-price (EUR 60/MWh)
LAMBDA = 26.0       # jump intensity per year
JUMP_MU, JUMP_SD = 0.8, 0.3   # log-jump size distribution
HOURS = 8760
DT = 1.0 / HOURS
SEED = 7


def simulate_mrjd(kappa=KAPPA, sigma=SIGMA, theta=THETA, lam=LAMBDA,
                  hours=HOURS, seed=SEED):
    """Euler-Maruyama for dx = kappa(theta-x)dt + sigma dW + J dN."""
    rng = np.random.default_rng(seed)
    n = hours
    x = np.empty(n + 1)
    x[0] = theta
    z = rng.standard_normal(n)
    # jump indicators with intensity lam per year
    jump_flag = rng.random(n) < lam * DT
    jump_size = rng.normal(JUMP_MU, JUMP_SD, n)
    n_jumps = 0
    for t in range(n):
        dx = kappa * (theta - x[t]) * DT + sigma * np.sqrt(DT) * z[t]
        if jump_flag[t]:
            dx += jump_size[t]
            n_jumps += 1
        x[t + 1] = x[t] + dx
    return x, n_jumps


def half_life_days(kappa=KAPPA) -> float:
    return np.log(2) / kappa * 365.0


if __name__ == "__main__":
    x, nj = simulate_mrjd()
    tail = x[len(x) // 2:]
    print(f"theta = {THETA:.4f} (ln 60)   sample mean (2nd half) = {tail.mean():.4f}")
    print(f"jumps realised = {nj} (expected ~{LAMBDA:.0f})")
    print(f"stationary std (diffusion only) = {np.sqrt(SIGMA**2 / (2 * KAPPA)):.4f}")
    print(f"sample std (2nd half, with jumps) = {tail.std():.4f}")
    print(f"mean-reversion half-life = {half_life_days():.1f} days")
