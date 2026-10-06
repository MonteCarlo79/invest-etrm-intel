"""Worked example for tolling_agreement_structure_and_valuation (anchor lab).

Buyer-side toll value: annual strip of monthly spread options (headline),
then the cost of a contractual restart limit on a representative volatile
month (daily on/off DP). Deterministic seed=21. Units: EUR per MW.
"""

import numpy as np
from scipy.stats import norm

SPREADS = np.array([-5, 0, 8, 15, 25, 40, 60, 35, 20, 10, 5, -2], dtype=float)
HOURS = 720.0                # per month
SIG_M = 12.0                 # monthly spread uncertainty, EUR/MWh
FEES = 2.0                   # operating fees, EUR/MWh run
SEED = 21


def month_option_value(f: float, sig: float = SIG_M) -> float:
    """E[max(S~, 0)] with S~ ~ N(f, sig^2) — one month's dispatch option."""
    if sig <= 0:
        return max(f, 0.0)
    z = f / sig
    return sig * norm.pdf(z) + f * norm.cdf(z)


def unconstrained_strip() -> float:
    """Annual toll value with no constraints: monthly option margins net of fees."""
    return sum(month_option_value(f - FEES) * HOURS for f in SPREADS)


def daily_margins(seed: int = SEED) -> np.ndarray:
    """Representative volatile month: 30 daily margins, EUR/MW-day (OU draw)."""
    rng = np.random.default_rng(seed)
    n, x = 30, 15.0
    out = []
    for _ in range(n):
        x = x + 0.4 * (15.0 - x) + 25.0 * rng.standard_normal()
        out.append(x)
    return np.array(out)


def dp_restart_limit(margins: np.ndarray, max_starts: int) -> float:
    """Optimal on/off value with at most max_starts starts (no start cost)."""
    n = len(margins)
    NEG = -1e18
    dp = np.full((n + 1, max_starts + 1, 2), NEG)
    dp[n, :, :] = 0.0
    for t in range(n - 1, -1, -1):
        for k in range(max_starts + 1):
            run = margins[t] + dp[t + 1, k, 1]          # running earns the day's margin (can be negative)
            dp[t, k, 1] = max(run, dp[t + 1, k, 0])
            start = margins[t] + dp[t + 1, k + 1, 1] if k < max_starts else NEG
            dp[t, k, 0] = max(dp[t + 1, k, 0], start)
    return dp[0, 0, 0]


def unconstrained_month(margins: np.ndarray) -> float:
    """Run every positive day: the no-constraint month value."""
    return np.maximum(margins, 0.0).sum()


if __name__ == "__main__":
    u = unconstrained_strip()
    print(f"annual toll strip value (unconstrained): {u:,.0f} EUR/MW-yr "
          f"(~{u / 1000:.0f} EUR/kW-yr)")
    m = daily_margins()
    u_m = unconstrained_month(m)
    print(f"representative month unconstrained: {u_m:,.0f} EUR/MW")
    for ns in (10, 5, 3, 2, 1):
        v = dp_restart_limit(m, ns)
        print(f"  max {ns:2d} starts: {v:8,.0f}  (constraint cost "
              f"{(u_m - v) / u_m * 100:.1f}%)")
