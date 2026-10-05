"""Worked example for ccgt_investment_option_and_optimal_timing (anchor lab).

Perpetual option to build a CCGT when the long-run clean spread is
mean-reverting (OU). Trinomial lattice (Kushner-style moment matching)
backward to a quasi-perpetual solution; reports the build trigger and
compares it with the static NPV break-even spread.
Units: EUR/MWh (spread), EUR/kW (cost). Deterministic; no external data.
"""

import numpy as np

THETA = 16.0        # long-run clean spark spread, EUR/MWh
KAPPA = 0.5         # mean-reversion speed /yr
SIGMA = 4.0         # spread volatility, EUR/MWh/sqrt(yr)
R = 0.08            # discount rate
H = 4000.0          # equivalent full-load hours per year
I_COST = 800.0      # build cost, EUR/kW
DT = 0.25           # quarterly steps
YEARS = 40
N_J = 8             # grid half-width; keeps |drift| <= ds so trinomial probs stay in [0,1]


def build_value(s: float) -> float:
    """NPV of building today at spread s, EUR/kW (already net of I_COST).

    Annuity of expected margins under OU: E[S_t] = theta + (s-theta)e^{-k t}.
    """
    return (H / 1000.0) * (s / (R + KAPPA) + THETA * KAPPA / (R * (R + KAPPA))) - I_COST


def static_breakeven() -> float:
    """Spread at which build_value == 0 (static NPV trigger)."""
    lo, hi = 0.0, 50.0
    for _ in range(80):
        mid = 0.5 * (lo + hi)
        if build_value(mid) > 0:
            hi = mid
        else:
            lo = mid
    return 0.5 * (lo + hi)


def lattice():
    """OU trinomial lattice; returns (grid, option values, trigger spread)."""
    m0 = np.exp(-KAPPA * DT)
    var = SIGMA ** 2 * (1 - np.exp(-2 * KAPPA * DT)) / (2 * KAPPA)
    ds = np.sqrt(3.0 * var)          # trinomial spacing (probs stay in [0,1])
    grid = THETA + ds * np.arange(-N_J, N_J + 1)
    n = len(grid)
    # transition probabilities via moment matching
    P = np.zeros((n, n))
    for i, s in enumerate(grid):
        m = THETA + (s - THETA) * m0
        drift = m - s
        pu = var / (2 * ds ** 2) + drift ** 2 / (2 * ds ** 2) + drift / (2 * ds)
        pd = var / (2 * ds ** 2) + drift ** 2 / (2 * ds ** 2) - drift / (2 * ds)
        pm = 1.0 - pu - pd
        row = {(i + 1): pu, i: pm, (i - 1): pd}
        tot = sum(max(p, 0.0) for p in row.values())
        for j, p in row.items():
            jj = min(max(j, 0), n - 1)
            P[i, jj] += max(p, 0.0) / tot
    disc = np.exp(-R * DT)
    exercise = np.array([max(build_value(s), 0.0) for s in grid])
    v = exercise.copy()
    for _ in range(int(YEARS / DT)):
        cont = disc * (P @ v)
        v = np.maximum(exercise, cont)
    cont = disc * (P @ v)
    trigger = next(s for s, e, c in zip(grid, exercise, cont) if e >= c and e > 0)
    return grid, v, trigger


def option_value(s: float) -> float:
    grid, v, _ = lattice()
    return float(np.interp(s, grid, v))


if __name__ == "__main__":
    sb = static_breakeven()
    grid, v, trig = lattice()
    print(f"static break-even spread : {sb:.2f} EUR/MWh")
    print(f"OU build trigger S*      : {trig:.2f} EUR/MWh  ({trig / sb:.2f}x static)")
    print(f"option value at S=theta  : {option_value(THETA):.2f} EUR/kW "
          f"(exercise value {max(build_value(THETA), 0):.2f})")
