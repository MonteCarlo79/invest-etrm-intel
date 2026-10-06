"""Worked example for dispatch_optimisation_and_delta_hedging.

DP dispatch of a 100 MW plant with min up/down + start costs on a synthetic
week, vs a greedy heuristic; then finite-difference deltas per day.
Deterministic seed=9. Units: MW, EUR/MWh.
"""

import numpy as np

MEL = 100.0            # MW
MIN_UP, MIN_DOWN = 4, 2
START_COST = 2000.0    # EUR
SEED = 9


def synthetic_week(seed: int = SEED) -> np.ndarray:
    """168 hourly spreads: seasonal shape + OU noise, EUR/MWh."""
    rng = np.random.default_rng(seed)
    h = np.arange(168)
    base = 10.0 + 20.0 * np.sin(2 * np.pi * (h % 24 - 8) / 24)
    x = 0.0
    noise = []
    for _ in h:
        x = 0.7 * x + 8.0 * rng.standard_normal()
        noise.append(x)
    return base + np.array(noise)


def dp_dispatch(spreads, mel=MEL, min_up=MIN_UP, min_down=MIN_DOWN,
                start_cost=START_COST):
    """Exact optimal dispatch via backward DP.

    State: (u, d) = consecutive hours online (0..min_up) / offline (0..min_down).
    Returns (profit EUR, schedule of 0/1 per hour).
    """
    n = len(spreads)
    NEG = -1e18
    # value function over states, backward
    V = {}
    for u in range(min_up + 1):
        for d in range(min_down + 1):
            V[(u, d)] = 0.0
    choice = {}
    for t in range(n - 1, -1, -1):
        Vn, cn = {}, {}
        for (u, d), _ in V.items():
            opts = []
            if u > 0:                                   # online
                if u >= min_up:
                    opts.append((mel * spreads[t] + V[(min_up, 0)], 1))
                    opts.append((V[(0, 1)], 0))         # stop
                else:                                    # must keep running (min up)
                    opts.append((mel * spreads[t] + V[(u + 1, 0)], 1))
            else:                                       # offline
                stay = V[(0, min(d + 1, min_down))]
                if d >= min_down or t == 0:
                    start = mel * spreads[t] - start_cost + V[(1, 0)]
                    opts.append((start, 1))
                opts.append((stay, 0))
            best = max(opts, key=lambda x: x[0])
            Vn[(u, d)], cn[(u, d)] = best
        V = Vn
        choice[t] = cn
    # roll forward
    sched, u, d = [], 0, min_down
    profit = 0.0
    for t in range(n):
        a = choice[t][(u, d)]
        sched.append(a)
        if a == 1:
            profit += mel * spreads[t] - (start_cost if u == 0 else 0.0)
            u, d = min(u + 1, min_up) if u > 0 else 1, 0
        else:
            u, d = 0, min(d + 1, min_down)
    return profit, np.array(sched)


def greedy_dispatch(spreads, mel=MEL, min_up=MIN_UP, min_down=MIN_DOWN,
                    start_cost=START_COST):
    """Run when spread > 0, stop when < 0, respecting min up/down."""
    n = len(spreads)
    sched, u, d, profit = [], 0, min_down, 0.0
    for t in range(n):
        if u > 0:
            a = 1 if (u < min_up or spreads[t] >= 0) else 0
        else:
            a = 1 if (d >= min_down and spreads[t] > 0) else 0
        sched.append(a)
        if a == 1:
            profit += mel * spreads[t] - (start_cost if u == 0 else 0.0)
            u, d = (min(u + 1, min_up) if u > 0 else 1), 0
        else:
            u, d = 0, min(d + 1, min_down)
    return profit, np.array(sched)


def day_deltas(spreads, eps: float = 1.0):
    """Finite-difference delta per day: dV/d(spread of that day's hours)."""
    base, _ = dp_dispatch(spreads)
    out = []
    for day in range(7):
        sl = slice(day * 24, (day + 1) * 24)
        up = spreads.copy(); up[sl] += eps
        dn = spreads.copy(); dn[sl] -= eps
        v_up, _ = dp_dispatch(up)
        v_dn, _ = dp_dispatch(dn)
        out.append((v_up - v_dn) / (2 * eps) / MEL)   # hours-equivalent
    return np.array(out)


if __name__ == "__main__":
    s = synthetic_week()
    p_dp, sched_dp = dp_dispatch(s)
    p_gr, sched_gr = greedy_dispatch(s)
    print(f"DP profit     = {p_dp:,.0f} EUR (run {sched_dp.sum()}h, "
          f"starts {int(np.diff(np.r_[0, sched_dp]).clip(0).sum())})")
    print(f"greedy profit = {p_gr:,.0f} EUR (run {sched_gr.sum()}h)")
    print(f"DP - greedy   = {p_dp - p_gr:,.0f} EUR")
    deltas = day_deltas(s)
    runh = [sched_dp[d * 24:(d + 1) * 24].sum() for d in range(7)]
    for d in range(7):
        print(f"  day {d + 1}: delta {deltas[d]:5.1f} h-equiv  (run hours {runh[d]})")
