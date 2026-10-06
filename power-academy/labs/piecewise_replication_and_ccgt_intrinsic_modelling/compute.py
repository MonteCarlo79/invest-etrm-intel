"""Worked example for piecewise_replication_and_ccgt_intrinsic_modelling.

Part 1: two-unit station payoff == exact option ladder.
Part 2: smooth-cost CCGT payoff approximated by an 8-segment option ladder
(secant-slope increments; interpolation lies above a convex payoff).
Deterministic; no external data. Units: EUR, MW.
"""

import numpy as np

# ---- Part 1: exact two-unit ladder ----
UNITS = [(300.0, 60.0), (200.0, 72.0)]   # (MW, marginal cost EUR/MWh)


def two_unit_payoff(s):
    return sum(q * max(s - k, 0.0) for q, k in UNITS)


def option_ladder_payoff(s, ladder):
    return sum(w * max(s - k, 0.0) for w, k in ladder)


# ---- Part 2: smooth CCGT ----
COST_A, COST_B = 30.0, 0.024   # C(q) = COST_A*q + COST_B*q^2 ; C'(q) = 30 + 0.048q
QMAX = 500.0


def cost_q(q):
    return COST_A * q + COST_B * q * q


def q_star(s: float) -> float:
    """Optimal output where S = C'(q), interior on (30, 54)."""
    if s <= COST_A:
        return 0.0
    return min((s - COST_A) / (2 * COST_B), QMAX)


def smooth_payoff(s):
    q = q_star(s)
    return q * s - cost_q(q)


STRIKES = [30, 34, 38, 42, 46, 50, 54, 60]


def build_ladder(strikes=STRIKES):
    """Option ladder (weights, strikes) from secant-slope increments + constant."""
    k = list(strikes)
    pi = [smooth_payoff(x) for x in k]
    secant = [(pi[i + 1] - pi[i]) / (k[i + 1] - k[i]) for i in range(len(k) - 1)]
    weights, prev = [], 0.0
    for sl in secant:
        weights.append(sl - prev)
        prev = sl
    ladder = [(w, k_i) for w, k_i in zip(weights, k) if abs(w) > 1e-12]
    return pi[0], ladder   # (constant, [(weight, strike)])


def replicated_payoff(s, const, ladder):
    return const + option_ladder_payoff(s, ladder)


if __name__ == "__main__":
    grid = [40, 55, 60, 65, 72, 80, 100]
    exact = all(abs(two_unit_payoff(s) - option_ladder_payoff(
        s, [(300.0, 60.0), (200.0, 72.0)])) < 1e-9 for s in grid)
    print(f"two-unit replication exact on grid: {exact}")
    const, ladder = build_ladder()
    dense = np.linspace(30, 100, 141)
    errs = [abs(replicated_payoff(s, const, ladder) - smooth_payoff(s)) for s in dense]
    print(f"8-segment ladder: {[(round(w,1), k) for w, k in ladder]}")
    print(f"max abs error on [30,100]: {max(errs):.2f} EUR "
          f"(full-load payoff at S=100: {smooth_payoff(100):.0f})")
