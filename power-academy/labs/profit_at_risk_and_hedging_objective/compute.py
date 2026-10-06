"""Worked example for profit_at_risk_and_hedging_objective.

Annual earnings simulation (10k paths, seed 13): monthly margins
N(30k, 18k) + 5% stress months at -40k. PaR95 = E[X] - q5%(X).
Unhedged vs 60% forward-hedged. Deterministic.
"""

import numpy as np

N_PATHS, SEED = 10_000, 13
MU, SIG, STRESS_P, STRESS_M = 30.0, 18.0, 0.05, -40.0   # kEUR per month
FWD_MARGIN, HEDGE_SHARE = 28.0, 0.60


def monthly_margins(n=N_PATHS, seed=SEED):
    rng = np.random.default_rng(seed)
    base = rng.normal(MU, SIG, (n, 12))
    stress = rng.random((n, 12)) < STRESS_P
    return np.where(stress, STRESS_M, base)


def annual(margins, hedge_share: float = 0.0, fwd: float = FWD_MARGIN):
    """Annual earnings; hedge_share of each month's margin locked at fwd."""
    hedged = hedge_share * fwd + (1 - hedge_share) * margins
    return hedged.sum(axis=1)


def par(x, alpha: float = 0.05) -> float:
    """Profit-at-Risk: E[X] - q_alpha(X), shortfall convention."""
    return float(x.mean() - np.quantile(x, alpha))


if __name__ == "__main__":
    m = monthly_margins()
    u = annual(m)
    h = annual(m, hedge_share=HEDGE_SHARE)
    print(f"unhedged: E = {u.mean():.1f} kEUR, q5% = {np.quantile(u, 0.05):.1f}, "
          f"PaR95 = {par(u):.1f}")
    print(f"hedged  : E = {h.mean():.1f} kEUR, q5% = {np.quantile(h, 0.05):.1f}, "
          f"PaR95 = {par(h):.1f}")
    print(f"exchange rate: dPaR {par(u) - par(h):.1f} kEUR for "
          f"dMargin {u.mean() - h.mean():.1f} kEUR")
