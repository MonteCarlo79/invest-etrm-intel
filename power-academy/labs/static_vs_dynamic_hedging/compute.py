"""Worked example for static_vs_dynamic_hedging.

The classic delta-hedging-error experiment. The plant is long optionality:
model it as short-delta-hedging a weekly spark-spread call (K=55) re-issued
weekly over 26 weeks. Static: set the delta hedge once per week. Dynamic:
re-set it at each intra-week step. Compare hedge-error variance, in a
diffusion regime and with jumps. Deterministic seed=23.
"""

import numpy as np
from scipy.stats import norm

S0, K = 55.0, 55.0
SIG_W = 3.0                    # weekly vol, EUR/MWh
STEPS = 5                      # intra-week steps (dynamic rebalances)
WEEKS = 26
N_PATHS = 4000
JUMP_P, JUMP_MU = 0.08, 10.0   # per intra-week step
SEED = 23


def simulate(jumps: bool, seed=SEED):
    """(n, WEEKS, STEPS+1) intra-week price paths, each week starting at S0."""
    rng = np.random.default_rng(seed)
    p = np.full((N_PATHS, WEEKS, STEPS + 1), S0)
    sig_log = SIG_W / S0                      # weekly log-vol
    ds = sig_log / np.sqrt(STEPS)
    for t in range(STEPS):
        z = rng.standard_normal((N_PATHS, WEEKS))
        j = (rng.random((N_PATHS, WEEKS)) < (JUMP_P if jumps else 0.0)) \
            * rng.exponential(JUMP_MU, (N_PATHS, WEEKS))
        p[:, :, t + 1] = p[:, :, t] * np.exp(-0.5 * ds ** 2 + ds * z) + j
    return p


def call_delta(s, tau):
    """Black delta of the weekly call at time-to-expiry tau (in week units)."""
    if tau <= 0:
        return (s > K).astype(float)
    sig = (SIG_W / S0) * np.sqrt(tau)
    d1 = (np.log(s / K) + 0.5 * sig ** 2) / sig
    return norm.cdf(d1)


def hedge_errors(p):
    """Per-path total hedge error over all weeks, static vs dynamic.

    Short the option at week start at its Black value; delta-hedge with the
    underlying. Error = hedged position value at week end - option sold value.
    """
    sig = SIG_W / S0
    d1 = 0.5 * sig
    d2 = -d1
    opt0 = S0 * (norm.cdf(d1) - norm.cdf(d2))  # ATM call value (r=0)
    err_static, err_dyn = [], []
    for w in range(WEEKS):
        pw = p[:, w, :]
        # static: hedge at week start, hold all week
        d0 = call_delta(pw[:, 0], 1.0)
        hedge_static = d0 * (pw[:, -1] - pw[:, 0])
        # dynamic: re-hedge at every step
        hedge_dyn = np.zeros(p.shape[0])
        for t in range(STEPS):
            tau = (STEPS - t) / STEPS
            d = call_delta(pw[:, t], tau)
            hedge_dyn += d * (pw[:, t + 1] - pw[:, t])
        payoff = np.maximum(pw[:, -1] - K, 0.0)
        err_static.append(-payoff + hedge_static + opt0)
        err_dyn.append(-payoff + hedge_dyn + opt0)
    return np.sum(err_static, axis=0), np.sum(err_dyn, axis=0)


if __name__ == "__main__":
    for label, jumps in (("diffusion", False), ("with jumps", True)):
        p = simulate(jumps)
        es, ed = hedge_errors(p)
        print(f"--- {label} ---")
        print(f"static : mean {es.mean():8.0f}  std {es.std():8.0f}")
        print(f"dynamic: mean {ed.mean():8.0f}  std {ed.std():8.0f}")
