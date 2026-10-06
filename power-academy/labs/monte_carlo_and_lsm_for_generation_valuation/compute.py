"""Worked example for monte_carlo_and_lsm_for_generation_valuation.

One-shot start option under uncertainty: offline plant may start once
(pay START_COST), must then run U hours, done. Shows the bias bracket:
greedy <= LSM policy <= true value <= perfect foresight.
Deterministic seed=5; no external data. Units: EUR/MWh per unit capacity.
"""

import numpy as np

THETA, KAPPA, SIGMA = 30.0, 0.5, 8.0   # OU in levels, hourly
H = 12                                  # horizon, hours
U = 3                                   # min run, hours
START_COST = 40.0                       # EUR
N_TRAIN, N_REPLAY = 1000, 1000
SEED = 5


def simulate(n: int, seed: int = SEED) -> np.ndarray:
    """OU spread paths, shape (n, H+1) including t=0."""
    rng = np.random.default_rng(seed)
    s = np.empty((n, H + 1))
    s[:, 0] = THETA
    z = rng.standard_normal((n, H))
    for t in range(H):
        s[:, t + 1] = s[:, t] + KAPPA * (THETA - s[:, t]) + SIGMA * z[:, t]
    return s


def exercise_value(paths: np.ndarray, t: int) -> np.ndarray:
    """Realised payoff of starting at hour t: sum of next U spreads - cost."""
    return paths[:, t:t + U].sum(axis=1) - START_COST


def dp_perfect_foresight(paths: np.ndarray) -> np.ndarray:
    """Upper bound: best start hour with full knowledge of the path."""
    best = np.zeros(len(paths))
    for t in range(0, H - U + 1):
        best = np.maximum(best, exercise_value(paths, t))
    return best


def greedy(paths: np.ndarray) -> np.ndarray:
    """Start at the first hour whose spread covers START_COST/U; else never."""
    val = np.zeros(len(paths))
    threshold = START_COST / U
    for i, p in enumerate(paths):
        for t in range(0, H - U + 1):
            if p[t] > threshold:
                val[i] = exercise_value(paths, t)[i]
                break
    return val


def _basis(s: np.ndarray) -> np.ndarray:
    return np.column_stack([np.ones_like(s), s, s * s])


def lsm_train(paths: np.ndarray):
    """Backward LSM: per-step polynomials for continuation and exercise value.

    Returns (exercise_coefs, continuation_coefs), each a list indexed by t.
    """
    n = len(paths)
    nxt = np.zeros(n)                    # value of the option from t+1 on
    ex_coefs, cont_coefs = {}, {}
    for t in range(H - U, -1, -1):
        s_t = paths[:, t]
        ex = exercise_value(paths, t)
        b = _basis(s_t)
        ex_coefs[t] = np.linalg.lstsq(b, ex, rcond=None)[0]
        cont_coefs[t] = np.linalg.lstsq(b, nxt, rcond=None)[0]
        # optimal decision value at t becomes next-step value for t-1
        nxt = np.maximum(ex, b @ cont_coefs[t])
        nxt = np.maximum(nxt, 0.0)       # may never start
    return ex_coefs, cont_coefs


def lsm_replay(paths: np.ndarray, coefs) -> np.ndarray:
    """Forward policy value on fresh paths (out-of-sample)."""
    ex_coefs, cont_coefs = coefs
    val = np.zeros(len(paths))
    for i, p in enumerate(paths):
        for t in range(0, H - U + 1):
            b = _basis(np.array([p[t]]))
            ex_hat = float((b @ ex_coefs[t])[0])
            cont_hat = float((b @ cont_coefs[t])[0])
            if ex_hat >= cont_hat and ex_hat > 0:
                val[i] = exercise_value(paths, t)[i]
                break
    return val


if __name__ == "__main__":
    train = simulate(N_TRAIN, seed=SEED)
    replay = simulate(N_REPLAY, seed=SEED + 1000)
    coefs = lsm_train(train)
    g = greedy(replay).mean()
    l = lsm_replay(replay, coefs).mean()
    d = dp_perfect_foresight(replay).mean()
    print(f"greedy            = {g:6.2f}")
    print(f"LSM policy (oos)  = {l:6.2f}  ({l / d * 100:.1f}% of foresight)")
    print(f"perfect foresight = {d:6.2f}  (upper bound)")
