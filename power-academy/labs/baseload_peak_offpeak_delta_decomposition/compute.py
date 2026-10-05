"""Worked example for baseload_peak_offpeak_delta_decomposition (anchor lab).

Two plants on a synthetic month of 20 weekdays (hourly profile + noise):
must-run (K=20) and peaker (K=60). Per-block deltas two ways:
  (a) production delta   = average run MWh per block (average-production)
  (b) finite-difference  = dV/d(block price) incl. incremental conversion
Identity: FD_base = FD_peak + FD_offpeak.
AOM caveat: hedging with (a) instead of (b) leaves a bigger residual PaR.
Deterministic seed=17. Units: MW, EUR/MWh.
"""

import numpy as np

PEAK_PROFILE = np.array([45, 50, 55, 58, 59, 62, 68, 75, 70, 62, 59, 48], dtype=float)
OFFPEAK_LEVEL = 35.0
N_DAYS, SIG = 20, 6.0
SEED = 17
K_PEAKER, K_MUSTRUN = 60.0, 20.0
SHOCK = 2.0


def prices_day(rng, level_sd: float = 0.0, hour_sd: float = SIG, peak_sd: float = 0.0):
    noise = rng.normal(0, hour_sd, 24)
    if level_sd > 0.0:
        noise = noise + rng.normal(0, level_sd)
    if peak_sd > 0.0:                       # extra common shock on peak hours only
        noise[:12] = noise[:12] + rng.normal(0, peak_sd)
    return np.concatenate([PEAK_PROFILE, np.full(12, OFFPEAK_LEVEL)]) + noise


def profit(prices, k, mel=1.0):
    return mel * np.maximum(prices - k, 0.0).sum()


def run_mwh(prices, k, mel=1.0):
    return mel * (prices > k).astype(float)


def simulate(n_days=N_DAYS, seed=SEED, level_sd: float = 0.0, hour_sd: float = SIG,
             peak_sd: float = 0.0):
    rng = np.random.default_rng(seed)
    return np.array([prices_day(rng, level_sd, hour_sd, peak_sd) for _ in range(n_days)])


def block_mask(block):
    m = np.zeros(24, dtype=float)
    if block in ("peak", "base"):
        m[:12] = 1.0
    if block in ("offpeak", "base"):
        m[12:] = 1.0
    return m


def production_delta(days, k, block):
    mask = block_mask(block)
    return float((np.array([run_mwh(p, k) for p in days]) @ mask).mean())


def fd_delta(days, k, block, shock=SHOCK):
    mask = block_mask(block)
    up = days + shock * mask
    dn = days - shock * mask
    v0 = np.mean([profit(p, k) for p in up])
    v1 = np.mean([profit(p, k) for p in dn])
    return (v0 - v1) / (2 * shock)


def residual_par(days, k, block, hedge_volume):
    """PaR95 of daily profit hedged with hedge_volume MWh of the block product."""
    pnl = np.array([profit(p, k) for p in days])
    mask = block_mask(block)
    settle = np.array([run_mwh(p, 0.0) @ mask for p in days])  # block hours (const)
    fwd_price = np.array([(p * mask).sum() for p in days]) / 12.0
    hedged = pnl + hedge_volume * (fwd_price.mean() - fwd_price)
    return float(hedged.mean() - np.quantile(hedged, 0.05))


if __name__ == "__main__":
    days = simulate()
    for k, name in ((K_MUSTRUN, "must-run"), (K_PEAKER, "peaker  ")):
        pd_ = {b: production_delta(days, k, b) for b in ("peak", "offpeak")}
        fd = {b: fd_delta(days, k, b) for b in ("peak", "offpeak", "base")}
        print(f"{name}: prod peak {pd_['peak']:.2f} off {pd_['offpeak']:.2f} | "
              f"FD peak {fd['peak']:.2f} off {fd['offpeak']:.2f} base {fd['base']:.2f} "
              f"(identity {fd['peak'] + fd['offpeak']:.2f})")
    k = K_PEAKER
    naive = production_delta(days, k, "peak")
    full = fd_delta(days, k, "peak")
    print(f"AOM caveat: hedge peak with prod-delta {naive:.2f} -> residual PaR "
          f"{residual_par(days, k, 'peak', naive):.2f} vs FD-delta {full:.2f} -> "
          f"{residual_par(days, k, 'peak', full):.2f}")
