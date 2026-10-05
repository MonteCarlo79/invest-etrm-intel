"""Worked example for hedging_under_incomplete_markets_and_spike_dynamics.

Prices have a level component AND a peak-basis component. The peak product
doesn't trade: hedge a peaker with baseload only (proxy) vs the full
base+peak ladder. Residual basis quantified; proxy still beats unhedged.
Deterministic seed=29 (level_sd=8, hour_sd=1.5, peak_sd=4).
"""

import numpy as np

from ..baseload_peak_offpeak_delta_decomposition import compute as dec

MEL, N_DAYS = 100.0, 20


def hedge_pnl(days, k, base_vol=0.0, peak_vol=0.0):
    """Daily P&L: plant + short forward legs (per-day MWh volumes)."""
    pnl = np.array([dec.profit(p, k, mel=MEL) for p in days])
    out = pnl.copy()
    for vol, block in ((base_vol, "base"), (peak_vol, "peak")):
        if vol:
            mask = dec.block_mask(block)
            n_h = 24.0 if block == "base" else 12.0
            settle = np.array([(p * mask).sum() for p in days]) / n_h
            out = out + vol / N_DAYS * (settle.mean() - settle)
    return out


def par95(x):
    return float(x.mean() - np.quantile(x, 0.05))


def results(seed=29, peak_sd=4.0):
    """Regression hedges: optimal single-product (base) vs two-product (base+peak)."""
    days = dec.simulate(seed=seed, level_sd=8.0, hour_sd=1.5, peak_sd=peak_sd)
    k = dec.K_PEAKER
    u = np.array([dec.profit(p, k, mel=MEL) for p in days])
    s_b = np.array([(p * dec.block_mask("base")).sum() for p in days]) / 24.0
    s_p = np.array([(p * dec.block_mask("peak")).sum() for p in days]) / 12.0
    b_single = np.cov(u, s_b)[0, 1] / np.var(s_b)
    X = np.column_stack([s_b - s_b.mean(), s_p - s_p.mean()])
    coef, *_ = np.linalg.lstsq(X, u - u.mean(), rcond=None)
    resid_single = u - u.mean() - b_single * (s_b - s_b.mean())
    resid_double = u - u.mean() - X @ coef
    return {"u": u, "resid_single": resid_single, "resid_double": resid_double,
            "b_single": b_single, "coef": coef}


if __name__ == "__main__":
    for psd in (0.0, 4.0, 8.0):
        r = results(peak_sd=psd)
        print(f"peak_sd={psd}: resid std single {r['resid_single'].std():,.0f} "
              f"double {r['resid_double'].std():,.0f} "
              f"(unhedged {r['u'].std():,.0f})")
    r = results()
    print(f"PaR95 unhedged {par95(r['u']):,.0f} -> single-product {par95(r['u'].mean() + r['resid_single']):,.0f} "
          f"-> two-product {par95(r['u'].mean() + r['resid_double']):,.0f}")
