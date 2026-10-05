"""Worked example for delta_sensitivity_and_hedge_volume.

Delta ladder -> tradeable volumes (with base/peak overlap adjustment)
-> residual PaR after settlement. Reuses the decomposition lab's
synthetic month. Deterministic seed=17. Units: MW, MWh, EUR.
"""

import numpy as np

from ..baseload_peak_offpeak_delta_decomposition import compute as dec

MEL, N_DAYS = 100.0, 20


def ladder(days, k):
    """Per-day FD deltas (h-equiv) for base/peak/offpeak + production peak."""
    return {
        "base": dec.fd_delta(days, k, "base"),
        "peak": dec.fd_delta(days, k, "peak"),
        "offpeak": dec.fd_delta(days, k, "offpeak"),
        "prod_peak": dec.production_delta(days, k, "peak"),
    }


def volumes(lad, mel=MEL, n_days=N_DAYS):
    """Tradeable MWh: base leg + incremental peak leg (no double count)."""
    inc_peak = lad["peak"] - lad["prod_peak"]
    return {"base": mel * n_days * lad["base"],
            "inc_peak": mel * n_days * inc_peak}


def residual_par(days, k, vols, fwd_entry=None):
    """PaR95 of daily P&L after settling base + inc_peak forward legs."""
    pnl = np.array([dec.profit(p, k, mel=MEL) for p in days])
    mask_b = dec.block_mask("base")
    mask_p = dec.block_mask("peak")
    settle_b = np.array([(p * mask_b).sum() for p in days]) / 24.0
    settle_p = np.array([(p * mask_p).sum() for p in days]) / 12.0
    fwd_b = fwd_entry[0] if fwd_entry else settle_b.mean()
    fwd_p = fwd_entry[1] if fwd_entry else settle_p.mean()
    # per-day volume in MWh (legs spread over the block hours)
    hedged = (pnl
              + vols["base"] / N_DAYS * (fwd_b - settle_b)
              + vols["inc_peak"] / N_DAYS * (fwd_p - settle_p))
    return float(hedged.mean() - np.quantile(hedged, 0.05)), hedged


if __name__ == "__main__":
    days = dec.simulate(level_sd=8.0, hour_sd=1.5)
    lad = ladder(days, dec.K_PEAKER)
    vols = volumes(lad)
    print("ladder (h-equiv/day):", {k: round(v, 2) for k, v in lad.items()})
    print("volumes (MWh):", {k: round(v) for k, v in vols.items()})
    par_u, _ = residual_par(days, dec.K_PEAKER, {"base": 0.0, "inc_peak": 0.0})
    par_h, _ = residual_par(days, dec.K_PEAKER, vols)
    print(f"residual PaR95 unhedged {par_u:.1f} -> hedged {par_h:.1f}")
