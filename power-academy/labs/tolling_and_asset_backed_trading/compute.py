"""Worked example for tolling_and_asset_backed_trading.

Shared MC paths: the same plant, two lives. Merchant: full P&L distribution.
Tolled: premium + residual, variance ~0. Where the deal zone sits.
Deterministic seed=37. Units: EUR per MW-year.
"""

import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from tolling_agreement_structure_and_valuation.compute import (SPREADS, HOURS,
                                                               SIG_M, FEES,
                                                               month_option_value)

SEED = 37
N = 4000


def merchant_pnl(n=N, seed=SEED):
    """Annual merchant margin distribution (monthly spread draws, seed 37)."""
    rng = np.random.default_rng(seed)
    total = np.zeros(n)
    for f in SPREADS:
        s_m = rng.normal(f, SIG_M, n)
        total += np.maximum(s_m - FEES, 0.0) * HOURS
    return total


def toll_pnl(premium_per_mw: float, availability: float = 0.97, n=N, seed=SEED):
    """Seller's tolled income: premium + small residual variance (outages)."""
    rng = np.random.default_rng(seed + 1)
    outage = rng.random(n) > availability
    return premium_per_mw * np.where(outage, 0.9, 1.0) + rng.normal(0, 300, n)


if __name__ == "__main__":
    m = merchant_pnl()
    strip = m.mean()
    print(f"merchant: mean {strip:,.0f}  std {m.std():,.0f}  "
          f"q5% {np.quantile(m, 0.05):,.0f}")
    for prem in (0.85, 1.00, 1.15):
        t = toll_pnl(prem * strip)
        print(f"toll at {prem:.0%} of strip: mean {t.mean():,.0f}  "
              f"std {t.std():,.0f} (variance ratio {t.var() / m.var():.4f})")
    fixed_om = 0.60 * strip
    print(f"seller floor (premium covers fixed O&M {fixed_om:,.0f} + risk margin); "
          f"buyer ceiling = strip value {strip:,.0f} minus risk/capital charge")
