"""Worked example for ppa_structures_and_route_to_market.

Synthetic wind year (8760h): pay-as-produced vs baseload PPA cashflows.
PaP revenue tracks volume x price; baseload PPA creates imbalance cost.
Deterministic seed=31. Units: MWh, EUR/MWh.
"""

import numpy as np

CAP = 100.0          # MW
PRICE_MEAN, PRICE_SD = 55.0, 18.0
PPA_STRIKE = 52.0
SEED = 31


def wind_year(seed=SEED):
    """Hourly volume factor (0..1) with daily+seasonal shape, and prices."""
    rng = np.random.default_rng(seed)
    h = np.arange(8760)
    seas = 0.45 + 0.15 * np.sin(2 * np.pi * h / 8760 - 0.8)
    daily = 1 + 0.2 * np.sin(2 * np.pi * (h % 24) / 24 + 1.0)
    vol = np.clip(seas * daily + rng.normal(0, 0.12, len(h)), 0, 1)
    price = PRICE_MEAN + 10 * np.sin(2 * np.pi * (h % 24 - 6) / 24) \
        + rng.normal(0, 8, len(h)) - 25 * np.clip(vol - 0.6, 0, 1)  # cannibalisation
    return vol, price


def cashflows(vol, price, strike=PPA_STRIKE):
    """Annual cashflows under three routes: merchant, PaP PPA, baseload PPA."""
    gen = CAP * vol
    merchant = (gen * price).sum()
    pap = (gen * strike).sum()
    # baseload PPA: sell flat volume at strike; the imbalance (gen - flat) settles at spot
    flat = gen.mean()
    imbalance = ((gen - flat) * price).sum()
    baseload = flat * strike * len(vol) + imbalance
    return merchant, pap, baseload, flat


if __name__ == "__main__":
    vol, price = wind_year()
    m, p, b, flat = cashflows(vol, price)
    print(f"annual generation: {vol.sum() * CAP:,.0f} MWh (flat volume {flat:.1f} MW)")
    print(f"merchant (spot) : {m:,.0f} EUR")
    print(f"pay-as-produced : {p:,.0f} EUR")
    print(f"baseload PPA    : {b:,.0f} EUR  (imbalance cost {b - m:,.0f} vs merchant)")
    # capture rate: merchant price achieved vs average price
    print(f"capture rate: {(m / (vol.sum() * CAP)) / price.mean():.2f} "
          f"(wind sells below the average price)")
