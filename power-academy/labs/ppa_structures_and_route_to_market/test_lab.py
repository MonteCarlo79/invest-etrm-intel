import numpy as np

from . import compute as c


def test_pap_tracks_volume_times_strike():
    vol, price = c.wind_year()
    m, p, b, flat = c.cashflows(vol, price)
    assert abs(p - (c.CAP * vol).sum() * c.PPA_STRIKE) < 1e-6
    # PaP is independent of prices by construction
    _, p2, _, _ = c.cashflows(vol, price + 20.0)
    assert abs(p2 - p) < 1e-6


def test_capture_rate_below_one_with_cannibalisation():
    vol, price = c.wind_year()
    m, _, _, _ = c.cashflows(vol, price)
    capture = (m / (vol.sum() * c.CAP)) / price.mean()
    assert capture < 1.0
    assert abs(capture - 0.98) < 0.02


def test_baseload_imbalance_cost_positive_when_profile_anticorrelates():
    vol, price = c.wind_year()
    m, p, b, flat = c.cashflows(vol, price)
    # baseload PPA underperforms merchant when generation is low in high-price hours
    assert b < m
    assert abs((b - m) - (-995_752)) < 20_000
    # identity: baseload = PaP + imbalance - (PaP - flat*strike*8760 adjustment)
    assert abs(b - (flat * c.PPA_STRIKE * len(vol) + ((c.CAP * vol - flat) * price).sum())) < 1e-6
