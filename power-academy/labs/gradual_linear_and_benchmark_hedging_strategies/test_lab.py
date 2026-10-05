import numpy as np

from . import compute as c


def _all():
    p = c.price_years()
    return (c.earnings_unhedged(p), c.earnings_linear(p), c.earnings_benchmark(p))


def test_both_strategies_beat_unhedged():
    u, l, b = _all()
    assert c.par95(l) < c.par95(u)
    assert c.par95(b) < c.par95(u)


def test_measured_numbers():
    u, l, b = _all()
    assert abs(c.par95(u) - 9_313) < 150
    assert abs(c.par95(l) - 7_241) < 150
    assert abs(c.par95(b) - 9_007) < 150


def test_linear_wins_under_mean_reversion():
    # OU with kappa>0 and no trend: entry-price averaging captures more variance
    u, l, b = _all()
    assert c.par95(l) < c.par95(b)


def test_expectations_roughly_equal():
    # neither strategy is supposed to change the mean much (no edge assumed)
    u, l, b = _all()
    assert abs(l.mean() - u.mean()) < 200
    assert abs(b.mean() - u.mean()) < 200
