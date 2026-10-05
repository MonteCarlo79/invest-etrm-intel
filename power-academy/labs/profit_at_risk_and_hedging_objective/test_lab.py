import numpy as np

from . import compute as c


def test_par_definition_matches_empirical_quantile():
    x = c.annual(c.monthly_margins())
    assert abs(c.par(x, 0.05) - (x.mean() - np.quantile(x, 0.05))) < 1e-12


def test_unhedged_expectation_accounts_for_stress():
    u = c.annual(c.monthly_margins())
    assert abs(u.mean() - 318) < 5.0            # not 12*30=360: stress months cost ~42
    assert abs(c.par(u) - 138) < 5.0


def test_hedge_reduces_par_and_costs_little():
    m = c.monthly_margins()
    u = c.annual(m)
    h = c.annual(m, hedge_share=c.HEDGE_SHARE)
    assert c.par(h) < c.par(u)
    assert abs(c.par(h) - 55) < 5.0
    # forward at 28 > true monthly mean 26.5 -> hedged expectation slightly higher
    assert h.mean() > u.mean()


def test_deterministic_seeding():
    a = c.monthly_margins()
    b = c.monthly_margins()
    assert np.array_equal(a, b)
