import compute as c


def test_spread_vol():
    assert abs(c.spread_vol(0.5, 0.3, 0.4) - 0.4690) < 1e-3
    # correlation compresses spread vol
    assert c.spread_vol(0.5, 0.3, 0.9) < c.spread_vol(0.5, 0.3, 0.4)
    # rho -> -1 gives the sum
    assert abs(c.spread_vol(0.5, 0.3, -1.0) - 0.8) < 1e-9


def test_margrabe_worked_example():
    v, d1, d2 = c.margrabe(c.F1, c.F2, c.S1, c.S2, c.RHO, c.T, c.DF)
    assert abs(v - 20.65) < 0.05
    assert abs(d1 - 1.344) < 0.01 and abs(d2 - 1.110) < 0.01


def test_kirk_reduces_to_margrabe_at_zero_strike():
    vm, _, _ = c.margrabe(c.F1, c.F2, c.S1, c.S2, c.RHO, c.T, c.DF)
    vk, _, _ = c.kirk(c.F1, c.F2, 0.0, c.S1, c.S2, c.RHO, c.T, c.DF)
    assert abs(vk / vm - 1) < 0.01


def test_kirk_worked_example():
    vk, _, _ = c.kirk(c.F1, c.F2, c.K, c.S1, c.S2, c.RHO, c.T, c.DF)
    assert abs(vk - 12.88) < 0.10
    assert 0 < vk < c.margrabe(c.F1, c.F2, c.S1, c.S2, c.RHO, c.T, c.DF)[0]


def test_mc_crosscheck():
    vm, _, _ = c.margrabe(c.F1, c.F2, c.S1, c.S2, c.RHO, c.T, c.DF)
    mc0 = c.mc_spread(c.F1, c.F2, 0.0, c.S1, c.S2, c.RHO, c.T, c.DF)
    assert abs(mc0 / vm - 1) < 0.02
    vk, _, _ = c.kirk(c.F1, c.F2, c.K, c.S1, c.S2, c.RHO, c.T, c.DF)
    mck = c.mc_spread(c.F1, c.F2, c.K, c.S1, c.S2, c.RHO, c.T, c.DF)
    assert abs(mck / vk - 1) < 0.05


def test_deltas():
    d1, d2 = c.margrabe_deltas(c.F1, c.F2, c.S1, c.S2, c.RHO, c.T, c.DF)
    assert 0 < d1 < 1 and -1 < d2 < 0
    assert abs(d1 - 0.99 * 0.9105) < 0.01 and abs(d2 + 0.99 * 0.8664) < 0.01
