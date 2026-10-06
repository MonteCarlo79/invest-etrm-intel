from . import compute as c


def test_diffusion_dynamic_cuts_hedge_error():
    p = c.simulate(jumps=False)
    es, ed = c.hedge_errors(p)
    assert ed.std() < 0.5 * es.std()
    assert abs(ed.mean()) < 1.0 and abs(es.mean()) < 1.0


def test_jumps_break_both_regimes():
    p = c.simulate(jumps=True)
    es, ed = c.hedge_errors(p)
    ps = c.simulate(jumps=False)
    es0, ed0 = c.hedge_errors(ps)
    # gap risk: mean error turns negative (short the tail) and std jumps
    assert es.mean() < -10.0
    assert es.std() > 3 * es0.std()
    # rebalancing does NOT rescue the jumpy case (unlike the diffusion case)
    assert ed.std() > 3 * ed0.std()


def test_deterministic():
    a = c.simulate(jumps=True, seed=1)
    b = c.simulate(jumps=True, seed=1)
    import numpy as np
    assert np.array_equal(a, b)
