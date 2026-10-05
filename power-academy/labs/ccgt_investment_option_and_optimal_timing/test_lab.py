from . import compute as c


def test_static_breakeven():
    assert abs(c.static_breakeven() - 16.0) < 0.05
    assert abs(c.build_value(16.0)) < 1.0


def test_ou_trigger_above_static_in_band():
    _, _, trig = c.lattice()
    sb = c.static_breakeven()
    assert 1.25 <= trig / sb <= 1.60
    assert 20.0 <= trig <= 25.0


def test_option_alive_at_long_run_mean():
    assert c.build_value(c.THETA) <= 0.0          # static says don't build
    assert c.option_value(c.THETA) > 15.0         # option to wait is valuable


def test_trigger_rises_with_volatility():
    _, _, trig_base = c.lattice()
    old = c.SIGMA
    try:
        c.SIGMA = 5.0
        _, _, trig_hi = c.lattice()
    finally:
        c.SIGMA = old
    assert trig_hi > trig_base
