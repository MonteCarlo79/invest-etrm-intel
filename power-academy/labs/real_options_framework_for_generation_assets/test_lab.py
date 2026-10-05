import compute as c


def test_closed_form_numbers():
    assert abs(c.beta1() - 2.843) < 0.01
    assert abs(c.wait_multiplier() - 1.543) < 0.005
    assert abs(c.build_trigger() - 154.3) < 0.5
    assert abs(c.perpetual_value(100.0) - 15.81) < 0.15


def test_volatility_raises_wait_premium():
    assert c.wait_multiplier(sigma=0.30) > c.wait_multiplier(sigma=0.25)


def test_tree_value_positive_and_above_immediate_exercise():
    v = c.tree_value(T=0.5, n=100)
    assert v > 0.0                      # option alive even though V < I
    assert v > max(c.V0 - c.I, 0.0)     # waiting beats exercising now


def test_tree_grows_with_horizon_and_converges():
    v1 = c.tree_value(T=1.0, n=200)
    v3 = c.tree_value(T=3.0, n=300)
    v5 = c.tree_value(T=5.0, n=400)
    assert v1 < v3 < v5
    assert abs(v5 / c.perpetual_value(100.0) - 1) < 0.20
