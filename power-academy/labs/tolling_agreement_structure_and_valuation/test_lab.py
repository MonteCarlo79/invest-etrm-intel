from . import compute as c


def test_unconstrained_strip_value():
    assert abs(c.unconstrained_strip() - 156_924) < 500


def test_month_option_value_formula():
    # deep ITM: option value ~ forward
    assert abs(c.month_option_value(60.0, sig=0.0) - 60.0) < 1e-9
    # at the money: E[max(N(0,sig),0)] = sig / sqrt(2pi)
    assert abs(c.month_option_value(0.0, 12.0) - 12.0 / (2 * 3.14159265) ** 0.5) < 1e-6


def test_restart_limit_curve():
    m = c.daily_margins()
    u = c.unconstrained_month(m)
    assert abs(u - 907) < 10
    assert abs(c.dp_restart_limit(m, 10) - u) < 1e-6
    v1, v2, v3 = (c.dp_restart_limit(m, k) for k in (1, 2, 3))
    assert v1 < v2 < v3 <= u
    assert abs((u - v1) / u - 0.122) < 0.02
    assert abs((u - v3) / u - 0.004) < 0.02


def test_buyer_breakeven_premium():
    # buyer pays premium + fees; breakeven premium = strip value
    u = c.unconstrained_strip()
    premium_breakeven = u
    assert premium_breakeven > 0
