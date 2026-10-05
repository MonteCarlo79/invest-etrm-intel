import numpy as np

from . import compute as c


def test_two_unit_ladder_is_exact():
    for s in [40, 55, 60, 65, 72, 80, 100]:
        direct = c.two_unit_payoff(s)
        ladder = c.option_ladder_payoff(s, [(300.0, 60.0), (200.0, 72.0)])
        assert abs(direct - ladder) < 1e-9


def test_ladder_slopes_match_merit_order():
    # payoff slope: 0 below 60, 300 on (60,72), 500 above 72
    eps = 1e-6
    assert c.two_unit_payoff(50 + eps) - c.two_unit_payoff(50) < 1e-6
    assert abs((c.two_unit_payoff(65 + eps) - c.two_unit_payoff(65)) / eps - 300) < 1e-3
    assert abs((c.two_unit_payoff(80 + eps) - c.two_unit_payoff(80)) / eps - 500) < 1e-3


def test_smooth_payoff_and_qstar():
    assert c.q_star(20) == 0.0
    assert abs(c.q_star(54) - 500.0) < 1e-9
    assert abs(c.smooth_payoff(54) - (500 * 54 - c.cost_q(500))) < 1e-9


def test_replication_error_bound():
    const, ladder = c.build_ladder()
    dense = np.linspace(30, 100, 141)
    errs = [abs(c.replicated_payoff(s, const, ladder) - c.smooth_payoff(s))
            for s in dense]
    assert max(errs) < 50.0                          # absolute, EUR
    assert max(errs) < 0.01 * c.smooth_payoff(100)   # <<1% of full-load payoff
    # interpolation of a convex function lies above it (bar at kinks)
    for s in dense[1:-1]:
        assert c.replicated_payoff(s, const, ladder) >= c.smooth_payoff(s) - 1e-9
    # exact at the kinks
    for k in c.STRIKES:
        assert abs(c.replicated_payoff(k, const, ladder) - c.smooth_payoff(k)) < 1e-9
