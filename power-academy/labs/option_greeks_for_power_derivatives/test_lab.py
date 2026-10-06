from . import compute as c


def test_deltas_match_and_bounded():
    a1, a2 = c.analytic_deltas()
    assert 0 < a1 < 1 and -1 < a2 < 0
    assert abs(a1 - 0.9014) < 0.01 and abs(a2 + 0.8577) < 0.01


def test_gamma_matches_fd():
    assert abs(c.analytic_gamma() - c.fd_gamma()) < 1e-4
    assert abs(c.analytic_gamma() - 0.008532) < 0.0005


def test_vega_matches_fd():
    assert abs(c.analytic_vega() - c.fd_vega()) < 0.05
    assert abs(c.analytic_vega() - 6.403) < 0.05


def test_delta_is_probability_like():
    # deeper ITM -> delta1 -> 1; deeper OTM -> 0
    hi, _ = c.analytic_deltas(f1=200.0)
    lo, _ = c.analytic_deltas(f1=30.0)
    assert hi > 0.985 and lo < 0.01  # DF=0.99 caps delta1 below 1
