from . import compute as c


def test_hill_estimate_recovers_alpha():
    a = c.hill_estimate(c.pareto_sample())
    assert abs(a - 3.0) < 0.4


def test_pareto_sampler_respects_threshold():
    s = c.pareto_sample()
    assert s.min() >= c.U_THR


def test_closed_form_spike_call():
    assert abs(c.pareto_call(100.0, 3.0, 150.0) - 22.222) < 0.01
    assert abs(c.pareto_call(100.0, 3.0, 300.0) - 5.556) < 0.01


def test_gaussian_underprices_far_tail_not_near_money():
    near_p = c.pareto_call(100.0, 3.0, 150.0)
    near_g = c.gaussian_call_matched(100.0, 3.0, 150.0)
    assert near_g > near_p                       # Gaussian fatter near the mean
    far_p = c.pareto_call(100.0, 3.0, 300.0)
    far_g = c.gaussian_call_matched(100.0, 3.0, 300.0)
    assert abs(far_g - 1.46) < 0.25
    assert far_p / far_g > 3.0                   # heavy underpricing far OTM
    deeper = c.pareto_call(100.0, 3.0, 400.0) / c.gaussian_call_matched(100.0, 3.0, 400.0)
    assert deeper > 10.0


def test_pareto_moments():
    m, sd = c.pareto_moments(100.0, 3.0)
    assert abs(m - 150.0) < 1e-9 and abs(sd - 86.6025) < 0.01
