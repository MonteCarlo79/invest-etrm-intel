import numpy as np

from . import compute as c


def test_implied_offpeak_uses_actual_block_hours():
    # calendar per month: 8/7/7 peak days -> 96/84/84 peak h, 144/156/156 off-peak h
    op = c.implied_offpeak()
    expected = [(c.block_hours(m, "peak") + c.block_hours(m, "offpeak")) * c.BASE_Q[m]
                for m in range(3)]
    expected = [(expected[m] - c.block_hours(m, "peak") * c.PEAK_Q[m])
                / c.block_hours(m, "offpeak") for m in range(3)]
    assert np.allclose(op, expected)
    assert np.allclose(op, [36.6667, 44.2308, 38.8462], atol=1e-3)


def test_curve_reprices_quotes_volume_weighted():
    curve = c.build_curve()
    for m in range(3):
        assert abs(c.block_average(curve, m, "peak") - c.PEAK_Q[m]) < 1e-6
        n_pk = sum(c.is_peak_hour(m, d, h) for d in range(c.DAYS) for h in range(24))
        n_op = c.DAYS * 24 - n_pk
        base = (n_pk * c.block_average(curve, m, "peak")
                + n_op * c.block_average(curve, m, "offpeak")) / (n_pk + n_op)
        assert abs(base - c.BASE_Q[m]) < 1e-6


def test_equal_hours_special_case():
    # with a 12/12 split the general formula reduces to 2*base - peak
    f_op_general = (24 * 50.0 - 12 * 70.0) / 12
    assert abs(f_op_general - (2 * 50.0 - 70.0)) < 1e-12


def test_shaped_curve_preserves_block_energy_and_average():
    w = c.normalised_profile({8: 1.3, 9: 1.2, 12: 0.7, 13: 0.7, 18: 1.25, 19: 1.15})
    shaped = c.build_curve(profile=w)
    for m in range(3):
        assert abs(c.block_average(shaped, m, "peak") - c.PEAK_Q[m]) < 1e-6
    assert shaped[8] > shaped[12]


def test_normalised_profile_mean_one():
    w = c.normalised_profile({8: 1.3, 12: 0.7})
    assert abs(np.mean([w[h] for h in c.PEAK_H]) - 1.0) < 1e-12
