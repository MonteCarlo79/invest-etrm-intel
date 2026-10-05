import numpy as np

from . import compute as c


def test_implied_offpeak():
    assert np.allclose(c.implied_offpeak(), [30.0, 35.0, 31.0])


def test_curve_reprices_quotes():
    curve = c.build_curve()
    for m in range(3):
        assert abs(c.block_average(curve, m, "peak") - c.PEAK_Q[m]) < 1e-6
        base = (c.block_average(curve, m, "peak") + c.block_average(curve, m, "offpeak")) / 2
        assert abs(base - c.BASE_Q[m]) < 1e-6


def test_shaped_curve_preserves_block_energy_and_average():
    w = c.normalised_profile({8: 1.3, 9: 1.2, 12: 0.7, 13: 0.7, 18: 1.25, 19: 1.15})
    shaped = c.build_curve(profile=w)
    for m in range(3):
        assert abs(c.block_average(shaped, m, "peak") - c.PEAK_Q[m]) < 1e-6
    # shape actually applied: hour 8 costs more than hour 12
    assert shaped[8] > shaped[12]


def test_normalised_profile_mean_one():
    w = c.normalised_profile({8: 1.3, 12: 0.7})
    assert abs(np.mean([w[h] for h in c.PEAK_H]) - 1.0) < 1e-12
