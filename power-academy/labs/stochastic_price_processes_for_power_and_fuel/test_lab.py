import numpy as np

import compute as c


def shifted_mean():
    # positive jumps lift the stationary mean above theta by lambda*mu_J/kappa
    return c.THETA + c.LAMBDA * c.JUMP_MU / c.KAPPA


def test_mean_reversion_to_shifted_mean():
    x, _ = c.simulate_mrjd()
    tail = x[len(x) // 2:]
    assert abs(tail.mean() - shifted_mean()) < 0.05


def test_jump_count_within_twenty_percent():
    _, nj = c.simulate_mrjd()
    assert abs(nj - c.LAMBDA) <= 0.3 * c.LAMBDA


def test_stationary_std_bounded_unlike_gbm():
    x, _ = c.simulate_mrjd()
    tail = x[len(x) // 2:]
    # diffusion-only stationary std is ~0.088; jumps add variance but it stays bounded
    assert 0.05 < tail.std() < 0.7
    # the path does not wander off: max deviation from the shifted mean stays finite
    assert np.abs(tail - shifted_mean()).max() < 3.5


def test_half_life():
    assert abs(c.half_life_days() - 4.8) < 0.2


def test_deterministic_seed():
    x1, _ = c.simulate_mrjd()
    x2, _ = c.simulate_mrjd()
    assert np.array_equal(x1, x2)
