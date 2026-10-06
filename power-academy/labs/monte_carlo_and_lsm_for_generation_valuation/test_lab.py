import numpy as np

from . import compute as c


def _results():
    train = c.simulate(c.N_TRAIN, seed=c.SEED)
    replay = c.simulate(c.N_REPLAY, seed=c.SEED + 1000)
    coefs = c.lsm_train(train)
    return (c.greedy(replay).mean(),
            c.lsm_replay(replay, coefs).mean(),
            c.dp_perfect_foresight(replay).mean())


def test_bias_bracket_ordering():
    g, l, d = _results()
    assert g < l < d


def test_lsm_captures_most_of_foresight():
    _, l, d = _results()
    assert l >= 0.85 * d


def test_worked_example_numbers():
    g, l, d = _results()
    assert abs(g - 50.7) < 1.5
    assert abs(l - 62.3) < 1.5
    assert abs(d - 73.1) < 1.5


def test_deterministic_seeding():
    a = c.simulate(10, seed=1)
    b = c.simulate(10, seed=1)
    assert np.array_equal(a, b)
