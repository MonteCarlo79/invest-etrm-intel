import numpy as np
import pandas as pd
from services.bess_map.price_lab.hybrid import combine, clip_bounds


def test_combine_adds_and_clips():
    level = np.array([300.0, 300.0, 300.0])
    shape = np.array([-400.0, 0.0, 400.0])
    out = combine(level, shape, clip_lo=-80.0, clip_hi=1500.0)
    assert out[0] == -80.0 and out[1] == 300.0 and out[2] == 700.0


def test_combine_nan_propagates():
    out = combine(np.array([np.nan, 300.0]), np.array([10.0, np.nan]), -80.0, 1500.0)
    assert np.isnan(out[0]) and np.isnan(out[1])


def test_clip_bounds_quantiles_and_empty():
    s = pd.Series(np.concatenate([np.full(50, -500.0), np.full(895, 400.0), np.full(55, 3000.0)]))
    lo, hi = clip_bounds(s)
    assert lo <= 400.0 <= hi and lo < 0.0
    assert clip_bounds(pd.Series([], dtype=float)) == (0.0, 1500.0)
