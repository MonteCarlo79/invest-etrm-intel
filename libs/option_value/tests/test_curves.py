import numpy as np, pandas as pd
from datetime import date
from libs.option_value.curves import build_expected_curve

def _series(n_days, fn):
    idx = pd.date_range("2025-11-01", periods=n_days*24, freq="h")
    return pd.Series([fn(ts) for ts in idx], index=idx)

def test_month_hour_means_exact():
    # price = month_number*1000 + hour → mean curve is exact by construction
    s = _series(400, lambda ts: ts.month*1000.0 + ts.hour)
    c = build_expected_curve(s, date(2026, 11, 30))
    assert c.month_hour_mean.shape == (12, 24)
    assert c.month_hour_mean[2, 7] == 2007.0
    assert not c.insufficient_history
    assert c.hourly.shape == (c.n_days*24,)

def test_incomplete_day_dropped():
    s = _series(400, lambda ts: 100.0 + ts.hour)
    s = s.drop(s.index[37])            # remove one hour of day 2
    c = build_expected_curve(s, date(2026, 11, 30))
    assert c.n_days == 399

def test_insufficient_history_flag():
    s = _series(300, lambda ts: 100.0)  # 300/30.44 ≈ 9.9 months
    c = build_expected_curve(s, date(2026, 8, 26))
    assert c.insufficient_history

def test_floor_cap_and_tile_order():
    s = _series(400, lambda ts: ts.month*1000.0 + ts.hour)
    c = build_expected_curve(s, date(2026, 11, 30))
    assert (c.floor, c.cap) == (1000.0, 12023.0)      # Jan-2026 h0 / Dec h23
    # hourly[0:24] is the FIRST complete day's month-hour row (November → row 11%12)
    assert np.allclose(c.hourly[:24], c.month_hour_mean[11 % 12])
