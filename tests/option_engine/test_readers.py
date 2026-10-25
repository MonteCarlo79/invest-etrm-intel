# test_readers.py — pure transforms only (no DB)
import numpy as np, pandas as pd
from services.option_engine.readers import _normalize_units, _rt_da_fallback

def test_normalize_units_boundary():
    df = pd.DataFrame({"rt_price": [0.35, 0.42, 0.38], "da_price": [320.0, 350.0, 310.0]})
    out, meta = _normalize_units(df)
    assert out["rt_price"].iloc[0] == 350.0      # kWh legacy scaled
    assert out["da_price"].iloc[0] == 320.0      # already MWh, untouched
    assert meta["rt_price"]["scaled"] is True and meta["da_price"]["scaled"] is False

def test_rt_da_fallback_per_hour():
    df = pd.DataFrame({"rt_price": [0.0, np.nan, 400.0], "da_price": [300.0, 310.0, 350.0]})
    out, meta = _rt_da_fallback(df)
    assert out["rt_price"].tolist() == [300.0, 310.0, 400.0] and meta["filled"] == 2

def test_negative_rt_prices_kept():
    # Genuine negative RT prices (山东 has real negative-price hours) must NOT
    # be masked to DA — the capture pipeline behind V_hist keeps them, and the
    # engine must match or the calibration ratio biases upward.
    df = pd.DataFrame({"rt_price": [-50.0, 0.0, 400.0], "da_price": [320.0, 300.0, 350.0]})
    out, meta = _rt_da_fallback(df)
    assert out["rt_price"].tolist() == [-50.0, 300.0, 400.0] and meta["filled"] == 1
    assert meta["negatives_kept"] == 1

def test_normalize_units_mixed_days():
    # Real-data lesson (T8 gate): spot_prices_hourly mixes ¥/kWh and ¥/MWh
    # segments inside one window (山东 flips ~18×/12m). Per-column medians then
    # rescale already-¥/MWh days 1000× — normalization must be per (day, col),
    # with a max-ABS signature: day-median rules misread half-placeholder days
    # (山东 2026-09-24: 13 zeros + 11 genuine 400-512 rows → median 0) and
    # genuinely all-negative ¥/MWh days (山东 2026-01, day median −80).
    idx = pd.date_range("2026-01-01", periods=96, freq="h")
    df = pd.DataFrame({
        "datetime": idx,
        "rt_price": [0.156]*24 + [400.0]*24 + [-80.0]*24 + [0.0]*12 + [512.5]*12,
        "da_price": [0.31]*24 + [380.0]*24 + [390.0]*24 + [0.0]*12 + [500.0]*12,
    })
    out, meta = _normalize_units(df)
    assert out["rt_price"].iloc[0] == 156.0     # ¥/kWh day scaled
    assert out["rt_price"].iloc[24] == 400.0    # ¥/MWh day untouched
    assert out["rt_price"].iloc[48] == -80.0    # all-negative ¥/MWh day untouched
    assert out["rt_price"].iloc[72] == 0.0      # half-placeholder ¥/MWh day: zeros stay
    assert out["rt_price"].iloc[84] == 512.5    # ...and its real rows NOT scaled
    assert out["da_price"].iloc[0] == 310.0     # da leg scaled independently
    assert meta["rt_price"]["days_scaled"] == 1 and meta["rt_price"]["days_total"] == 4
    assert meta["da_price"]["days_scaled"] == 1
