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
