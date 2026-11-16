"""drop_constant_runs: smeared filler / multi-day zero runs never reach spot_prices_hourly."""
import numpy as np
import pandas as pd

from services.bess_map.db import drop_constant_runs


def _frame(rt, da=None):
    idx = pd.date_range("2025-10-30", periods=len(rt), freq="h")
    return pd.DataFrame({"datetime": idx, "rt_price": rt,
                         "da_price": da if da is not None else [np.nan] * len(rt)})


def test_smeared_filler_run_is_nulled_real_hours_kept():
    real = list(np.linspace(100, 400, 30))
    df = _frame(real + [0.2602534822436943] * 720 + real)
    out = drop_constant_runs(df)
    assert len(out) == 60
    assert not (out["rt_price"] == 0.2602534822436943).any()


def test_multi_day_zero_run_nulled_one_side_only():
    rt = [0.0] * 192
    da = list(np.linspace(200, 500, 192))
    out = drop_constant_runs(_frame(rt, da))
    assert len(out) == 192 and out["rt_price"].isna().all() and out["da_price"].notna().all()


def test_short_flat_stretch_at_price_floor_is_kept():
    rt = list(np.linspace(100, 300, 20)) + [-50.0] * 12 + list(np.linspace(100, 300, 20))
    out = drop_constant_runs(_frame(rt))
    assert len(out) == 52 and (out["rt_price"] == -50.0).sum() == 12


def test_upsert_skips_when_every_hour_is_guarded(monkeypatch):
    # 2026-11-16 backfill: 新疆 2025 chunks were pure filler -> empty batch ->
    # "A value is required for bind parameter 'province'".
    import services.bess_map.db as db
    monkeypatch.setattr(db, "_province_has_nonzero_price", lambda *a: True)

    class Boom:
        def begin(self):
            raise AssertionError("must not touch the DB for an empty batch")
    idx = pd.date_range("2025-03-01", periods=96, freq="h")
    hourly = pd.DataFrame({"rt_price": 0.0396, "da_price": 0.0336}, index=idx)
    hourly.index.name = "datetime"
    db.upsert_hourly_prices(Boom(), "marketdata", "新疆", hourly, "新疆.xlsx")
