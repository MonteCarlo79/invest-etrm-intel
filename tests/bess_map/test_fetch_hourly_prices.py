# tests/bess_map/test_fetch_hourly_prices.py
"""RT fallback semantics (2026-11-09).

A genuine 0 ¥/MWh RT hour (东北 heating season: 黑龙江 floor is 0) is a real price
and must NOT be replaced by DA — the old `.replace(0, nan).fillna(da)` inflated
capture. Placeholder zeros are now nulled at ingest + historically, so only a
NULL RT falls back to DA.
"""
from datetime import datetime

import pandas as pd

import services.bess_map.run_capture_pipeline as rcp


def _patch(monkeypatch, rows):
    df = pd.DataFrame(rows, columns=["datetime", "rt_price", "da_price"])
    monkeypatch.setattr(rcp.pd, "read_sql", lambda *a, **k: df.copy())


def test_real_zero_rt_hour_is_kept(monkeypatch):
    _patch(monkeypatch, [(datetime(2026, 1, 20, 3), 0.0, 85.0)])
    out = rcp.fetch_hourly_prices(None, "marketdata", "黑龙江")
    assert out["rt_price"].iloc[0] == 0.0


def test_null_rt_falls_back_to_da(monkeypatch):
    _patch(monkeypatch, [(datetime(2026, 9, 7, 3), None, 241.7)])
    out = rcp.fetch_hourly_prices(None, "marketdata", "辽宁")
    assert out["rt_price"].iloc[0] == 241.7


def test_both_null_stays_nan(monkeypatch):
    _patch(monkeypatch, [(datetime(2026, 1, 21, 3), None, None)])
    out = rcp.fetch_hourly_prices(None, "marketdata", "黑龙江")
    assert pd.isna(out["rt_price"].iloc[0])
