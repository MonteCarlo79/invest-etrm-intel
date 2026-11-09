# tests/bess_map/test_zero_day_guard.py
"""Placeholder-zero-day guard for the LingFeng price loader.

2026-09-16/17/18 + 2026-09-24 蒙西: the SaaS publishes zero-filled frames for
unpublished days; written into spot_prices_hourly they poisoned every RT
fallback (the nodal dry-run formed strategies against a 0.0 price level).
"""
from datetime import datetime, timedelta

import pandas as pd
import pytest

from services.bess_map.db import drop_placeholder_zero_days


def _frame(day_values: dict) -> pd.DataFrame:
    rows = []
    for d, vals in day_values.items():
        for h, v in enumerate(vals):
            rows.append({"datetime": datetime(d.year, d.month, d.day, h),
                         "rt_price": v, "da_price": v})
    return pd.DataFrame(rows)


D1 = datetime(2026, 9, 23).date()
D2 = datetime(2026, 9, 24).date()


def test_drops_all_zero_day_in_active_market():
    df = _frame({D1: [300.0] * 24, D2: [0.0] * 24})
    out = drop_placeholder_zero_days(df, active_market=True)
    assert set(pd.to_datetime(out["datetime"]).dt.date) == {D1}


def test_drops_nan_only_day_as_placeholder():
    df = _frame({D1: [300.0] * 24, D2: [None] * 24})
    out = drop_placeholder_zero_days(df, active_market=True)
    assert set(pd.to_datetime(out["datetime"]).dt.date) == {D1}


def test_keeps_legit_zero_hours_mixed_day():
    # 蒙西 2026-09-15 pattern: 13 zero hours but real prices elsewhere — kept
    df = _frame({D1: [0.0] * 13 + [240.0] * 11})
    out = drop_placeholder_zero_days(df, active_market=True)
    assert len(out) == 24


def test_keeps_all_zero_day_for_no_market_province():
    # provinces with no spot disclosure publish genuine zeros — not placeholders
    df = _frame({D1: [0.0] * 24, D2: [0.0] * 24})
    out = drop_placeholder_zero_days(df, active_market=False)
    assert len(out) == 48


def test_empty_frame_is_safe():
    out = drop_placeholder_zero_days(pd.DataFrame(columns=["datetime", "rt_price"]),
                                     active_market=True)
    assert out.empty


# ── Per-side placeholders (2026-11-09 audit) ─────────────────────────────────
# LingFeng also zero-fills ONE side: 广东/海南/广西 DA all-zero while RT is real
# (still arriving 2026-11), and RT-zero/DA-real "shift" days (黑龙江 2025-07-15).
# Only the placeholder side is nulled; the real side must survive.

def _frame2(day_rt_da: dict) -> pd.DataFrame:
    rows = []
    for d, (rt, da) in day_rt_da.items():
        for h in range(24):
            rows.append({"datetime": datetime(d.year, d.month, d.day, h),
                         "rt_price": rt[h], "da_price": da[h]})
    return pd.DataFrame(rows)


def test_da_only_zero_day_nulls_da_keeps_rt():
    df = _frame2({D1: ([400.0] * 24, [0.0] * 24)})
    out = drop_placeholder_zero_days(df, active_market=True)
    assert len(out) == 24
    assert out["rt_price"].tolist() == [400.0] * 24
    assert out["da_price"].isna().all()


def test_rt_only_zero_day_nulls_rt_keeps_da():
    df = _frame2({D1: ([0.0] * 24, [445.0] * 24)})
    out = drop_placeholder_zero_days(df, active_market=True)
    assert len(out) == 24
    assert out["rt_price"].isna().all()
    assert out["da_price"].tolist() == [445.0] * 24


def test_partial_zero_side_untouched():
    # real low-price hours (东北 heating season) stay as genuine 0
    df = _frame2({D1: ([0.0] * 20 + [50.0] * 4, [0.0] * 18 + [60.0] * 6)})
    out = drop_placeholder_zero_days(df, active_market=True)
    assert (out["rt_price"] == 0).sum() == 20
    assert (out["da_price"] == 0).sum() == 18


def test_one_side_zero_untouched_in_inactive_market():
    df = _frame2({D1: ([400.0] * 24, [0.0] * 24)})
    out = drop_placeholder_zero_days(df, active_market=False)
    assert (out["da_price"] == 0).all()


def test_upsert_sends_null_not_nan(monkeypatch):
    # psycopg2 writes float NaN as 'NaN'::float — COALESCE would not skip it
    import services.bess_map.db as dbm
    captured = {}

    class _Conn:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def execute(self, sql, recs=None): captured["recs"] = recs

    class _Eng:
        def begin(self): return _Conn()

    monkeypatch.setattr(dbm, "_province_has_nonzero_price", lambda *a: True)
    df = _frame2({D1: ([400.0] * 24, [0.0] * 24)}).set_index("datetime")
    dbm.upsert_hourly_prices(_Eng(), "marketdata", "广东", df, "f.xlsx")
    assert len(captured["recs"]) == 24
    assert all(r["da_price"] is None for r in captured["recs"])
    assert all(r["rt_price"] == 400.0 for r in captured["recs"])
