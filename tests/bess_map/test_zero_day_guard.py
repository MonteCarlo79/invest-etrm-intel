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
