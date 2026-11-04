# tests/bess_map/test_upsert_capture_daily.py
"""Upsert must not discard a realized-only recompute (fossilization, second half).

The 2026-10-05 guard fix made NaN-realized days eligible for recompute, but
upsert_capture_daily returned early whenever the theoretical series was empty
(theoretical branch skips as up-to-date) and drove the row index from the
theoretical series alone — so a healed realized series was computed and then
thrown away (安徽 2026-10-05/07/10 stayed NaN through a full --force pass).

Fixed behaviour:
- empty theoretical + non-empty realized → rows written for the realized days
- ON CONFLICT SET overwrites only what this run recomputed (a skipped branch
  must not NULL out existing values)
- capture_rate is overwritten only when both sides were recomputed
- union index: days present in only one series are still upserted
"""
import datetime as dt
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "services" / "bess_map"))
import run_capture_pipeline as rcp  # noqa: E402

ARGS = ("marketdata", "安徽", "ols_rt_time_v1", 4.0, 1.0, 0.85)
COLS = ["province", "date", "model", "duration_h", "power_mw", "roundtrip_eff",
        "realized_profit", "realized_profit_per_mwh_day",
        "theoretical_profit", "theoretical_profit_per_mwh_day",
        "capture_rate", "capturable_profit_per_mwh_day"]


def _capture(realized, theoretical):
    """Run upsert_capture_daily with a fake engine; return {sql, rows} actually sent."""
    captured = {}
    orig = rcp._execute_values
    rcp._execute_values = lambda conn, sql, rows: captured.update(sql=sql, rows=rows)
    try:
        rcp.upsert_capture_daily(MagicMock(), *ARGS,
                                 realized_profit_by_day=realized,
                                 theoretical_profit_by_day=theoretical)
    finally:
        rcp._execute_values = orig
    return captured


def test_realized_only_still_writes():
    realized = pd.Series([100.0, 200.0],
                         index=[dt.date(2026, 10, 5), dt.date(2026, 10, 7)])
    captured = _capture(realized, pd.Series(dtype=float))
    assert captured, "realized-only recompute must not return early"
    assert len(captured["rows"]) == 2
    assert "realized_profit = EXCLUDED.realized_profit" in captured["sql"]


def test_skipped_branch_not_nulled_on_conflict():
    realized = pd.Series([100.0], index=[dt.date(2026, 10, 5)])
    captured = _capture(realized, pd.Series(dtype=float))
    sql = captured["sql"]
    assert "theoretical_profit = EXCLUDED.theoretical_profit" not in sql
    assert "capture_rate = EXCLUDED.capture_rate" not in sql
    # realized-side columns still overwritten
    assert "realized_profit = EXCLUDED.realized_profit" in sql
    assert "capturable_profit_per_mwh_day = EXCLUDED.capturable_profit_per_mwh_day" in sql


def test_both_series_recomputed_overwrites_everything():
    days = [dt.date(2026, 10, 5)]
    captured = _capture(pd.Series([100.0], index=days),
                        pd.Series([400.0], index=days))
    sql = captured["sql"]
    for col in ["realized_profit", "theoretical_profit", "capture_rate",
                "capturable_profit_per_mwh_day"]:
        assert f"{col} = EXCLUDED.{col}" in sql
    # capture_rate = (100/4) / (400/4) = 0.25  (e_cap = power 1 MW × 4 h)
    rec = dict(zip(COLS, captured["rows"][0]))
    assert rec["capture_rate"] == pytest.approx(0.25)
    assert rec["realized_profit_per_mwh_day"] == pytest.approx(25.0)


def test_union_index_covers_days_present_in_one_series_only():
    theo = pd.Series([400.0, 410.0], index=[dt.date(2026, 10, 1), dt.date(2026, 10, 2)])
    realized = pd.Series([100.0, 110.0], index=[dt.date(2026, 10, 2), dt.date(2026, 10, 3)])
    captured = _capture(realized, theo)
    dates = sorted(r[1] for r in captured["rows"])
    assert dates == [dt.date(2026, 10, 1), dt.date(2026, 10, 2), dt.date(2026, 10, 3)]


def test_both_empty_is_a_noop():
    captured = _capture(pd.Series(dtype=float), pd.Series(dtype=float))
    assert captured == {}
