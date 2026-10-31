"""DB-free unit tests for the book forecast job — OLS core, node-candidate
expansion, frame guards. run_day/get_forecast hit the DB and are exercised
by callers, not here (same convention as the settlement bridge tests).
"""
from datetime import date

import numpy as np

from libs.trading_book.registry import get_station
from services.mengxi_nodal.book_forecast import (
    _HISTORY_SQL,
    _candidate_nodes,
    _history_window,
    _ols_time_forecast,
)


def test_forecast_returns_96_finite():
    rng = np.random.default_rng(7)
    hist = [np.full(96, 300.0) + rng.normal(0, 20, 96) for _ in range(30)]
    fc = _ols_time_forecast(hist)
    assert len(fc) == 96 and np.all(np.isfinite(fc))


def test_forecast_handles_short_history():
    fc = _ols_time_forecast([np.full(96, 250.0)] * 3)
    assert len(fc) == 96 and np.allclose(fc, 250.0)


def test_forecast_tolerates_nan_slots():
    hist = [np.full(96, 300.0) for _ in range(10)]
    hist[3][10] = np.nan  # one missing slot in one day
    hist[5][11:20] = np.nan  # a partial day
    fc = _ols_time_forecast(hist)
    assert len(fc) == 96 and np.all(np.isfinite(fc))
    assert np.allclose(fc, 300.0, atol=1e-6)  # constant input -> constant fc


def test_forecast_empty_history_is_nan_not_crash():
    fc = _ols_time_forecast([])
    assert len(fc) == 96 and not np.isfinite(fc).any()


def test_candidate_nodes_expands_ranges_and_fragments():
    # siziwangqi has no own node — parents only, range shorthand
    assert _candidate_nodes(get_station("siziwangqi")) == [
        "内蒙.杜尔伯特站/220kV.1M", "内蒙.杜尔伯特站/220kV.2M"]
    suyou = _candidate_nodes(get_station("suyou"))
    assert suyou[0] == "内蒙.景蓝乌尔图储能电站/220kV.1M"  # own node first
    # bare fragments continue the previous entry's prefix
    assert "内蒙.苏尼特站/220kV.2M" in suyou
    assert "内蒙.苏尼特站/220kV.4M" in suyou
    assert "内蒙.苏尼特站/500kV.1M" in suyou
    # hangjinqi parent range expands to 4 literal node names
    hangjinqi = _candidate_nodes(get_station("hangjinqi"))
    assert hangjinqi[0] == "内蒙.悦杭独贵储能电站/220kV.1M"
    assert hangjinqi[1:] == [f"内蒙.谷山梁站/220kV.{i}M" for i in range(1, 5)]


def test_history_window_and_sql_naive_cst():
    s, e = _history_window(date(2026, 11, 1))
    assert s == "2026-09-02 00:00:00" and e == "2026-11-01 00:00:00"
    assert "+08" not in s and "+08" not in e  # POSIX-inversion guard
    assert "md_mengxi_nodal_price_96_filled" in _HISTORY_SQL
    assert "+08" not in _HISTORY_SQL
    assert "AT TIME ZONE" not in _HISTORY_SQL
