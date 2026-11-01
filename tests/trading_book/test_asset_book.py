"""DB-free unit tests for AssetBook readers — SQL text + window/alignment only.

No RDS in unit tests: the readers themselves are exercised against the live
DB by callers, not here. The Task 11 data_path guards run BEFORE any engine
use, so engine=None suffices to exercise them.
"""
from datetime import date

import pandas as pd
import pytest

from libs.trading_book.asset_book import (
    AssetBook,
    _ACTUAL_SQL, _CLEARED_SQL, _STRATEGY_SQL, _cst_naive_window, _to_96,
)
from libs.trading_book.registry import get_station


def test_window_is_naive_cst():
    s, e = _cst_naive_window(date(2026, 11, 1))
    assert s == "2026-11-01 00:00:00" and e == "2026-11-02 00:00:00"
    assert "+08" not in s and "+08" not in e  # POSIX-inversion guard


def test_sql_targets_right_tables():
    assert "md_id_cleared_energy" in _CLEARED_SQL
    assert "ops_bess_dispatch_15min" in _ACTUAL_SQL
    assert "Asia/Shanghai" not in _CLEARED_SQL  # columns already CST-naive; no rendering needed


def test_strategy_sql_reads_nodal_agent_family():
    # nodal_strategy_daily has no scope column — the nodal-agent scope is the
    # model_version family ('nodal_agent_v1', …), latest created_at wins.
    assert "nodal_strategy_daily" in _STRATEGY_SQL
    assert "nodal_agent" in _STRATEGY_SQL
    assert "target_date" in _STRATEGY_SQL


def test_to_96_aligns_quarter_hours():
    df = pd.DataFrame({
        "ts": ["2026-11-01 00:00:00", "2026-11-01 00:15:00", "2026-11-01 23:45:00"],
        "v": [1.0, 2.0, 96.0],
    })
    out = _to_96(df, "ts", "v")
    assert len(out) == 96
    assert out[0] == 1.0 and out[1] == 2.0 and out[95] == 96.0
    assert out[2] != out[2]  # NaN for slots with no source row
    empty = _to_96(df.iloc[0:0], "ts", "v")
    assert len(empty) == 96 and all(x != x for x in empty)  # all NaN


# ── Task 11: data_path guards — honest degradation, never silent NaN ────────

DAY = date(2026, 10, 15)


def test_registry_only_station_strategy_layer_raises():
    # wuchuan: registry_only — cleared/actual/strategies/forecast raise with a
    # clear message before any engine use (engine=None proves the ordering).
    book = AssetBook(engine=None, station=get_station("wuchuan"))
    for meth in ("cleared", "actual", "strategies", "forecast"):
        with pytest.raises(NotImplementedError, match="no data path yet"):
            getattr(book, meth)(DAY)


def test_settlement_only_wind_strategy_layer_raises():
    # lingt46: wind settlement_only — no strategy layer at all.
    book = AssetBook(engine=None, station=get_station("lingt46"))
    for meth in ("cleared", "actual", "strategies", "forecast"):
        with pytest.raises(NotImplementedError, match="no strategy layer"):
            getattr(book, meth)(DAY)


def test_settlement_only_bess_strategy_layer_raises():
    # wuhai is BESS but settlement_only (no node/ops path yet) — same guard.
    book = AssetBook(engine=None, station=get_station("wuhai"))
    for meth in ("cleared", "actual", "strategies", "forecast"):
        with pytest.raises(NotImplementedError, match="no strategy layer"):
            getattr(book, meth)(DAY)


def test_full_station_guard_passes_through():
    # suyou (full): the guard is a no-op — reads proceed to the engine.
    book = AssetBook(engine=None, station=get_station("suyou"))
    book._require_strategy_layer()  # must not raise


def _settled_frame():
    return pd.DataFrame({
        "data_date": [date(2026, 10, 5)],
        "amount_cny": [1000.0],
        "accrual_grade": ["bill"]})


def test_settlement_works_for_settlement_only_wind(monkeypatch):
    # settlement() is NOT guarded: the Task 4 facade serves any asset_code.
    monkeypatch.setattr(pd, "read_sql",
                        lambda sql, engine, params=None: _settled_frame())
    book = AssetBook(engine=None, station=get_station("lingt46"))
    s = book.settlement("2026-10")
    assert s.period == "2026-10" and s.settled_cny == 1000.0
    assert s.daily == [(date(2026, 10, 5), 1000.0, "bill")]


def test_attribution_works_for_settlement_only_wind(monkeypatch, capsys):
    # attribution() is NOT guarded: zero attribution rows + settled rows ->
    # degenerate waterfall (residual = settled) with the loud warning.
    def fake_read_sql(sql, engine, params=None):
        s = str(sql)
        if "bess_asset_daily_attribution" in s:
            return pd.DataFrame([{
                "n_rows": 0, "strategy_pnl": None,
                "forecast_error_loss": None, "strategy_selection": None,
                "execution_clearing_loss": None, "realisation_gap": None}])
        if "book_settled_daily" in s:
            return _settled_frame()
        raise AssertionError(f"unexpected SQL: {s[:120]}")

    monkeypatch.setattr(pd, "read_sql", fake_read_sql)
    book = AssetBook(engine=None, station=get_station("lingt46"))
    w = book.attribution(date(2026, 10, 1), date(2026, 10, 31))
    assert w.strategy_pnl_cny == 0.0 and w.settled_pnl_cny == 1000.0
    resid = [l for l in w.lines if l.name == "settlement_residual"][0]
    assert resid.delta_cny == 1000.0
    assert "WARNING" in capsys.readouterr().out
