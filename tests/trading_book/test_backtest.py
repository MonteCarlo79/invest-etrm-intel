"""DB-free tests for the backtest ladder reader + summariser — Task 14 of the
trading-book plan (2026-10-31).

get_backtest is driven with a monkeypatched pd.read_sql (no RDS); summarise is
pure pandas on a synthetic ladder frame.
"""
from datetime import date
from decimal import Decimal

import pandas as pd
import pytest

from libs.trading_book.backtest import (
    CHART_COLUMNS,
    LADDER_COLUMNS,
    _BACKTEST_SQL,
    get_backtest,
    summarise,
)


def _ladder_df() -> pd.DataFrame:
    """3 covered days; day 2 has NULL strategy-rung cells (partial scenario
    coverage) to exercise skip semantics."""
    return pd.DataFrame({
        "trade_date": pd.to_datetime(
            ["2026-10-01", "2026-10-02", "2026-10-03"]),
        "pf_unrestricted_pnl": [1100.0, 1200.0, 1300.0],
        "pf_grid_feasible_pnl": [1000.0, 1000.0, 1000.0],
        "tt_forecast_optimal_pnl": [950.0, 950.0, 950.0],
        "tt_strategy_pnl": [900.0, None, 900.0],
        "nominated_pnl": [850.0, 850.0, None],
        "cleared_actual_pnl": [800.0, 800.0, 800.0],
        "grid_restriction_loss": [100.0, 200.0, 300.0],
        "forecast_error_loss": [50.0, 50.0, 50.0],
        "strategy_error_loss": [50.0, 50.0, 50.0],
        "nomination_loss": [50.0, 50.0, 50.0],
        "execution_clearing_loss": [50.0, 50.0, 50.0],
        "realisation_gap_vs_pf": [300.0, 400.0, 500.0],
        "realisation_gap_vs_pf_grid": [200.0, 200.0, 200.0],
    })


# ----------------------------------------------------------------- SQL shape

def test_sql_targets_daily_attribution_table():
    assert "reports.bess_asset_daily_attribution" in _BACKTEST_SQL
    assert "asset_code" in _BACKTEST_SQL
    assert "ORDER BY trade_date" in _BACKTEST_SQL
    for col in LADDER_COLUMNS:
        assert col in _BACKTEST_SQL, col


def test_sql_uses_naive_cst_date_bounds():
    # Carried frame convention: naive CST literals via CAST(:s AS date);
    # never a '+08' suffix or AT TIME ZONE rendering.
    assert "CAST(:s AS date)" in _BACKTEST_SQL
    assert "CAST(:e AS date)" in _BACKTEST_SQL
    assert "+08" not in _BACKTEST_SQL
    assert "AT TIME ZONE" not in _BACKTEST_SQL.upper()


def test_chart_columns_are_brief_rungs_in_order():
    assert CHART_COLUMNS == ["nominated_pnl", "tt_strategy_pnl",
                             "cleared_actual_pnl", "pf_grid_feasible_pnl"]
    assert set(CHART_COLUMNS) <= set(LADDER_COLUMNS)


# ----------------------------------------------------------------- reader

def test_get_backtest_applies_attribution_alias(monkeypatch):
    """bameng (registry) -> hetao (attribution stack), same mapping the
    waterfall uses; date bounds go out as naive ISO literals."""
    captured = {}

    def fake_read_sql(sql, engine, params=None):
        captured.update(params or {})
        return _ladder_df()

    monkeypatch.setattr("libs.trading_book.backtest.pd.read_sql", fake_read_sql)
    df = get_backtest(engine=None, asset_code="bameng",
                      start=date(2026, 10, 1), end=date(2026, 10, 31))
    assert captured == {"ac": "hetao", "s": "2026-10-01", "e": "2026-10-31"}
    assert len(df) == 3
    assert str(df["trade_date"].dtype).startswith("datetime64")
    for col in LADDER_COLUMNS:
        assert pd.api.types.is_float_dtype(df[col]), col


def test_get_backtest_coerces_decimal_numeric(monkeypatch):
    """DB numeric columns arrive as Decimal on real reads — the reader must
    coerce to float64 (Decimal would break plotly in the tab)."""
    def fake_read_sql(sql, engine, params=None):
        return pd.DataFrame({
            "trade_date": [date(2026, 10, 1)],
            "nominated_pnl": [Decimal("850.5")],
        })

    monkeypatch.setattr("libs.trading_book.backtest.pd.read_sql", fake_read_sql)
    df = get_backtest(None, "suyou", date(2026, 10, 1), date(2026, 10, 1))
    assert pd.api.types.is_float_dtype(df["nominated_pnl"])
    assert df["nominated_pnl"].iloc[0] == pytest.approx(850.5)


def test_get_backtest_empty_keeps_full_columns(monkeypatch):
    monkeypatch.setattr("libs.trading_book.backtest.pd.read_sql",
                        lambda *a, **k: pd.DataFrame())
    df = get_backtest(None, "suyou", date(2026, 10, 1), date(2026, 10, 31))
    assert list(df.columns) == ["trade_date"] + LADDER_COLUMNS
    assert df.empty


# ----------------------------------------------------------------- summarise

def test_summarise_totals_per_column():
    s = summarise(_ladder_df())
    assert s["days"] == 3
    t = s["totals"]
    assert t["pf_unrestricted_pnl"] == pytest.approx(3600.0)
    assert t["pf_grid_feasible_pnl"] == pytest.approx(3000.0)
    # NaN cells skipped within present rows
    assert t["tt_strategy_pnl"] == pytest.approx(1800.0)
    assert t["nominated_pnl"] == pytest.approx(1700.0)
    assert t["cleared_actual_pnl"] == pytest.approx(2400.0)
    assert t["realisation_gap_vs_pf"] == pytest.approx(1200.0)
    assert t["realisation_gap_vs_pf_grid"] == pytest.approx(600.0)


def test_summarise_strategy_selection_cost_rowwise():
    """Σ(nominated − tt_strategy) over rows having BOTH rungs — day 2 has
    tt_strategy NULL, day 3 has nominated NULL, so only day 1 counts:
    850 − 900 = −50. Totals-of-columns would give −100 and be wrong here;
    this matches the waterfall's SUM(nominated_pnl - tt_strategy_pnl)."""
    s = summarise(_ladder_df())
    assert s["strategy_selection_cost"] == pytest.approx(-50.0)


def test_summarise_loss_bucket_aliases():
    s = summarise(_ladder_df())
    assert s["nomination_loss"] == pytest.approx(150.0)
    assert s["execution_clearing_loss"] == pytest.approx(150.0)
    assert s["realisation_gap_vs_pf_grid"] == pytest.approx(600.0)


def test_summarise_capture_ratios():
    s = summarise(_ladder_df())
    # cleared_actual / nominated = 2400 / 1700
    assert s["capture_actual_vs_nominated"] == pytest.approx(2400.0 / 1700.0)
    # nominated / pf_grid_feasible = 1700 / 3000
    assert s["capture_nominated_vs_pf_grid"] == pytest.approx(1700.0 / 3000.0)


def test_summarise_zero_denominator_gives_none_not_fabricated():
    df = _ladder_df()
    df["nominated_pnl"] = [0.0, 0.0, 0.0]
    s = summarise(df)
    assert s["capture_actual_vs_nominated"] is None
    assert s["capture_nominated_vs_pf_grid"] == pytest.approx(0.0)


def test_summarise_empty_frame():
    s = summarise(pd.DataFrame(columns=["trade_date"] + LADDER_COLUMNS))
    assert s["days"] == 0
    assert s["totals"]["nominated_pnl"] == 0.0
    assert s["strategy_selection_cost"] == 0.0
    assert s["capture_actual_vs_nominated"] is None
    assert s["capture_nominated_vs_pf_grid"] is None
