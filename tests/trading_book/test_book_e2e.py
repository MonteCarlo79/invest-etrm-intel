"""End-to-end AssetBook interface test (plan task 9) — fakes only, no RDS.

The brief's interface check plus a full fake-driven chain: one
pandas.read_sql dispatcher serves every reader (cleared / actual / strategy /
settled / attribution / forecast-artifact — all modules share the pandas
module object, so patching pandas.read_sql covers them all), and
fleet_response.estimate_stack is stubbed, so the whole book assembles
without a database.

Wiring under test: AssetBook.forecast() attaches the Task 8 fleet-response
stack to ForecastSet.fleet_response — a real stack passes through as-is, an
empty stack (bands=[], the estimator's loud-empty) collapses to None ("no
fleet model"), and an estimator failure degrades to None with a warning,
never raising from the book.
"""
import json
from datetime import date

import numpy as np
import pandas as pd
import pytest

from libs.trading_book.asset_book import AssetBook
from libs.trading_book.models import (
    ActualDispatch,
    ClearedDispatch,
    ForecastSet,
    Settlement,
    Waterfall,
)
from libs.trading_book.registry import get_station

DAY = date(2026, 10, 15)
START, END = date(2026, 10, 1), date(2026, 10, 31)

_STACK = {"bands": [(0.0, 400.0, 10000.0), (400.0, 800.0, 20000.0)],
          "floors": {"coal_var_cost_cny_mwh": 220.0,
                     "bess_spread_cny_mwh": 0.0},
          "province": "蒙西", "as_of": DAY.isoformat(),
          "lookback_days": 60, "n_obs": 1000}


def _frame_for(sql_text: str) -> pd.DataFrame:
    """Fake read_sql dispatcher — routes on the source table in the SQL."""
    if "md_id_cleared_energy" in sql_text:
        ts = pd.date_range(f"{DAY} 00:00", periods=96, freq="15min")
        return pd.DataFrame({"datetime": ts,
                             "cleared_price": np.full(96, 350.0),
                             "cleared_energy_mwh": np.full(96, 20.0)})
    if "ops_bess_dispatch_15min" in sql_text:
        ts = pd.date_range(f"{DAY} 00:00", periods=96, freq="15min")
        return pd.DataFrame({"interval_start": ts,
                             "nominated_dispatch_mw": np.full(96, 19.0),
                             "actual_dispatch_mw": np.full(96, 18.0)})
    if "nodal_strategy_daily" in sql_text:
        return pd.DataFrame([{
            "target_date": DAY,
            "curve_json": json.dumps([20.0] * 96),
            "model_version": "nodal_agent_v1"}])
    if "book_settled_daily" in sql_text:
        return pd.DataFrame({
            "data_date": [date(2026, 10, 5), date(2026, 10, 6)],
            "amount_cny": [9000.0, -500.0],
            "accrual_grade": ["bill", "bill"]})
    if "bess_asset_daily_attribution" in sql_text:
        return pd.DataFrame([{
            "n_rows": 2, "strategy_pnl": 10000.0,
            "forecast_error_loss": 500.0, "strategy_selection": -200.0,
            "execution_clearing_loss": 300.0, "realisation_gap": 400.0}])
    if "book_forecast_daily" in sql_text:
        return pd.DataFrame({
            "slot": list(range(1, 97)),
            "price_fc": [300.0] * 96,
            "net_load_fc_mw": [None] * 96})
    raise AssertionError(f"unexpected SQL: {sql_text[:120]}")


@pytest.fixture
def fake_db(monkeypatch):
    """One dispatcher patches pandas.read_sql for every book module at once."""
    monkeypatch.setattr(pd, "read_sql",
                        lambda sql, engine, params=None: _frame_for(str(sql)))


@pytest.fixture
def fake_stack(monkeypatch):
    """Stub estimate_stack; returns the captured call kwargs for assertions."""
    calls = {}

    def fake_estimate_stack(engine, province="蒙西", lookback_days=60,
                            n_bands=8, as_of=None):
        calls.update(province=province, as_of=as_of)
        return dict(_STACK)

    monkeypatch.setattr("services.mengxi_nodal.fleet_response.estimate_stack",
                        fake_estimate_stack)
    return calls


def test_book_interface_complete():
    from libs.trading_book.registry import get_station
    from libs.trading_book.asset_book import AssetBook
    book = AssetBook(engine=None, station=get_station("suyou"))
    for meth in ("cleared", "actual", "strategies", "settlement", "forecast", "attribution"):
        assert callable(getattr(book, meth))


def test_full_chain_assembles_with_fakes(fake_db, fake_stack):
    book = AssetBook(engine=None, station=get_station("suyou"))

    cleared = book.cleared(DAY)
    assert isinstance(cleared, ClearedDispatch)
    assert len(cleared.cleared_mw) == 96 and cleared.cleared_mw[0] == 20.0
    assert len(cleared.cleared_price) == 96

    actual = book.actual(DAY)
    assert isinstance(actual, ActualDispatch)
    assert len(actual.actual_mw) == 96 and actual.actual_mw[0] == 18.0

    strategies = book.strategies(DAY)
    assert len(strategies) == 1 and strategies[0].scope == "nodal_agent"
    assert strategies[0].primary and len(strategies[0].series) == 96

    s = book.settlement("2026-10")
    assert isinstance(s, Settlement)
    assert s.period == "2026-10" and s.settled_cny == 8500.0

    fs = book.forecast(DAY)
    assert isinstance(fs, ForecastSet)
    assert len(fs.nodal_price_fc) == 96 and fs.nodal_price_fc[0] == 300.0
    assert fs.net_load_fc_mw is None  # fundamentals deferred in v1
    assert fs.fleet_response == _STACK
    # stack estimated as-of the forecast day — lookback ends at DAY, no lookahead
    assert fake_stack == {"province": "蒙西", "as_of": DAY}

    w = book.attribution(START, END)
    assert isinstance(w, Waterfall)
    assert w.strategy_pnl_cny == 10000.0
    assert w.settled_pnl_cny == 8500.0  # settled rows day-filtered to range
    total = w.strategy_pnl_cny + sum(line.delta_cny for line in w.lines)
    assert abs(total - w.settled_pnl_cny) < 1e-6


def test_forecast_empty_stack_collapses_to_none(fake_db, monkeypatch):
    """Task 8's loud-empty (bands=[]) is 'no fleet model' -> None."""
    monkeypatch.setattr(
        "services.mengxi_nodal.fleet_response.estimate_stack",
        lambda *a, **k: {**_STACK, "bands": [], "n_obs": 0})
    fs = AssetBook(engine=None, station=get_station("suyou")).forecast(DAY)
    assert fs.fleet_response is None


def test_forecast_stack_failure_degrades_to_none(fake_db, monkeypatch, capsys):
    """estimate_stack raising must never propagate out of the book."""
    def boom(*a, **k):
        raise RuntimeError("DB unreachable")

    monkeypatch.setattr(
        "services.mengxi_nodal.fleet_response.estimate_stack", boom)
    fs = AssetBook(engine=None, station=get_station("suyou")).forecast(DAY)
    assert fs.fleet_response is None
    assert fs.nodal_price_fc[0] == 300.0  # price forecast survives the degrade
    assert "WARNING" in capsys.readouterr().out
