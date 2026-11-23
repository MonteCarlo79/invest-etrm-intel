"""Batch gating for the daily attribution catch-up (2026-11-23 repair):
complete-day price check, ops-aware "already computed", rolling window."""
from datetime import date

import pandas as pd
import pytest

from libs.decision_models.resources import bess_context
from services.decision_models import run_daily_strategy_batch as b

D = date(2026, 11, 18)


def _scen(names):
    return pd.DataFrame({"scenario_name": names,
                         "scenario_available": [True] * len(names)})


@pytest.mark.parametrize("slots,expected", [(96, True), (92, True), (32, False), (0, False)])
def test_has_prices_requires_complete_day(monkeypatch, slots, expected):
    calls = []

    def fake(sql, params):
        calls.append(sql)
        if "canon." in sql:
            return pd.DataFrame(), None
        return pd.DataFrame({"n": [slots]}), None

    monkeypatch.setattr(bess_context, "_run_query_safe", fake)
    assert b._has_prices("suyou", D) is expected
    assert "LIMIT 1" not in calls[0]


def test_already_computed_false_without_pf(monkeypatch):
    monkeypatch.setattr(bess_context, "load_precomputed_scenario_pnl",
                        lambda a, s, e: (_scen(["nominated_dispatch"]), []))
    assert b._already_computed("suyou", D) is False


def test_forecast_scenario_is_not_required(monkeypatch):
    # forecast_ols_rt_time_v1 never solves in this path; requiring it re-ran
    # the LP for every day on every run.
    monkeypatch.setattr(bess_context, "load_precomputed_scenario_pnl", lambda a, s, e: (
        _scen(["perfect_foresight_hourly", "nominated_dispatch"]), []))
    assert b._already_computed("suyou", D) is True


def test_already_computed_true_with_nominated(monkeypatch):
    monkeypatch.setattr(bess_context, "load_precomputed_scenario_pnl", lambda a, s, e: (
        _scen(["perfect_foresight_hourly", "forecast_ols_rt_time_v1",
               "nominated_dispatch"]), []))
    monkeypatch.setattr(bess_context, "load_ops_dispatch_15min",
                        lambda *a: pytest.fail("ops must not be queried"))
    assert b._already_computed("suyou", D) is True


@pytest.mark.parametrize("ops_rows,expected", [(0, True), (96, False)])
def test_lp_only_day_is_redone_once_ops_arrive(monkeypatch, ops_rows, expected):
    monkeypatch.setattr(bess_context, "load_precomputed_scenario_pnl", lambda a, s, e: (
        _scen(["perfect_foresight_hourly", "forecast_ols_rt_time_v1"]), []))
    monkeypatch.setattr(bess_context, "load_ops_dispatch_15min", lambda a, s, e: (
        pd.DataFrame({"nominated_dispatch_mw": [1.0] * ops_rows}), []))
    assert b._already_computed("suyou", D) is expected


def test_catch_up_walks_window_oldest_first_and_survives_failures(monkeypatch):
    seen = []

    def fake_batch(d, asset_codes=None, **k):
        seen.append(("b", d))
        if d == date(2026, 11, 17):
            raise RuntimeError("LP blew up")
        return 4

    import services.monitoring.run_daily_attribution as attr
    monkeypatch.setattr(b, "run_for_date", fake_batch)
    monkeypatch.setattr(attr, "run_for_date", lambda d: seen.append(("a", d)) or 4)
    out = b.catch_up(D, lookback_days=3)
    assert [d for k, d in seen if k == "b"] == [date(2026, 11, 16), date(2026, 11, 17), D]
    assert [d for k, d in seen if k == "a"] == [date(2026, 11, 16), date(2026, 11, 17), D]
    assert out == {"batch": 8, "attribution": 12, "days": 3}
