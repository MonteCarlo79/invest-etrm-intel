"""Forecast-optimal + strategy rungs (services.monitoring.ladder_rungs)."""
from datetime import date

import pandas as pd
import pytest

import services.monitoring.ladder_rungs as lr

D = date(2026, 11, 18)
META = {"asset_code": "suyou", "power_mw": 100.0, "duration_h": 4.0,
        "roundtrip_eff": 0.85, "compensation_yuan_per_mwh": 350.0,
        "max_cycles_per_day": 1.5}


def _prices(fn):
    return [{"time": pd.Timestamp(lr.slot_time(D, i)), "price": fn(i)} for i in range(96)]


def test_slot_time_is_naive_cst_interval_start():
    assert lr.slot_time(D, 0) == "2026-11-18 00:00:00"
    assert lr.slot_time(D, 95) == "2026-11-18 23:45:00"


def test_value_curve_matches_hand_calc():
    # charge 100 MW for 4 slots at 100 ¥/MWh, discharge 100 MW for 4 slots at 500
    curve = [0.0] * 96
    curve[0:4] = [-100.0] * 4
    curve[40:44] = [100.0] * 4
    v = lr.value_curve(D, curve, _prices(lambda i: 500.0 if 40 <= i < 44 else 100.0),
                       350.0, energy_capacity_mwh=400.0)
    assert v["discharge_mwh"] == 100.0 and v["charge_mwh"] == 100.0
    assert v["market"] == pytest.approx(100.0 * 500.0 - 100.0 * 100.0)
    assert v["compensation"] == pytest.approx(100.0 * 350.0)
    assert v["total"] == pytest.approx(40000.0 + 35000.0)
    assert v["cycles"] == 0.25


def test_value_curve_refuses_incomplete_series():
    assert lr.value_curve(D, [1.0] * 50 + [float("nan")] * 46,
                          _prices(lambda i: 100.0), 350.0) is None


def test_strategy_rung_guards_rating_and_missing_curve():
    px = _prices(lambda i: 300.0)
    assert lr.strategy_rung(D, None, px, META) is None
    # writer registry sized this station 4x before 2026-11-23: not valued
    assert lr.strategy_rung(D, [400.0] * 96, px, META) is None
    ok = lr.strategy_rung(D, [100.0] * 4 + [0.0] * 92, px, META)
    assert ok["discharge_mwh"] == 100.0


def test_forecast_rung_solves_on_forecast_values_at_actual(monkeypatch):
    seen = {}

    def fake_pf(ctx):
        seen["prices"] = [r["price"] for r in ctx["actual_prices_15min"]]
        seen["meta"] = ctx["asset_metadata"]
        recs = [{"datetime": lr.slot_time(D, i).replace(" ", "T"),
                 "dispatch_grid_mw": 100.0 if i == 10 else (-100.0 if i == 2 else 0.0)}
                for i in range(96)]
        return {"pnl": {"n_days_solved": 1}, "dispatch_hourly": recs}

    import libs.decision_models.workflows.strategy_comparison as sc
    monkeypatch.setattr(sc, "run_perfect_foresight_dispatch", fake_pf)
    forecast = [float(i) for i in range(96)]
    actual = _prices(lambda i: 1000.0 if i == 10 else 200.0)
    v = lr.forecast_rung(D, forecast, actual, META)
    assert seen["prices"] == forecast and seen["meta"] is META   # LP sees the forecast
    # valued at ACTUAL prices: +25 MWh x 1000 - 25 MWh x 200 + 25 x 350
    assert v["market"] == pytest.approx(25.0 * 1000.0 - 25.0 * 200.0)
    assert v["total"] == pytest.approx(20000.0 + 8750.0)


def test_forecast_rung_none_when_forecast_incomplete_or_unsolved(monkeypatch):
    import libs.decision_models.workflows.strategy_comparison as sc
    monkeypatch.setattr(sc, "run_perfect_foresight_dispatch",
                        lambda ctx: {"pnl": {"n_days_solved": 0}, "dispatch_hourly": []})
    px = _prices(lambda i: 100.0)
    assert lr.forecast_rung(D, None, px, META) is None
    assert lr.forecast_rung(D, [float("nan")] * 96, px, META) is None
    assert lr.forecast_rung(D, [100.0] * 96, px, META) is None      # LP unsolved


# ---- TSO traffic-light windows (2026-11-24) ---------------------------------

def test_zone_masks_red_no_charge_yellow_no_discharge():
    zones = ["green"] * 96
    zones[0] = "red"; zones[40] = "yellow"; zones[41] = "yellow"
    assert lr.zone_masks(zones) == ({1}, {41, 42})        # 1-based slots
    assert lr.zone_masks([]) == (None, None)              # no data: unrestricted
    assert lr.zone_masks(["green"] * 96) == (None, None)


def test_lp_rung_passes_zone_constraints_to_the_lp(monkeypatch):
    seen = {}

    def fake_pf(ctx):
        seen.update(nc=ctx["no_charge_slots"], nd=ctx["no_discharge_slots"])
        return {"pnl": {"n_days_solved": 1}, "dispatch_hourly": [
            {"datetime": lr.slot_time(D, i).replace(" ", "T"), "dispatch_grid_mw": 0.0}
            for i in range(96)]}

    import libs.decision_models.workflows.strategy_comparison as sc
    monkeypatch.setattr(sc, "run_perfect_foresight_dispatch", fake_pf)
    zones = ["red"] * 4 + ["green"] * 88 + ["yellow"] * 4
    assert lr.lp_rung(D, [100.0] * 96, _prices(lambda i: 100.0), META, zones) is not None
    assert seen == {"nc": {1, 2, 3, 4}, "nd": {93, 94, 95, 96}}
    lr.lp_rung(D, [100.0] * 96, _prices(lambda i: 100.0), META, None)
    assert seen == {"nc": None, "nd": None}


def test_actual_price_series_aligns_and_handles_tz():
    recs = [{"time": pd.Timestamp("2026-11-18 00:00:00"), "price": 11.0},
            {"time": pd.Timestamp("2026-11-17 16:15:00", tz="UTC"), "price": 12.0}]
    s = lr.actual_price_series(D, recs)
    assert s[0] == 11.0 and s[1] == 12.0 and s[2] is None and len(s) == 96
