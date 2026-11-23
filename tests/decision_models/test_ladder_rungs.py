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


def test_zone_signature_free_vs_restricted():
    assert lr.zone_signature([]) == "free" and lr.zone_signature(None) == "free"
    assert lr.zone_signature(["green"] * 96) == "free"
    a = lr.zone_signature(["red"] * 4 + ["green"] * 92)
    b = lr.zone_signature(["red"] * 5 + ["green"] * 91)
    assert a != "free" and a != b and len(a) == 10
    assert a == lr.zone_signature(["red"] * 4 + ["green"] * 92)      # stable


class _FakeEngine:
    """connect()/begin() returning a conn whose execute() is scripted."""
    def __init__(self, existing, batch_pf):
        self.existing, self.batch_pf, self.upserts = existing, batch_pf, []

    def _conn(self):
        eng = self

        class C:
            def __enter__(s): return s
            def __exit__(s, *x): return False

            def execute(s, sql, params=None):
                q = str(sql)
                class R:
                    def __init__(r, rows): r.rows = rows
                    def __iter__(r): return iter(r.rows)
                    def fetchone(r): return r.rows[0] if r.rows else None
                if "INSERT INTO" in q:
                    eng.upserts.append(params); return R([])
                if "scenario_name = 'perfect_foresight_hourly'" in q:
                    return R([eng.batch_pf] if eng.batch_pf else [])
                return R(list(eng.existing.items()))
        return C()
    connect = begin = _conn


def _patch_inputs(monkeypatch, zones):
    import libs.decision_models.resources.bess_context as bc
    import services.mengxi_nodal.dispatch_zones as dz
    import services.mengxi_nodal.zone_forecast as zf
    import services.mengxi_nodal.book_forecast as bf
    from libs.trading_book import asset_book
    from libs.trading_book.models import ForecastSet
    monkeypatch.setattr(bc, "load_actual_prices_15min", lambda a, s, e: (
        pd.DataFrame(_prices(lambda i: 100.0)), []))
    monkeypatch.setattr(bc, "load_asset_metadata", lambda a, m: (dict(META), []))
    monkeypatch.setattr(dz, "get_zones", lambda e, a, d: (zones, False))
    monkeypatch.setattr(zf, "forecast_zones", lambda e, a, d: (["green"] * 96, [0.0] * 96))
    monkeypatch.setattr(bf, "get_forecast", lambda e, st, d: ForecastSet(d, [float("nan")] * 96, None, None))
    monkeypatch.setattr(asset_book.AssetBook, "strategies", lambda self, d: [])
    monkeypatch.setattr(lr, "forecast_rung", lambda *a, **k: None)   # no forecast that day


def test_free_day_copies_batch_pf_without_solving(monkeypatch):
    _patch_inputs(monkeypatch, ["green"] * 96)
    monkeypatch.setattr(lr, "lp_rung", lambda *a, **k: pytest.fail("no LP on a free day"))
    eng = _FakeEngine({}, (1000.0, 2000.0, 3000.0, 6.0, 9.0, 1.5))
    out = lr.write_ladder_rungs("suyou", D, engine=eng)
    assert out["grid_pf"] is True and len(eng.upserts) == 1
    u = eng.upserts[0]
    assert u["scenario_name"] == lr.SCENARIO_GRID_PF and u["total"] == 3000.0
    assert u["source_system"] == "lp_actual_zones:free"


def test_restricted_day_solves_once_then_only_when_zones_change(monkeypatch):
    zones = ["red"] * 8 + ["green"] * 88
    _patch_inputs(monkeypatch, zones)
    calls = []
    monkeypatch.setattr(lr, "lp_rung", lambda day, lp, px, meta, z=None: calls.append(z) or {
        "market": 1.0, "compensation": 2.0, "total": 3.0, "discharge_mwh": 1.0,
        "charge_mwh": 1.0, "cycles": 0.1})
    sig = lr.zone_signature(zones)
    eng = _FakeEngine({}, (1.0, 1.0, 9.0, 1.0, 1.0, 1.0))
    lr.write_ladder_rungs("suyou", D, engine=eng)
    assert calls == [zones] and eng.upserts[0]["source_system"] == f"lp_actual_zones:{sig}"
    # stored under the same zone pattern -> no re-solve
    eng2 = _FakeEngine({lr.SCENARIO_GRID_PF: f"lp_actual_zones:{sig}"}, None)
    lr.write_ladder_rungs("suyou", D, engine=eng2)
    assert len(calls) == 1
    # colours repainted since -> re-solved
    eng3 = _FakeEngine({lr.SCENARIO_GRID_PF: "lp_actual_zones:free"}, None)
    lr.write_ladder_rungs("suyou", D, engine=eng3)
    assert len(calls) == 2 and len(eng3.upserts) == 1
