# tests/nodal_trading/test_agents.py
import json
from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

from services.nodal_agents import agent, behavior, recursion, writer


def test_zone_bess_forecast_is_daytype_matched():
    hist = np.vstack([np.full(96, -20.0), np.full(96, -40.0), np.full(96, -20.0)])
    # two "weekday-like" days and one "weekend-like" → weekend forecast uses the -40 day
    out = behavior._daytype_mean(hist, mask=[True, False, True])
    assert out.mean() == pytest.approx(-40.0)

def test_optimize_respects_substation_cap():
    asset = dict(plant_name="谷山梁", capacity_mw=100.0, duration_h=2.0, rte_pct=85.0)
    curve = np.array([200.0]*48 + [500.0]*48)
    others = np.full(96, 50.0)   # other BESS discharging 50 MW at peak
    cap = 100.0                  # substation allows only 100 MW total discharge
    out = agent.optimize_asset(asset, curve, others, cap_mw=cap,
                               zones=np.zeros(96, dtype=int))
    assert out["curve"].shape == (96,)
    assert out["curve"].max() <= cap - others.max() + 1e-6

def test_recursion_converges_and_stops_on_small_delta():
    calls = {"n": 0}
    def curves_fn(iter_no, dispatch):
        calls["n"] += 1
        return np.array([200.0]*48 + [500.0 - 5*iter_no]*48)
    asset = dict(plant_name="谷山梁", capacity_mw=100.0, duration_h=2.0, rte_pct=85.0)
    out = recursion.converge([asset], curves_fn, max_iter=3, tol_mwh_pct=2.0)
    assert out["iterations"] <= 3 and calls["n"] >= 2
    assert out["strategies"]["谷山梁"]["curve"].shape == (96,)


# --- supplementary tests (implementer) -------------------------------------

def test_zone_masks_force_charge_only_and_discharge_only():
    asset = dict(plant_name="谷山梁", capacity_mw=100.0, duration_h=2.0, rte_pct=85.0)
    curve = np.array([200.0]*48 + [500.0]*48)
    zones = np.zeros(96, dtype=int)
    zones[0:24] = agent.ZONE_NO_CHARGE        # discharge-only: no charging here
    zones[48:72] = agent.ZONE_NO_DISCHARGE    # charge-only: no discharging here
    out = agent.optimize_asset(asset, curve, np.zeros(96), cap_mw=None, zones=zones)
    c = out["curve"]
    assert c[0:24].min() >= -1e-6      # never charges in a no-charge zone
    assert c[48:72].max() <= 1e-6      # never discharges in a no-discharge zone
    assert c[48:72].min() < 0.0        # negative price pays to charge → it charges
    assert out["assumptions"]["zone_no_charge_intervals"] == 24
    assert out["assumptions"]["zone_no_discharge_intervals"] == 24


def test_optimize_uncapped_uses_full_capacity():
    asset = dict(plant_name="谷山梁", capacity_mw=100.0, duration_h=2.0, rte_pct=85.0)
    curve = np.array([200.0]*48 + [500.0]*48)
    out = agent.optimize_asset(asset, curve, np.zeros(96), cap_mw=None,
                               zones=np.zeros(96, dtype=int))
    assert out["curve"].max() == pytest.approx(100.0, abs=1e-6)
    assert out["assumptions"]["power_mw_eff"] == pytest.approx(100.0)


def test_optimize_rejects_nan_curve():
    asset = dict(plant_name="谷山梁", capacity_mw=100.0, duration_h=2.0, rte_pct=85.0)
    curve = np.array([200.0]*48 + [np.nan]*48)
    with pytest.raises(ValueError):
        agent.optimize_asset(asset, curve, np.zeros(96), cap_mw=None,
                             zones=np.zeros(96, dtype=int))


def test_forecast_zone_bess_dataframe_weekend_target():
    # 2026-09-19 Sat, 2026-09-20 Sun, 2026-09-21 Mon
    days = [date(2026, 9, 19), date(2026, 9, 20), date(2026, 9, 21)]
    vals = [-10.0, -30.0, -99.0]
    rows = [dict(d=d, interval=i, dispatch_mw=v)
            for d, v in zip(days, vals) for i in range(96)]
    hist = pd.DataFrame(rows)
    out = behavior.forecast_zone_bess("乌兰察布", date(2026, 9, 26), hist)  # Saturday
    assert out.shape == (96,)
    assert out.mean() == pytest.approx(-20.0)  # mean of the two weekend days only


def test_coal_stack_response_monotonic_bounded():
    fuel = {"segments": [
        {"capacity_mw": 1000.0, "must_run_mw": 400.0, "cost_yuan_per_mwh": 250.0},
        {"capacity_mw": 500.0, "must_run_mw": 100.0, "cost_yuan_per_mwh": 320.0},
    ]}
    residual = np.linspace(1000.0, 5000.0, 96)
    out = behavior.coal_stack_response(residual, fuel)
    assert out.shape == (96,)
    assert np.all(np.diff(out) >= -1e-9)          # monotone non-decreasing
    assert out.min() >= 500.0 - 1e-9              # Σ must_run floor
    assert out.max() <= 1500.0 + 1e-9             # Σ capacity ceiling
    assert behavior.coal_stack_response(residual, {"segments": []}).max() == 0.0


def test_converge_multi_asset_dict_curves_and_substation_others():
    def curves_fn(iter_no, dispatch):
        return {"甲": np.array([200.0]*48 + [500.0]*48),
                "乙": np.array([200.0]*48 + [450.0]*48)}
    assets = [
        dict(plant_name="甲", capacity_mw=100.0, duration_h=2.0, rte_pct=85.0,
             substation="S1", cap_mw=120.0),
        dict(plant_name="乙", capacity_mw=80.0, duration_h=2.0, rte_pct=85.0,
             substation="S1", cap_mw=120.0),
    ]
    out = recursion.converge(assets, curves_fn, max_iter=3, tol_mwh_pct=2.0)
    assert set(out["strategies"]) == {"甲", "乙"}
    # shared 120 MW substation cap: 乙 (80 MW) leaves 甲 at most 40 MW headroom
    # in 乙's discharge window; 甲's peak must respect it once 乙 is counted.
    for name, strat in out["strategies"].items():
        assert strat["curve"].shape == (96,)
        assert "assumptions" in strat
    assert 2 <= out["iterations"] <= 3
    assert np.isfinite(out["convergence_delta_mwh"])


def test_converge_requires_two_iterations():
    def curves_fn(iter_no, dispatch):
        return np.full(96, 300.0)
    asset = dict(plant_name="谷山梁", capacity_mw=100.0, duration_h=2.0, rte_pct=85.0)
    with pytest.raises(ValueError):
        recursion.converge([asset], curves_fn, max_iter=1)


class _Cur:
    def __init__(self):
        self.upserts = []
    def execute(self, sql, params=None):
        raise AssertionError("data injection must bypass SELECTs")
    def executemany(self, sql, seq):
        self.upserts.append((sql, list(seq)))
    def fetchall(self):
        return []


class _Conn:
    def __init__(self):
        self.cur = _Cur()
        self.committed = 0
    def cursor(self):
        return self.cur
    def commit(self):
        self.committed += 1


def test_writer_run_day_synthetic():
    data = {
        "assets": [
            dict(plant_name="谷山梁", capacity_mw=100.0, duration_h=2.0, rte_pct=85.0,
                 node="谷山梁500kV", substation="谷山梁", zone="乌兰察布", settle_node="谷山梁"),
            dict(plant_name="景蓝乌尔图", capacity_mw=100.0, duration_h=4.0, rte_pct=88.0,
                 node="苏尼特500kV", substation="苏尼特", zone="锡林郭勒", settle_node="苏尼特"),
        ],
        "grid_price_hat": 320.0,
        "shapes": {"谷山梁500kV": np.array([0.5]*48 + [1.5]*48)},
        "caps": {"谷山梁": 150.0},
    }
    conn = _Conn()
    out = writer.run_day(conn, date(2026, 9, 23), data=data)
    assert out["plants"] == 2 and out["upserted"] == 2
    assert 2 <= out["iterations"] <= 3
    assert conn.committed == 1
    sql, rows = conn.cur.upserts[0]
    assert "nodal_strategy_daily" in sql
    assert "ON CONFLICT (plant_name, target_date, model_version) DO UPDATE" in sql
    assert len(rows) == 2
    by_plant = {r["plant_name"]: r for r in rows}
    curve = json.loads(by_plant["谷山梁"]["curve_json"])
    assert len(curve) == 96
    # substation cap 150 MW binds 谷山梁's 100 MW unit only via others; shape
    # must shift discharge into the high half: afternoon energy > morning energy
    arr = np.array(curve)
    assert arr[48:].sum() > arr[:48].sum()
    assert by_plant["谷山梁"]["model_version"] == out["model_version"]
    asm = json.loads(by_plant["谷山梁"]["assumptions_json"])
    assert asm["grid_price_hat"] == pytest.approx(320.0)
