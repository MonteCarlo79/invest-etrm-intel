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
    # shared 120 MW substation cap: 甲 solves first in list order and takes
    # up to 100 MW, so 乙 (80 MW) is the one squeezed — it sees only the
    # headroom 甲 leaves in 甲's discharge window.
    s = out["strategies"]
    assert (s["甲"]["curve"] + s["乙"]["curve"]).max() <= 120 + 1e-6
    for name, strat in s.items():
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


# --- review round 1: raise paths + version-filtered grid price --------------

def test_converge_rejects_non_finite_curve():
    def curves_fn(iter_no, dispatch):
        return np.array([200.0]*48 + [np.nan]*48)
    asset = dict(plant_name="谷山梁", capacity_mw=100.0, duration_h=2.0, rte_pct=85.0)
    with pytest.raises(ValueError):
        recursion.converge([asset], curves_fn, max_iter=2)


def test_coal_stack_response_rejects_bad_residual_shape():
    fuel = {"segments": [{"capacity_mw": 100.0, "must_run_mw": 40.0}]}
    with pytest.raises(ValueError):
        behavior.coal_stack_response(np.zeros(48), fuel)


def test_forecast_zone_bess_rejects_out_of_range_interval():
    rows = [dict(d=date(2026, 9, 21), interval=100, dispatch_mw=-10.0)]
    with pytest.raises(ValueError):
        behavior.forecast_zone_bess("乌兰察布", date(2026, 9, 22), pd.DataFrame(rows))


class _RowsCur:
    """Cursor serving queued row-sets (one per execute's fetchall); records SQL."""
    def __init__(self, row_sets):
        self._row_sets = list(row_sets)
        self.calls = []
    def execute(self, sql, params=None):
        self.calls.append((sql, params))
    def executemany(self, sql, seq):
        self.calls.append((sql, list(seq)))
    def fetchall(self):
        return self._row_sets.pop(0) if self._row_sets else []


class _RowsConn:
    def __init__(self, row_sets):
        self.cur = _RowsCur(row_sets)
    def cursor(self):
        return self.cur
    def commit(self):
        pass


def test_load_grid_price_hat_filters_model_version():
    conn = _RowsConn(row_sets=[[(300.0, "v1", date(2026, 9, 22)),
                                (340.0, "v2", date(2026, 9, 22))]])
    out = writer._load_grid_price_hat(conn, date(2026, 9, 23), model_version="v2")
    assert out == pytest.approx(340.0)


def test_load_grid_price_hat_falls_back_to_latest_fc_date():
    conn = _RowsConn(row_sets=[[(300.0, "v1", date(2026, 9, 21)),
                                (320.0, "v1", date(2026, 9, 22))]])
    out = writer._load_grid_price_hat(conn, date(2026, 9, 23), model_version="v9")
    assert out == pytest.approx(320.0)


def test_load_grid_price_hat_lingfeng_fallback_when_table_empty(monkeypatch):
    conn = _RowsConn(row_sets=[[]])
    monkeypatch.setattr(writer.fc_db, "get_grid_forecast",
                        lambda c, d: pd.DataFrame({"price_hat": [310.0]}))
    out = writer._load_grid_price_hat(conn, date(2026, 9, 23), model_version="v2")
    assert out == pytest.approx(310.0)


def test_run_day_db_mode_zero_assets_skips_forecast():
    conn = _RowsConn(row_sets=[[]])  # _load_assets -> []
    out = writer.run_day(conn, date(2026, 9, 23), model_version="v2")
    assert out["plants"] == 0 and out["upserted"] == 0
    assert len(conn.cur.calls) == 1
    assert "nodal_asset_registry" in conn.cur.calls[0][0]


# --- Task 6: promote loop registration ----------------------------------------

_REG_DAYS = [date(2026, 9, 21), date(2026, 9, 22)]
_REG_CURVE_JSON = json.dumps([-100.0] * 48 + [100.0] * 48)


def _reg_strategy_rows():
    asm_a = json.dumps({"plant_name": "甲站", "node": "甲node",
                        "settle_node": "甲settle", "capacity_mw": 100.0,
                        "duration_h": 2.0, "rte_pct": 85.0})
    asm_b = json.dumps({"plant_name": "乙站", "node": "乙node",
                        "settle_node": "N2", "capacity_mw": 50.0,
                        "duration_h": 2.0, "rte_pct": 85.0})
    return ([("甲站", d, _REG_CURVE_JSON, asm_a, "v1") for d in _REG_DAYS]
            + [("乙站", d, _REG_CURVE_JSON, asm_b, "v1") for d in _REG_DAYS])


def _reg_registry_rows():
    # 甲站 prices via Fengxing mapping; 乙站 falls back to settle_node (N2)
    return [("甲站", "甲node", "甲settle", "N1"),
            ("乙站", "乙node", "N2", None)]


def _reg_price_rows(node, p_lo, p_hi):
    return [(node, d, slot, p_lo if slot <= 48 else p_hi)
            for d in _REG_DAYS for slot in range(1, 97)]


def _reg_pf_rows():
    return [(d, "N1", 300000.0) for d in _REG_DAYS] + \
           [(d, "N2", 1200000.0) for d in _REG_DAYS]


def test_register_strategies_upserts_per_plant():
    # Hand-computed expectations (per day, identical on both days):
    #   甲站: curve -100/+100 vs N1 price 200/400
    #         realized = (48*-100*200 + 48*100*400) * 0.25 = 240,000 CNY
    #         basis = 100MW*2h = 200 MWh -> 1,200 CNY/MWh
    #         PF N1 300,000 @100MW, scale 1.0 -> theo 1,500 CNY/MWh, capture 0.8
    #   乙站: same curve vs N2 price 100/300 -> realized 240,000 CNY
    #         basis = 50MW*2h = 100 MWh -> 2,400 CNY/MWh
    #         PF N2 1,200,000 @100MW, scale 0.5 -> theo 6,000 CNY/MWh, capture 0.4
    conn = _RowsConn(row_sets=[
        _reg_strategy_rows(), _reg_registry_rows(),
        _reg_price_rows("N1", 200.0, 400.0) + _reg_price_rows("N2", 100.0, 300.0),
        _reg_pf_rows(),
        [],  # no previous experiment windows
    ])
    n = writer.register_strategies(conn, date(2026, 9, 22), window_days=3)
    assert n == 2
    inserts = [(sql, p) for sql, p in conn.cur.calls
               if "INSERT INTO marketdata.strategy_experiments" in sql]
    assert len(inserts) == 2
    assert "'nodal_agent'" in inserts[0][0]  # scope literal in the SQL
    by_plant = {p["province"]: p for _, p in inserts}
    assert set(by_plant) == {"甲站", "乙站"}   # province column carries plant name
    a = by_plant["甲站"]
    assert a["model"] == "nodal_agent_v1"
    assert a["power_mw"] == pytest.approx(100.0)
    assert a["duration_h"] == pytest.approx(2.0)
    assert a["roundtrip_eff"] == pytest.approx(0.85)
    assert a["window_days"] == 3 and a["window_end"] == date(2026, 9, 22)
    assert a["days"] == 2
    assert a["mean_capture_rate"] == pytest.approx(0.8)
    assert a["mean_realized_per_mwh"] == pytest.approx(1200.0)
    assert a["mean_theoretical_per_mwh"] == pytest.approx(1500.0)
    assert a["status"] == "champion"           # higher-capture plant
    b = by_plant["乙站"]
    assert b["model"] == "nodal_agent_v1"
    assert b["mean_capture_rate"] == pytest.approx(0.4)
    assert b["mean_realized_per_mwh"] == pytest.approx(2400.0)
    assert b["mean_theoretical_per_mwh"] == pytest.approx(6000.0)
    # assign_status champions per province (= per plant here), so each plant
    # leads its own leaderboard; delta_vs_champion is 0 for both.
    assert b["status"] == "champion"
    assert b["delta_vs_champion"] == pytest.approx(0.0)


def test_register_strategies_skips_when_no_actuals():
    conn = _RowsConn(row_sets=[
        _reg_strategy_rows(), _reg_registry_rows(),
        [],  # no actual RT prices in window
    ])
    n = writer.register_strategies(conn, date(2026, 9, 22), window_days=3)
    assert n == 0
    assert all("INSERT" not in sql for sql, _ in conn.cur.calls)
    # PF / prev-experiment SELECTs are never reached without actuals
    assert len(conn.cur.calls) == 3


def test_register_strategies_does_not_double_prefix_version():
    # prod MODEL_VERSION is already namespaced ("nodal_agent_v1") — the
    # experiment model key must stay "nodal_agent_v1", never double up.
    rows = [(p, d, cj, aj, "nodal_agent_v1")
            for (p, d, cj, aj, _mv) in _reg_strategy_rows()]
    conn = _RowsConn(row_sets=[
        rows, _reg_registry_rows(),
        _reg_price_rows("N1", 200.0, 400.0) + _reg_price_rows("N2", 100.0, 300.0),
        _reg_pf_rows(), [],
    ])
    n = writer.register_strategies(conn, date(2026, 9, 22), window_days=3)
    assert n == 2
    models = {p["model"] for sql, p in conn.cur.calls
              if "INSERT INTO marketdata.strategy_experiments" in sql}
    assert models == {"nodal_agent_v1"}

def test_load_shapes_slot_alignment_1_based():
    # time_order_96 is 1-based: slot 1 -> index 0, slot 96 -> index 95.
    # Regression (2026-09-22): reindex(range(96)) dropped slot 96 and shifted
    # every node's shape one interval.
    conn = _RowsConn(row_sets=[[("N1", 1, 100.0), ("N1", 96, 500.0)]])
    shapes = writer._load_shapes(conn, date(2026, 9, 23))
    assert shapes["N1"][0] == pytest.approx(100.0 / 300.0)   # slot 1 at index 0
    assert shapes["N1"][95] == pytest.approx(500.0 / 300.0)  # slot 96 at index 95


def test_register_prices_sql_buckets_cst_not_utc():
    # Final review 2026-09-22 (C-1): plain metric_time::date in the UTC
    # session sources slots 1-32 (overnight charge window) from the FOLLOWING
    # CST day. Mocks bypass the SQL, so pin the bucketing at the string level.
    assert "AT TIME ZONE 'Asia/Shanghai'" in writer._REGISTER_PRICES_SQL
    assert "metric_time::date" not in writer._REGISTER_PRICES_SQL


def test_run_day_shape_fallback_chain_and_miss_counter():
    # shapes keyed by Fengxing node_name: asset.node misses, settle_node hits
    data = {
        "assets": [
            dict(plant_name="甲站", capacity_mw=100.0, duration_h=2.0, rte_pct=85.0,
                 node="母线名甲", substation="S1", zone="Z1", settle_node="FN_甲"),
            dict(plant_name="乙站", capacity_mw=100.0, duration_h=2.0, rte_pct=85.0,
                 node="母线名乙", substation="S2", zone="Z2", settle_node="母线名乙"),
        ],
        "grid_price_hat": 320.0,
        "shapes": {"FN_甲": np.array([0.5]*48 + [1.5]*48)},
        "caps": {},
    }
    conn = _Conn()
    out = writer.run_day(conn, date(2026, 9, 23), data=data)
    assert out["plants"] == 2
    # 甲站 resolved via settle_node FN_甲; 乙站 missed both -> flat shape
    assert out["shape_misses"] == 1
    sql, rows = conn.cur.upserts[0]
    by_plant = {r["plant_name"]: r for r in rows}
    curve_a = np.array(json.loads(by_plant["甲站"]["curve_json"]))
    # with the 0.5/1.5 shape the discharge half dominates the charge half
    assert curve_a[48:].sum() > 0


def test_converge_damping_blends_with_previous_dispatch():
    # damping=0.5: second-iteration dispatch = midpoint of LP curve and prev
    seq = {"n": 0}
    def curves_fn(iter_no, dispatch):
        seq["n"] += 1
        # price level flips hard between iterations -> LP curves differ
        return np.array([100.0]*48 + [900.0]*48) if iter_no == 0 else np.array([900.0]*48 + [100.0]*48)
    asset = dict(plant_name="谷山梁", capacity_mw=100.0, duration_h=2.0, rte_pct=85.0)
    out1 = recursion.converge([asset], curves_fn, max_iter=2, damping=1.0)
    out2 = recursion.converge([asset], curves_fn, max_iter=2, damping=0.5)
    # both complete; damped run's measured delta is smaller (blend pulls halves apart less)
    assert out1["iterations"] == 2 and out2["iterations"] == 2
    assert out2["convergence_delta_mwh"] < out1["convergence_delta_mwh"]


def test_converge_rejects_bad_damping():
    asset = dict(plant_name="谷山梁", capacity_mw=100.0, duration_h=2.0, rte_pct=85.0)
    with pytest.raises(ValueError):
        recursion.converge([asset], lambda i, d: np.full(96, 300.0), damping=0.0)
    with pytest.raises(ValueError):
        recursion.converge([asset], lambda i, d: np.full(96, 300.0), damping=1.5)
