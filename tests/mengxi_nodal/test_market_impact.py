"""Tests for the 蒙西 market-impact λ pipeline (plan task 13). No RDS:
- OLS core on synthetic frames with a KNOWN λ (recovery + charge/discharge
  sign split + identification gate);
- estimate_lambda / persist_lambda / get_latest_lambdas end-to-end on a
  sqlite-ATTACH harness with synthetic filled-price and dispatch tables;
- engine impact on tiny synthetic LPs (HiGHS monkeypatch — pulp's bundled
  CBC is an x86_64 binary and cannot execute on this arm64 Mac; same
  workaround as T12, production Fargate keeps CBC).
"""
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from services.mengxi_nodal.market_impact import (
    _DDL,
    _DISPATCH_SQL,
    _MIN_REGIME_OBS,
    _NODE_PROBE_SQL,
    _PRICE_SQL,
    _UPSERT,
    ensure_table,
    estimate_from_frames,
    estimate_lambda,
    get_latest_lambdas,
    persist_lambda,
)
from services.bess_map.nodal_pf_daily import _mengxi_node_impacts
from services.bess_map.optimisation_engine import (
    compute_dispatch_from_15min_prices,
    optimise_window,
)

# ── synthetic data with a known impact coefficient ───────────────────────────
# DGP: node_price = fleet + hour_fx − λ_r·net + ε   (net > 0 discharge, < 0
# charge), so price_dev = node − fleet = hour_fx − λ·net + ε and the hour
# dummies absorb hour_fx. Fleet noise is common to node and fleet, so it
# cancels in the deviation.

_LAM_D, _LAM_C = 0.8, 0.3


def _synth_series(days: int = 12, lam_d: float = _LAM_D, lam_c: float = _LAM_C,
                  noise: float = 0.05, seed: int = 7,
                  start: date = date(2026, 8, 1)):
    """Rows of (ts_str, net_mw, fleet_price, node_price) with known λ."""
    rng = np.random.default_rng(seed)
    rows = []
    for d in range(days):
        day = start + timedelta(days=d)
        for slot in range(96):
            ts = pd.Timestamp(day) + pd.Timedelta(minutes=15 * slot)
            h = ts.hour
            if 18 <= h < 22:                       # evening discharge window
                net = float(rng.uniform(20.0, 90.0))
            elif h < 6 or 11 <= h < 14:            # night + midday charge
                net = -float(rng.uniform(20.0, 90.0))
            else:
                net = 0.0
            fleet = 300.0 + 80.0 * np.sin(2 * np.pi * slot / 96.0) + rng.normal(0, 10.0)
            hour_fx = 4.0 * np.sin(2 * np.pi * h / 24.0)
            lam = lam_d if net > 0 else lam_c
            node = fleet + hour_fx - lam * net + rng.normal(0, noise)
            rows.append((ts.strftime("%Y-%m-%d %H:%M:%S"), net, fleet, node))
    return rows


def _synth_frames(**kw):
    rows = _synth_series(**kw)
    price_df = pd.DataFrame(
        [(r[0], r[3], r[2]) for r in rows],
        columns=["metric_time", "node_price", "fleet_price"])
    disp_df = pd.DataFrame(
        [(r[0], r[1]) for r in rows],
        columns=["interval_start", "actual_dispatch_mw"])
    return price_df, disp_df


# ── DDL + frame guards ────────────────────────────────────────────────────────

def test_ddl_shape():
    assert "reports.station_market_impact" in _DDL
    for col in ("asset_code", "estimated_at", "lambda_discharge",
                "lambda_charge", "n_obs", "r2", "se_discharge", "se_charge",
                "lookback_days"):
        assert col in _DDL
    assert "PRIMARY KEY (asset_code, estimated_at)" in _DDL


def test_sql_frame_guard_naive_cst():
    for sql in (_PRICE_SQL, _DISPATCH_SQL, _NODE_PROBE_SQL):
        assert "+08" not in sql            # POSIX-inversion guard
        assert "AT TIME ZONE" not in sql   # columns are already CST-naive
    assert "md_mengxi_nodal_price_96_filled" in _PRICE_SQL
    assert "ops_bess_dispatch_15min" in _DISPATCH_SQL
    assert "actual_dispatch_mw" in _DISPATCH_SQL


# ── OLS core (pure frames) ────────────────────────────────────────────────────

def test_estimate_from_frames_recovers_known_lambda():
    price_df, disp_df = _synth_frames()
    est = estimate_from_frames(price_df, disp_df)
    assert est["per_regime"]["discharge"] == pytest.approx(_LAM_D, abs=0.01)
    assert est["n_discharge"] >= _MIN_REGIME_OBS
    assert est["n_obs"] == est["n_discharge"] + est["n_charge"]
    assert est["r2_discharge"] > 0.99
    assert est["se_discharge"] < 0.01


def test_sign_split_recovers_both_regimes():
    price_df, disp_df = _synth_frames()
    est = estimate_from_frames(price_df, disp_df)
    lam_d, lam_c = est["per_regime"]["discharge"], est["per_regime"]["charge"]
    # Distinct per-regime λs, both positive (discharge depresses, charge lifts)
    assert lam_d == pytest.approx(_LAM_D, abs=0.01)
    assert lam_c == pytest.approx(_LAM_C, abs=0.01)
    assert lam_d > lam_c > 0
    assert est["se_charge"] is not None and est["r2_charge"] > 0.99


def test_regime_min_obs_gate():
    price_df, disp_df = _synth_frames()
    # Keep only 5 discharge slots — below the identification floor.
    dis_mask = disp_df["actual_dispatch_mw"] > 0
    keep_idx = disp_df.index[dis_mask][:5]
    disp_df.loc[dis_mask, "actual_dispatch_mw"] = 0.0
    disp_df.loc[keep_idx, "actual_dispatch_mw"] = 50.0
    est = estimate_from_frames(price_df, disp_df)
    assert est["per_regime"]["discharge"] is None
    assert est["n_discharge"] < _MIN_REGIME_OBS
    assert est["per_regime"]["charge"] == pytest.approx(_LAM_C, abs=0.01)


# ── sqlite-ATTACH harness ─────────────────────────────────────────────────────

_SUYOU_OWN = "内蒙.景蓝乌尔图储能电站/220kV.1M"
_FLEET_A, _FLEET_B = "内蒙.甲站/220kV.1M", "内蒙.乙站/220kV.1M"


@pytest.fixture()
def sqlite_engine():
    eng = create_engine("sqlite:///:memory:")
    with eng.begin() as conn:
        conn.execute(text("ATTACH DATABASE ':memory:' AS marketdata"))
        conn.execute(text("ATTACH DATABASE ':memory:' AS reports"))
        conn.execute(text("""
            CREATE TABLE marketdata.md_mengxi_nodal_price_96_filled (
                node_name TEXT, metric_time TEXT, time_order_96 INTEGER,
                avg_node_price NUMERIC,
                PRIMARY KEY (node_name, metric_time))"""))
        conn.execute(text("""
            CREATE TABLE marketdata.ops_bess_dispatch_15min (
                asset_code TEXT, interval_start TEXT,
                actual_dispatch_mw NUMERIC,
                PRIMARY KEY (asset_code, interval_start))"""))
    ensure_table(eng)  # creates reports.station_market_impact
    return eng


def _load_synth(eng, days: int = 12):
    """Insert synth prices (suyou own node + 2 fleet nodes constructed so the
    cross-sectional AVG equals the intended fleet series exactly) and suyou
    dispatch rows."""
    rows = _synth_series(days=days)
    with eng.begin() as conn:
        for ts, net, fleet, node in rows:
            other = fleet - (node - fleet) / 2.0
            for name, px in ((_SUYOU_OWN, node), (_FLEET_A, other),
                             (_FLEET_B, other)):
                conn.execute(text(
                    "INSERT INTO marketdata.md_mengxi_nodal_price_96_filled "
                    "(node_name, metric_time, time_order_96, avg_node_price) "
                    "VALUES (:n, :t, 1, :p)"), {"n": name, "t": ts, "p": px})
            conn.execute(text(
                "INSERT INTO marketdata.ops_bess_dispatch_15min "
                "(asset_code, interval_start, actual_dispatch_mw) "
                "VALUES ('suyou', :t, :mw)"), {"t": ts, "mw": net})
        # A second station's price node (flat at fleet — leaves the mean
        # intact) with NO dispatch rows, for the loud-skip test.
        for ts, _, fleet, _ in rows[::8]:
            conn.execute(text(
                "INSERT OR IGNORE INTO marketdata.md_mengxi_nodal_price_96_filled "
                "(node_name, metric_time, time_order_96, avg_node_price) "
                "VALUES ('内蒙.远景乌拉特储能电站/220kV.1M', :t, 1, :p)"),
                {"t": ts, "p": fleet})


def test_estimate_lambda_sqlite_end_to_end(sqlite_engine):
    _load_synth(sqlite_engine)
    est = estimate_lambda(sqlite_engine, "suyou", lookback_days=20,
                          as_of=date(2026, 8, 21))
    assert est["reason"] is None
    assert est["node"] == _SUYOU_OWN  # own node preferred over parents
    assert est["lambda_cny_per_mwh_per_mw"] == pytest.approx(_LAM_D, abs=0.01)
    assert est["per_regime"]["charge"] == pytest.approx(_LAM_C, abs=0.01)
    assert est["r2"] == est["r2_discharge"] and est["se"] == est["se_discharge"]
    assert est["n_obs"] > 0


def test_estimate_lambda_missing_dispatch_loud_skip(sqlite_engine, capsys):
    _load_synth(sqlite_engine)
    est = estimate_lambda(sqlite_engine, "wulate", lookback_days=20,
                          as_of=date(2026, 8, 21))
    assert est["lambda_cny_per_mwh_per_mw"] is None
    assert "ops_bess_dispatch_15min" in est["reason"]
    assert "WARNING" in capsys.readouterr().out


def test_estimate_lambda_no_node_skips_before_engine_use(capsys):
    # wuhai has price_nodes=[] (registry) — skip without touching the engine.
    est = estimate_lambda(None, "wuhai", lookback_days=20,
                          as_of=date(2026, 8, 21))
    assert est["lambda_cny_per_mwh_per_mw"] is None
    assert "no registry price node" in est["reason"]
    assert "WARNING" in capsys.readouterr().out


def test_persist_upsert_and_latest_read(sqlite_engine):
    _load_synth(sqlite_engine)
    ts = datetime(2026, 8, 21, 10, 0, 0)
    est = persist_lambda(sqlite_engine, "suyou", lookback_days=20,
                         as_of=date(2026, 8, 21), estimated_at=ts)
    assert est["per_regime"]["discharge"] == pytest.approx(_LAM_D, abs=0.01)
    # Same estimated_at again -> upsert, still one row.
    persist_lambda(sqlite_engine, "suyou", lookback_days=20,
                   as_of=date(2026, 8, 21), estimated_at=ts)
    with sqlite_engine.connect() as conn:
        n = conn.execute(text(
            "SELECT COUNT(*) FROM reports.station_market_impact "
            "WHERE asset_code = 'suyou'")).scalar()
    assert n == 1
    # An older, different estimate must not win the latest-read.
    with sqlite_engine.begin() as conn:
        conn.execute(_UPSERT, {
            "asset_code": "suyou",
            "estimated_at": datetime(2026, 8, 20, 10, 0, 0),
            "lambda_discharge": 9.9, "lambda_charge": 9.9,
            "n_obs": 1, "r2": 0.1, "se_discharge": 1.0, "se_charge": 1.0,
            "lookback_days": 20})
    latest = get_latest_lambdas(sqlite_engine)
    assert set(latest) == {"suyou"}
    assert latest["suyou"]["lambda_discharge"] == pytest.approx(_LAM_D, abs=0.01)
    assert latest["suyou"]["lambda_charge"] == pytest.approx(_LAM_C, abs=0.01)


def test_persist_loud_skip_writes_nothing(sqlite_engine):
    _load_synth(sqlite_engine)
    est = persist_lambda(sqlite_engine, "wulate", lookback_days=20,
                         as_of=date(2026, 8, 21),
                         estimated_at=datetime(2026, 8, 21, 10, 0, 0))
    assert est["lambda_cny_per_mwh_per_mw"] is None
    with sqlite_engine.connect() as conn:
        n = conn.execute(text(
            "SELECT COUNT(*) FROM reports.station_market_impact")).scalar()
    assert n == 0


def test_mengxi_node_impacts_mapping(sqlite_engine):
    """nodal_pf_daily's node→λ map: expanded registry nodes of stations with a
    positive discharge λ; non-positive or absent estimates stay price-taker."""
    with sqlite_engine.begin() as conn:
        conn.execute(_UPSERT, {
            "asset_code": "suyou",
            "estimated_at": datetime(2026, 8, 21, 10, 0, 0),
            "lambda_discharge": 0.55, "lambda_charge": 0.2,
            "n_obs": 100, "r2": 0.5, "se_discharge": 0.01, "se_charge": 0.02,
            "lookback_days": 90})
        conn.execute(_UPSERT, {
            "asset_code": "wulate",  # reverse-causality sign -> excluded
            "estimated_at": datetime(2026, 8, 21, 10, 0, 0),
            "lambda_discharge": -0.4, "lambda_charge": None,
            "n_obs": 100, "r2": 0.5, "se_discharge": 0.01, "se_charge": None,
            "lookback_days": 90})
    impacts = _mengxi_node_impacts(sqlite_engine)
    assert impacts[_SUYOU_OWN] == pytest.approx(0.55)
    # expanded parent fragments share the station λ
    assert impacts["内蒙.苏尼特站/220kV.2M"] == pytest.approx(0.55)
    assert impacts["内蒙.苏尼特站/500kV.1M"] == pytest.approx(0.55)
    assert not any(n.startswith("内蒙.远景乌拉特") for n in impacts)
    assert not any("谷山梁" in n for n in impacts)  # no estimate -> absent


# ── engine impact (tiny synthetic LPs, HiGHS) ────────────────────────────────

@pytest.fixture()
def lp_solver(monkeypatch):
    """Swap the engine's CBC for HiGHS: pulp's bundled CBC is an x86_64 binary
    and cannot execute on this arm64 Mac (no Rosetta). The objective structure
    under test is solver-agnostic; production Fargate (amd64) keeps CBC."""
    import pulp
    monkeypatch.setattr(
        pulp, "PULP_CBC_CMD",
        lambda **kw: pulp.HiGHS(msg=False, timeLimit=kw.get("timeLimit")))


def _solve8(**kw):
    prices = np.array([10, 10, 10, 10, 100, 100, 100, 100], dtype=float)
    return optimise_window(prices, power_mw=1.0, duration_h=2.0,
                           roundtrip_eff=1.0, dt=1.0, **kw)


def test_engine_baseline_price_taker_unchanged(lp_solver):
    res = _solve8()
    assert res.status == "Optimal"
    assert res.profit == pytest.approx(2 * (100 - 10))


def test_engine_impact_shrinks_dispatch_and_profit(lp_solver):
    # λ=50, 8 blocks of 0.125 MW: block-pair slope = 90 − 25(k+0.5) turns
    # negative after block 3 -> 0.5 MW/slot (energy cap binds exactly).
    res50 = _solve8(impact_lambda=50.0)
    assert res50.discharge_mw.sum() == pytest.approx(2.0)
    assert res50.profit == pytest.approx(4 * ((100 - 50 * 0.5) * 0.5
                                              - (10 + 50 * 0.5) * 0.5))
    # λ=200: only block 0 survives -> 0.125 MW/slot, 0.5 MWh total.
    res200 = _solve8(impact_lambda=200.0)
    assert res200.discharge_mw.sum() == pytest.approx(0.5)
    assert res200.profit == pytest.approx(4 * ((100 - 200 * 0.125) * 0.125
                                               - (10 + 200 * 0.125) * 0.125))
    assert res200.profit < res50.profit < 180.0


def test_engine_impact_adjusts_effective_prices_exactly(lp_solver):
    """Reported profit must equal the impact-adjusted market cash flow
    Σ[(p − λ·dis)·dis − (p + λ·cha)·cha]·dt recomputed from the dispatch."""
    lam = 50.0
    res = _solve8(impact_lambda=lam)
    prices = np.array([10, 10, 10, 10, 100, 100, 100, 100], dtype=float)
    expect = float(np.sum((prices - lam * res.discharge_mw) * res.discharge_mw
                          - (prices + lam * res.charge_mw) * res.charge_mw))
    assert res.profit == pytest.approx(expect)
    assert res.profit == pytest.approx(80.0)  # vs 180 price-taker


def test_engine_impact_zero_is_price_taker(lp_solver):
    base, zero = _solve8(), _solve8(impact_lambda=0.0)
    assert np.allclose(base.charge_mw, zero.charge_mw)
    assert np.allclose(base.discharge_mw, zero.discharge_mw)
    assert zero.profit == pytest.approx(base.profit)


def test_engine_impact_negative_raises(lp_solver):
    with pytest.raises(ValueError, match="impact_lambda"):
        _solve8(impact_lambda=-1.0)


def test_engine_impact_respects_zone_masks(lp_solver):
    # Masks bound every block variable in impact mode: no discharge at all.
    res = _solve8(impact_lambda=50.0, no_discharge_slots={4, 5, 6, 7})
    assert np.allclose(res.discharge_mw, 0)
    assert res.profit == pytest.approx(0.0)


def test_15min_wrapper_passes_impact(lp_solver):
    prices = pd.Series([10.0] * 48 + [100.0] * 48,
                       index=pd.date_range("2026-09-02", periods=96, freq="15min"))
    _, base_profit = compute_dispatch_from_15min_prices(
        prices, power_mw=1.0, duration_h=2.0, roundtrip_eff=1.0)
    assert float(base_profit.sum()) == pytest.approx(180.0)
    # Huge λ kills every block's margin -> no dispatch.
    disp, profit = compute_dispatch_from_15min_prices(
        prices, power_mw=1.0, duration_h=2.0, roundtrip_eff=1.0,
        impact_lambda=1e4)
    assert np.allclose(disp["discharge_mw"], 0)
    assert float(profit.sum()) == pytest.approx(0.0)
