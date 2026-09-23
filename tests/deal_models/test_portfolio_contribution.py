"""Tests for asset_registry seed/load + portfolio_contribution compute."""
import numpy as np
import pandas as pd
import pytest

from libs.deal_models.workflows.portfolio_contribution import (
    contribution_metrics, fleet_proxy,
)


def _prices(days=60, seed=1):
    idx = pd.date_range("2026-06-01", periods=days, freq="D")
    rng = np.random.default_rng(seed)
    base = 300 + 60 * np.sin(2 * np.pi * np.arange(days) / 30) + rng.normal(0, 15, days)
    return pd.DataFrame({
        "nodeA": base,
        "nodeB": base * 0.9 + rng.normal(0, 10, days),       # correlated with A
        "nodeC": 250 + rng.normal(0, 40, days),              # low correlation
    }, index=idx)


def _fleet():
    return pd.DataFrame([
        {"zone_price_node": "nodeA", "capacity_mw": 100.0, "duration_h": 2.0},
        {"zone_price_node": "nodeB", "capacity_mw": 50.0, "duration_h": 2.0},
    ])


def test_fleet_proxy_weights_and_shape():
    p = _prices()
    s = fleet_proxy(p, _fleet())
    assert len(s) == 60
    # weight A=200, B=100 → proxy = A*200 + B*100
    expected = p["nodeA"] * 200 + p["nodeB"] * 100
    pd.testing.assert_series_equal(s, expected, check_names=False)


def test_fleet_proxy_skips_unknown_nodes():
    p = _prices()
    fleet = pd.DataFrame([
        {"zone_price_node": "nodeA", "capacity_mw": 100.0, "duration_h": 2.0},
        {"zone_price_node": "nodeZZZ", "capacity_mw": 999.0, "duration_h": 9.0},
    ])
    s = fleet_proxy(p, fleet)
    pd.testing.assert_series_equal(s, p["nodeA"] * 200, check_names=False)


def test_diversifying_candidate_lowers_vol_per_mw():
    p = _prices()
    f = fleet_proxy(p, _fleet())
    # candidate on low-correlation nodeC
    m = contribution_metrics(f, p["nodeC"], fleet_weight=300.0, candidate_weight=200.0)
    assert m["corr"] is not None and m["corr"] < 0.75
    assert m["diversification"] is not None and m["diversification"] > 0
    assert m["vol_per_mw_after"] < m["vol_per_mw_before"]


def test_cloned_candidate_raises_correlation_near_one():
    p = _prices()
    f = fleet_proxy(p, _fleet())
    clone = f / 300.0 * 200.0  # same shape, rescaled
    m = contribution_metrics(f, clone, fleet_weight=300.0, candidate_weight=200.0)
    assert m["corr"] == pytest.approx(1.0)
    assert m["diversification"] == pytest.approx(0.0, abs=1e-9)


def test_insufficient_overlap_returns_empty():
    f = pd.Series([1.0, 2.0, 3.0], index=pd.date_range("2026-01-01", periods=3))
    c = pd.Series([1.0, 2.0, 3.0], index=pd.date_range("2026-02-01", periods=3))
    m = contribution_metrics(f, c, 300.0, 200.0)
    assert m["corr"] is None and m["diversification"] is None


def test_seed_is_idempotent_and_status_ordered():
    from services.common import asset_registry as ar
    codes = [r["asset_code"] for r in ar._SEED]
    assert len(codes) == len(set(codes)) == 9
    op = [r for r in ar._SEED if r["status"] == "operating"]
    up = [r for r in ar._SEED if r["status"] == "upcoming"]
    assert len(op) == 6 and len(up) == 3
    # every operating asset has a zone price node; upcoming alashan is the only one without
    assert all(r["zone_price_node"] for r in op)
    missing = [r["asset_code"] for r in up if not r["zone_price_node"]]
    assert missing == ["alashan"]
