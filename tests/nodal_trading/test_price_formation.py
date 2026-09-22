# tests/nodal_trading/test_price_formation.py
from datetime import date

import numpy as np
import pandas as pd
import pytest

from services.nodal_forecast import backtest
from services.nodal_forecast import price_formation as pf


def _hist(days=60, congested_from=30):
    rows = []
    for d in range(days):
        congested = d >= congested_from
        grid = 300.0
        nodal = grid + (50 if congested else 5)
        rows.append(dict(d=d, grid_price=grid, headroom=-10.0 if congested else 200.0,
                         substation_renewable=900.0 if congested else 100.0,
                         congested=congested, nodal_price=nodal))
    return pd.DataFrame(rows)


def test_fit_recovers_regime_coefficients():
    model = pf.fit_cluster_model(_hist())
    free = model["regimes"][False]
    cong = model["regimes"][True]
    assert free["delta_intercept"] == pytest.approx(5, abs=3)
    assert cong["delta_intercept"] == pytest.approx(50, abs=8)


def test_nodal_curve_applies_regime_delta_to_shape():
    model = pf.fit_cluster_model(_hist())
    curve = pf.nodal_curve(310.0, dict(headroom=-10.0, substation_renewable=900.0,
                                       congested=True), model)
    assert curve.shape == (96,)
    assert curve.mean() == pytest.approx(310 + 50, abs=8)


def test_fallback_to_cluster_when_node_thin():
    model = pf.fit_cluster_model(_hist())
    assert pf.resolve_model(model, node="nonexistent_node") is model["cluster_default"]


def _nodal_hist_with_nodes():
    base = _hist()
    frames = []
    for node, extra in (("node_a", 20.0), ("node_b", -15.0)):
        sub = base.copy()
        sub["node"] = node
        sub["nodal_price"] = sub["nodal_price"] + extra
        frames.append(sub)
    thin = base.head(5).copy()
    thin["node"] = "node_thin"
    frames.append(thin)
    return pd.concat(frames, ignore_index=True)


def test_resolve_model_returns_per_node_when_fitted():
    model = pf.fit_cluster_model(_nodal_hist_with_nodes(), min_node_samples=10)
    ma = pf.resolve_model(model, node="node_a")
    assert ma is not model["cluster_default"]
    # node_a is shifted +20 vs cluster in both regimes
    assert ma["regimes"][False]["delta_intercept"] == pytest.approx(25, abs=3)
    assert ma["regimes"][True]["delta_intercept"] == pytest.approx(70, abs=8)


def test_resolve_model_falls_back_for_thin_node():
    model = pf.fit_cluster_model(_nodal_hist_with_nodes(), min_node_samples=10)
    assert pf.resolve_model(model, node="node_thin") is model["cluster_default"]


def test_fit_rejects_missing_columns():
    with pytest.raises(ValueError):
        pf.fit_cluster_model(pd.DataFrame(dict(d=[1], grid_price=[300.0])))


def test_nodal_mae_reports_per_cluster_and_grid_baseline():
    def model_fn(day):
        return pd.DataFrame([
            dict(cluster="乌海", nodal_hat=350.0, grid_hat=300.0, actual=345.0),
            dict(cluster="乌海", nodal_hat=352.0, grid_hat=300.0, actual=355.0),
            dict(cluster="包头", nodal_hat=301.0, grid_hat=300.0, actual=305.0),
        ])
    out = backtest.nodal_mae(model_fn, days=[date(2026, 9, 20), date(2026, 9, 21)])
    assert out["乌海"] == pytest.approx(4.0)                 # (5+3)/2 each day
    assert out["乌海__grid_baseline"] == pytest.approx(50.0)  # (45+55)/2
    assert out["包头"] == pytest.approx(4.0)
    assert out["包头__grid_baseline"] == pytest.approx(5.0)
    # gate: nodal model must not be worse than the grid-only baseline
    for c in ("乌海", "包头"):
        assert out[c] <= out[f"{c}__grid_baseline"]


def test_nodal_mae_exposes_gate_when_nodal_is_worse():
    def model_fn(day):
        return pd.DataFrame([
            dict(cluster="x", nodal_hat=400.0, grid_hat=300.0, actual=300.0)])
    out = backtest.nodal_mae(model_fn, days=[date(2026, 9, 20)])
    assert out["x"] == pytest.approx(100.0)
    assert out["x__grid_baseline"] == pytest.approx(0.0)
