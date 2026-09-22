# services/nodal_forecast/price_formation.py
"""L2 nodal price formation: turn a grid-wide price forecast into per-node
15-min curves with congestion regimes.

Model shape (per cluster, per regime):
    nodal = grid_price_hat + delta
    delta = delta_intercept[regime] + a * headroom_norm + b * renewable_norm

- Regime is the boolean `congested` flag (True = substation headroom binding).
- `headroom_norm` / `renewable_norm` are z-scores using feature mean/std
  computed over the whole fitting history (cluster-wide), so per-node models
  and the cluster default share one normalization scale.
- Coefficients are ridge (lambda = 1.0), intercept unpenalized, closed form.
- Per-node models are fitted when hist carries a `node` column and the node
  has >= min_node_samples rows; thin/absent nodes resolve to cluster_default.

Inputs downstream (Task 5/7 wiring, not this module):
    grid_price_hat   <- db.get_grid_forecast(...).price_hat (day average)
    headroom         <- marketdata.spot_fundamentals_hourly bidding space
    substation_renewable <- renewable output behind the substation
    nodal actuals    <- marketdata.md_mengxi_nodal_price_96.avg_node_price

grid_price_hat is a single day-average value, so nodal_curve returns a flat
(96,) curve; intraday shape is applied by the L3 layer on top of this level.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

RIDGE_LAMBDA = 1.0
_FEATURES = ("headroom", "substation_renewable")
_REQUIRED = ("d", "grid_price", "headroom", "substation_renewable",
             "congested", "nodal_price")


def _feature_stats(hist: pd.DataFrame) -> dict[str, tuple[float, float]]:
    """Population mean/std per feature over the full history; std floored at
    1.0 when degenerate (constant feature -> z-score 0)."""
    stats = {}
    for f in _FEATURES:
        v = hist[f].to_numpy(dtype=float)
        mean = float(v.mean())
        std = float(v.std())  # ddof=0; NaN-safe for n>=1 via floor below
        stats[f] = (mean, std if std > 1e-9 else 1.0)
    return stats


def _ridge(X: np.ndarray, y: np.ndarray, lam: float,
           fallback_intercept: float) -> dict:
    """Ridge with unpenalized intercept (center X and y). A regime with no
    rows borrows the overall mean delta and zero coefficients."""
    n = len(y)
    if n == 0:
        return {"delta_intercept": float(fallback_intercept),
                "headroom_coef": 0.0, "renewable_coef": 0.0, "n": 0}
    xm = X.mean(axis=0)
    ym = float(y.mean())
    Xc = X - xm
    yc = y - ym
    a = Xc.T @ Xc + lam * np.eye(X.shape[1])
    beta = np.linalg.solve(a, Xc.T @ yc)
    return {"delta_intercept": float(ym - xm @ beta),
            "headroom_coef": float(beta[0]),
            "renewable_coef": float(beta[1]),
            "n": int(n)}


def _fit_regimes(hist: pd.DataFrame, stats: dict, lam: float) -> dict:
    delta = (hist["nodal_price"] - hist["grid_price"]).to_numpy(dtype=float)
    h = (hist["headroom"].to_numpy(dtype=float) - stats["headroom"][0]) / stats["headroom"][1]
    r = ((hist["substation_renewable"].to_numpy(dtype=float)
          - stats["substation_renewable"][0]) / stats["substation_renewable"][1])
    X = np.column_stack([h, r])
    congested = hist["congested"].astype(bool).to_numpy()
    overall = float(delta.mean())
    return {flag: _ridge(X[congested == flag], delta[congested == flag],
                         lam, fallback_intercept=overall)
            for flag in (False, True)}


def fit_cluster_model(hist: pd.DataFrame, min_node_samples: int = 30,
                      lam: float = RIDGE_LAMBDA) -> dict:
    """Fit ridge coefficients per congestion regime for one cluster.

    hist columns: d, grid_price, headroom, substation_renewable, congested,
    nodal_price; optional `node` enables per-node models. Returns:
        {"regimes": {False: {...}, True: {...}},
         "feature_stats": {feature: (mean, std)},
         "cluster_default": {"regimes": ..., "feature_stats": ..., "n_samples": int},
         "nodes": {node: same shape as cluster_default}}
    Each regime carries delta_intercept, headroom_coef, renewable_coef, n.
    """
    missing = [c for c in _REQUIRED if c not in hist.columns]
    if missing:
        raise ValueError(f"hist missing required columns: {missing}")
    if hist.empty:
        raise ValueError("hist is empty")

    stats = _feature_stats(hist)
    cluster_default = {"regimes": _fit_regimes(hist, stats, lam),
                       "feature_stats": stats, "n_samples": int(len(hist))}
    model = {"regimes": cluster_default["regimes"],
             "feature_stats": stats,
             "cluster_default": cluster_default,
             "nodes": {}}
    if "node" in hist.columns:
        for node, sub in hist.groupby("node"):
            if len(sub) >= min_node_samples:
                model["nodes"][node] = {
                    "regimes": _fit_regimes(sub, stats, lam),
                    "feature_stats": stats,
                    "n_samples": int(len(sub))}
    return model


def nodal_curve(grid_price_hat: float, features: dict, model: dict) -> np.ndarray:
    """Nodal 15-min curve for one node for one day: grid + regime delta.
    Missing features default to the fitting mean (z-score 0 -> intercept only).
    Flat (96,) because grid_price_hat is a day-average level; intraday shape
    belongs to the L3 layer."""
    regime = model["regimes"][bool(features.get("congested", False))]
    stats = model["feature_stats"]
    h_mean, h_std = stats["headroom"]
    r_mean, r_std = stats["substation_renewable"]
    h_n = (float(features.get("headroom", h_mean)) - h_mean) / h_std
    r_n = (float(features.get("substation_renewable", r_mean)) - r_mean) / r_std
    delta = (regime["delta_intercept"]
             + regime["headroom_coef"] * h_n
             + regime["renewable_coef"] * r_n)
    return np.full(96, float(grid_price_hat) + delta, dtype=float)


def resolve_model(model: dict, node: str) -> dict:
    """Per-node model when the node is in the fitted set, else cluster_default."""
    m = model.get("nodes", {}).get(node)
    return m if m is not None else model["cluster_default"]
