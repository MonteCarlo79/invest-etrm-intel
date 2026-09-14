import numpy as np
import pandas as pd
from services.bess_map.price_lab.pca_shapes import build_deviation_matrix, compute_pca


def _toy_days(n_days=60, seed=1):
    rng = np.random.default_rng(seed)
    hours = np.arange(24)
    days = []
    for d in range(n_days):
        level = 300 + rng.normal(0, 30)
        duck = -80 * np.exp(-((hours - 12) ** 2) / 8) * (1 + 0.2 * rng.normal())
        peak = 120 * np.exp(-((hours - 19) ** 2) / 6) * (1 + 0.2 * rng.normal())
        days.append(level + duck + peak + rng.normal(0, 5, 24))
    idx = pd.date_range("2026-01-01", periods=n_days * 24, freq="h")
    vals = np.repeat(np.arange(n_days), 24)
    df = pd.DataFrame({"rt_price": np.array(days).ravel()}, index=idx)
    df["_day"] = vals
    return df


def test_deviation_matrix_shape_and_zero_mean():
    df = _toy_days(30)
    mat = build_deviation_matrix(df)
    assert mat.shape == (30, 24)
    assert np.allclose(mat.mean(axis=1).values, 0.0, atol=1e-8)


def test_deviation_matrix_drops_short_days():
    df = _toy_days(10).iloc[:-5]   # last day has 19 hours
    mat = build_deviation_matrix(df)
    assert mat.shape[0] == 9


def test_compute_pca_recovers_dominant_shape():
    df = _toy_days(60)
    mat = build_deviation_matrix(df)
    out = compute_pca(mat, n_pcs=3)
    assert len(out["loadings"]) == 3
    assert out["variance_explained"][0] > 40.0     # duck+peak dominate
    assert out["scores"].shape == (60, 3)
