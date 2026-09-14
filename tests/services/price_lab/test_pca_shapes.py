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


def test_ridge_recovers_linear_scores():
    import numpy as np, pandas as pd
    from services.bess_map.price_lab.pca_shapes import fit_score_models, predict_scores
    rng = np.random.default_rng(2)
    n, k = 200, 2
    X = pd.DataFrame({"load_d1_mw": rng.normal(25000, 3000, n),
                      "bidding_space_d1_mw": rng.normal(15000, 2000, n)})
    true_w = np.array([[10.0, -5.0], [0.02, 0.0], [0.0, 0.03]])
    Xs = (X - X.mean()) / X.std(ddof=0)
    # NOTE: brief specified noise scale 0.5, but signal std here is 0.02-0.03,
    # which caps achievable correlation at ~0.06. Noise reduced to 0.002 so the
    # >0.95 assertion tests what it intends (ridge recovers the linear map).
    scores = np.column_stack([np.ones(n)] + [Xs.values]) @ true_w + rng.normal(0, 0.002, (n, k))
    w, mu, sd = fit_score_models(scores, X, lam=1.0)
    pred = predict_scores(w, mu, sd, X)
    assert np.corrcoef(pred[:, 0], scores[:, 0])[0, 1] > 0.95
    assert np.corrcoef(pred[:, 1], scores[:, 1])[0, 1] > 0.95


def test_reconstruct_shape_matches_projection():
    import numpy as np
    from services.bess_map.price_lab.pca_shapes import reconstruct_shape, compute_pca
    rng = np.random.default_rng(3)
    mat = pd.DataFrame(rng.normal(0, 50, (40, 24)))
    out = compute_pca(mat, n_pcs=4)
    # reconstruct day 0 from its scores must match the raw eigenvector projection
    recon = reconstruct_shape(out["loadings"], out["scores"][0], out.get("_raw_eigvecs"))
    centered = mat.values[0] - out["mean_profile"]
    raw_proj = centered @ out["_raw_eigvecs"] @ out["_raw_eigvecs"].T
    assert np.allclose(recon, raw_proj, atol=1e-6)
