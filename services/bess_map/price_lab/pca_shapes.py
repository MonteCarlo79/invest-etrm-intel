"""PCA shape model — pure computation, no DB/I/O.

compute_pca is extracted verbatim from apps/bess-map/app.py (covariance
eigendecomposition with sum=24 loading normalisation). The returned dict also
carries "_raw_eigvecs" (un-normalised eigenvectors used for the score
projection) so Task 6 can do exact shape reconstruction.
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def build_deviation_matrix(hourly_prices: pd.DataFrame) -> pd.DataFrame:
    """(days × 24) matrix of rt_price − daily mean. Days with <24 hours dropped."""
    df = hourly_prices.copy()
    df["_day"] = df.index.date
    df["_hour"] = df.index.hour
    counts = df.groupby("_day")["_hour"].count()
    full_days = counts[counts == 24].index
    df = df[df["_day"].isin(set(full_days))]
    mat = df.pivot_table(index="_day", columns="_hour", values="rt_price")
    mat = mat.sort_index().reindex(columns=list(range(24)))
    return mat.sub(mat.mean(axis=1), axis=0)


def compute_pca(price_matrix: pd.DataFrame, n_pcs: int = 4) -> dict:
    """
    PCA on a (days × 24) price matrix using covariance matrix eigendecomposition.
    Loadings are normalised to sum=24 (mean=1.0) — same convention as reference model.
    Returns dict with keys: loadings, eigenvalues, variance_explained, mean_profile,
                            n_days, scores (n_days × n_pcs, raw projection), dates.
    """
    X = price_matrix.values.astype(float)
    mean_profile = X.mean(axis=0)          # shape (24,)
    X_centered = X - mean_profile          # mean-centre each day

    cov_mat = np.cov(X_centered.T)         # 24×24 covariance matrix
    # eigh for symmetric real matrix — guaranteed real eigenvalues, stable
    eig_vals, eig_vecs = np.linalg.eigh(cov_mat)

    # Sort descending by eigenvalue magnitude
    order = np.argsort(np.abs(eig_vals))[::-1]
    eig_vals = eig_vals[order]
    eig_vecs = eig_vecs[:, order]          # columns are eigenvectors

    total_var = eig_vals.sum()
    variance_explained = (eig_vals / total_var * 100) if total_var > 0 else eig_vals * 0

    n_keep = min(n_pcs, eig_vecs.shape[1])
    loadings = []
    for i in range(n_keep):
        vec = eig_vecs[:, i].copy()
        s = vec.sum()
        if abs(s) > 1e-10:
            vec = vec / s * 24.0           # normalise: mean=1, sum=24
        else:
            vec = vec / (np.abs(vec).sum() + 1e-10) * 24.0
        loadings.append(vec)

    # Daily PC scores: project mean-centred price vectors onto raw eigenvectors
    scores = X_centered @ eig_vecs[:, :n_keep]  # shape (n_days, n_keep)

    return {
        "loadings": loadings,              # list of n_pcs arrays, each shape (24,)
        "eigenvalues": eig_vals,
        "variance_explained": variance_explained,
        "mean_profile": mean_profile,
        "n_days": len(X),
        "scores": scores,                  # ndarray (n_days, n_pcs)
        "dates": list(price_matrix.index), # trading dates aligned with scores
        "_raw_eigvecs": eig_vecs[:, :n_keep],  # ndarray (24, n_keep), un-normalised
    }
