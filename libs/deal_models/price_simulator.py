"""libs/deal_models/price_simulator.py — OU and PCA price path simulators."""
from __future__ import annotations

import numpy as np
from libs.deal_models.contracts import OUParams, PCAModelParams, PCScoreParams, PriceSimRequest


# ── OU Model ─────────────────────────────────────────────────────────────────

def fit_ou(prices: list[float], dt: float = 1 / 8760) -> OUParams:
    """Fit OU params via AR(1) regression on hourly price series."""
    p = np.asarray(prices, dtype=float)
    x, y = p[:-1], p[1:]
    # y = slope*x + intercept  (np.polyfit returns [slope, intercept])
    slope, intercept = np.polyfit(x, y, 1)
    slope = np.clip(slope, 1e-9, 1.0 - 1e-9)
    residuals = y - (slope * x + intercept)
    kappa = float(max(-np.log(slope) / dt, 0.01))
    mu = float(intercept / (1.0 - slope))
    sigma = float(max(residuals.std() / np.sqrt(dt), 1.0))
    return OUParams(kappa=kappa, mu=mu, sigma=sigma)


def simulate_ou(params: OUParams, n_sim: int, n_years: int, seed: int = 42) -> np.ndarray:
    """Simulate OU price paths. Returns (n_sim, n_years*8760)."""
    n_hours = n_years * 8760
    dt = 1.0 / 8760
    rng = np.random.default_rng(seed)
    paths = np.empty((n_sim, n_hours))
    paths[:, 0] = params.mu
    sqrt_dt = np.sqrt(dt)
    for t in range(1, n_hours):
        drift = params.kappa * (params.mu - paths[:, t - 1]) * dt
        diff = params.sigma * sqrt_dt * rng.standard_normal(n_sim)
        paths[:, t] = paths[:, t - 1] + drift + diff
    np.maximum(paths, 0.0, out=paths)
    return paths


# ── PCA Model ────────────────────────────────────────────────────────────────

def fit_pca(prices: list[float], n_components: int = 4) -> PCAModelParams:
    """
    Fit PCA to hourly price history.
    prices: flat list of hourly prices (len must be divisible by 24, recommend >= 8760*2).
    Returns PCAModelParams with loadings, mean_profile, and fitted normal per PC.
    """
    p = np.asarray(prices, dtype=float)
    n_complete_days = len(p) // 24
    X = p[: n_complete_days * 24].reshape(n_complete_days, 24)

    mean_profile = X.mean(axis=0)
    Xc = X - mean_profile

    # SVD-based PCA (no sklearn dependency)
    _, _, Vt = np.linalg.svd(Xc, full_matrices=False)
    loadings = Vt[:n_components]           # (n_components, 24)
    scores = Xc @ loadings.T               # (n_complete_days, n_components)

    pc_params = [
        PCScoreParams(
            pc_index=i,
            loc=float(scores[:, i].mean()),
            scale=float(max(scores[:, i].std(), 1e-6)),
        )
        for i in range(n_components)
    ]

    return PCAModelParams(
        n_components=n_components,
        pc_params=pc_params,
        loadings=loadings.tolist(),
        mean_profile=mean_profile.tolist(),
    )


def simulate_pca(params: PCAModelParams, n_sim: int, n_years: int, seed: int = 42) -> np.ndarray:
    """Simulate price paths via PCA. Returns (n_sim, n_years*8760)."""
    n_days = n_years * 365
    n_hours = n_days * 24
    rng = np.random.default_rng(seed)

    loadings = np.array(params.loadings)       # (n_components, 24)
    mean_profile = np.array(params.mean_profile)  # (24,)

    # Sample PC scores: (n_sim * n_days, n_components)
    total_days = n_sim * n_days
    scores = np.column_stack([
        rng.normal(loc=pc.loc, scale=pc.scale, size=total_days)
        for pc in params.pc_params
    ])

    # Reconstruct daily 24h profiles
    daily_profiles = scores @ loadings + mean_profile  # (total_days, 24)
    np.maximum(daily_profiles, 0.0, out=daily_profiles)

    # Reshape to (n_sim, n_hours)
    paths = daily_profiles.reshape(n_sim, n_hours)
    return paths


# ── Public entrypoint ─────────────────────────────────────────────────────────

def simulate_prices(req: PriceSimRequest, seed: int = 42) -> np.ndarray:
    """
    Dispatch to OU or PCA simulator based on req.model.
    If params are None, fits from req.price_history_yuan_mwh.
    Returns np.ndarray (req.n_simulations, req.n_years * 8760).
    """
    if req.model == "ou":
        ou_params = req.ou_params
        if ou_params is None:
            if req.price_history_yuan_mwh is None:
                raise ValueError("PriceSimRequest: ou_params or price_history_yuan_mwh required for OU model")
            ou_params = fit_ou(req.price_history_yuan_mwh)
        return simulate_ou(ou_params, req.n_simulations, req.n_years, seed=seed)

    if req.model == "pca":
        pca_params = req.pca_params
        if pca_params is None:
            if req.price_history_yuan_mwh is None:
                raise ValueError("PriceSimRequest: pca_params or price_history_yuan_mwh required for PCA model")
            pca_params = fit_pca(req.price_history_yuan_mwh)
        return simulate_pca(pca_params, req.n_simulations, req.n_years, seed=seed)

    raise ValueError(f"Unknown model: {req.model!r}")
