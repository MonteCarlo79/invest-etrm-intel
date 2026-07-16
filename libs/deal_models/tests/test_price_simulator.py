from __future__ import annotations
import numpy as np
import pytest
from libs.deal_models.contracts import OUParams, PriceSimRequest


def _synthetic_prices(n: int = 8760, mu: float = 300.0, seed: int = 0) -> list[float]:
    rng = np.random.default_rng(seed)
    prices = [mu]
    for _ in range(n - 1):
        prices.append(max(prices[-1] + 2.0 * (mu - prices[-1]) / 8760 + 60.0 / 8760**0.5 * rng.standard_normal(), 0.0))
    return prices


def test_simulate_ou_shape():
    from libs.deal_models.price_simulator import simulate_ou
    params = OUParams(kappa=2.0, mu=300.0, sigma=80.0)
    paths = simulate_ou(params, n_sim=10, n_years=1, seed=42)
    assert paths.shape == (10, 8760)


def test_simulate_ou_nonnegative():
    from libs.deal_models.price_simulator import simulate_ou
    params = OUParams(kappa=2.0, mu=300.0, sigma=80.0)
    paths = simulate_ou(params, n_sim=200, n_years=1, seed=42)
    assert (paths >= 0).all()


def test_simulate_ou_mean_reverts():
    from libs.deal_models.price_simulator import simulate_ou
    params = OUParams(kappa=5.0, mu=300.0, sigma=40.0)
    paths = simulate_ou(params, n_sim=500, n_years=1, seed=1)
    # Mean of all paths at year-end should be within 50 yuan of mu
    assert abs(paths[:, -1].mean() - 300.0) < 50.0


def test_fit_ou_recovers_mu():
    from libs.deal_models.price_simulator import fit_ou
    prices = _synthetic_prices(8760, mu=280.0, seed=5)
    params = fit_ou(prices)
    assert abs(params.mu - 280.0) < 80.0  # rough recovery


def test_fit_pca_returns_correct_shape():
    from libs.deal_models.price_simulator import fit_pca
    prices = _synthetic_prices(8760 * 2, mu=300.0, seed=7)
    pca_params = fit_pca(prices, n_components=3)
    assert len(pca_params.loadings) == 3
    assert len(pca_params.loadings[0]) == 24
    assert len(pca_params.mean_profile) == 24
    assert len(pca_params.pc_params) == 3


def test_simulate_pca_shape():
    from libs.deal_models.price_simulator import fit_pca, simulate_pca
    prices = _synthetic_prices(8760 * 2, mu=300.0, seed=8)
    pca_params = fit_pca(prices, n_components=3)
    paths = simulate_pca(pca_params, n_sim=15, n_years=1, seed=42)
    assert paths.shape == (15, 8760)


def test_simulate_prices_ou_dispatch():
    from libs.deal_models.price_simulator import simulate_prices
    req = PriceSimRequest(
        province="蒙西", n_simulations=20, n_years=1, model="ou",
        ou_params=OUParams(kappa=2.0, mu=300.0, sigma=80.0),
    )
    paths = simulate_prices(req, seed=42)
    assert paths.shape == (20, 8760)


def test_simulate_prices_pca_dispatch():
    from libs.deal_models.price_simulator import fit_pca, simulate_prices
    prices = _synthetic_prices(8760 * 2, mu=300.0, seed=9)
    pca_params = fit_pca(prices, n_components=3)
    req = PriceSimRequest(
        province="蒙西", n_simulations=20, n_years=1, model="pca",
        pca_params=pca_params,
    )
    paths = simulate_prices(req, seed=42)
    assert paths.shape == (20, 8760)
