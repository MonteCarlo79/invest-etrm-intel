from __future__ import annotations
import numpy as np
import pytest
from libs.deal_models.contracts import (
    MCRequest, PriceSimRequest, OUParams, DispatchRequest, ProjectFinancials,
)


def _small_mc_request(n: int = 50) -> MCRequest:
    return MCRequest(
        price_sim=PriceSimRequest(
            province="蒙西", n_simulations=n, n_years=1, model="ou",
            ou_params=OUParams(kappa=2.0, mu=300.0, sigma=60.0),
        ),
        dispatch=DispatchRequest(
            asset_type="bess", capacity_mwh=100.0, power_mw=50.0,
            roundtrip_eff=0.85, cycles_per_day=1.0,
        ),
        financials=ProjectFinancials(
            capex_total_yuan=1e8, project_life_years=20,
            annual_revenue_yuan=[2e7] * 20, annual_om_yuan=3e6,
        ),
        n_simulations=n,
    )


def test_mc_result_array_shapes():
    from libs.deal_models.monte_carlo import run_monte_carlo
    result = run_monte_carlo(_small_mc_request(50))
    assert result.revenue_paths.shape == (50,)
    assert result.equity_irr_paths.shape == (50,)
    assert result.npv_paths.shape == (50,)


def test_mc_percentile_ordering():
    from libs.deal_models.monte_carlo import run_monte_carlo
    result = run_monte_carlo(_small_mc_request(200))
    assert result.revenue_p10 < result.revenue_p50 < result.revenue_p90
    assert result.equity_irr_p10 < result.equity_irr_p50 < result.equity_irr_p90


def test_mc_irr_prob_in_unit_interval():
    from libs.deal_models.monte_carlo import run_monte_carlo
    result = run_monte_carlo(_small_mc_request(50))
    assert 0.0 <= result.irr_prob_below_hurdle <= 1.0


def test_mc_tornado_non_empty_and_sorted():
    from libs.deal_models.monte_carlo import run_monte_carlo
    result = run_monte_carlo(_small_mc_request(50))
    assert len(result.tornado) > 0
    swings = [t["swing"] for t in result.tornado]
    assert swings == sorted(swings, reverse=True)


def test_mc_cvar_le_var():
    from libs.deal_models.monte_carlo import run_monte_carlo
    result = run_monte_carlo(_small_mc_request(200))
    # CVaR (expected shortfall below 5th pct) <= VaR (5th pct) + small tolerance
    assert result.revenue_cvar_5pct <= result.revenue_var_5pct + 1.0
