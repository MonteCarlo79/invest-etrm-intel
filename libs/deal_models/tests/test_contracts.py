from __future__ import annotations
import pytest
from pydantic import ValidationError


def test_ou_params_rejects_negative_kappa():
    from libs.deal_models.contracts import OUParams
    with pytest.raises(ValidationError):
        OUParams(kappa=-1.0, mu=300.0, sigma=50.0)


def test_project_financials_requires_revenue_list():
    from libs.deal_models.contracts import ProjectFinancials
    with pytest.raises(ValidationError):
        ProjectFinancials(capex_total_yuan=1e8, annual_om_yuan=2e6)  # missing annual_revenue_yuan


def test_project_financials_valid():
    from libs.deal_models.contracts import ProjectFinancials
    fin = ProjectFinancials(
        capex_total_yuan=1e8,
        annual_revenue_yuan=[2e7] * 20,
        annual_om_yuan=3e6,
    )
    assert fin.project_life_years == 20
    assert fin.debt_ratio == 0.7


def test_dispatch_request_asset_type_literal():
    from libs.deal_models.contracts import DispatchRequest
    with pytest.raises(ValidationError):
        DispatchRequest(asset_type="gas_turbine")


def test_mc_request_valid():
    from libs.deal_models.contracts import MCRequest, PriceSimRequest, OUParams, DispatchRequest, ProjectFinancials
    req = MCRequest(
        price_sim=PriceSimRequest(
            province="蒙西", n_simulations=100, n_years=1, model="ou",
            ou_params=OUParams(kappa=2.0, mu=300.0, sigma=50.0),
        ),
        dispatch=DispatchRequest(asset_type="bess", capacity_mwh=100.0, power_mw=50.0),
        financials=ProjectFinancials(
            capex_total_yuan=1e8, annual_revenue_yuan=[2e7] * 20, annual_om_yuan=3e6,
        ),
        n_simulations=100,
    )
    assert req.n_simulations == 100
