"""libs/deal_models/contracts.py — Input schemas (Pydantic) + result types (dataclasses)."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, List, Literal, Optional
from pydantic import BaseModel, Field


# ── Input schemas (validated) ─────────────────────────────────────────────────

class OUParams(BaseModel):
    kappa: float = Field(2.0, gt=0, description="Mean-reversion speed (yr⁻¹)")
    mu: float = Field(300.0, description="Long-run mean yuan/MWh")
    sigma: float = Field(80.0, gt=0, description="Annualised vol yuan/MWh")


class PCScoreParams(BaseModel):
    pc_index: int
    loc: float = 0.0
    scale: float = Field(1.0, gt=0)


class PCAModelParams(BaseModel):
    n_components: int = Field(4, ge=1, le=8)
    pc_params: List[PCScoreParams]
    loadings: List[List[float]]   # shape (n_components, 24)
    mean_profile: List[float]     # len=24, yuan/MWh


class PriceSimRequest(BaseModel):
    province: str
    n_simulations: int = Field(1000, ge=10, le=10000)
    n_years: int = Field(1, ge=1, le=30)
    model: Literal["ou", "pca"] = "ou"
    ou_params: Optional[OUParams] = None
    pca_params: Optional[PCAModelParams] = None
    price_history_yuan_mwh: Optional[List[float]] = None  # needed when params=None


class DispatchRequest(BaseModel):
    asset_type: Literal["bess", "wind", "wind_bess"]
    # BESS
    capacity_mwh: float = 0.0
    power_mw: float = 0.0
    roundtrip_eff: float = Field(0.85, gt=0, le=1.0)
    cycles_per_day: float = Field(1.0, gt=0)
    om_cost_yuan_per_mwh: float = 10.0
    # Wind / Wind+BESS
    installed_mw: float = 0.0
    capacity_factor_profile: Optional[List[float]] = None  # len=8760


class ProjectFinancials(BaseModel):
    capex_total_yuan: float = Field(..., gt=0)
    commissioning_year: int = 2026
    project_life_years: int = Field(20, ge=1, le=40)
    debt_ratio: float = Field(0.7, ge=0.0, le=0.95)
    loan_term_years: int = Field(10, ge=1, le=30)
    interest_rate: float = Field(0.05, ge=0.0, le=0.30)
    grace_years: int = Field(1, ge=0)
    annual_revenue_yuan: List[float]           # len = project_life_years
    annual_om_yuan: float = Field(..., ge=0)
    annual_degradation_rate: float = Field(0.01, ge=0.0, le=0.10)
    corporate_tax_rate: float = Field(0.25, ge=0.0, le=0.50)
    depreciation_years: int = Field(20, ge=1, le=40)
    residual_value_ratio: float = Field(0.05, ge=0.0, le=0.30)
    hurdle_rate: float = Field(0.08, ge=0.0)


class MCRequest(BaseModel):
    price_sim: PriceSimRequest
    dispatch: DispatchRequest
    financials: ProjectFinancials
    n_simulations: int = Field(1000, ge=10, le=10000)
    random_seed: int = 42


# ── Result types (dataclasses — support numpy arrays) ─────────────────────────

@dataclass
class DispatchResult:
    revenue_paths: Any   # np.ndarray (n_sim,) annual yuan
    p10: float
    p50: float
    p90: float
    mean: float
    std: float


@dataclass
class AnnualRow:
    year: int
    revenue: float
    opex: float
    ebitda: float
    depreciation: float
    ebit: float
    interest: float
    ebt: float
    tax: float
    net_income: float
    debt_service: float
    equity_fcf: float


@dataclass
class CashFlowResult:
    annual: List[AnnualRow]
    project_irr: float
    equity_irr: float
    roace: float
    dscr_min: float
    dscr_avg: float
    npv: float
    payback_years: float


@dataclass
class MCResult:
    revenue_p10: float
    revenue_p50: float
    revenue_p90: float
    revenue_var_5pct: float
    revenue_cvar_5pct: float
    equity_irr_p10: float
    equity_irr_p50: float
    equity_irr_p90: float
    irr_prob_below_hurdle: float
    npv_p10: float
    npv_p50: float
    npv_p90: float
    tornado: List[dict]         # [{"param": str, "irr_high": float, "irr_low": float, "swing": float}]
    revenue_paths: Any          # np.ndarray (n_sim,)
    equity_irr_paths: Any       # np.ndarray (n_sim,)
    npv_paths: Any              # np.ndarray (n_sim,)
