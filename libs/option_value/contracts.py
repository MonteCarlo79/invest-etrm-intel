"""Shared dataclass contracts for the option-value pricing engine.

Pure data definitions only: no DB, no file I/O, no streamlit.
Prices are ¥/MWh internally everywhere.
"""

from dataclasses import dataclass
from datetime import date

import numpy as np


@dataclass
class ExpectedCurve:
    """Expected price curve for one province over a history window.

    `month_hour_mean` row index is `month % 12`: row 0 = December,
    rows 1-11 = January-November (see curves.build_expected_curve).
    """

    province: str
    window_start: date
    window_end: date
    month_hour_mean: np.ndarray  # (12, 24) ¥/MWh
    hourly: np.ndarray  # (n_complete_days*24,) ¥/MWh tiled curve
    floor: float
    cap: float
    n_days: int
    coverage_months: float
    insufficient_history: bool


@dataclass
class PCASimConfig:
    n_components: int = 4
    n_paths: int = 500
    seed: int = 42
    innovations: str = "student_t"


@dataclass
class PCAv2Params:
    loadings: np.ndarray  # (k, 24)
    phi: np.ndarray  # (k,) AR(1) coef
    inno_scale: np.ndarray  # (k,)
    inno_df: np.ndarray  # (k,) — NaN means Normal


@dataclass
class BESSParams:
    power_mw: float = 1.0
    duration_h: float = 2.0
    rte: float = 0.85
    soc_grid_step_frac: float = 0.1


@dataclass
class RenewableParams:
    capacity_mw: float = 1.0
    cf_profile: np.ndarray = None  # (12, 24)
    price_volume_corr: float = 0.0
    cf_sigma: float = 0.15
    curtailment_floor: bool = True
    asset_type: str = "wind"


@dataclass
class DispatchResult:
    revenue_per_path: np.ndarray  # (n_paths,) ¥ for power_mw unit
    discharge_mwh_per_path: np.ndarray  # (n_paths,) grid-delivered


@dataclass
class PolicyLevers:
    capacity_price_yuan_kw_yr: float = 0.0
    cap_comp_yuan_kw_yr: float = 0.0
    fr_yuan_kw_yr: float = 0.0
    discharge_comp_yuan_kwh: float = 0.0
    sysopfee_yuan_kwh: float = 0.0
    mechanism_premium_yuan_mwh: float = 0.0
    green_cert_yuan_mwh: float = 0.0
    notes: str = ""


@dataclass
class ValueStack:
    intrinsic_yuan_kw: float
    extrinsic_yuan_kw: float
    policy_net_yuan_kw: float
    total_yuan_kw: float
    capture_adj_yuan_kw: float | None
    extrinsic_p10: float
    extrinsic_p90: float


@dataclass
class PolicyScenario:
    name: str
    lever_overrides: dict
    effective_date: date
    probability: float


@dataclass
class PermitInputs:
    permit_cost_yuan: float
    validity_years: float
    capex_yuan_kwh_today: float
    capex_decline_rate: float
    power_mw: float
    duration_h: float
    hurdle: float = 0.08
    life_years: int = 15


@dataclass
class PermitResult:
    v_permit_yuan: float
    breakeven_permit_yuan: float
    go: bool
    decomposition: dict
    dp_check_value: float


@dataclass
class AcquisitionResult:
    fair_npv_yuan: float
    margin_yuan: float
    go: bool
    rows: list
