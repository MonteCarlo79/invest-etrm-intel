# Deal Structurer & Pricing Platform — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a reproducible, probabilistic deal-structuring platform (`libs/deal_models` + `services/deal_engine` + `apps/deal-structurer`) that simulates spot price uncertainty, values BESS/wind dispatch revenue, projects full project IRR/DSCR/NPV, and prices structured deals (floor, collar, swap, PPA) via a 5-tab Streamlit app + Claude Strategist.

**Architecture:** Two-layer — `libs/deal_models` (pure computation, zero I/O) feeds `services/deal_engine` (DB + persistence) feeds `apps/deal-structurer` (Streamlit). Mirrors the `libs/decision_models` / `services/` pattern already in the platform.

**Tech Stack:** Python 3.11, NumPy, Pydantic v2, SciPy (available via statsmodels dep), Streamlit, Plotly, Anthropic SDK, PostgreSQL via `services/common/db_utils.py`.

**Spec:** `docs/superpowers/specs/2026-07-09-deal-structurer-design.md`

---

## File Map

```
libs/deal_models/
├── __init__.py
├── contracts.py          # Pydantic input schemas + dataclass result types
├── registry.py           # DealStructureSpec dict registry + helpers
├── price_simulator.py    # fit_ou / simulate_ou / fit_pca / simulate_pca / simulate_prices
├── dispatch_valuation.py # dispatch_annual → DispatchResult (bess/wind/wind_bess)
├── project_cashflow.py   # compute_cashflow → CashFlowResult + _irr helper
├── monte_carlo.py        # run_monte_carlo → MCResult + _tornado helper
├── deal_structures.py    # 6 registered payoff fns + price_structure()
├── adapters/
│   ├── __init__.py
│   └── agent_tools.py    # AGENT_TOOLS list + dispatch fn for Claude API tool use
└── tests/
    ├── __init__.py
    ├── test_contracts.py
    ├── test_price_simulator.py
    ├── test_dispatch_valuation.py
    ├── test_project_cashflow.py
    ├── test_monte_carlo.py
    └── test_deal_structures.py

services/deal_engine/
├── __init__.py
├── price_data.py         # fetch_price_history() → list[float] from marketdata.spot_prices_hourly
├── scenario_store.py     # save_scenario / load_scenario / list_scenarios (JSON files)
└── batch_runner.py       # run_batch() with st.progress() integration

apps/deal-structurer/
├── Dockerfile
├── requirements.txt
├── app.py                # sidebar nav + tab routing + session state init
├── price_tab.py          # Tab 1: province selector, OU/PCA, price path chart
├── dispatch_tab.py       # Tab 2: P10/P50/P90 bar + revenue histogram
├── cashflow_tab.py       # Tab 3: P&L table + KPI summary + waterfall
├── mc_tab.py             # Tab 4: IRR distribution + VaR table + tornado
├── deal_tab.py           # Tab 5: deal selector + pricing output
└── strategist.py         # Claude Strategist agent (session persistence + tool use)
```

---

## Task 1: Foundation — `libs/deal_models/contracts.py` + `registry.py`

**Files:**
- Create: `libs/deal_models/__init__.py`
- Create: `libs/deal_models/contracts.py`
- Create: `libs/deal_models/registry.py`
- Create: `libs/deal_models/tests/__init__.py`
- Create: `libs/deal_models/tests/test_contracts.py`
- Create: `libs/deal_models/tests/test_registry.py`

- [ ] **Step 1: Write failing tests**

```python
# libs/deal_models/tests/test_contracts.py
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
```

```python
# libs/deal_models/tests/test_registry.py
from __future__ import annotations
import pytest
import numpy as np


def test_register_and_get():
    from libs.deal_models.registry import DealStructureSpec, register, get, list_structures
    from pydantic import BaseModel

    class P(BaseModel):
        x: float

    spec = DealStructureSpec(
        name="_test_struct",
        description="test",
        payoff_fn=lambda rev, p: rev - p.x,
        params_schema=P,
    )
    register(spec)
    assert get("_test_struct").name == "_test_struct"
    assert "_test_struct" in list_structures()


def test_get_unknown_raises():
    from libs.deal_models.registry import get
    with pytest.raises(KeyError):
        get("__nonexistent__")
```

- [ ] **Step 2: Run tests to verify they fail**

```
pytest libs/deal_models/tests/test_contracts.py libs/deal_models/tests/test_registry.py -v
```
Expected: `ModuleNotFoundError` (files don't exist yet).

- [ ] **Step 3: Create `libs/deal_models/__init__.py`**

```python
# libs/deal_models/__init__.py
```

- [ ] **Step 4: Create `libs/deal_models/contracts.py`**

```python
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
    annual_revenue_yuan: List[float]           # len = project_life_years (year-1 base; degradation applied inside)
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
```

- [ ] **Step 5: Create `libs/deal_models/registry.py`**

```python
"""libs/deal_models/registry.py — Registry for DealStructureSpec instances."""
from __future__ import annotations
from dataclasses import dataclass
from typing import Callable, Dict, List, Type
from pydantic import BaseModel

_REGISTRY: Dict[str, "DealStructureSpec"] = {}


@dataclass
class DealStructureSpec:
    name: str
    description: str
    payoff_fn: Callable   # (revenue_paths: np.ndarray, params: BaseModel) -> np.ndarray
    params_schema: Type[BaseModel]


def register(spec: DealStructureSpec) -> DealStructureSpec:
    _REGISTRY[spec.name] = spec
    return spec


def get(name: str) -> DealStructureSpec:
    if name not in _REGISTRY:
        raise KeyError(f"{name!r} not in registry. Available: {list(_REGISTRY)}")
    return _REGISTRY[name]


def list_structures() -> List[str]:
    return list(_REGISTRY.keys())
```

- [ ] **Step 6: Create `libs/deal_models/tests/__init__.py`** (empty)

- [ ] **Step 7: Run tests**

```
pytest libs/deal_models/tests/test_contracts.py libs/deal_models/tests/test_registry.py -v
```
Expected: all PASS.

- [ ] **Step 8: Commit**

```bash
git add libs/deal_models/
git commit -m "feat(deal_models): foundation — contracts, registry, and tests"
```

---

## Task 2: Price Simulator — `libs/deal_models/price_simulator.py`

**Files:**
- Create: `libs/deal_models/price_simulator.py`
- Create: `libs/deal_models/tests/test_price_simulator.py`

- [ ] **Step 1: Write failing tests**

```python
# libs/deal_models/tests/test_price_simulator.py
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
    from libs.deal_models.contracts import PCAModelParams
    prices = _synthetic_prices(8760 * 2, mu=300.0, seed=9)
    pca_params = fit_pca(prices, n_components=3)
    req = PriceSimRequest(
        province="蒙西", n_simulations=20, n_years=1, model="pca",
        pca_params=pca_params,
    )
    paths = simulate_prices(req, seed=42)
    assert paths.shape == (20, 8760)
```

- [ ] **Step 2: Run tests to verify they fail**

```
pytest libs/deal_models/tests/test_price_simulator.py -v
```
Expected: `ModuleNotFoundError`.

- [ ] **Step 3: Implement `libs/deal_models/price_simulator.py`**

```python
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
    prices: flat list of hourly prices (len must be divisible by 24).
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
```

- [ ] **Step 4: Run tests**

```
pytest libs/deal_models/tests/test_price_simulator.py -v
```
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add libs/deal_models/price_simulator.py libs/deal_models/tests/test_price_simulator.py
git commit -m "feat(deal_models): price simulator — OU and PCA models"
```

---

## Task 3: Dispatch Valuation — `libs/deal_models/dispatch_valuation.py`

**Files:**
- Create: `libs/deal_models/dispatch_valuation.py`
- Create: `libs/deal_models/tests/test_dispatch_valuation.py`

- [ ] **Step 1: Write failing tests**

```python
# libs/deal_models/tests/test_dispatch_valuation.py
from __future__ import annotations
import numpy as np
import pytest
from libs.deal_models.contracts import DispatchRequest


def _flat_paths(n_sim: int, price: float, n_hours: int = 8760) -> np.ndarray:
    return np.full((n_sim, n_hours), price)


def _spread_paths(n_sim: int, offpeak: float, peak: float, n_hours: int = 8760) -> np.ndarray:
    """12h offpeak then 12h peak each day."""
    day = np.array([offpeak] * 12 + [peak] * 12)
    return np.tile(day, (n_sim, n_hours // 24))


def test_bess_shape():
    from libs.deal_models.dispatch_valuation import dispatch_annual
    paths = _spread_paths(50, 100.0, 500.0)
    req = DispatchRequest(asset_type="bess", capacity_mwh=100.0, power_mw=50.0)
    result = dispatch_annual(paths, req)
    assert result.revenue_paths.shape == (50,)


def test_bess_positive_on_high_spread():
    from libs.deal_models.dispatch_valuation import dispatch_annual
    paths = _spread_paths(20, 50.0, 800.0)
    req = DispatchRequest(
        asset_type="bess", capacity_mwh=100.0, power_mw=50.0,
        roundtrip_eff=0.85, cycles_per_day=1.0, om_cost_yuan_per_mwh=5.0,
    )
    result = dispatch_annual(paths, req)
    assert (result.revenue_paths > 0).all()


def test_bess_zero_on_flat_prices():
    from libs.deal_models.dispatch_valuation import dispatch_annual
    paths = _flat_paths(10, 300.0)
    req = DispatchRequest(
        asset_type="bess", capacity_mwh=100.0, power_mw=50.0,
        roundtrip_eff=0.85, om_cost_yuan_per_mwh=10.0,
    )
    result = dispatch_annual(paths, req)
    # Flat prices → no spread → no revenue (charge cost ≥ discharge revenue after eta loss)
    assert (result.revenue_paths == 0.0).all()


def test_wind_shape_and_percentiles():
    from libs.deal_models.dispatch_valuation import dispatch_annual
    rng = np.random.default_rng(42)
    paths = rng.uniform(200, 400, (30, 8760))
    req = DispatchRequest(asset_type="wind", installed_mw=100.0)
    result = dispatch_annual(paths, req)
    assert result.revenue_paths.shape == (30,)
    assert result.p10 < result.p50 < result.p90


def test_wind_scales_with_installed_mw():
    from libs.deal_models.dispatch_valuation import dispatch_annual
    paths = _flat_paths(5, 300.0)
    req50 = DispatchRequest(asset_type="wind", installed_mw=50.0)
    req100 = DispatchRequest(asset_type="wind", installed_mw=100.0)
    r50 = dispatch_annual(paths, req50)
    r100 = dispatch_annual(paths, req100)
    assert abs(r100.mean / r50.mean - 2.0) < 0.01


def test_wind_bess_exceeds_wind_alone():
    from libs.deal_models.dispatch_valuation import dispatch_annual
    rng = np.random.default_rng(42)
    paths = rng.uniform(100, 600, (50, 8760))
    wind_req = DispatchRequest(asset_type="wind", installed_mw=50.0)
    wb_req = DispatchRequest(
        asset_type="wind_bess", installed_mw=50.0,
        capacity_mwh=50.0, power_mw=50.0, roundtrip_eff=0.85,
    )
    assert dispatch_annual(paths, wb_req).p50 > dispatch_annual(paths, wind_req).p50
```

- [ ] **Step 2: Run tests to verify they fail**

```
pytest libs/deal_models/tests/test_dispatch_valuation.py -v
```

- [ ] **Step 3: Implement `libs/deal_models/dispatch_valuation.py`**

```python
"""libs/deal_models/dispatch_valuation.py — Annual revenue estimation via spread call strip."""
from __future__ import annotations

import numpy as np
from libs.deal_models.contracts import DispatchRequest, DispatchResult


def _percentiles(arr: np.ndarray) -> tuple[float, float, float]:
    return float(np.percentile(arr, 10)), float(np.percentile(arr, 50)), float(np.percentile(arr, 90))


def _dispatch_bess(price_paths: np.ndarray, req: DispatchRequest) -> np.ndarray:
    """
    Greedy daily dispatch: charge in cheapest n_cycles hours, discharge in most expensive.
    Returns (n_sim,) annual revenue yuan.
    """
    n_sim, n_hours = price_paths.shape
    n_days = n_hours // 24
    n_cycles = max(1, int(req.cycles_per_day))
    # MWh discharged per cycle slot (constrained by both power and capacity)
    energy_mwh = min(req.power_mw * 1.0, req.capacity_mwh / n_cycles)

    daily = price_paths[:, : n_days * 24].reshape(n_sim, n_days, 24)
    sorted_prices = np.sort(daily, axis=2)  # ascending

    charge_prices = sorted_prices[:, :, :n_cycles]           # (n_sim, n_days, n_cycles)
    discharge_prices = sorted_prices[:, :, -n_cycles:]        # (n_sim, n_days, n_cycles)

    # Revenue from discharging energy_mwh * eta back to grid per cycle
    discharge_rev = discharge_prices.sum(axis=2) * energy_mwh * req.roundtrip_eff
    # Cost to charge energy_mwh from grid per cycle
    charge_cost = charge_prices.sum(axis=2) * energy_mwh
    # O&M cost per MWh discharged
    om = req.om_cost_yuan_per_mwh * energy_mwh * req.roundtrip_eff * n_cycles

    daily_rev = np.maximum(discharge_rev - charge_cost - om, 0.0)
    return daily_rev.sum(axis=1)


def _dispatch_wind(price_paths: np.ndarray, req: DispatchRequest) -> np.ndarray:
    """Simple energy revenue: price * installed_mw * CF per hour."""
    n_hours = price_paths.shape[1]
    if req.capacity_factor_profile:
        cf = np.asarray(req.capacity_factor_profile[:n_hours], dtype=float)
    else:
        cf = np.full(n_hours, 0.30)   # default 30% CF
    hourly_gen = req.installed_mw * cf  # MWh/h
    return (price_paths * hourly_gen).sum(axis=1)


def dispatch_annual(price_paths: np.ndarray, req: DispatchRequest) -> DispatchResult:
    """
    Compute annual revenue for each simulation path.

    price_paths: np.ndarray (n_sim, n_hours)  — yuan/MWh hourly prices
    Returns DispatchResult with revenue_paths (n_sim,) annual yuan + statistics.
    """
    if req.asset_type == "bess":
        rev = _dispatch_bess(price_paths, req)
    elif req.asset_type == "wind":
        rev = _dispatch_wind(price_paths, req)
    elif req.asset_type == "wind_bess":
        rev = _dispatch_wind(price_paths, req) + _dispatch_bess(price_paths, req)
    else:
        raise ValueError(f"Unknown asset_type: {req.asset_type!r}")

    p10, p50, p90 = _percentiles(rev)
    return DispatchResult(
        revenue_paths=rev,
        p10=p10, p50=p50, p90=p90,
        mean=float(rev.mean()),
        std=float(rev.std()),
    )
```

- [ ] **Step 4: Run tests**

```
pytest libs/deal_models/tests/test_dispatch_valuation.py -v
```

- [ ] **Step 5: Commit**

```bash
git add libs/deal_models/dispatch_valuation.py libs/deal_models/tests/test_dispatch_valuation.py
git commit -m "feat(deal_models): dispatch valuation — BESS, wind, wind+BESS spread call strip"
```

---

## Task 4: Project Cash Flow — `libs/deal_models/project_cashflow.py`

**Files:**
- Create: `libs/deal_models/project_cashflow.py`
- Create: `libs/deal_models/tests/test_project_cashflow.py`

- [ ] **Step 1: Write failing tests**

```python
# libs/deal_models/tests/test_project_cashflow.py
from __future__ import annotations
import math
import pytest
from libs.deal_models.contracts import ProjectFinancials


def _base_financials(**overrides) -> ProjectFinancials:
    defaults = dict(
        capex_total_yuan=1e8,
        project_life_years=20,
        debt_ratio=0.7,
        loan_term_years=10,
        interest_rate=0.05,
        grace_years=1,
        annual_revenue_yuan=[2e7] * 20,
        annual_om_yuan=3e6,
        annual_degradation_rate=0.01,
    )
    defaults.update(overrides)
    return ProjectFinancials(**defaults)


def test_annual_rows_count():
    from libs.deal_models.project_cashflow import compute_cashflow
    result = compute_cashflow(_base_financials(project_life_years=15, annual_revenue_yuan=[2e7] * 15))
    assert len(result.annual) == 15


def test_positive_equity_irr_for_profitable_project():
    from libs.deal_models.project_cashflow import compute_cashflow
    result = compute_cashflow(_base_financials())
    assert result.equity_irr > 0.05


def test_negative_equity_irr_for_zero_revenue():
    from libs.deal_models.project_cashflow import compute_cashflow
    result = compute_cashflow(_base_financials(annual_revenue_yuan=[0.0] * 20))
    assert result.equity_irr < 0.0


def test_ebitda_equals_revenue_minus_opex():
    from libs.deal_models.project_cashflow import compute_cashflow
    result = compute_cashflow(_base_financials())
    row = result.annual[0]
    assert abs(row.ebitda - (row.revenue - row.opex)) < 1.0


def test_dscr_positive_during_debt_period():
    from libs.deal_models.project_cashflow import compute_cashflow
    result = compute_cashflow(_base_financials())
    # Rows 2-10 have active debt service (grace_years=1, loan_term=10)
    for row in result.annual[1:10]:
        assert row.debt_service > 0
    assert result.dscr_min > 0.0


def test_no_debt_service_after_loan_term():
    from libs.deal_models.project_cashflow import compute_cashflow
    result = compute_cashflow(_base_financials(loan_term_years=5, project_life_years=20, annual_revenue_yuan=[2e7]*20))
    for row in result.annual[5:]:   # years 6-20
        assert row.debt_service == 0.0 or abs(row.debt_service - row.debt_service * 0) < 1.0  # only interest on $0


def test_npv_positive_above_hurdle():
    from libs.deal_models.project_cashflow import compute_cashflow
    result = compute_cashflow(_base_financials())
    # project_irr should exceed hurdle_rate=0.08 → NPV > 0
    assert result.npv > 0


def test_irr_helper_known_value():
    from libs.deal_models.project_cashflow import _irr
    # -100 now, +110 in year 1 → IRR = 10%
    assert abs(_irr([-100.0, 110.0]) - 0.10) < 1e-5
```

- [ ] **Step 2: Run tests to verify they fail**

```
pytest libs/deal_models/tests/test_project_cashflow.py -v
```

- [ ] **Step 3: Implement `libs/deal_models/project_cashflow.py`**

```python
"""libs/deal_models/project_cashflow.py — Full project financial model."""
from __future__ import annotations
import math
from libs.deal_models.contracts import AnnualRow, CashFlowResult, ProjectFinancials


def _irr(cashflows: list[float], guess: float = 0.10, max_iter: int = 500, tol: float = 1e-7) -> float:
    """Newton-Raphson IRR solver. cashflows[0] is the initial investment (negative)."""
    rate = guess
    for _ in range(max_iter):
        npv = sum(cf / (1.0 + rate) ** t for t, cf in enumerate(cashflows))
        d_npv = sum(-t * cf / (1.0 + rate) ** (t + 1) for t, cf in enumerate(cashflows))
        if abs(d_npv) < 1e-12:
            break
        new_rate = rate - npv / d_npv
        if abs(new_rate - rate) < tol:
            return float(new_rate)
        rate = max(new_rate, -0.9999)   # guard against divergence
    return float(rate)


def compute_cashflow(financials: ProjectFinancials) -> CashFlowResult:
    """Compute full project P&L, KPIs, and IRR."""
    n = financials.project_life_years
    total_capex = financials.capex_total_yuan
    debt = total_capex * financials.debt_ratio
    equity = total_capex * (1.0 - financials.debt_ratio)

    # Straight-line depreciation
    depn_per_yr = (total_capex * (1.0 - financials.residual_value_ratio)) / financials.depreciation_years

    # Level principal repayment (after grace period)
    active_loan_yrs = max(financials.loan_term_years - financials.grace_years, 1)
    annual_principal = debt / active_loan_yrs

    rows: list[AnnualRow] = []
    equity_cfs = [-equity]      # t=0 equity outflow
    project_cfs = [-total_capex]  # t=0 total outflow (unlevered)
    remaining_debt = debt

    for yr in range(1, n + 1):
        # Revenue degraded from base year-1 figure
        base_rev = (
            financials.annual_revenue_yuan[yr - 1]
            if yr <= len(financials.annual_revenue_yuan)
            else financials.annual_revenue_yuan[-1]
        )
        rev = base_rev * (1.0 - financials.annual_degradation_rate) ** (yr - 1)

        opex = financials.annual_om_yuan
        ebitda = rev - opex
        depn = depn_per_yr if yr <= financials.depreciation_years else 0.0
        ebit = ebitda - depn

        # Debt service
        interest = remaining_debt * financials.interest_rate
        if yr <= financials.grace_years or yr > financials.loan_term_years:
            principal = 0.0
        else:
            principal = min(annual_principal, remaining_debt)
        remaining_debt = max(remaining_debt - principal, 0.0)
        debt_service = principal + interest

        ebt = ebit - interest
        tax = max(ebt * financials.corporate_tax_rate, 0.0)
        net_income = ebt - tax
        equity_fcf = net_income + depn - principal

        rows.append(AnnualRow(
            year=yr, revenue=rev, opex=opex, ebitda=ebitda,
            depreciation=depn, ebit=ebit, interest=interest,
            ebt=ebt, tax=tax, net_income=net_income,
            debt_service=debt_service, equity_fcf=equity_fcf,
        ))
        equity_cfs.append(equity_fcf)
        # Unlevered project CF = EBITDA - tax_on_ebit
        project_cfs.append(ebitda - max(ebit * financials.corporate_tax_rate, 0.0))

    # KPIs
    equity_irr = _irr(equity_cfs)
    project_irr = _irr(project_cfs)

    avg_net_income = sum(r.net_income for r in rows) / n
    roace = avg_net_income / total_capex

    dscr_vals = [r.ebitda / r.debt_service for r in rows if r.debt_service > 1.0]
    dscr_min = min(dscr_vals) if dscr_vals else float("nan")
    dscr_avg = sum(dscr_vals) / len(dscr_vals) if dscr_vals else float("nan")

    npv = sum(cf / (1.0 + financials.hurdle_rate) ** t for t, cf in enumerate(project_cfs))

    payback = float("nan")
    cumulative = 0.0
    for row in rows:
        cumulative += row.equity_fcf
        if cumulative >= equity:
            payback = float(row.year)
            break

    return CashFlowResult(
        annual=rows,
        project_irr=project_irr,
        equity_irr=equity_irr,
        roace=roace,
        dscr_min=dscr_min,
        dscr_avg=dscr_avg,
        npv=npv,
        payback_years=payback,
    )
```

- [ ] **Step 4: Run tests**

```
pytest libs/deal_models/tests/test_project_cashflow.py -v
```

- [ ] **Step 5: Commit**

```bash
git add libs/deal_models/project_cashflow.py libs/deal_models/tests/test_project_cashflow.py
git commit -m "feat(deal_models): project cashflow — IRR, ROACE, DSCR, NPV"
```

---

## Task 5: Monte Carlo — `libs/deal_models/monte_carlo.py`

**Files:**
- Create: `libs/deal_models/monte_carlo.py`
- Create: `libs/deal_models/tests/test_monte_carlo.py`

- [ ] **Step 1: Write failing tests**

```python
# libs/deal_models/tests/test_monte_carlo.py
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
    # CVaR (expected shortfall below 5th pct) ≤ VaR (5th pct)
    assert result.revenue_cvar_5pct <= result.revenue_var_5pct + 1.0  # small tolerance
```

- [ ] **Step 2: Run tests to verify they fail**

```
pytest libs/deal_models/tests/test_monte_carlo.py -v
```

- [ ] **Step 3: Implement `libs/deal_models/monte_carlo.py`**

```python
"""libs/deal_models/monte_carlo.py — MC orchestrator: price → dispatch → cashflow."""
from __future__ import annotations

import numpy as np
from libs.deal_models.contracts import MCRequest, MCResult, ProjectFinancials
from libs.deal_models.price_simulator import simulate_prices
from libs.deal_models.dispatch_valuation import dispatch_annual
from libs.deal_models.project_cashflow import compute_cashflow


def _tornado(financials: ProjectFinancials, base_rev: float) -> list[dict]:
    """Sensitivity: ±10% perturbation of key financial parameters → IRR swing."""
    params = [
        ("capex", "capex_total_yuan", 0.10),
        ("interest_rate", "interest_rate", 0.10),
        ("om_cost", "annual_om_yuan", 0.10),
        ("degradation", "annual_degradation_rate", 0.10),
        ("revenue_scale", None, 0.10),      # special: perturb base_rev itself
    ]
    n = financials.project_life_years
    results = []
    for label, field, pct in params:
        rows_for_irr = []
        for direction in (+1, -1):
            if field is None:
                # Perturb revenue directly
                rev = base_rev * (1.0 + direction * pct)
                fin = financials.model_copy(update={"annual_revenue_yuan": [rev] * n})
            else:
                base_val = getattr(financials, field)
                new_val = base_val * (1.0 + direction * pct)
                fin = financials.model_copy(
                    update={field: new_val, "annual_revenue_yuan": [base_rev] * n}
                )
            rows_for_irr.append(compute_cashflow(fin).equity_irr)
        irr_high, irr_low = rows_for_irr
        results.append({
            "param": label,
            "irr_high": irr_high,
            "irr_low": irr_low,
            "swing": abs(irr_high - irr_low),
        })
    return sorted(results, key=lambda x: x["swing"], reverse=True)


def run_monte_carlo(req: MCRequest) -> MCResult:
    """Full MC: simulate prices → dispatch revenue → project cashflow for each path."""
    # 1. Price paths
    price_paths = simulate_prices(req.price_sim, seed=req.random_seed)  # (n_sim, 8760)

    # 2. Annual dispatch revenue per path
    dispatch_result = dispatch_annual(price_paths, req.dispatch)
    revenue_paths = dispatch_result.revenue_paths  # (n_sim,)

    # 3. Project cashflow per path
    n = req.financials.project_life_years
    equity_irr_paths = np.empty(req.n_simulations)
    npv_paths = np.empty(req.n_simulations)

    for i in range(req.n_simulations):
        fin = req.financials.model_copy(
            update={"annual_revenue_yuan": [float(revenue_paths[i])] * n}
        )
        cf = compute_cashflow(fin)
        equity_irr_paths[i] = cf.equity_irr
        npv_paths[i] = cf.npv

    # 4. Statistics
    rv5 = float(np.percentile(revenue_paths, 5))
    cvar_mask = revenue_paths <= rv5
    revenue_cvar = float(revenue_paths[cvar_mask].mean()) if cvar_mask.any() else rv5

    hurdle = req.financials.hurdle_rate

    # 5. Tornado at P50 revenue
    p50_rev = float(np.median(revenue_paths))
    tornado = _tornado(req.financials, p50_rev)

    return MCResult(
        revenue_p10=float(np.percentile(revenue_paths, 10)),
        revenue_p50=float(np.percentile(revenue_paths, 50)),
        revenue_p90=float(np.percentile(revenue_paths, 90)),
        revenue_var_5pct=rv5,
        revenue_cvar_5pct=revenue_cvar,
        equity_irr_p10=float(np.percentile(equity_irr_paths, 10)),
        equity_irr_p50=float(np.percentile(equity_irr_paths, 50)),
        equity_irr_p90=float(np.percentile(equity_irr_paths, 90)),
        irr_prob_below_hurdle=float((equity_irr_paths < hurdle).mean()),
        npv_p10=float(np.percentile(npv_paths, 10)),
        npv_p50=float(np.percentile(npv_paths, 50)),
        npv_p90=float(np.percentile(npv_paths, 90)),
        tornado=tornado,
        revenue_paths=revenue_paths,
        equity_irr_paths=equity_irr_paths,
        npv_paths=npv_paths,
    )
```

- [ ] **Step 4: Run tests**

```
pytest libs/deal_models/tests/test_monte_carlo.py -v
```

- [ ] **Step 5: Commit**

```bash
git add libs/deal_models/monte_carlo.py libs/deal_models/tests/test_monte_carlo.py
git commit -m "feat(deal_models): monte carlo orchestrator — price→dispatch→cashflow, VaR/CVaR/tornado"
```

---

## Task 6: Deal Structures — `libs/deal_models/deal_structures.py`

**Files:**
- Create: `libs/deal_models/deal_structures.py`
- Create: `libs/deal_models/tests/test_deal_structures.py`

- [ ] **Step 1: Write failing tests**

```python
# libs/deal_models/tests/test_deal_structures.py
from __future__ import annotations
import numpy as np
import pytest


def test_all_six_structures_registered():
    import libs.deal_models.deal_structures  # triggers self-registration
    from libs.deal_models.registry import list_structures
    for name in ["revenue_floor", "revenue_cap", "collar", "fixed_revenue_swap", "tolling", "ppa_fixed_price"]:
        assert name in list_structures(), f"{name} not registered"


def test_floor_payout_only_on_shortfall():
    import libs.deal_models.deal_structures
    from libs.deal_models.deal_structures import FloorParams, price_structure
    revs = np.array([12e6, 15e6, 8e6])
    params = FloorParams(floor_yuan=10e6)
    result = price_structure("revenue_floor", revs, params)
    assert result.payout_paths[0] == pytest.approx(0.0)
    assert result.payout_paths[1] == pytest.approx(0.0)
    assert result.payout_paths[2] == pytest.approx(2e6, rel=1e-5)


def test_floor_well_below_p5_has_zero_expected_cost():
    import libs.deal_models.deal_structures
    from libs.deal_models.deal_structures import FloorParams, price_structure
    rng = np.random.default_rng(42)
    revs = rng.normal(10e6, 1e6, 5000)
    p1 = float(np.percentile(revs, 1))
    params = FloorParams(floor_yuan=p1 * 0.5)
    result = price_structure("revenue_floor", revs, params)
    assert result.expected_cost == pytest.approx(0.0, abs=1.0)


def test_cap_payout_only_above_cap():
    import libs.deal_models.deal_structures
    from libs.deal_models.deal_structures import CapParams, price_structure
    revs = np.array([8e6, 10e6, 14e6])
    result = price_structure("revenue_cap", revs, CapParams(cap_yuan=12e6))
    assert result.payout_paths[0] == pytest.approx(0.0)
    assert result.payout_paths[2] == pytest.approx(2e6, rel=1e-5)


def test_collar_bounded_net_exposure():
    import libs.deal_models.deal_structures
    from libs.deal_models.deal_structures import CollarParams, price_structure
    revs = np.linspace(5e6, 20e6, 100)
    result = price_structure("collar", revs, CollarParams(floor_yuan=8e6, cap_yuan=15e6))
    # collar net payout can be negative (owner receives cap income)
    assert result.expected_cost is not None


def test_suggested_premium_ge_min_premium():
    import libs.deal_models.deal_structures
    from libs.deal_models.deal_structures import FloorParams, price_structure
    rng = np.random.default_rng(42)
    revs = rng.normal(10e6, 2e6, 1000)
    result = price_structure("revenue_floor", revs, FloorParams(floor_yuan=9e6))
    assert result.suggested_premium >= result.min_premium
```

- [ ] **Step 2: Run tests to verify they fail**

```
pytest libs/deal_models/tests/test_deal_structures.py -v
```

- [ ] **Step 3: Implement `libs/deal_models/deal_structures.py`**

```python
"""libs/deal_models/deal_structures.py — Deal payoff functions + pricing. Self-registers on import."""
from __future__ import annotations
from dataclasses import dataclass
import numpy as np
from pydantic import BaseModel, Field
from libs.deal_models.registry import DealStructureSpec, register


# ── Params schemas ────────────────────────────────────────────────────────────

class FloorParams(BaseModel):
    floor_yuan: float = Field(..., gt=0)

class CapParams(BaseModel):
    cap_yuan: float = Field(..., gt=0)

class CollarParams(BaseModel):
    floor_yuan: float = Field(..., gt=0)
    cap_yuan: float = Field(..., gt=0)

class SwapParams(BaseModel):
    fixed_revenue_yuan: float = Field(..., gt=0)

class TollingParams(BaseModel):
    toll_yuan: float = Field(..., gt=0)

class PPAParams(BaseModel):
    fixed_price_yuan_mwh: float = Field(..., gt=0)
    annual_volume_mwh: float = Field(..., gt=0)


# ── Pricing result ────────────────────────────────────────────────────────────

@dataclass
class DealPricingResult:
    expected_cost: float
    p95_cost: float
    min_premium: float
    suggested_premium: float
    payout_paths: np.ndarray


# ── Universal pricing function ────────────────────────────────────────────────

def price_structure(
    name: str,
    revenue_paths: np.ndarray,
    params: BaseModel,
    cost_of_capital: float = 0.10,
) -> DealPricingResult:
    """Price any registered deal structure against simulated revenue paths."""
    from libs.deal_models.registry import get
    spec = get(name)
    raw_payouts = spec.payoff_fn(revenue_paths, params)
    payouts = np.maximum(raw_payouts, 0.0).astype(float)

    expected_cost = float(payouts.mean())
    p95_cost = float(np.percentile(payouts, 95))
    threshold = float(np.percentile(payouts, 95))
    tail = payouts[payouts >= threshold]
    cvar = float(tail.mean()) if len(tail) else p95_cost
    risk_charge = 0.30 * max(cvar - expected_cost, 0.0)

    return DealPricingResult(
        expected_cost=expected_cost,
        p95_cost=p95_cost,
        min_premium=expected_cost,
        suggested_premium=expected_cost + risk_charge,
        payout_paths=payouts,
    )


# ── Register the 6 standard structures ───────────────────────────────────────

register(DealStructureSpec(
    name="revenue_floor",
    description="Guarantees minimum annual revenue. Payout = max(floor − R, 0)",
    payoff_fn=lambda rev, p: np.maximum(p.floor_yuan - rev, 0.0),
    params_schema=FloorParams,
))
register(DealStructureSpec(
    name="revenue_cap",
    description="Upside sharing above cap. Cost = max(R − cap, 0)",
    payoff_fn=lambda rev, p: np.maximum(rev - p.cap_yuan, 0.0),
    params_schema=CapParams,
))
register(DealStructureSpec(
    name="collar",
    description="Revenue band: floor protection minus cap income.",
    payoff_fn=lambda rev, p: np.maximum(p.floor_yuan - rev, 0.0) - np.maximum(rev - p.cap_yuan, 0.0),
    params_schema=CollarParams,
))
register(DealStructureSpec(
    name="fixed_revenue_swap",
    description="Owner exchanges variable revenue for fixed amount.",
    payoff_fn=lambda rev, p: p.fixed_revenue_yuan - rev,
    params_schema=SwapParams,
))
register(DealStructureSpec(
    name="tolling",
    description="Operator pays toll, keeps all upside. Payout = max(toll − R, 0)",
    payoff_fn=lambda rev, p: np.maximum(p.toll_yuan - rev, 0.0),
    params_schema=TollingParams,
))
register(DealStructureSpec(
    name="ppa_fixed_price",
    description="Fixed-price PPA. Payout = max(fixed_price × vol − R, 0)",
    payoff_fn=lambda rev, p: np.maximum(p.fixed_price_yuan_mwh * p.annual_volume_mwh - rev, 0.0),
    params_schema=PPAParams,
))
```

- [ ] **Step 4: Run tests**

```
pytest libs/deal_models/tests/test_deal_structures.py -v
```

- [ ] **Step 5: Run all libs tests**

```
pytest libs/deal_models/tests/ -v
```
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add libs/deal_models/deal_structures.py libs/deal_models/tests/test_deal_structures.py
git commit -m "feat(deal_models): deal structures — 6 payoff functions + universal pricer"
```

---

## Task 7: Agent Tools Adapter — `libs/deal_models/adapters/agent_tools.py`

**Files:**
- Create: `libs/deal_models/adapters/__init__.py`
- Create: `libs/deal_models/adapters/agent_tools.py`

No tests for this task (it's a thin serialisation layer — integration-tested via the app).

- [ ] **Step 1: Create `libs/deal_models/adapters/__init__.py`** (empty)

- [ ] **Step 2: Create `libs/deal_models/adapters/agent_tools.py`**

```python
"""libs/deal_models/adapters/agent_tools.py — Claude API tool definitions + dispatch."""
from __future__ import annotations
import json
import numpy as np
from libs.deal_models.contracts import (
    OUParams, PCAModelParams, PriceSimRequest, DispatchRequest,
    ProjectFinancials, MCRequest,
)
from libs.deal_models.price_simulator import simulate_prices
from libs.deal_models.dispatch_valuation import dispatch_annual
from libs.deal_models.project_cashflow import compute_cashflow
from libs.deal_models.monte_carlo import run_monte_carlo
import libs.deal_models.deal_structures as _ds  # triggers registration

# Cached last MC result so Strategist can reference it across tool calls
_last_mc_result = None
_last_revenue_paths = None


def _j(obj) -> str:
    """JSON serialize; convert numpy scalars."""
    def _default(o):
        if isinstance(o, (np.integer, np.floating)):
            return float(o)
        if isinstance(o, np.ndarray):
            return o.tolist()
        raise TypeError(f"Not serializable: {type(o)}")
    return json.dumps(obj, default=_default)


AGENT_TOOLS = [
    {
        "name": "run_price_simulation",
        "description": "Simulate forward price paths for a province using OU or PCA model. Returns P10/P50/P90 price levels and stores paths for downstream dispatch valuation.",
        "input_schema": {
            "type": "object",
            "properties": {
                "province": {"type": "string", "description": "Province name e.g. 蒙西"},
                "model": {"type": "string", "enum": ["ou", "pca"], "description": "Price model"},
                "kappa": {"type": "number", "description": "OU mean-reversion speed (default 2.0)"},
                "mu": {"type": "number", "description": "OU long-run mean yuan/MWh (default 300)"},
                "sigma": {"type": "number", "description": "OU annualised vol yuan/MWh (default 80)"},
                "n_simulations": {"type": "integer", "description": "Number of paths (default 1000)"},
            },
            "required": ["province"],
        },
    },
    {
        "name": "run_dispatch_valuation",
        "description": "Estimate annual BESS or wind revenue using the last simulated price paths.",
        "input_schema": {
            "type": "object",
            "properties": {
                "asset_type": {"type": "string", "enum": ["bess", "wind", "wind_bess"]},
                "capacity_mwh": {"type": "number"},
                "power_mw": {"type": "number"},
                "roundtrip_eff": {"type": "number"},
                "installed_mw": {"type": "number", "description": "Wind installed capacity MW"},
            },
            "required": ["asset_type"],
        },
    },
    {
        "name": "run_project_cashflow",
        "description": "Compute project IRR, equity IRR, DSCR, and NPV for given financial assumptions.",
        "input_schema": {
            "type": "object",
            "properties": {
                "capex_total_yuan": {"type": "number"},
                "annual_revenue_yuan": {"type": "number", "description": "Single annual revenue figure (repeated over project life)"},
                "annual_om_yuan": {"type": "number"},
                "debt_ratio": {"type": "number"},
                "interest_rate": {"type": "number"},
                "loan_term_years": {"type": "integer"},
                "project_life_years": {"type": "integer"},
                "hurdle_rate": {"type": "number"},
            },
            "required": ["capex_total_yuan", "annual_revenue_yuan", "annual_om_yuan"],
        },
    },
    {
        "name": "run_monte_carlo",
        "description": "Full MC simulation: price paths → dispatch revenue → project cashflow. Returns IRR/NPV distributions and tornado chart.",
        "input_schema": {
            "type": "object",
            "properties": {
                "province": {"type": "string"},
                "asset_type": {"type": "string", "enum": ["bess", "wind", "wind_bess"]},
                "capacity_mwh": {"type": "number"},
                "power_mw": {"type": "number"},
                "installed_mw": {"type": "number"},
                "capex_total_yuan": {"type": "number"},
                "annual_om_yuan": {"type": "number"},
                "n_simulations": {"type": "integer"},
                "mu": {"type": "number", "description": "OU long-run mean price yuan/MWh"},
            },
            "required": ["province", "asset_type", "capex_total_yuan", "annual_om_yuan"],
        },
    },
    {
        "name": "price_deal_structure",
        "description": "Price a deal structure (floor, cap, collar, swap, tolling, PPA) against the last MC revenue paths.",
        "input_schema": {
            "type": "object",
            "properties": {
                "structure_type": {"type": "string", "enum": ["revenue_floor", "revenue_cap", "collar", "fixed_revenue_swap", "tolling", "ppa_fixed_price"]},
                "floor_yuan": {"type": "number"},
                "cap_yuan": {"type": "number"},
                "fixed_revenue_yuan": {"type": "number"},
                "toll_yuan": {"type": "number"},
                "fixed_price_yuan_mwh": {"type": "number"},
                "annual_volume_mwh": {"type": "number"},
            },
            "required": ["structure_type"],
        },
    },
]


def dispatch_tool(name: str, inputs: dict) -> str:
    """Route a tool call from the Claude API to the appropriate function."""
    global _last_mc_result, _last_revenue_paths
    try:
        if name == "run_price_simulation":
            ou = OUParams(
                kappa=inputs.get("kappa", 2.0),
                mu=inputs.get("mu", 300.0),
                sigma=inputs.get("sigma", 80.0),
            )
            req = PriceSimRequest(
                province=inputs["province"],
                n_simulations=inputs.get("n_simulations", 500),
                model=inputs.get("model", "ou"),
                ou_params=ou,
            )
            paths = simulate_prices(req)
            _last_revenue_paths = paths  # store for dispatch tool
            return _j({
                "province": req.province,
                "n_simulations": req.n_simulations,
                "p10_yuan_mwh": float(np.percentile(paths, 10)),
                "p50_yuan_mwh": float(np.percentile(paths, 50)),
                "p90_yuan_mwh": float(np.percentile(paths, 90)),
            })

        if name == "run_dispatch_valuation":
            if _last_revenue_paths is None:
                return _j({"error": "Run run_price_simulation first"})
            req = DispatchRequest(
                asset_type=inputs["asset_type"],
                capacity_mwh=inputs.get("capacity_mwh", 0.0),
                power_mw=inputs.get("power_mw", 0.0),
                installed_mw=inputs.get("installed_mw", 0.0),
                roundtrip_eff=inputs.get("roundtrip_eff", 0.85),
            )
            result = dispatch_annual(_last_revenue_paths, req)
            _last_revenue_paths = result.revenue_paths[:, np.newaxis] if result.revenue_paths.ndim == 1 else None
            # Store revenue for deal pricing
            _last_mc_result_rev = result.revenue_paths
            return _j({"p10": result.p10, "p50": result.p50, "p90": result.p90, "mean": result.mean})

        if name == "run_project_cashflow":
            n = inputs.get("project_life_years", 20)
            rev = inputs["annual_revenue_yuan"]
            fin = ProjectFinancials(
                capex_total_yuan=inputs["capex_total_yuan"],
                annual_revenue_yuan=[rev] * n,
                annual_om_yuan=inputs["annual_om_yuan"],
                debt_ratio=inputs.get("debt_ratio", 0.7),
                interest_rate=inputs.get("interest_rate", 0.05),
                loan_term_years=inputs.get("loan_term_years", 10),
                project_life_years=n,
                hurdle_rate=inputs.get("hurdle_rate", 0.08),
            )
            cf = compute_cashflow(fin)
            return _j({
                "project_irr": cf.project_irr,
                "equity_irr": cf.equity_irr,
                "roace": cf.roace,
                "dscr_min": cf.dscr_min,
                "dscr_avg": cf.dscr_avg,
                "npv": cf.npv,
                "payback_years": cf.payback_years,
            })

        if name == "run_monte_carlo":
            n_sim = inputs.get("n_simulations", 500)
            n = inputs.get("project_life_years", 20)
            req = MCRequest(
                price_sim=PriceSimRequest(
                    province=inputs["province"],
                    n_simulations=n_sim,
                    model="ou",
                    ou_params=OUParams(kappa=2.0, mu=inputs.get("mu", 300.0), sigma=80.0),
                ),
                dispatch=DispatchRequest(
                    asset_type=inputs["asset_type"],
                    capacity_mwh=inputs.get("capacity_mwh", 0.0),
                    power_mw=inputs.get("power_mw", 0.0),
                    installed_mw=inputs.get("installed_mw", 0.0),
                ),
                financials=ProjectFinancials(
                    capex_total_yuan=inputs["capex_total_yuan"],
                    annual_revenue_yuan=[inputs.get("mu", 300.0) * 50.0 * 8760 * 0.3] * n,
                    annual_om_yuan=inputs["annual_om_yuan"],
                ),
                n_simulations=n_sim,
            )
            _last_mc_result = run_monte_carlo(req)
            return _j({
                "revenue_p10": _last_mc_result.revenue_p10,
                "revenue_p50": _last_mc_result.revenue_p50,
                "revenue_p90": _last_mc_result.revenue_p90,
                "equity_irr_p10": _last_mc_result.equity_irr_p10,
                "equity_irr_p50": _last_mc_result.equity_irr_p50,
                "equity_irr_p90": _last_mc_result.equity_irr_p90,
                "irr_prob_below_hurdle": _last_mc_result.irr_prob_below_hurdle,
                "tornado_top3": _last_mc_result.tornado[:3],
            })

        if name == "price_deal_structure":
            if _last_mc_result is None:
                return _j({"error": "Run run_monte_carlo first to get revenue paths"})
            struct = inputs["structure_type"]
            import libs.deal_models.deal_structures as ds
            schema = ds._ds.get(struct).params_schema  # type: ignore[attr-defined]
            # Build params from inputs
            param_fields = {k: v for k, v in inputs.items() if k != "structure_type" and v is not None}
            params = schema(**param_fields)
            result = _ds.price_structure(struct, _last_mc_result.revenue_paths, params)
            return _j({
                "expected_cost": result.expected_cost,
                "p95_cost": result.p95_cost,
                "min_premium": result.min_premium,
                "suggested_premium": result.suggested_premium,
            })

        return _j({"error": f"Unknown tool: {name}"})
    except Exception as e:
        return _j({"error": str(e)})
```

- [ ] **Step 3: Commit**

```bash
git add libs/deal_models/adapters/
git commit -m "feat(deal_models): agent tools adapter — AGENT_TOOLS list + dispatch_tool()"
```

---

## Task 8: Services — `services/deal_engine/`

**Files:**
- Create: `services/deal_engine/__init__.py`
- Create: `services/deal_engine/price_data.py`
- Create: `services/deal_engine/scenario_store.py`
- Create: `services/deal_engine/batch_runner.py`

No unit tests (these wrap I/O). Integration tested via the app smoke test.

- [ ] **Step 1: Create `services/deal_engine/__init__.py`** (empty)

- [ ] **Step 2: Create `services/deal_engine/price_data.py`**

```python
"""services/deal_engine/price_data.py — Fetch historical hourly prices from DB."""
from __future__ import annotations
from typing import Optional
import pandas as pd
from services.common.db_utils import get_engine


def fetch_price_history(
    province: str,
    start_date: str,
    end_date: str,
    price_col: str = "da_price",   # "da_price" or "rt_price"
) -> list[float]:
    """
    Fetch hourly price series from marketdata.spot_prices_hourly.

    Returns flat list of yuan/MWh values ordered by datetime.
    Missing hours are forward-filled. Raises ValueError if fewer than 168 hours returned.

    price_col: "da_price" uses day-ahead clearing price (default).
               "rt_price" uses real-time clearing price.
    """
    engine = get_engine()
    sql = f"""
        SELECT datetime, {price_col} AS price
        FROM marketdata.spot_prices_hourly
        WHERE province = :province
          AND datetime >= :start_date
          AND datetime <  :end_date
        ORDER BY datetime
    """
    df = pd.read_sql(sql, engine, params={"province": province, "start_date": start_date, "end_date": end_date})
    if df.empty:
        raise ValueError(f"No price data for province={province!r} between {start_date} and {end_date}")
    if len(df) < 168:
        raise ValueError(f"Insufficient data: only {len(df)} hours returned (need ≥ 168)")

    # Forward-fill gaps
    df["price"] = df["price"].fillna(method="ffill").fillna(method="bfill")
    # Convert yuan/kWh → yuan/MWh if values look like kWh scale (< 5)
    if df["price"].median() < 5.0:
        df["price"] = df["price"] * 1000.0

    return df["price"].tolist()
```

- [ ] **Step 3: Create `services/deal_engine/scenario_store.py`**

```python
"""services/deal_engine/scenario_store.py — JSON-file scenario persistence."""
from __future__ import annotations
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

_STORE_DIR = Path(__file__).parent / "scenarios"


def _ensure_dir() -> None:
    _STORE_DIR.mkdir(parents=True, exist_ok=True)


def save_scenario(scenario_id: str, data: dict[str, Any]) -> Path:
    """Persist a named scenario (inputs + MC results) to JSON. Returns file path."""
    _ensure_dir()
    data["_saved_at"] = datetime.utcnow().isoformat()
    path = _STORE_DIR / f"{scenario_id}.json"
    path.write_text(json.dumps(data, default=str), encoding="utf-8")
    return path


def load_scenario(scenario_id: str) -> dict[str, Any]:
    """Load a scenario by ID. Raises FileNotFoundError if not found."""
    path = _STORE_DIR / f"{scenario_id}.json"
    if not path.exists():
        raise FileNotFoundError(f"Scenario {scenario_id!r} not found in {_STORE_DIR}")
    return json.loads(path.read_text(encoding="utf-8"))


def list_scenarios() -> list[str]:
    """Return list of saved scenario IDs (without .json extension)."""
    _ensure_dir()
    return [p.stem for p in sorted(_STORE_DIR.glob("*.json"))]


def delete_scenario(scenario_id: str) -> None:
    path = _STORE_DIR / f"{scenario_id}.json"
    if path.exists():
        path.unlink()
```

- [ ] **Step 4: Create `services/deal_engine/batch_runner.py`**

```python
"""services/deal_engine/batch_runner.py — Run MC with Streamlit progress tracking."""
from __future__ import annotations
from typing import TYPE_CHECKING, Optional, Callable
import numpy as np
from libs.deal_models.contracts import MCRequest, MCResult, ProjectFinancials
from libs.deal_models.price_simulator import simulate_prices
from libs.deal_models.dispatch_valuation import dispatch_annual
from libs.deal_models.project_cashflow import compute_cashflow
from libs.deal_models.monte_carlo import _tornado


def run_batch(
    req: MCRequest,
    progress_callback: Optional[Callable[[float], None]] = None,
) -> MCResult:
    """
    Execute MC run with optional progress updates.

    progress_callback(fraction: float) is called every 5% of sims.
    Compatible with Streamlit's st.progress() via: run_batch(req, progress_callback=bar.progress)
    """
    from libs.deal_models.contracts import MCResult

    price_paths = simulate_prices(req.price_sim, seed=req.random_seed)
    dispatch_result = dispatch_annual(price_paths, req.dispatch)
    revenue_paths = dispatch_result.revenue_paths

    n = req.financials.project_life_years
    n_sim = req.n_simulations
    equity_irr_paths = np.empty(n_sim)
    npv_paths = np.empty(n_sim)

    report_every = max(1, n_sim // 20)
    for i in range(n_sim):
        fin = req.financials.model_copy(
            update={"annual_revenue_yuan": [float(revenue_paths[i])] * n}
        )
        cf = compute_cashflow(fin)
        equity_irr_paths[i] = cf.equity_irr
        npv_paths[i] = cf.npv
        if progress_callback and i % report_every == 0:
            progress_callback(i / n_sim)

    if progress_callback:
        progress_callback(1.0)

    rv5 = float(np.percentile(revenue_paths, 5))
    cvar_mask = revenue_paths <= rv5
    revenue_cvar = float(revenue_paths[cvar_mask].mean()) if cvar_mask.any() else rv5
    p50_rev = float(np.median(revenue_paths))
    tornado = _tornado(req.financials, p50_rev)

    hurdle = req.financials.hurdle_rate
    return MCResult(
        revenue_p10=float(np.percentile(revenue_paths, 10)),
        revenue_p50=float(np.percentile(revenue_paths, 50)),
        revenue_p90=float(np.percentile(revenue_paths, 90)),
        revenue_var_5pct=rv5,
        revenue_cvar_5pct=revenue_cvar,
        equity_irr_p10=float(np.percentile(equity_irr_paths, 10)),
        equity_irr_p50=float(np.percentile(equity_irr_paths, 50)),
        equity_irr_p90=float(np.percentile(equity_irr_paths, 90)),
        irr_prob_below_hurdle=float((equity_irr_paths < hurdle).mean()),
        npv_p10=float(np.percentile(npv_paths, 10)),
        npv_p50=float(np.percentile(npv_paths, 50)),
        npv_p90=float(np.percentile(npv_paths, 90)),
        tornado=tornado,
        revenue_paths=revenue_paths,
        equity_irr_paths=equity_irr_paths,
        npv_paths=npv_paths,
    )
```

- [ ] **Step 5: Commit**

```bash
git add services/deal_engine/
git commit -m "feat(deal_engine): price_data, scenario_store, batch_runner"
```

---

## Task 9: App — `apps/deal-structurer/`

**Files:**
- Create: `apps/deal-structurer/Dockerfile`
- Create: `apps/deal-structurer/requirements.txt`
- Create: `apps/deal-structurer/app.py`
- Create: `apps/deal-structurer/price_tab.py`
- Create: `apps/deal-structurer/dispatch_tab.py`
- Create: `apps/deal-structurer/cashflow_tab.py`
- Create: `apps/deal-structurer/mc_tab.py`
- Create: `apps/deal-structurer/deal_tab.py`
- Create: `apps/deal-structurer/strategist.py`

- [ ] **Step 1: Create `apps/deal-structurer/Dockerfile`**

```dockerfile
FROM python:3.11-slim
WORKDIR /app

RUN pip install --no-cache-dir \
    "streamlit==1.58.0" \
    "plotly==6.0.1" \
    pandas \
    numpy \
    scipy \
    "psycopg2-binary==2.9.10" \
    "python-dotenv==1.0.1" \
    "anthropic[bedrock]>=0.40" \
    boto3 \
    pydantic \
    "statsmodels>=0.14"

COPY apps/deal-structurer/    ./apps/deal-structurer/
COPY libs/deal_models/        ./libs/deal_models/
COPY services/deal_engine/    ./services/deal_engine/
COPY services/common/         ./services/common/

ENV PYTHONPATH=/app

EXPOSE 8510

CMD ["streamlit", "run", "apps/deal-structurer/app.py", \
     "--server.port=8510", \
     "--server.address=0.0.0.0", \
     "--server.baseUrlPath=deal-structurer", \
     "--server.enableCORS=false", \
     "--server.enableXsrfProtection=false", \
     "--server.headless=true", \
     "--server.fileWatcherType=none"]
```

- [ ] **Step 2: Create `apps/deal-structurer/requirements.txt`**

```
streamlit==1.58.0
plotly==6.0.1
pandas
numpy
scipy
psycopg2-binary==2.9.10
python-dotenv==1.0.1
anthropic[bedrock]>=0.40
boto3
pydantic
statsmodels>=0.14
```

- [ ] **Step 3: Create `apps/deal-structurer/app.py`**

```python
"""apps/deal-structurer/app.py — Shell: sidebar, session state, tab routing."""
from __future__ import annotations
import os
import sys
sys.path.insert(0, "/app")

import streamlit as st
from dotenv import load_dotenv
load_dotenv()

st.set_page_config(page_title="Deal Structurer", layout="wide", page_icon="📊")

# ── Session state defaults ────────────────────────────────────────────────────
_DEFAULTS = {
    "price_paths": None,          # np.ndarray (n_sim, 8760)
    "dispatch_result": None,      # DispatchResult
    "mc_result": None,            # MCResult
    "last_dispatch_req": None,    # DispatchRequest
    "last_financials": None,      # ProjectFinancials
    "agent_messages": [],
    "agent_display": [],
}
for k, v in _DEFAULTS.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("📊 Deal Structurer")
    st.caption("Quant deal pricing platform")
    st.divider()
    tab_choice = st.radio(
        "Navigate",
        ["1 · Price Simulation", "2 · Dispatch Revenue", "3 · Project Cash Flow",
         "4 · Monte Carlo", "5 · Deal Pricing", "💬 Strategist"],
        label_visibility="collapsed",
    )
    st.divider()
    st.caption("Province → Price Paths → Dispatch → Cashflow → MC → Deal Pricing")

# ── Route to tabs ─────────────────────────────────────────────────────────────
if tab_choice == "1 · Price Simulation":
    from apps.deal_structurer import price_tab; price_tab.render()
elif tab_choice == "2 · Dispatch Revenue":
    from apps.deal_structurer import dispatch_tab; dispatch_tab.render()
elif tab_choice == "3 · Project Cash Flow":
    from apps.deal_structurer import cashflow_tab; cashflow_tab.render()
elif tab_choice == "4 · Monte Carlo":
    from apps.deal_structurer import mc_tab; mc_tab.render()
elif tab_choice == "5 · Deal Pricing":
    from apps.deal_structurer import deal_tab; deal_tab.render()
else:
    from apps.deal_structurer import strategist; strategist.render()
```

> **Note:** Because `apps/deal-structurer/` can't be imported as `apps.deal-structurer` (hyphen), add an `apps/deal_structurer/` symlink or rename the directory to `apps/deal_structurer/`. Use `apps/deal_structurer/` as the actual Python package directory. The Dockerfile COPY line stays as `apps/deal-structurer/` for build context; inside the container the directory is `/app/apps/deal_structurer/`.

- [ ] **Step 4: Create `apps/deal-structurer/price_tab.py`**

```python
"""Tab 1 — Price Simulation: province selector, OU/PCA params, price path chart."""
from __future__ import annotations
import numpy as np
import plotly.graph_objects as go
import streamlit as st
from libs.deal_models.contracts import OUParams, PriceSimRequest
from libs.deal_models.price_simulator import simulate_prices, fit_ou


def render() -> None:
    st.header("1 · Price Simulation")
    st.caption("Simulate forward hourly price paths for the target province.")

    col1, col2 = st.columns([1, 2])
    with col1:
        province = st.selectbox("Province", ["蒙西", "蒙东", "山西", "河北南网", "山东", "陕西", "甘肃", "新疆"], key="ps_province")
        model = st.radio("Price Model", ["OU (Ornstein-Uhlenbeck)", "PCA (Distribution Sliders)"], key="ps_model")
        n_sim = st.slider("Simulations", 100, 2000, 500, 100, key="ps_nsim")
        n_years = st.slider("Horizon (years)", 1, 10, 1, key="ps_nyears")

        use_ou = model.startswith("OU")
        if use_ou:
            st.subheader("OU Parameters")
            mu = st.number_input("Long-run mean (¥/MWh)", 50.0, 800.0, 300.0, key="ou_mu")
            kappa = st.number_input("Mean-reversion κ", 0.1, 20.0, 2.0, key="ou_kappa")
            sigma = st.number_input("Volatility σ (¥/MWh ann.)", 10.0, 300.0, 80.0, key="ou_sigma")
        else:
            st.info("PCA mode: provide historical prices in the text box, then click Fit + Simulate.")
            history_text = st.text_area("Paste hourly prices (one per line, ¥/MWh)", height=100, key="ps_history")

        run_btn = st.button("▶ Run Simulation", type="primary", key="ps_run")

    with col2:
        if run_btn:
            with st.spinner("Simulating price paths…"):
                try:
                    if use_ou:
                        req = PriceSimRequest(
                            province=province, n_simulations=n_sim, n_years=n_years,
                            model="ou",
                            ou_params=OUParams(kappa=kappa, mu=mu, sigma=sigma),
                        )
                    else:
                        raw = [float(x) for x in history_text.strip().splitlines() if x.strip()]
                        if len(raw) < 24 * 7:
                            st.error("Need at least 168 hours of history for PCA fitting.")
                            return
                        req = PriceSimRequest(
                            province=province, n_simulations=n_sim, n_years=n_years,
                            model="pca", price_history_yuan_mwh=raw,
                        )
                    paths = simulate_prices(req)
                    st.session_state["price_paths"] = paths
                    st.session_state["price_sim_req"] = req
                except Exception as e:
                    st.error(f"Simulation failed: {e}")
                    return

        paths = st.session_state.get("price_paths")
        if paths is not None:
            # Plot fan chart: P10/P50/P90 + 10 sample paths
            hours = np.arange(paths.shape[1])
            p10 = np.percentile(paths, 10, axis=0)
            p50 = np.percentile(paths, 50, axis=0)
            p90 = np.percentile(paths, 90, axis=0)
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=hours, y=p90, name="P90", line=dict(color="rgba(99,110,250,0.3)"), showlegend=True))
            fig.add_trace(go.Scatter(x=hours, y=p10, name="P10", fill="tonexty", line=dict(color="rgba(99,110,250,0.3)"), fillcolor="rgba(99,110,250,0.08)"))
            fig.add_trace(go.Scatter(x=hours, y=p50, name="P50", line=dict(color="rgb(99,110,250)", width=2)))
            for i in range(min(8, paths.shape[0])):
                fig.add_trace(go.Scatter(x=hours, y=paths[i], line=dict(color="rgba(200,200,200,0.4)", width=0.5), showlegend=False))
            fig.update_layout(title=f"Price Paths — {province}", xaxis_title="Hour", yaxis_title="¥/MWh", height=450)
            st.plotly_chart(fig, use_container_width=True)

            summary_cols = st.columns(3)
            summary_cols[0].metric("P10 Mean Price", f"¥{p10.mean():.0f}/MWh")
            summary_cols[1].metric("P50 Mean Price", f"¥{p50.mean():.0f}/MWh")
            summary_cols[2].metric("P90 Mean Price", f"¥{p90.mean():.0f}/MWh")
        else:
            st.info("Configure parameters and click **▶ Run Simulation** to generate price paths.")
```

- [ ] **Step 5: Create `apps/deal-structurer/dispatch_tab.py`**

```python
"""Tab 2 — Dispatch Revenue: P10/P50/P90 bar + histogram + decomposition."""
from __future__ import annotations
import numpy as np
import plotly.graph_objects as go
import streamlit as st
from libs.deal_models.contracts import DispatchRequest
from libs.deal_models.dispatch_valuation import dispatch_annual


def render() -> None:
    st.header("2 · Dispatch Revenue")
    paths = st.session_state.get("price_paths")
    if paths is None:
        st.warning("Run **Tab 1 · Price Simulation** first to generate price paths.")
        return

    col1, col2 = st.columns([1, 2])
    with col1:
        asset_type = st.selectbox("Asset Type", ["bess", "wind", "wind_bess"], key="dr_asset")
        if asset_type in ("bess", "wind_bess"):
            st.subheader("BESS Parameters")
            power_mw = st.number_input("Power (MW)", 1.0, 500.0, 50.0, key="dr_power")
            capacity_mwh = st.number_input("Capacity (MWh)", 1.0, 2000.0, 100.0, key="dr_cap")
            roundtrip_eff = st.slider("Roundtrip Efficiency", 0.70, 0.95, 0.85, 0.01, key="dr_eff")
            cycles = st.slider("Cycles/day", 0.5, 2.0, 1.0, 0.5, key="dr_cycles")
            om = st.number_input("O&M ¥/MWh discharged", 0.0, 50.0, 10.0, key="dr_om")
        else:
            power_mw = capacity_mwh = roundtrip_eff = om = 0.0; cycles = 1.0
        if asset_type in ("wind", "wind_bess"):
            st.subheader("Wind Parameters")
            installed_mw = st.number_input("Installed MW", 1.0, 2000.0, 100.0, key="dr_wind_mw")
        else:
            installed_mw = 0.0

        run_btn = st.button("▶ Calculate Revenue", type="primary", key="dr_run")

    with col2:
        if run_btn:
            req = DispatchRequest(
                asset_type=asset_type,
                capacity_mwh=capacity_mwh, power_mw=power_mw,
                roundtrip_eff=roundtrip_eff, cycles_per_day=cycles,
                om_cost_yuan_per_mwh=om, installed_mw=installed_mw,
            )
            with st.spinner("Calculating dispatch revenue…"):
                result = dispatch_annual(paths, req)
            st.session_state["dispatch_result"] = result
            st.session_state["last_dispatch_req"] = req

        result = st.session_state.get("dispatch_result")
        if result is not None:
            m_cols = st.columns(3)
            m_cols[0].metric("P10 Annual Revenue", f"¥{result.p10/1e6:.1f}M")
            m_cols[1].metric("P50 Annual Revenue", f"¥{result.p50/1e6:.1f}M")
            m_cols[2].metric("P90 Annual Revenue", f"¥{result.p90/1e6:.1f}M")

            # Revenue histogram
            fig = go.Figure()
            fig.add_trace(go.Histogram(
                x=result.revenue_paths / 1e6, nbinsx=40,
                name="Revenue (¥M)", marker_color="rgb(99,110,250)",
            ))
            fig.add_vline(x=result.p10 / 1e6, line_dash="dot", line_color="orange", annotation_text="P10")
            fig.add_vline(x=result.p50 / 1e6, line_dash="dash", line_color="green", annotation_text="P50")
            fig.add_vline(x=result.p90 / 1e6, line_dash="dot", line_color="blue", annotation_text="P90")
            fig.update_layout(title="Annual Revenue Distribution", xaxis_title="Revenue (¥M)", height=400)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Configure asset parameters and click **▶ Calculate Revenue**.")
```

- [ ] **Step 6: Create `apps/deal-structurer/cashflow_tab.py`**

```python
"""Tab 3 — Project Cash Flow: P&L table, KPI summary, waterfall."""
from __future__ import annotations
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from libs.deal_models.contracts import ProjectFinancials
from libs.deal_models.project_cashflow import compute_cashflow


def render() -> None:
    st.header("3 · Project Cash Flow")
    dr = st.session_state.get("dispatch_result")
    base_rev = dr.p50 if dr is not None else 15_000_000.0

    col1, col2 = st.columns([1, 2])
    with col1:
        st.subheader("Project Inputs")
        capex = st.number_input("Capex (¥M)", 10.0, 5000.0, 100.0, key="cf_capex") * 1e6
        om = st.number_input("Annual O&M (¥M)", 0.1, 100.0, 3.0, key="cf_om") * 1e6
        base_rev_input = st.number_input("Base Annual Revenue (¥M)", 1.0, 500.0, base_rev / 1e6, key="cf_rev") * 1e6
        life = st.slider("Project Life (years)", 10, 30, 20, key="cf_life")
        st.subheader("Financing")
        debt_ratio = st.slider("Debt Ratio", 0.0, 0.90, 0.70, 0.05, key="cf_debt")
        interest = st.slider("Interest Rate", 0.02, 0.12, 0.05, 0.005, format="%.3f", key="cf_rate")
        loan_term = st.slider("Loan Term (years)", 5, 20, 10, key="cf_term")
        grace = st.slider("Grace Period (years)", 0, 3, 1, key="cf_grace")
        hurdle = st.slider("Hurdle Rate", 0.04, 0.20, 0.08, 0.01, format="%.2f", key="cf_hurdle")
        run_btn = st.button("▶ Calculate", type="primary", key="cf_run")

    with col2:
        if run_btn or st.session_state.get("last_financials") is not None:
            if run_btn:
                fin = ProjectFinancials(
                    capex_total_yuan=capex, annual_revenue_yuan=[base_rev_input] * life,
                    annual_om_yuan=om, project_life_years=life,
                    debt_ratio=debt_ratio, interest_rate=interest,
                    loan_term_years=loan_term, grace_years=grace, hurdle_rate=hurdle,
                )
                cf = compute_cashflow(fin)
                st.session_state["last_financials"] = fin
                st.session_state["last_cf_result"] = cf

            cf = st.session_state.get("last_cf_result")
            if cf is None:
                return

            # KPI metrics
            m = st.columns(4)
            m[0].metric("Equity IRR", f"{cf.equity_irr:.1%}")
            m[1].metric("Project IRR", f"{cf.project_irr:.1%}")
            m[2].metric("DSCR (min)", f"{cf.dscr_min:.2f}x" if cf.dscr_min == cf.dscr_min else "—")
            m[3].metric("NPV (¥M)", f"{cf.npv/1e6:.1f}")
            m2 = st.columns(3)
            m2[0].metric("ROACE", f"{cf.roace:.1%}")
            m2[1].metric("Payback", f"{cf.payback_years:.1f}yr" if cf.payback_years == cf.payback_years else "—")
            m2[2].metric("DSCR (avg)", f"{cf.dscr_avg:.2f}x" if cf.dscr_avg == cf.dscr_avg else "—")

            # P&L table
            rows = [
                {"Year": r.year, "Revenue (¥M)": r.revenue/1e6, "Opex (¥M)": r.opex/1e6,
                 "EBITDA (¥M)": r.ebitda/1e6, "EBIT (¥M)": r.ebit/1e6,
                 "Net Income (¥M)": r.net_income/1e6, "Equity FCF (¥M)": r.equity_fcf/1e6,
                 "Debt Service (¥M)": r.debt_service/1e6}
                for r in cf.annual
            ]
            df = pd.DataFrame(rows).set_index("Year")
            st.dataframe(df.style.format("{:.2f}"), use_container_width=True)

            # Equity FCF waterfall
            fig = go.Figure(go.Bar(
                x=[f"Y{r.year}" for r in cf.annual],
                y=[r.equity_fcf / 1e6 for r in cf.annual],
                marker_color=["green" if r.equity_fcf >= 0 else "red" for r in cf.annual],
            ))
            fig.update_layout(title="Equity Free Cash Flow (¥M)", height=300)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Configure project parameters and click **▶ Calculate**.")
```

- [ ] **Step 7: Create `apps/deal-structurer/mc_tab.py`**

```python
"""Tab 4 — Monte Carlo: IRR distribution, VaR table, tornado chart."""
from __future__ import annotations
import numpy as np
import plotly.graph_objects as go
import streamlit as st
from libs.deal_models.contracts import MCRequest, PriceSimRequest, OUParams
from services.deal_engine.batch_runner import run_batch


def render() -> None:
    st.header("4 · Monte Carlo Results")

    fin = st.session_state.get("last_financials")
    dispatch_req = st.session_state.get("last_dispatch_req")
    price_req = st.session_state.get("price_sim_req")

    if fin is None or dispatch_req is None or price_req is None:
        st.warning("Complete Tabs 1, 2, and 3 first.")
        return

    col1, col2 = st.columns([1, 3])
    with col1:
        n_sim = st.select_slider("Simulations", [500, 1000, 2000, 5000], 1000, key="mc_nsim")
        run_btn = st.button("▶ Run Monte Carlo", type="primary", key="mc_run")

    with col2:
        if run_btn:
            req = MCRequest(price_sim=price_req, dispatch=dispatch_req, financials=fin, n_simulations=n_sim)
            bar = st.progress(0.0, text="Running simulations…")
            mc = run_batch(req, progress_callback=bar.progress)
            st.session_state["mc_result"] = mc
            bar.empty()

        mc = st.session_state.get("mc_result")
        if mc is None:
            st.info("Click **▶ Run Monte Carlo** to compute distributions.")
            return

        # Summary metrics
        m = st.columns(4)
        m[0].metric("Revenue P50", f"¥{mc.revenue_p50/1e6:.1f}M")
        m[1].metric("Revenue VaR (5%)", f"¥{mc.revenue_var_5pct/1e6:.1f}M")
        m[2].metric("Equity IRR P50", f"{mc.equity_irr_p50:.1%}")
        m[3].metric("P(IRR < hurdle)", f"{mc.irr_prob_below_hurdle:.1%}")

        c1, c2 = st.columns(2)
        with c1:
            # IRR distribution
            fig = go.Figure()
            fig.add_trace(go.Histogram(x=mc.equity_irr_paths * 100, nbinsx=40, name="Equity IRR %", marker_color="rgb(99,110,250)"))
            fig.add_vline(x=mc.equity_irr_p10 * 100, line_dash="dot", annotation_text="P10")
            fig.add_vline(x=mc.equity_irr_p50 * 100, line_dash="dash", line_color="green", annotation_text="P50")
            fig.add_vline(x=mc.equity_irr_p90 * 100, line_dash="dot", annotation_text="P90")
            fig.update_layout(title="Equity IRR Distribution", xaxis_title="IRR (%)", height=350)
            st.plotly_chart(fig, use_container_width=True)

        with c2:
            # Tornado chart
            params = [t["param"] for t in mc.tornado]
            swings = [t["swing"] * 100 for t in mc.tornado]
            fig2 = go.Figure(go.Bar(x=swings, y=params, orientation="h", marker_color="rgb(239,85,59)"))
            fig2.update_layout(title="IRR Sensitivity (Tornado)", xaxis_title="IRR Swing (pp)", height=350)
            st.plotly_chart(fig2, use_container_width=True)

        # VaR/CVaR table
        st.subheader("Risk Metrics")
        st.table({
            "Metric": ["Revenue P10", "Revenue P50", "Revenue P90", "Revenue VaR (5%)", "Revenue CVaR (5%)"],
            "Value": [
                f"¥{mc.revenue_p10/1e6:.1f}M", f"¥{mc.revenue_p50/1e6:.1f}M",
                f"¥{mc.revenue_p90/1e6:.1f}M", f"¥{mc.revenue_var_5pct/1e6:.1f}M",
                f"¥{mc.revenue_cvar_5pct/1e6:.1f}M",
            ],
        })
```

- [ ] **Step 8: Create `apps/deal-structurer/deal_tab.py`**

```python
"""Tab 5 — Deal Pricing: structure selector, params, payout distribution."""
from __future__ import annotations
import numpy as np
import plotly.graph_objects as go
import streamlit as st
import libs.deal_models.deal_structures as _ds_mod  # triggers registration
from libs.deal_models.registry import list_structures, get
from libs.deal_models.deal_structures import price_structure


def render() -> None:
    st.header("5 · Deal Pricing")
    mc = st.session_state.get("mc_result")
    if mc is None:
        st.warning("Run **Tab 4 · Monte Carlo** first to get revenue paths.")
        return

    revenue_paths = mc.revenue_paths
    p50_rev = mc.revenue_p50

    col1, col2 = st.columns([1, 2])
    with col1:
        structure = st.selectbox("Deal Structure", list_structures(), key="dp_struct")

        spec = get(structure)
        st.caption(spec.description)
        st.subheader("Parameters")

        # Dynamic parameter inputs based on schema
        schema = spec.params_schema
        param_vals = {}
        for field_name, field_info in schema.model_fields.items():
            default = p50_rev if "yuan" in field_name and "price" not in field_name else (300.0 if "price" in field_name else 1e5)
            label = field_name.replace("_", " ").title()
            val = st.number_input(label, value=float(default), key=f"dp_{field_name}")
            param_vals[field_name] = val

        price_btn = st.button("▶ Price Structure", type="primary", key="dp_price")

    with col2:
        if price_btn:
            try:
                params = spec.params_schema(**param_vals)
                result = price_structure(structure, revenue_paths, params)
                st.session_state["dp_result"] = result
            except Exception as e:
                st.error(f"Pricing failed: {e}")

        dp = st.session_state.get("dp_result")
        if dp is not None:
            m = st.columns(2)
            m[0].metric("Expected Cost", f"¥{dp.expected_cost/1e6:.2f}M/yr")
            m[1].metric("P95 Cost", f"¥{dp.p95_cost/1e6:.2f}M/yr")
            m2 = st.columns(2)
            m2[0].metric("Min Premium", f"¥{dp.min_premium/1e6:.2f}M/yr")
            m2[1].metric("Suggested Premium", f"¥{dp.suggested_premium/1e6:.2f}M/yr", delta=f"+¥{(dp.suggested_premium-dp.min_premium)/1e6:.2f}M risk charge")

            # Payout distribution
            fig = go.Figure()
            fig.add_trace(go.Histogram(x=dp.payout_paths / 1e6, nbinsx=40, marker_color="rgb(239,85,59)", name="Payout"))
            fig.add_vline(x=dp.expected_cost / 1e6, line_dash="dash", line_color="blue", annotation_text="Expected")
            fig.add_vline(x=dp.p95_cost / 1e6, line_dash="dot", line_color="red", annotation_text="P95")
            fig.update_layout(title="Payout Distribution", xaxis_title="Payout (¥M)", height=380)
            st.plotly_chart(fig, use_container_width=True)

            st.info(
                f"**Pricing recommendation:** Floor at ¥{list(param_vals.values())[0]/1e6:.1f}M/yr — "
                f"expected cost ¥{dp.expected_cost/1e6:.2f}M/yr · suggest charging ¥{dp.suggested_premium/1e6:.2f}M/yr"
            )
        else:
            st.info("Configure structure parameters and click **▶ Price Structure**.")
```

- [ ] **Step 9: Create `apps/deal-structurer/strategist.py`**

```python
"""Strategist agent — Claude tool-use chat with session persistence."""
from __future__ import annotations
import os
import streamlit as st
import anthropic as _ant
from libs.deal_models.adapters.agent_tools import AGENT_TOOLS, dispatch_tool

_SYSTEM = """You are a quantitative deal-structuring advisor for renewable energy assets in China's spot markets.

You have access to the following tools:
- run_price_simulation: simulate forward price paths (OU or PCA)
- run_dispatch_valuation: estimate BESS/wind annual revenue from price paths
- run_project_cashflow: compute project IRR, DSCR, NPV
- run_monte_carlo: full probabilistic analysis (price→dispatch→cashflow)
- price_deal_structure: price a floor/cap/collar/swap/tolling/PPA against MC revenue paths

Guidelines:
- Always run run_monte_carlo before price_deal_structure
- Cite P10/P50/P90 statistics when answering revenue questions
- Express premiums and revenues in ¥M/year for readability
- When asked "what floor guarantees X% IRR at P90?", iterate: try floor=P10 revenue, compute cashflow at P10, adjust
"""

_TOOL_ICONS = {
    "run_price_simulation": "📈",
    "run_dispatch_valuation": "⚡",
    "run_project_cashflow": "💰",
    "run_monte_carlo": "🎲",
    "price_deal_structure": "🤝",
}

# ── Agent turn loop ───────────────────────────────────────────────────────────

def _run_agent_turn(messages: list, text_ph) -> tuple[str, list]:
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return "ANTHROPIC_API_KEY not set.", messages

    client = _ant.Anthropic(api_key=api_key)
    status_ph = st.empty()

    while True:
        streamed = ""
        status_ph.caption("⏳ Thinking…")
        with client.messages.stream(
            model="claude-sonnet-4-6",
            max_tokens=4096,
            system=_SYSTEM,
            tools=AGENT_TOOLS,
            messages=messages,
        ) as stream:
            for chunk in stream.text_stream:
                streamed += chunk
                status_ph.empty()
                text_ph.markdown(streamed + "▌")
            final = stream.get_final_message()

        messages = messages + [{"role": "assistant", "content": final.content}]

        if final.stop_reason == "end_turn":
            status_ph.empty()
            text_ph.markdown(streamed)
            return streamed, messages

        if final.stop_reason != "tool_use":
            status_ph.empty()
            return f"Unexpected stop: {final.stop_reason}", messages

        tool_results = []
        for block in final.content:
            if block.type == "tool_use":
                icon = _TOOL_ICONS.get(block.name, "⚙️")
                status_ph.caption(f"{icon} Calling `{block.name}`…")
                result_str = dispatch_tool(block.name, block.input)
                tool_results.append({"type": "tool_result", "tool_use_id": block.id, "content": result_str})

        status_ph.empty()
        messages = messages + [{"role": "user", "content": tool_results}]


# ── Render ────────────────────────────────────────────────────────────────────

def render() -> None:
    st.header("💬 Strategist")
    st.caption("Ask quantitative questions about deal structure, pricing, and project returns.")

    if "agent_messages" not in st.session_state:
        st.session_state["agent_messages"] = []
    if "agent_display" not in st.session_state:
        st.session_state["agent_display"] = [{"role": "assistant", "content": "Hello! I can help you structure and price renewable energy deals. Try asking:\n- *What floor revenue guarantees 8% equity IRR at P90?*\n- *Price a revenue floor at ¥15M/year for this BESS project*\n- *How sensitive is IRR to capex vs average price?*"}]

    if st.button("🗑 Clear Chat", key="strat_clear"):
        st.session_state["agent_messages"] = []
        st.session_state["agent_display"] = []
        st.rerun()

    for msg in st.session_state["agent_display"]:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    user_input = st.chat_input("Ask the Strategist…")
    if user_input:
        st.session_state["agent_display"].append({"role": "user", "content": user_input})
        st.session_state["agent_messages"].append({"role": "user", "content": user_input})
        with st.chat_message("user"):
            st.markdown(user_input)

        with st.chat_message("assistant"):
            text_ph = st.empty()
            reply, new_msgs = _run_agent_turn(st.session_state["agent_messages"], text_ph)
            st.session_state["agent_messages"] = new_msgs
            st.session_state["agent_display"].append({"role": "assistant", "content": reply})
```

- [ ] **Step 10: Commit**

```bash
git add apps/deal-structurer/
git commit -m "feat(deal-structurer): full Streamlit app — 5 tabs + Claude Strategist"
```

---

## Verification Checklist

Run these after all tasks complete:

- [ ] **Unit tests pass:**
  ```
  pytest libs/deal_models/tests/ -v
  ```
  Expected: all PASS, ~35 tests.

- [ ] **Integration smoke test** (requires DB + PGURL env var):
  ```python
  # In Python REPL or notebook
  from services.deal_engine.price_data import fetch_price_history
  from libs.deal_models.price_simulator import fit_ou, simulate_prices
  from libs.deal_models.contracts import PriceSimRequest
  prices = fetch_price_history("蒙西", "2026-01-01", "2026-04-01")
  params = fit_ou(prices)
  print(f"Fitted: kappa={params.kappa:.2f}, mu={params.mu:.1f}, sigma={params.sigma:.1f}")
  ```

- [ ] **Financial model check** (match Excel within 0.1% IRR):
  ```python
  from libs.deal_models.contracts import ProjectFinancials
  from libs.deal_models.project_cashflow import compute_cashflow
  # Use inputs from one of the .xlsm models in Envision Energy/Asset Investment Platform/
  fin = ProjectFinancials(capex_total_yuan=..., annual_revenue_yuan=[...]*20, annual_om_yuan=...)
  result = compute_cashflow(fin)
  print(f"Equity IRR: {result.equity_irr:.4%}")  # compare to Excel
  ```

- [ ] **App smoke test:** `streamlit run apps/deal-structurer/app.py` — confirm all 5 tabs render without error using mock data (no DB).

- [ ] **Deal pricing sanity:**
  ```python
  import numpy as np, libs.deal_models.deal_structures as ds
  from libs.deal_models.deal_structures import FloorParams, price_structure
  revs = np.random.normal(10e6, 2e6, 2000)
  # Floor at P50 → expected cost ≈ 1M (some paths below)
  r1 = price_structure("revenue_floor", revs, FloorParams(floor_yuan=np.median(revs)))
  assert r1.expected_cost > 0
  # Floor at half P1 → zero cost
  r2 = price_structure("revenue_floor", revs, FloorParams(floor_yuan=np.percentile(revs, 1) * 0.5))
  assert r2.expected_cost == 0.0
  ```

- [ ] **Strategist integration:** Start app, navigate to Strategist tab, ask:
  *"Run a Monte Carlo for a 50MW/100MWh BESS in 蒙西, capex ¥80M, O&M ¥2M/yr. What equity IRR do I get at P50? And price a revenue floor at ¥8M/yr."*
  Expected: agent calls `run_monte_carlo` then `price_deal_structure`, returns coherent ¥/year figure.

```

- [ ] **Step 4: Run tests**

```
pytest libs/deal_models/tests/test_deal_structures.py -v
```

- [ ] **Step 5: Run all libs tests**

```
pytest libs/deal_models/tests/ -v
```
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add libs/deal_models/deal_structures.py libs/deal_models/tests/test_deal_structures.py
git commit -m "feat(deal_models): deal structures — 6 payoff functions + universal pricer"
```
