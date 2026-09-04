# Deal Structurer & Pricing Platform — Design Spec
*Created: 2026-07-09*

---

## Context

Envision Energy needs a systematic, quantitative platform to structure and price deals involving renewable and storage assets in China's spot markets. Today this is done in Excel (see `Envision Energy/Asset Investment Platform/Valuation/Models/*.xlsm`). The goal is to replace these with a reproducible, probabilistic Python system that:

1. Simulates spot price uncertainty using market-calibrated models
2. Values asset dispatch revenue under each price scenario
3. Projects full project economics (IRR, ROACE, DSCR) including financing
4. Prices structured deals (floor guarantees, tolling, PPA, collars) as financial instruments
5. Exposes all of this through a Streamlit app + Claude Strategist agent

This is the **quant foundation** sub-project. Later expansions (operations service pricing, retail load curve pricing, asset management connectivity) build on top of this layer.

---

## Architecture: Two-Layer (Approach B)

```
apps/deal-structurer/          ← Streamlit UI + Claude Strategist
        ↓ imports
services/deal_engine/          ← DB access, scenario persistence, batch MC
        ↓ imports
libs/deal_models/              ← Pure computation (no I/O)
        ↓ reads data via service
DB: lingfeng (provincial) + md_id_cleared_energy (nodal, fengxing)
```

**Dependency rule:** `libs/deal_models` has zero I/O — no DB calls, no file reads. All data plumbing lives in `services/deal_engine`. This mirrors the `libs/decision_models` / `services/` split already established in the platform.

---

## Layer 1: `libs/deal_models/`

### File structure

```
libs/deal_models/
├── __init__.py
├── registry.py              # Same registry pattern as libs/decision_models/registry.py
├── contracts.py             # Pydantic schemas for all model inputs/outputs
├── price_simulator.py       # Model 1 (OU) + Model 2 (PCA + distribution fitting)
├── dispatch_valuation.py    # Spread call strip: bess / wind / wind_bess / solar / gas_peaker
├── project_cashflow.py      # Full project P&L → equity IRR / ROACE / DSCR / NPV
├── monte_carlo.py           # Orchestrator: price paths → dispatch → cashflow → distributions
├── deal_structures.py       # Registry of deal payoff + pricing functions
├── adapters/
│   ├── agent/tools.py       # Claude tool definitions for the Strategist
│   └── app/
│       ├── price_tab.py     # @Risk-style PCA distribution editor
│       ├── dispatch_tab.py  # Dispatch revenue visualisation
│       ├── cashflow_tab.py  # Project P&L + waterfall chart
│       ├── mc_tab.py        # MC results: distributions, VaR, CVaR, tornado
│       └── deal_tab.py      # Deal structure selector + pricing output
└── tests/
    ├── test_price_simulator.py
    ├── test_dispatch_valuation.py
    ├── test_project_cashflow.py
    ├── test_monte_carlo.py
    └── test_deal_structures.py
```

### `price_simulator.py` — Two models registered separately

**Model 1: OU (Ornstein-Uhlenbeck)**
- Fits mean-reversion speed (κ), long-run mean (μ), and volatility (σ) to historical hourly prices
- Simulates N forward price paths at hourly resolution
- Well-suited for stable provincial markets; allows "what if mean reverts to ¥0.15?" stress tests

**Model 2: PCA + Parametric Distribution**
- PCA decomposes historical price matrix shaped `(n_days × 24 hours)` — each row is one day's hourly profile → top 3–4 principal components
- PC interpretation: PC1 ≈ level/trend, PC2 ≈ intraday shape, PC3 ≈ seasonality, PC4 ≈ spike/tail
- Fits a parametric distribution to each PC's score series (normal, log-normal, or t-distribution selected by AIC)
- UI exposes histogram of each PC with fitted distribution overlay + sliders for μ, σ, shape (like @Risk)
- MC samples from the fitted distributions for each PC independently, reassembles via loadings
- Motivation: China's markets evolve rapidly — PCA separates structural components so each can be stressed independently

Both models return: `price_paths: np.ndarray shape (n_simulations, n_hours_per_year)`

### `dispatch_valuation.py` — Spread call strip approximation

Approximates annual dispatch revenue as a portfolio of spread call options on the hourly price series. Much faster than running the full LP dispatch optimization for each MC path (~100× speedup with <5% error for single-cycle BESS).

**Asset types supported:**
| Asset type | Revenue components | Additional inputs |
|---|---|---|
| `bess` | Peak-offpeak spread capture, ancillary services | capacity (MWh), power (MW), efficiency, cycles/day |
| `wind` | Energy sales at clearing price | capacity factor time series, installed capacity (MW) |
| `wind_bess` | Wind energy sales + BESS spread capture | both above combined |
| `solar` | Energy sales at clearing price (same as wind, different profile) | same as wind |
| `solar_bess` | Solar energy + BESS spread | combined |
| `gas_peaker` | Spark spread (power − gas × heat_rate) + capacity payment | heat rate, fuel cost, capacity payment |

Initial build: `bess`, `wind`, `wind_bess`. Others follow the same pattern.

### `project_cashflow.py` — Full project financial model

Replicates the Excel `.xlsm` model structure in Python. One `ProjectCashFlowModel` class; asset type is a config parameter, not a subclass.

**Input groups:**
- **Capital:** Capex (¥/kW or total), construction period, commissioning date
- **Financing:** Debt ratio (%), loan term (years), interest rate (%), grace period
- **Revenue:** Annual revenue series (from dispatch_valuation or manual input)
- **Opex:** Annual O&M (¥/kW/year), LTSA cost, land/grid fees, insurance
- **Degradation:** Annual capacity/efficiency degradation rate (%)
- **Tax & accounting:** Corporate tax rate, depreciation method, residual value

**Computed outputs (annual time series + summary KPIs):**
- EBITDA, EBIT, Net income
- Debt service schedule (principal + interest)
- Equity free cash flow
- **KPIs:** Project IRR (unlevered), Equity IRR (post-debt), ROACE, DSCR (min/avg), NPV at hurdle rate, Payback period

### `monte_carlo.py` — Orchestrator

Chains: `price_simulator → dispatch_valuation → project_cashflow`

**Part A output (revenue distribution):**
- P10/P50/P90 annual revenue
- Revenue VaR at user-specified confidence (default 5%)
- Revenue CVaR (expected shortfall below VaR threshold)

**Part B output (project distribution):**
- Equity IRR distribution, P10/P50/P90
- IRR VaR: probability of falling below hurdle rate
- NPV distribution
- ROACE distribution
- Tornado chart: Sobol sensitivity index per input assumption

### `deal_structures.py` — Payoff function registry

**Extension model:** Each deal structure is a `DealStructure` dataclass with:
- `payoff_fn(revenue_paths: np.ndarray, params: dict) → payout_distribution: np.ndarray`
- `pricing_fn(payout_distribution, cost_of_capital) → fair_premium: float`
- Pydantic `ParamsSchema` for structure-specific inputs

**Initial structures:**
| Structure | Payoff | Use case |
|---|---|---|
| `revenue_floor` | E[max(F − R, 0)] | Ops service guarantee, O&M contract floor |
| `revenue_cap` | E[max(R − C, 0)] | Upside sharing agreement |
| `collar` | floor_value − cap_value | Bounded revenue range |
| `fixed_revenue_swap` | F − E[R] (swap spread) | Asset owner exchanges variable for fixed |
| `tolling` | E[R] − toll | Tolling fee valuation |
| `ppa_fixed_price` | (P_fixed − E[P_spot]) × vol | Fixed-price PPA |

**To add a new structure:** implement `payoff_fn` + `pricing_fn` + `ParamsSchema`, register via `@deal_structures.register("name")`. No other files change.

---

## Layer 2: `services/deal_engine/`

```
services/deal_engine/
├── __init__.py
├── price_data.py        # Fetch historical prices from DB
├── scenario_store.py    # Save/load named deal scenarios
└── batch_runner.py      # Execute MC with progress tracking
```

### `price_data.py`

Two data sources, switchable via `source` parameter:

| Source | Default | DB table | Notes |
|---|---|---|---|
| `provincial` | ✅ Yes | lingfeng pipeline tables (same as bess-map) | 29 provinces, daily hourly data |
| `nodal` | Optional | `md_id_cleared_energy` (fengxing ingest) | Asset-specific nodal clearing prices |

Uses `services/common/db_utils.py` for DB connections (existing utility).

### `scenario_store.py`

Persists named deal scenarios (inputs + MC results) to `services/deal_engine/scenarios/<id>.json`. Enables the Strategist to recall and compare past scenarios. Migrate to DB table when needed.

### `batch_runner.py`

Executes MC runs (default 1,000 paths, configurable to 10,000) with `st.progress()` integration. Returns full results dict consumed by app tabs.

---

## App: `apps/deal-structurer/`

```
apps/deal-structurer/
├── Dockerfile
├── requirements.txt
├── app.py               # Shell: sidebar nav, asset type selector, tab routing
├── price_tab.py         # Tab 1: Price Simulation
├── dispatch_tab.py      # Tab 2: Dispatch Revenue
├── cashflow_tab.py      # Tab 3: Project Cash Flow
├── mc_tab.py            # Tab 4: Monte Carlo Results
├── deal_tab.py          # Tab 5: Deal Pricing
└── strategist.py        # Claude Strategist agent
```

### Tab 1 — Price Simulation
- Province + date range selector; provincial/nodal toggle
- Asset type selector (affects which dispatch model runs)
- Model selector: OU or PCA
- **PCA mode:** histogram per top PC, fitted distribution overlay, sliders for μ/σ/shape, live chart update — @Risk style interactive parameter adjustment
- Output: N sample price paths plotted over historical actuals

### Tab 2 — Dispatch Revenue
- Runs spread call strip on simulated paths
- P10/P50/P90 annual revenue bar chart + revenue histogram
- Revenue decomposition by source (arbitrage / ancillary / capacity payment)

### Tab 3 — Project Cash Flow
- Input form: capex, debt ratio, loan term, interest rate, opex, degradation, tax, commissioning date
- Annual P&L table (EBITDA → net income)
- Debt repayment schedule chart
- Equity cash flow waterfall
- KPI summary: Project IRR, Equity IRR, ROACE, DSCR, NPV, Payback

### Tab 4 — Monte Carlo Results
- IRR distribution histogram (project + equity)
- Revenue P10/P50/P90 + VaR + CVaR table
- IRR VaR: probability of missing hurdle rate
- Tornado chart (Sobol sensitivity): which input drives most IRR variance
- Scenario comparison: run 2–4 named scenarios side by side

### Tab 5 — Deal Pricing
- Deal structure selector (dropdown from `deal_structures` registry)
- Structure-specific parameter inputs
- Output: payout distribution, expected cost of guarantee, P95 worst-case payout
- Pricing recommendation: minimum premium (cost-based) + suggested market premium (cost + CVaR risk charge)
- Example: "Guarantee ¥8M/year floor → expected cost ¥1.2M/year, suggest charging ¥1.8M/year"

### Strategist Agent (`strategist.py`)
- Same pattern as spot-market Strategist (session persistence, tool use, sidebar chat panel)
- Registered tools from `libs/deal_models/adapters/agent/tools.py`:
  - `run_price_simulation(province, model, params)`
  - `run_dispatch_valuation(asset_type, price_paths, asset_params)`
  - `run_project_cashflow(revenue_series, financial_params)`
  - `run_monte_carlo(full_params, n_simulations)`
  - `price_deal_structure(structure_type, structure_params, revenue_paths)`
  - `compare_scenarios(scenario_ids)`
  - `explain_tornado(tornado_results)`
- Example interactions:
  - *"What floor revenue is needed for 8% equity IRR at P90?"*
  - *"How sensitive is equity IRR to capex vs average price?"*
  - *"Price a revenue floor at ¥10M/year for this BESS project"*
  - *"Compare base case vs stress scenario with ¥0.15 average price"*

---

## Connections to Existing Platform

| This app needs | Source | How |
|---|---|---|
| Provincial hourly prices | lingfeng pipeline | `services/deal_engine/price_data.py` → same DB tables as bess-map |
| Nodal clearing prices | fengxing ingest | `md_id_cleared_energy` table |
| DB connection utility | `services/common/db_utils.py` | Direct import |
| Registry / adapter pattern | `libs/decision_models/` | Mirror pattern, independent registry |
| Strategist agent pattern | `apps/spot-market/strategist.py` | Reference implementation |

---

## Build Order

1. `libs/deal_models/contracts.py` + `registry.py` — schemas first
2. `libs/deal_models/price_simulator.py` — OU model, then PCA model
3. `libs/deal_models/dispatch_valuation.py` — spread call strip (bess, wind, wind_bess)
4. `libs/deal_models/project_cashflow.py` — full financial model (replicate Excel structure)
5. `libs/deal_models/monte_carlo.py` — orchestrator + VaR/CVaR/tornado
6. `libs/deal_models/deal_structures.py` — payoff function registry (6 initial structures)
7. `libs/deal_models/adapters/` — agent tools + app tab components
8. `services/deal_engine/` — price_data, scenario_store, batch_runner
9. `apps/deal-structurer/` — Streamlit app (5 tabs + Strategist)

---

## Verification

1. **Unit tests:** `pytest libs/deal_models/tests/` — each model with synthetic price data
2. **Integration test:** End-to-end MC run pulling real lingfeng data for an Inner Mongolia BESS asset; verify P10/P50/P90 revenue is in plausible range
3. **Financial model check:** Run `project_cashflow.py` with same inputs as one of the `.xlsm` models; verify equity IRR matches Excel output within 0.1%
4. **App smoke test:** All 5 tabs render without error with sample data loaded
5. **Deal pricing sanity check:** Revenue floor priced at P50 revenue → expected payout ≈ 0; priced well below P10 → expected payout > 0
6. **Strategist integration:** Ask "what floor guarantees 8% equity IRR at P90?" → agent calls `run_monte_carlo` + `price_deal_structure` and returns a coherent ¥/year figure

---

## Future Expansions (out of scope for this phase)

- **Asset management connectivity:** Pull actual O&M costs and settlement data from asset-management app (to be built)
- **Power retail pricing:** C&I customer load curve pricing — `retail_load_margin` deal structure + customer tab
- **Solar / gas peaker asset types:** Follow same pattern, add `solar` and `gas_peaker` to dispatch valuation
- **Policy & market analytics module:** Connect to spot-market KB for regulatory context in Strategist responses
- **Debt optimisation:** Optimal financing structure given IRR hurdle and DSCR covenant
