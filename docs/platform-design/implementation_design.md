# Options Analytics: Platform Implementation Design
*Mapping the four-layer architecture to the bess-platform codebase*

---

## Framing

This document converts the four-layer architecture defined in `options_platform_roadmap.md` into a concrete build specification anchored to the existing `bess-platform` repository. Each module is placed into the platform's existing vocabulary:

- **Decision model**: a `libs/decision_models/<model>.py` asset registered in the platform registry, with schema, metadata, and tests
- **Workflow**: a `libs/decision_models/workflows/<name>.py` pipeline that chains model calls
- **Service**: a `services/<name>/` component — data ingestion, document intake, scheduled job
- **App layer**: a `apps/<name>/` Streamlit app or REST endpoint that consumes models
- **Agent tool**: an entry in `libs/decision_models/adapters/agent/tools.py` that exposes a model to Claude agents

The platform already has: `bess_dispatch_optimization`, `bess_dispatch_simulation_multiday`, `price_forecast_dayahead`, `revenue_scenario_engine` — plus strategy_comparison workflow, ops ingestion pipeline, and five production agents. The additions below slot into this structure without restructuring it.

---

## Section 1: Priority Module Classification

### Principle

The six priority modules belong to three distinct architecture classes. Mixing them flattens the design and creates wrong build priorities. The classes are:

| Class | What it is | Registry? | User-facing? | Daily persisted output? |
|---|---|---|---|---|
| **Analytics Kernel** | Pure computation primitive. No business logic. No data dependencies. Called by other models, not by users or agents. | No | No | No |
| **Core Decision Model** | Registered in `libs/decision_models/`. Has business metadata, typed schema, and tests. Called on-demand by agents, apps, or workflows. Answers a business question. | Yes | Yes (via agent/app) | Optional |
| **Monitoring / Control Service** | Runs on a schedule. Consumes model outputs. Persists a stable status table to DB each day. Its value is the persisted state, not the computation. | No (or thin wrapper) | Indirectly (via agent tool querying the table) | Yes — this is the point |

---

### Class 1: Analytics Kernels

**`spread_option_pricer`**

**Classification: Analytics Kernel**

A pure mathematical function: two correlated forward prices in, option price and Greeks out. Kirk's approximation for spread options with K ≠ 0; Margrabe's formula for exchange options at K = 0.

- **Why this class:** It has no business logic. It does not know what a BESS asset is, what "Mengxi" means, or what a dispatch cycle is. It takes numbers and returns numbers. The registry metadata questions (which market, which asset_type, which granularity) are category errors for this module — it is market-agnostic by construction.
- **User-facing:** No. Users never call a Kirk pricer directly. They call a model that uses one.
- **Registry:** No. It is a utility function, analogous to a stats library. Lives at `libs/decision_models/utils/spread_pricing.py` or a dedicated `libs/analytics/` module. Imported by `bess_optionality_decomp`; not exposed to the agent tool registry.
- **Daily persisted output:** No.
- **Build trigger:** Only when `bess_optionality_decomp` is being built. Not before.

---

### Class 2: Core Decision Models

These three modules answer business questions, have deterministic Input → Output contracts, and belong in the model registry.

---

**`dispatch_pnl_attribution`**

**Classification: Core Decision Model**

Decomposes the gap between cleared actual revenue and theoretical maximum (perfect-foresight) into causal buckets: forecast error / grid restriction / execution-nomination gap / clearing gap / residual.

- **Why this class:** It answers a specific business question — "why did we earn less than we could have?" — with a structured, auditable output. The logic already exists in `apps/trading/bess/mengxi/pnl_attribution/calc.py::build_daily_attribution_row` and in `workflows/strategy_comparison.py`. This is a formalization, not a rebuild.
- **User-facing:** Yes. Primary consumer is `portfolio-risk-agent` (conversational queries) and the Mengxi dashboard (tabular view). Operations team reads this daily.
- **Registry:** Yes. Metadata: `category=analytics`, `scope=asset_level`, `market=mengxi`, `granularity=daily`, `deterministic=True`.
- **Daily persisted output:** Yes — this is the highest-priority persisted table. A `daily_pnl_attribution` table (columns: trade_date, asset_code, bucket, value_yuan) should be populated every morning by a scheduled job. Agents then query the table rather than recomputing. The registered model is used for on-demand queries and backfills.
- **Relationship to existing code:** Thin wrapper over `calc.py::build_daily_attribution_row`. That function already implements the attribution waterfall; the work is adding the schema, metadata, and daily job — not reimplementing the logic.

---

**`bess_optionality_decomp`**

**Classification: Core Decision Model**

Takes a BESS asset specification and current market inputs (forward price pair, vol estimate, correlation) and decomposes the asset into a strip of daily spread call options. Returns embedded annual option value, effective strike, Greeks (vega, delta), and dominant risk factor.

- **Why this class:** It answers an investment question — "what is this asset worth as a financial option, and what drives that value?" — with a structured output. It has clear business metadata (asset_type=bess, category=analytics, scope=asset_level). It is not a monitoring function and does not run on a schedule.
- **User-facing:** Yes, but for a different audience than attribution. Consumed by `strategy-agent` for investment analysis and valuation discussions. Not part of the daily operations loop.
- **Registry:** Yes. Metadata: `category=analytics`, `scope=asset_level`, `asset_type=bess`, `deterministic=True`, `market=None` (market-agnostic — market is passed as an input parameter).
- **Daily persisted output:** No. Run on-demand when investment or valuation decisions require it. The vol input requires a manual estimate or model forecast — it is not something to automate daily without a vol forecasting service.
- **Depends on:** `spread_option_pricer` (the analytics kernel). Build these together.
- **Build priority:** Medium. Important for investment framing; not urgent for daily operations.

---

**`revenue_scenario_engine` (extend existing)**

**Classification: Core Decision Model — extension of existing registered model**

Already in production. The extension adds spread-vol sensitivity: parameterize over 5 spread vol assumptions and output a sensitivity table (¥ P&L per 1% change in spread vol) plus the breakeven spread vol below which NPV turns negative.

- **Why this class:** It is already a registered model. The extension is an increment to its output schema — no architecture change needed.
- **User-facing:** Yes. Investment team, asset managers, `strategy-agent`. Not part of daily ops.
- **Registry:** Already registered. Extension requires a schema update and metadata version bump only.
- **Daily persisted output:** No. On-demand.
- **Build priority:** Low-medium. Valuable for investment decisions but the platform already functions without it. Build after monitoring services are stable.

---

### Class 3: Monitoring / Control Services

These two modules are operationally important but are primarily services, not models. Their value is the stable persisted table they write each day — not the computation itself. Building them as registered models would misrepresent their architecture and add unnecessary overhead (registry metadata questions like "deterministic?" are ill-posed for a rolling-window monitor that depends on how much history is available).

---

**`realized_conditions_monitor`**

**Classification: Monitoring / Control Service** (with a thin callable interface for on-demand use)

Computes: rolling 30-day actual settlement mean vs forecast spread at dispatch time, realization ratio (actual / forecast), breakeven distance in ¥/MWh and σ-units, and alert level (NORMAL / WARN / ALERT / CRITICAL).

- **Why this class:** Its primary value is not answering a one-off question — it is maintaining a continuously updated status table that agents and dashboards read without triggering a recomputation. A monitoring service should be *always on*, not called on-demand.
- **User-facing:** Indirectly. Agents and dashboards query the persisted `asset_realization_status` table rather than calling a model run. An agent tool wraps a DB read, not a model call.
- **Registry:** Not as a primary model. The computation logic can be a standalone module (`services/monitoring/realization_monitor.py`) with a simple function interface — callable for backfills and unit tests. No need for the full model registry overhead.
- **Daily persisted output:** Yes — this is the point. Table: `asset_realization_status` (columns: snapshot_date, asset_code, window_days, realization_ratio, breakeven_distance_yuan, breakeven_distance_sigma, alert_level). Written every morning after clearing data lands.
- **Data dependency:** Actual 15-min settlement prices (in DB for suyou and wulate from TT API); perfect-foresight dispatch price as proxy baseline for assets without TT forecast.

---

**`asset_fragility_monitor`**

**Classification: Monitoring / Control Service**

Composite early-warning score per asset, aggregating: realization ratio (from `realized_conditions_monitor` table), remaining operating horizon, cycle performance ratio. Outputs: fragility score (LOW / MEDIUM / HIGH / CRITICAL) + ordered list of contributing factors.

- **Why this class:** It is an aggregation of previously persisted signals — not a pure computation from raw market data. It consumes a DB table (the realization status table above) and writes another DB table. It has no natural function signature that could be tested with deterministic inputs; it depends on how much history has accumulated and which assets have data gaps.
- **User-facing:** Indirectly. `portfolio-risk-agent` reads the `asset_fragility_status` table and surfaces the result in the daily briefing. The user sees: "suyou: HIGH — realization ratio 0.76 (30-day), 8 months remaining, cycles 12% below target."
- **Registry:** No. It is a service, not a model. Implemented as `services/monitoring/fragility_monitor.py`, called by a scheduled job.
- **Daily persisted output:** Yes — table: `asset_fragility_status` (columns: snapshot_date, asset_code, fragility_score, realization_ratio, breakeven_distance_sigma, operating_months_remaining, cycle_performance_ratio, primary_driver, secondary_driver).
- **Depends on:** `realized_conditions_monitor` having at least 30 days of populated history. Build 30 days after #2 above is live.

---

### Classification Summary

| Module | Class | Registry | User-facing | Daily persisted table | Build priority |
|---|---|---|---|---|---|
| `spread_option_pricer` | Analytics Kernel | No | No | No | 6 — build with optionality_decomp |
| `dispatch_pnl_attribution` | Core Decision Model | Yes | Yes (agent + dashboard) | Yes — `daily_pnl_attribution` | 1 — immediate |
| `realized_conditions_monitor` | Monitoring Service | No (thin callable) | Indirectly (via table) | Yes — `asset_realization_status` | 2 — immediate |
| `asset_fragility_monitor` | Monitoring Service | No | Indirectly (via table) | Yes — `asset_fragility_status` | 3 — after 30d of realization data |
| `revenue_scenario_engine` (extend) | Core Decision Model | Yes (existing) | Yes (agent + app) | No | 4 — medium term |
| `bess_optionality_decomp` | Core Decision Model | Yes | Yes (strategy-agent) | No | 5 — medium term, build with kernel |

---

## Section 2: Revised Build Sequence

The original sequence led with `spread_option_pricer` — a math kernel with no immediate operational user. The revised sequence leads with what operations teams need today: explanation of what happened (attribution) and early warning of what is deteriorating (realization monitor).

### Build Now — closes the gap between "we have the data" and "we can explain it"

**1. Formalize `dispatch_pnl_attribution` as a registered model + daily job**

The computation already exists in `calc.py`. The work is:
- Define `DispatchPnlAttributionInput` / `DispatchPnlAttributionOutput` dataclasses in `schemas/`
- Write `run_fn` that wraps `calc.py::build_daily_attribution_row`
- Add `MODEL_ASSUMPTIONS`, register in registry with category=analytics
- Add agent tool entry in `adapters/agent/tools.py`
- Write `services/monitoring/run_daily_attribution.py` — scheduled job, runs after clearing data lands each day, persists to `daily_pnl_attribution` table
- Wire to `portfolio-risk-agent` and Mengxi dashboard

**What this unlocks:** every day, the operations team and agents can answer "why was suyou's revenue ¥X today?" with a causal breakdown, not just a number.

---

**2. Build `realized_conditions_monitor` as a service + persisted table**

The core computation is:
- Rolling 30-day actual settlement prices from DB → compute mean_actual_spread
- Compare to forecast spread at position date (from `price_forecast_dayahead` output, or perfect-foresight proxy)
- Realization ratio = mean_actual_spread / forecast_spread
- Breakeven distance = (mean_actual_spread − effective_breakeven) / spread_stddev
- Alert thresholds: NORMAL (ratio > 0.85), WARN (0.70–0.85), ALERT (0.55–0.70), CRITICAL (< 0.55)

Implementation:
- `services/monitoring/realization_monitor.py` — core computation module, pure functions
- `services/monitoring/run_realization_monitor.py` — daily scheduled job writing to `asset_realization_status`
- Agent tool wraps a DB read from this table (not a model call)

**What this unlocks:** before month-end settlement, the portfolio-risk-agent can flag: "wulate has been clearing 22% below forecast for 18 days — breakeven is 1.3σ away." Currently this signal doesn't exist until the settlement PDF arrives.

---

**3. Build `asset_fragility_monitor` as a service** *(start 30 days after #2 is live)*

Once `asset_realization_status` has 30 days of history:
- `services/monitoring/fragility_monitor.py` — reads realization table, cycle count proxy, remaining horizon from asset master; computes composite score
- `services/monitoring/run_fragility_monitor.py` — daily job, writes `asset_fragility_status`
- `portfolio-risk-agent` tool reads this table and includes the result in the daily briefing

**What this unlocks:** ranked morning alert: "3 assets flagged — wulate: HIGH, suyou: WARN, wuhai: NORMAL." The agent presents the top issue first with contributing factors.

---

### Build Next — adds analytical depth once the operational loop is running

**4. Extend `revenue_scenario_engine` with spread vol sensitivity**

Add a vol-parameterized sweep to the existing model:
- Run P&L calculation across 5 spread vol assumptions (σ − 2 steps to σ + 2 steps, each ±5%)
- Output: sensitivity table + breakeven spread vol
- No new data needed; schema update + metadata version bump only

**What this unlocks:** investment team can stress-test an asset's annual revenue against vol assumptions before signing a management contract.

---

**5. Build `spread_option_pricer` (utility) + `bess_optionality_decomp` (model) together**

Build these as a paired unit when investment valuation use cases are confirmed:
- `libs/decision_models/utils/spread_pricing.py` (or `libs/analytics/`) — Kirk/Margrabe utility, no registry
- `libs/decision_models/bess_optionality_decomp.py` — imports spread_pricing utility, registered model
- Add `adapters/app/optionality_page.py` for model-catalogue visibility
- Wire to `strategy-agent` as an investment analysis tool

**What this unlocks:** for any BESS asset, produce an option-value summary: "suyou has ¥X annual embedded option value at current spread conditions. Effective strike: ¥Y/MWh. Dominant risk: spread vol (¥Z per 1 vol-point). A 30% decline in spread vol reduces asset value by ¥W."

---

### Build Later — requires data infrastructure not yet available

**6. Cross-market signal radar** — upstream coal/LNG/hydro vol feeds not yet ingested

**7. Spread vol surface calibration** — requires liquid power options market (not yet present in Inner Mongolia)

**8. Hedge recommendation engine** — requires confirmed execution path and tradeable instrument list

---

## Section 3: Platform Integration Pattern

```
External data sources
  (Lingfeng, EnoS, TT API, Excel ops reports)
        │
        ▼
services/data_ingestion/            ← EXISTING: market prices, dispatch, clearing
services/ops_ingestion/             ← EXISTING: Inner Mongolia Excel pipeline
services/document_intake/           ← EXISTING: PDF settlement extraction
        │
        ▼
─────────────────────────────────────────────────────────────────────
ANALYTICS UTILITIES (not in registry)
─────────────────────────────────────────────────────────────────────
libs/decision_models/utils/spread_pricing.py    ← Kirk + Margrabe (utility)
        │ (imported by bess_optionality_decomp only)
        │
─────────────────────────────────────────────────────────────────────
CORE DECISION MODELS (libs/decision_models/ — registered)
─────────────────────────────────────────────────────────────────────
bess_dispatch_optimization          ← EXISTING
bess_dispatch_simulation_multiday   ← EXISTING
price_forecast_dayahead             ← EXISTING
revenue_scenario_engine             ← EXISTING + extend (vol sensitivity)
dispatch_pnl_attribution            ← ADD: registered model + daily batch job
bess_optionality_decomp             ← ADD: registered model (medium-term)
        │
─────────────────────────────────────────────────────────────────────
MONITORING / CONTROL SERVICES (services/monitoring/ — scheduled, persisted)
─────────────────────────────────────────────────────────────────────
run_daily_attribution.py            ← calls dispatch_pnl_attribution
                                       writes: daily_pnl_attribution table
run_realization_monitor.py          ← calls realization_monitor.py logic
                                       writes: asset_realization_status table
run_fragility_monitor.py            ← reads realization table + asset master
                                       writes: asset_fragility_status table
        │
─────────────────────────────────────────────────────────────────────
AGENT / APP LAYER
─────────────────────────────────────────────────────────────────────
libs/decision_models/adapters/agent/tools.py    ← registered models exposed here
                                                   monitoring tables exposed via DB-read tools
        │
        ├──► apps/portfolio-risk-agent/   ← reads attribution + realization + fragility tables
        ├──► apps/strategy-agent/         ← calls dispatch_pnl_attribution + optionality_decomp
        ├──► apps/trading/bess/mengxi/    ← attribution dashboard + market_monitor
        └──► apps/mengxi-dashboard/       ← scenario_engine, realization status view
```

**Routing rules — what goes where:**

| Construct | Rule |
|---|---|
| Math utility | `libs/decision_models/utils/` — imported by models, never exposed to agents or apps directly |
| New computation with business meaning | `libs/decision_models/<model>.py` — registered, schema + tests required |
| Scheduled operational job | `services/monitoring/run_<job>.py` — uses `services/common/job_control.py` pattern |
| Persisted status table | Written by service, read by agent tools via DB query — not by model run_fn |
| Agent tool over a status table | `adapters/agent/tools.py` — DB read wrapper, not a model dispatch |
| Agent tool over a model | `adapters/agent/tools.py` — calls `runners/local.py::run(model_name, inputs)` |

---

## Section 4: Revised 90-Day Implementation View

### Days 1–30: Make existing data explainable

Priority: close the gap between "data exists in the DB" and "operations team and agents can explain it."

| Task | File(s) | Commercial outcome |
|---|---|---|
| Formalize `dispatch_pnl_attribution` | `libs/decision_models/dispatch_pnl_attribution.py` + `schemas/` + `tests/` | Registered model, callable on-demand from agents |
| Daily attribution job | `services/monitoring/run_daily_attribution.py` → writes `daily_pnl_attribution` | Every morning: yesterday's causal P&L breakdown per asset is available without triggering a model run |
| Wire attribution to `portfolio-risk-agent` | `adapters/agent/tools.py` — DB-read tool over `daily_pnl_attribution` | Agent answers: "Why did suyou earn less than expected on 18 April?" |
| Wire attribution to Mengxi dashboard | `libs/decision_models/adapters/app/attribution_page.py` | Dashboard shows daily attribution waterfall per asset |
| Build `realized_conditions_monitor` service | `services/monitoring/realization_monitor.py` + `run_realization_monitor.py` → writes `asset_realization_status` | Daily realization status for all Mengxi assets |
| Wire realization status to `portfolio-risk-agent` | `adapters/agent/tools.py` — DB-read tool over `asset_realization_status` | Agent answers: "Which assets are clearing below forecast this month?" |

**Checkpoint:** `portfolio-risk-agent` can answer attribution and realization questions for all Mengxi assets using persisted tables — not ad-hoc SQL or hard-coded logic.

---

### Days 31–60: Activate monitoring and composite warning

Priority: turn the daily tables into an operational alert system.

| Task | File(s) | Commercial outcome |
|---|---|---|
| Build `asset_fragility_monitor` service | `services/monitoring/fragility_monitor.py` + `run_fragility_monitor.py` → writes `asset_fragility_status` | Daily composite fragility score per asset |
| Wire fragility to `portfolio-risk-agent` | `adapters/agent/tools.py` — DB-read tool | Agent leads daily briefing with: ranked alert list, each asset with score + primary driver |
| Build daily briefing job | `services/monitoring/run_daily_briefing.py` — reads all three tables, produces ordered alert queue | Structured morning output: severity-ranked list of all Mengxi assets with attribution, realization, fragility |
| Extend `revenue_scenario_engine` | Modify `libs/decision_models/revenue_scenario_engine.py` — add vol sweep output | Investment team: scenario analysis with spread vol sensitivity |

**Checkpoint:** every morning a structured alert queue is generated automatically and readable by `portfolio-risk-agent` before anyone opens a dashboard. The agent knows which asset needs attention and why.

---

### Days 61–90: Investment-layer analytical capability

Priority: add option-value framing for asset management and investment decisions.

| Task | File(s) | Commercial outcome |
|---|---|---|
| Build `spread_option_pricer` utility | `libs/decision_models/utils/spread_pricing.py` — Kirk + Margrabe, no registry | Pricing primitive, not user-facing |
| Build `bess_optionality_decomp` model | `libs/decision_models/bess_optionality_decomp.py` + `schemas/` + `tests/` | Registered model: values any BESS asset as a strip of spread calls |
| Wire `bess_optionality_decomp` to `strategy-agent` | `adapters/agent/tools.py` | Strategy-agent answers: "What is suyou worth as an option at current spread conditions? How does value change if spread vol falls 5 points?" |
| Add optionality page to model-catalogue | `libs/decision_models/adapters/app/optionality_page.py` | Visible in catalogue: option value, effective strike, Greeks per asset |

**Checkpoint:** `strategy-agent` can produce a full option-value summary for any Mengxi BESS asset: embedded annual value, effective strike, spread vol sensitivity, dominant risk factor.

---

## Appendix: Translation Key (architecture module → platform name)

| Architecture module (roadmap) | Platform name | Class | Status |
|---|---|---|---|
| `PRICING_ENGINE` (dispatch optimization) | `bess_dispatch_optimization` | Core Decision Model | Existing |
| `PRICING_ENGINE` (spread option) | `libs/decision_models/utils/spread_pricing.py` | Analytics Kernel | Add with optionality_decomp |
| `SCENARIO_ENGINE` | `revenue_scenario_engine` | Core Decision Model | Existing + extend |
| `PNL_ATTRIBUTION` | `dispatch_pnl_attribution` | Core Decision Model + daily service | Promote from calc.py |
| `REALIZED_IMPLIED_MONITOR` | `realized_conditions_monitor` service | Monitoring Service | Add |
| `RISK_DECOMPOSITION` | `asset_fragility_monitor` service | Monitoring Service | Add |
| `OPTIONALITY_DECOMP` | `bess_optionality_decomp` | Core Decision Model | Add (medium-term) |
| `WORKFLOW_ENGINE` | `services/monitoring/` scheduled jobs | Monitoring Service | Add |
| `SIGNAL_RADAR` | *(deferred — no upstream vol feeds)* | — | Build Later |
| `HEDGE_RECOMMENDATION` | *(deferred — no execution path)* | — | Build Later |
| Agent layer (AS1–AS8) | `portfolio-risk-agent` + `strategy-agent` + tools | Agent Layer | Extend incrementally |
