# Platform Data Contracts
*Canonical persisted objects for the BESS / trading intelligence platform*

---

## 1. Object Type Definitions

Four distinct object types are used across the platform. Conflating them causes schema drift, incorrect consumption patterns, and fragile agents.

---

### Model Output Object

**What it is:** The result of a single, on-demand model execution. Tied to a specific run context (inputs, model version, timestamp). May be ephemeral — captured in a response and discarded — or written to a table by a service that ran the model on a schedule.

**Defining property:** Reproducible given the same inputs. Has an input context that explains every number in the output. A consumer who questions a value can re-run the model with the same inputs and get the same answer.

**Identity:** keyed by `(model_name, trade_date, asset_code)` for time-series models; by a `run_id` UUID for ad-hoc invocations.

**Examples:** `DispatchPnlAttributionOutput` for suyou on 2026-04-18; `BessOptionalityResult` for suyou with spread_vol=28%.

---

### Monitoring State Object

**What it is:** A point-in-time snapshot of a continuously-watched condition, written by a scheduled service. Not produced by a model call — produced by a service that runs on a schedule, reads current data, and writes a row.

**Defining property:** Agents and apps read it with "give me the current state" (`WHERE snapshot_date = today`), not "run this model with these inputs." The value is the persisted state, not the computation. If the table has today's row, no computation is triggered.

**Identity:** keyed by `(snapshot_date, asset_code)`. The latest row per asset is the current state. Old rows are auditable history.

**Examples:** `asset_realization_status`, `asset_fragility_status`.

---

### Recommendation Object

**What it is:** A prescribed action, linked to a trigger source (monitoring state or model output), with a lifecycle state.

**Defining property:** Has a workflow state (`open` / `acknowledged` / `resolved`). It is not a number — it is an action record. A recommendation that is resolved should be closed, not deleted.

**Identity:** UUID PK. Linked to its trigger by `trigger_object_type` + `trigger_object_id`.

**Examples:** "Review wulate dispatch strategy — realization ratio 0.72 for 14 consecutive days." "Escalate suyou to senior review — fragility CRITICAL."

---

### Scenario Object

**What it is:** A named, reusable configuration of input assumptions. Independent of any specific asset or date — it is a parameter template. Multiple model runs or reporting periods can reference the same scenario.

**Defining property:** It does not contain results — it contains the parameter set under which results were computed. The link `model_output → scenario` provides the "under what assumptions" context.

**Identity:** UUID PK + human-readable `scenario_name` (unique within scope).

**Examples:** `base_2026`, `stress_spread_vol_35pct`, `compensation_300_yuan`.

---

## 2. Canonical Entity Model

These entities form the shared vocabulary across all objects. Using them consistently means the same contract supports BESS, thermal, tolling, and PPA without schema changes — only the `asset_type` and `risk_factor` values change.

---

### `asset`

The fundamental entity. Everything else references it.

| Field | Type | Notes |
|---|---|---|
| `asset_code` | VARCHAR(64) PK | Stable canonical identifier: `suyou`, `wulate`, `wuhai` |
| `asset_name` | VARCHAR(256) | Human display name |
| `asset_type` | VARCHAR(32) | `bess` / `thermal` / `tolling` / `ppa` / `wind` / `solar` |
| `province` | VARCHAR(64) | Market context: `mengxi`, `guangdong`, etc. |
| `capacity_mw` | DECIMAL(10,2) | Rated power capacity |
| `energy_mwh` | DECIMAL(12,2) | Usable energy capacity (null for thermal/tolling) |
| `efficiency_rt` | DECIMAL(6,4) | Round-trip efficiency (null for non-storage) |
| `om_yuan_per_mwh` | DECIMAL(10,4) | Variable O&M cost |
| `commissioning_date` | DATE | |
| `decommissioning_date` | DATE | Null if operating |
| `status` | VARCHAR(16) | `active` / `inactive` / `development` |

---

### `strategy`

A dispatch or trading rule applied to an asset. The same strategy may be applied across multiple assets or periods.

| Field | Type | Notes |
|---|---|---|
| `strategy_id` | UUID PK | |
| `asset_code` | VARCHAR(64) FK → asset | |
| `strategy_name` | VARCHAR(128) | `perfect_foresight_unrestricted`, `tt_forecast_optimal`, `nominated_dispatch`, etc. |
| `strategy_type` | VARCHAR(32) | `benchmark` / `forecast_driven` / `rule_based` / `rl_agent` / `actual` |
| `parameters` | JSONB | Strategy-specific config (null for simple strategies) |
| `effective_from` | DATE | |
| `effective_to` | DATE | Null if current |

---

### `product`

A tradeable or contractual product that generates revenue or cost for an asset.

| Field | Type | Notes |
|---|---|---|
| `product_id` | UUID PK | |
| `product_name` | VARCHAR(128) | `day_ahead_spot`, `capacity_compensation`, `ancillary_frequency`, `ppa_fixed_price` |
| `product_type` | VARCHAR(32) | `spot` / `capacity` / `ancillary` / `structured` |
| `market` | VARCHAR(64) | `mengxi`, `guangdong`, `nord_pool`, etc. |
| `settlement_frequency` | VARCHAR(16) | `15min` / `hourly` / `daily` / `monthly` |
| `currency` | VARCHAR(8) | `CNY`, `EUR`, `USD` |
| `unit` | VARCHAR(16) | `yuan_per_mwh`, `yuan_per_mw_month` |

---

### `tenor`

A time period used for pricing, settlement, or reporting.

| Field | Type | Notes |
|---|---|---|
| `tenor_id` | UUID PK | |
| `tenor_label` | VARCHAR(64) | `2026-04-18`, `Apr-2026`, `Q2-2026`, `Cal26` |
| `tenor_type` | VARCHAR(16) | `daily` / `monthly` / `quarterly` / `annual` / `custom` |
| `start_date` | DATE | |
| `end_date` | DATE | |

---

### `scenario`

A named set of input assumption overrides for model runs.

| Field | Type | Notes |
|---|---|---|
| `scenario_id` | UUID PK | |
| `scenario_name` | VARCHAR(128) UNIQUE | Human-readable, stable identifier |
| `scenario_type` | VARCHAR(32) | `base` / `stress` / `sensitivity` / `historical` |
| `parameter_overrides` | JSONB | Key-value pairs that override model defaults |
| `description` | TEXT | |
| `created_by` | VARCHAR(128) | User or system that created it |
| `created_at` | TIMESTAMPTZ | |
| `parent_scenario_id` | UUID FK self | For sensitivity families derived from a base case |

---

### `risk_factor`

A named source of uncertainty or value driver. Asset-type-agnostic where possible; asset-specific where necessary.

| Field | Type | Notes |
|---|---|---|
| `risk_factor_id` | UUID PK | |
| `factor_name` | VARCHAR(64) | `spread_level`, `spread_vol`, `compensation_rate`, `grid_restriction`, `cycle_degradation`, `efficiency_decay` |
| `factor_type` | VARCHAR(32) | `price` / `vol` / `operational` / `regulatory` / `structural` |
| `unit` | VARCHAR(32) | `yuan_per_mwh`, `pct`, `mw`, `ratio` |
| `description` | TEXT | |

*Canonical factor names for BESS: `spread_level` (peak−offpeak), `spread_vol` (σ of spread), `compensation_rate` (¥/MWh capacity payment), `grid_restriction` (dispatch curtailment), `cycle_degradation` (battery health), `efficiency_decay` (η loss over time).*

---

### `exposure`

A quantified sensitivity of an asset's value to a risk factor at a point in time. The cross-asset generalization of "Greeks."

| Field | Type | Notes |
|---|---|---|
| `exposure_id` | UUID PK | |
| `asset_code` | VARCHAR(64) FK → asset | |
| `risk_factor_id` | UUID FK → risk_factor | |
| `valuation_date` | DATE | |
| `tenor_id` | UUID FK → tenor | Horizon over which exposure applies |
| `scenario_id` | UUID FK → scenario | Null = base case |
| `exposure_value` | DECIMAL(18,4) | ∂Value / ∂RiskFactor in stated units |
| `exposure_unit` | VARCHAR(64) | e.g. `yuan_per_yuan_per_mwh` (spread_level), `yuan_per_vol_point` (spread_vol) |
| `source_model` | VARCHAR(128) | Which model produced this |
| `computed_at` | TIMESTAMPTZ | |

---

### `realized_condition`

An observed market value versus the assumed value, for a given risk factor and asset, over a lookback window. The generalization of "realized vol vs implied vol."

| Field | Type | Notes |
|---|---|---|
| `condition_id` | UUID PK | |
| `asset_code` | VARCHAR(64) FK → asset | |
| `risk_factor_id` | UUID FK → risk_factor | The factor being tracked |
| `observation_date` | DATE | End of lookback window |
| `window_days` | INTEGER | Lookback period |
| `realized_value` | DECIMAL(18,4) | Observed value of the factor |
| `assumed_value` | DECIMAL(18,4) | Forecast or inception assumption |
| `realization_ratio` | DECIMAL(8,6) | realized / assumed |
| `deviation_sigma` | DECIMAL(8,4) | Deviation in σ-units |
| `alert_level` | VARCHAR(16) FK → status_level | |

---

### `valuation_state`

A point-in-time asset valuation. Captures total value, decomposed into intrinsic and optionality components.

| Field | Type | Notes |
|---|---|---|
| `valuation_id` | UUID PK | |
| `asset_code` | VARCHAR(64) FK → asset | |
| `valuation_date` | DATE | |
| `tenor_id` | UUID FK → tenor | Valuation horizon |
| `scenario_id` | UUID FK → scenario | |
| `model_name` | VARCHAR(128) | |
| `total_value` | DECIMAL(18,4) | |
| `intrinsic_value` | DECIMAL(18,4) | Value if exercised at today's prices |
| `optionality_value` | DECIMAL(18,4) | Total − intrinsic |
| `effective_strike` | DECIMAL(18,4) | For spread-option representation |
| `primary_risk_factor_id` | UUID FK → risk_factor | The dominant value driver |
| `computed_at` | TIMESTAMPTZ | |

---

### `attribution_bucket`

Controlled vocabulary for causal attribution. Defined once; referenced by all attribution tables.

| Field | Type | Notes |
|---|---|---|
| `bucket_id` | UUID PK | |
| `bucket_name` | VARCHAR(64) UNIQUE | `forecast_error`, `grid_restriction`, `execution_nomination`, `clearing`, `residual` — and for financial books: `delta`, `gamma`, `vega`, `theta`, `skew` |
| `bucket_type` | VARCHAR(32) | `operational` / `financial` / `model` |
| `description` | TEXT | |
| `sort_order` | INTEGER | Display order in waterfall |

---

### `status_level`

Controlled vocabulary for alert and severity levels. Used by all monitoring objects.

| Field | Type | Notes |
|---|---|---|
| `level_name` | VARCHAR(16) PK | `NORMAL`, `WARN`, `ALERT`, `CRITICAL` |
| `level_rank` | INTEGER UNIQUE | 1 (best) → 4 (worst) |
| `color_hex` | VARCHAR(7) | For dashboard rendering |
| `description` | TEXT | |

---

### `recommendation`

A prescribed action with a lifecycle state. Linked to the monitoring or model output that triggered it.

| Field | Type | Notes |
|---|---|---|
| `recommendation_id` | UUID PK | |
| `asset_code` | VARCHAR(64) FK → asset | |
| `created_at` | TIMESTAMPTZ | |
| `trigger_object_type` | VARCHAR(32) | `monitoring_state` / `model_output` / `agent_reasoning` |
| `trigger_object_id` | VARCHAR(128) | FK-by-convention to the triggering row |
| `action_type` | VARCHAR(32) | `review` / `escalate` / `rehedge` / `reduce_dispatch` / `investigate` |
| `action_description` | TEXT | Human-readable prescribed action |
| `priority_rank` | INTEGER | 1 = highest |
| `confidence_level` | VARCHAR(16) | `high` / `medium` / `low` |
| `expiry_at` | TIMESTAMPTZ | After this, recommendation is stale if unresolved |
| `status` | VARCHAR(16) | `open` / `acknowledged` / `resolved` / `expired` |
| `resolved_at` | TIMESTAMPTZ | |
| `resolved_by` | VARCHAR(128) | |

---

## 3. Concrete First-Version Schemas

### `dispatch_pnl_attribution`

**Object type:** Model output object — produced by the `dispatch_pnl_attribution` decision model; persisted daily by a scheduled job.

**Purpose:** Explains why an asset's cleared actual revenue differs from its theoretical maximum (perfect-foresight unrestricted). One row per causal bucket per asset per day.

**Grain:** `(trade_date, asset_code, bucket_name)` — one row per attribution bucket per asset per trading day.

```sql
CREATE TABLE dispatch_pnl_attribution (

    -- Primary key
    trade_date              DATE            NOT NULL,
    asset_code              VARCHAR(64)     NOT NULL,
    bucket_name             VARCHAR(64)     NOT NULL,   -- FK → attribution_bucket.bucket_name

    -- Lineage
    computed_at             TIMESTAMPTZ     NOT NULL,
    model_name              VARCHAR(128)    NOT NULL DEFAULT 'dispatch_pnl_attribution',
    model_version           VARCHAR(32),
    input_data_through      DATE,           -- latest settlement date in the input series

    -- Anchor values (denormalized once per trade_date+asset_code for query convenience)
    pf_unrestricted_pnl     DECIMAL(18,4),  -- theoretical ceiling
    cleared_actual_pnl      DECIMAL(18,4),  -- what was cleared
    total_gap_yuan          DECIMAL(18,4),  -- pf_unrestricted − cleared_actual

    -- Attribution measures
    value_yuan              DECIMAL(18,4),  -- signed: negative = loss vs ceiling
    pct_of_total_gap        DECIMAL(8,4),   -- value / total_gap; sums to ~1.0 across buckets

    -- Availability flag (some scenarios not available for all assets)
    bucket_available        BOOLEAN         NOT NULL DEFAULT TRUE,

    PRIMARY KEY (trade_date, asset_code, bucket_name)
);

CREATE INDEX idx_dpa_asset_date ON dispatch_pnl_attribution (asset_code, trade_date DESC);
```

**Upstream dependencies:** scenario dispatch series (6 scenarios from DB), actual settlement prices, compensation rate from `asset_realization_status` or asset master.

**Downstream consumers:**
- `portfolio-risk-agent` — direct table query: "Why did suyou underperform on [date]?"
- `apps/trading/bess/mengxi/pnl_attribution/` — waterfall chart view
- `apps/mengxi-dashboard/` — daily attribution summary
- Management reporting — monthly aggregation of `value_yuan` by bucket

---

### `asset_realization_status`

**Object type:** Monitoring state object — produced by the `realized_conditions_monitor` scheduled service.

**Purpose:** Tracks whether actual market conditions are realizing in line with the assumptions embedded in the dispatch strategy. The platform's early-warning signal against systematic underperformance.

**Grain:** `(snapshot_date, asset_code, window_days)` — one row per asset per lookback window per day.

```sql
CREATE TABLE asset_realization_status (

    -- Primary key
    snapshot_date               DATE            NOT NULL,
    asset_code                  VARCHAR(64)     NOT NULL,
    window_days                 INTEGER         NOT NULL DEFAULT 30,

    -- Lineage
    computed_at                 TIMESTAMPTZ     NOT NULL,
    price_data_through          DATE,           -- latest clearing date included
    assumed_value_source        VARCHAR(64),    -- 'tt_forecast' / 'perfect_foresight_proxy'

    -- Realized vs assumed spread
    realized_spread_yuan        DECIMAL(18,4),  -- rolling mean actual peak−offpeak spread
    assumed_spread_yuan         DECIMAL(18,4),  -- forecast or inception spread used in strategy
    realization_ratio           DECIMAL(8,6),   -- realized / assumed; 1.0 = on track
    spread_stddev_yuan          DECIMAL(18,4),  -- rolling stddev of actual spread

    -- Breakeven
    breakeven_spread_yuan       DECIMAL(18,4),  -- spread below which asset earns nothing
    breakeven_distance_yuan     DECIMAL(18,4),  -- realized_spread − breakeven_spread
    breakeven_distance_sigma    DECIMAL(8,4),   -- breakeven_distance / spread_stddev

    -- Status
    alert_level                 VARCHAR(16)     NOT NULL,   -- NORMAL / WARN / ALERT / CRITICAL

    -- Consecutive days at or above current alert level
    consecutive_alert_days      INTEGER,

    PRIMARY KEY (snapshot_date, asset_code, window_days)
);

CREATE INDEX idx_ars_asset_date ON asset_realization_status (asset_code, snapshot_date DESC);
```

**Alert thresholds (v1):**

| `realization_ratio` | `alert_level` |
|---|---|
| ≥ 0.85 | NORMAL |
| 0.70 – 0.85 | WARN |
| 0.55 – 0.70 | ALERT |
| < 0.55 | CRITICAL |

**Upstream dependencies:** actual settlement prices from `data_ingestion` (15-min clearing data); forecast prices from `price_forecast_dayahead` outputs (or `perfect_foresight_unrestricted` dispatch price as proxy where TT forecast is unavailable).

**Downstream consumers:**
- `asset_fragility_monitor` service — reads this table as primary input
- `portfolio-risk-agent` — direct table query: "Which assets are clearing below assumption?"
- `apps/mengxi-dashboard/` — realization ratio trend chart per asset
- Daily briefing job — reads `WHERE snapshot_date = today, window_days = 30`

---

### `asset_fragility_status`

**Object type:** Monitoring state object — produced by the `asset_fragility_monitor` scheduled service. Not a model output: it aggregates multiple monitoring signals.

**Purpose:** A single composite view of an asset's operational health. Designed so that `portfolio-risk-agent` can answer "which assets need attention today and why?" with a single table read.

**Grain:** `(snapshot_date, asset_code)` — one row per asset per day.

```sql
CREATE TABLE asset_fragility_status (

    -- Primary key
    snapshot_date                   DATE            NOT NULL,
    asset_code                      VARCHAR(64)     NOT NULL,

    -- Lineage
    computed_at                     TIMESTAMPTZ     NOT NULL,
    realization_snapshot_used       DATE,           -- which realization row drove this result

    -- Signal inputs (denormalized for single-query agent access)
    realization_ratio_30d           DECIMAL(8,6),   -- from asset_realization_status
    realization_alert_level         VARCHAR(16),    -- NORMAL / WARN / ALERT / CRITICAL
    breakeven_distance_sigma        DECIMAL(8,4),
    consecutive_realization_alert_days  INTEGER,
    operating_months_remaining      DECIMAL(6,1),   -- from asset master commissioning/decommissioning
    cycle_performance_ratio         DECIMAL(8,6),   -- actual_cycles_ytd / target_cycles_ytd

    -- Composite output
    composite_score                 DECIMAL(5,4),   -- 0.0 (best) to 1.0 (worst); weighted average of signals
    fragility_score                 VARCHAR(16)     NOT NULL,   -- LOW / MEDIUM / HIGH / CRITICAL
    primary_driver                  VARCHAR(64),    -- factor_name of dominant signal
    secondary_driver                VARCHAR(64),    -- factor_name of second signal (nullable)

    PRIMARY KEY (snapshot_date, asset_code)
);

CREATE INDEX idx_afs_asset_date ON asset_fragility_status (asset_code, snapshot_date DESC);
CREATE INDEX idx_afs_date_score ON asset_fragility_status (snapshot_date, composite_score DESC);
```

**Composite score formula (v1):**

```
w_realization   = 0.50   (most predictive of revenue underperformance)
w_breakeven     = 0.25   (proximity to loss threshold)
w_cycle_perf    = 0.15   (operational degradation)
w_horizon       = 0.10   (time to recover matters less if short)

composite_score = w_realization × normalize(1 − realization_ratio_30d)
               + w_breakeven   × normalize(max(0, 2.0 − breakeven_distance_sigma))
               + w_cycle_perf  × normalize(max(0, 1 − cycle_performance_ratio))
               + w_horizon     × normalize(max(0, 12 − operating_months_remaining) / 12)
```

Score → fragility_score mapping: 0.00–0.25 LOW, 0.25–0.50 MEDIUM, 0.50–0.75 HIGH, 0.75–1.00 CRITICAL.

**Upstream dependencies:** `asset_realization_status` (window_days=30); asset master (operating dates, cycle targets); cycle count from ops ingestion.

**Downstream consumers:**
- `portfolio-risk-agent` — primary consumer: single query returns ranked alert list
- Daily briefing job — reads all assets, orders by `composite_score DESC`
- Management reporting — weekly fragility trend per asset

---

## 4. On-Demand Output Schema: `bess_optionality_decomp`

**Object type:** Model output object — not persisted on a schedule. Produced on demand by the `bess_optionality_decomp` registered model. May be optionally cached with `(asset_code, valuation_date, scenario_id)` as the key.

**Purpose:** Values a BESS asset as an equivalent financial option structure. Produces option value, sensitivity to key risk factors (in cross-asset language), and interpretation for investment and management discussions.

```python
@dataclass
class BessOptionalityInput:
    asset_code:             str
    valuation_date:         str             # ISO date
    scenario_id:            Optional[str]   # FK → scenario; None = base case

    # Asset parameters (can be pulled from asset master or overridden)
    capacity_mw:            float
    energy_mwh:             float
    efficiency_rt:          float           # round-trip: 0 < η ≤ 1
    om_yuan_per_mwh:        float

    # Market inputs
    peak_forward_yuan:      float           # peak period forward price (¥/MWh)
    offpeak_forward_yuan:   float           # offpeak period forward price (¥/MWh)
    spread_vol_pct:         float           # annualised spread vol assumption (%)
    spread_correlation:     float           # ρ between peak and offpeak price processes

    # Horizon
    horizon_days:           int             # operating period to value (days)

    # Input quality tag
    vol_input_type:         str             # 'manual' | 'forecast' | 'historical_proxy'


@dataclass
class BessOptionalityOutput:
    asset_code:             str
    valuation_date:         str
    scenario_id:            Optional[str]

    # Effective option structure
    effective_strike_yuan:  float   # K = (1/η − 1) × offpeak_fwd + om_yuan_per_mwh
    horizon_days:           int
    n_option_days:          int     # number of daily spread calls in the strip

    # Valuation
    annual_option_value_yuan:   float   # total embedded option value over horizon
    intrinsic_value_yuan:       float   # value if spread > strike every day at today's prices
    time_value_yuan:            float   # annual_option_value − intrinsic
    option_value_per_mwh:       float   # normalised by capacity

    # Exposures (cross-asset language — not "Greeks")
    spread_level_exposure:      float   # ∂V/∂spread (¥ per ¥/MWh spread change)
    spread_vol_exposure:        float   # ∂V/∂σ (¥ per 1 vol-point change)
    efficiency_exposure:        float   # ∂V/∂η (¥ per 1 ppt efficiency change)
    time_exposure:              float   # ∂V/∂t (¥ per day, negative = time decay)

    # Interpretation
    dominant_risk_factor:       str     # 'spread_level' | 'spread_vol' | 'efficiency'
    breakeven_spread_yuan:      float   # spread at which asset earns zero = effective_strike
    pct_in_the_money:           float   # fraction of horizon days spread > strike (at today's fwd)

    # Quality
    caveats:                    list    # list[str] — assumptions flagged for review
    vol_input_type:             str
```

**Optional persistence:** if the platform adds a `valuation_state` table (see canonical entity model), `BessOptionalityOutput` maps to it with:
- `total_value` = `annual_option_value_yuan`
- `intrinsic_value` = `intrinsic_value_yuan`
- `optionality_value` = `time_value_yuan`
- `effective_strike` = `effective_strike_yuan`
- `primary_risk_factor_id` → FK to whichever `risk_factor` matches `dominant_risk_factor`

---

## 5. Consumption Patterns

Three distinct patterns govern how apps and agents consume platform objects. Using the wrong pattern for the object type causes either unnecessary computation (calling a model when a table exists) or stale results (reading a table when the model should be called fresh).

---

### Pattern A — Direct table query (monitoring state objects)

**When to use:** the object is a monitoring state object. The agent or app wants the current state, not to trigger a computation.

```python
# portfolio-risk-agent: "Which assets need attention today?"
# Correct: read the persisted table. Do not call a model.

rows = db.execute("""
    SELECT asset_code, fragility_score, primary_driver, composite_score,
           realization_ratio_30d, breakeven_distance_sigma
    FROM   asset_fragility_status
    WHERE  snapshot_date = CURRENT_DATE
    ORDER  BY composite_score DESC
""")
# Agent formats rows into a ranked briefing.
# If snapshot_date has no rows yet (job hasn't run): return "status not yet available today".
```

```python
# portfolio-risk-agent: "Has wulate been underperforming for more than 2 weeks?"

row = db.execute("""
    SELECT realization_ratio, alert_level, consecutive_alert_days
    FROM   asset_realization_status
    WHERE  snapshot_date = CURRENT_DATE
      AND  asset_code = 'wulate'
      AND  window_days = 30
""").fetchone()
```

**Rule:** never call `run("realized_conditions_monitor", ...)` from a production agent. The table is the interface.

---

### Pattern B — Model invocation (on-demand model output objects)

**When to use:** the object is a model output object and either (a) no persisted table exists for this model, or (b) the agent needs a computation with custom inputs that differ from the daily defaults.

```python
# strategy-agent: "What is suyou's option value if spread vol rises to 35%?"
# Correct: invoke the model with the user's specific parameters.

result = run("bess_optionality_decomp", {
    "asset_code":           "suyou",
    "valuation_date":       "2026-04-20",
    "capacity_mw":          100.0,
    "energy_mwh":           200.0,
    "efficiency_rt":        0.85,
    "om_yuan_per_mwh":      20.0,
    "peak_forward_yuan":    380.0,
    "offpeak_forward_yuan": 200.0,
    "spread_vol_pct":       35.0,       # user-specified override
    "spread_correlation":   0.65,
    "horizon_days":         365,
    "vol_input_type":       "manual",
    "scenario_id":          None,
})
# Agent formats result.annual_option_value_yuan, result.spread_vol_exposure, etc.
```

```python
# portfolio-risk-agent: backfill attribution for a date not in the persisted table
result = run("dispatch_pnl_attribution", {
    "asset_code":   "suyou",
    "trade_date":   "2026-03-15",
    "scenario_dispatch": {
        "perfect_foresight_unrestricted": [...],
        "cleared_actual": [...],
        ...
    },
    "actual_price":                [...],
    "compensation_yuan_per_mwh":   350.0,
})
```

---

### Pattern C — Mixed (try table, fall back to model)

**When to use:** the object has a persisted table (daily batch job), but the agent may be asked about dates before the batch job ran, or for historical dates not covered. The persisted table is the preferred path; model invocation is the fallback.

```python
# portfolio-risk-agent: "Why did suyou underperform on April 18?"

# Step 1: try persisted table
rows = db.execute("""
    SELECT bucket_name, value_yuan, pct_of_total_gap, total_gap_yuan
    FROM   dispatch_pnl_attribution
    WHERE  trade_date = '2026-04-18'
      AND  asset_code = 'suyou'
    ORDER  BY sort_order
""").fetchall()

if rows:
    # Step 2a: table hit — format and return
    return format_attribution_waterfall(rows)
else:
    # Step 2b: table miss — invoke model with raw inputs from DB
    inputs = load_attribution_inputs("suyou", "2026-04-18")  # reads scenario_dispatch + prices
    result = run("dispatch_pnl_attribution", inputs)
    return format_attribution_output(result)
```

**Rule for agent tool definitions:** agent tools over model output objects that have a persisted table should always implement Pattern C — table first, model fallback. This prevents the agent from triggering expensive model runs when the answer already exists.

---

## 6. Object Dependency Map

```
asset (master)
  │
  ├──► asset_realization_status        [monitoring state]
  │         │ snapshot_date, asset_code
  │         │ realized_spread, assumed_spread, realization_ratio,
  │         │ breakeven_distance_sigma, alert_level
  │         │
  │         ▼
  │    asset_fragility_status          [monitoring state]
  │         │ snapshot_date, asset_code
  │         │ composite_score, fragility_score, primary_driver
  │         │
  │         ▼
  │    recommendation                  [recommendation]
  │         │ (created when fragility_score ≥ HIGH and no open rec exists)
  │
  ├──► dispatch_pnl_attribution        [model output — persisted daily]
  │         trade_date, asset_code, bucket_name
  │         value_yuan, pct_of_total_gap, pf_unrestricted_pnl, cleared_actual_pnl
  │
  └──► BessOptionalityOutput           [model output — on demand]
            valuation_date, asset_code
            annual_option_value_yuan, effective_strike, spread_vol_exposure
            dominant_risk_factor

Consumed by:
  portfolio-risk-agent  →  asset_fragility_status (Pattern A)
                        →  asset_realization_status (Pattern A)
                        →  dispatch_pnl_attribution (Pattern C)
  strategy-agent        →  BessOptionalityOutput (Pattern B)
                        →  dispatch_pnl_attribution (Pattern C)
  mengxi-dashboard      →  asset_fragility_status (Pattern A, time series chart)
                        →  dispatch_pnl_attribution (Pattern A, daily waterfall)
  daily briefing job    →  asset_fragility_status (Pattern A, ranked alert queue)
                        →  asset_realization_status (Pattern A, status summary)
  management reporting  →  dispatch_pnl_attribution (aggregate by month, by bucket)
                        →  asset_fragility_status (weekly trend)
```

---

## 7. Schema Extensibility Notes

These first-version schemas are minimal but structured to accommodate future asset types without migration:

**Adding a thermal/tolling asset:** `asset.asset_type = 'thermal'`; `asset.energy_mwh = NULL`; `asset.efficiency_rt = NULL`. Attribution buckets remain the same (`forecast_error`, `grid_restriction`, `execution_nomination`, `clearing`, `residual`). The `risk_factor` table gains `spark_spread`, `heat_rate`, `carbon_cost`. No schema changes to the monitoring tables.

**Adding financial book attribution:** `attribution_bucket` already includes `delta`, `gamma`, `vega`, `theta`, `skew`. `dispatch_pnl_attribution` is reusable with `bucket_name` values from the financial vocabulary. The `pf_unrestricted_pnl` column maps to "theoretical P&L if all Greeks were zero" (i.e., the static position value).

**Adding a vol surface or scenario sweep:** `valuation_state` table (from canonical entity model) accepts `scenario_id` FK. Multiple rows per `(valuation_date, asset_code)` with different `scenario_id` values represent a scenario sweep. `BessOptionalityOutput` maps to this table when scenario sweeps are persisted.

**Versioning:** all persisted tables include `computed_at TIMESTAMPTZ` and `model_version VARCHAR(32)`. Recomputing historical rows updates `computed_at` and `model_version` but leaves the primary key unchanged — the latest `computed_at` per PK is authoritative.
