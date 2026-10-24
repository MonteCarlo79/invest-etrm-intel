# Asset-Backed Trading — Option-Value Engine on Deal Structurer

**Date:** 2026-10-24
**Status:** Draft for review
**Supersedes/extends:** `2026-07-09-deal-structurer-design.md` (quant foundation), `2026-09-09-structurer-agent-design.md`

---

## 1. Intent and why

Deal Structurer today prices *deals* (floors, tolls, PPAs) off an annual revenue distribution. This upgrade turns it into the platform's **asset-backed trading system**: it values each dispatchable asset as what it economically is — a **strip of options on hourly price spreads** — and uses that to answer three questions per province per asset type:

1. **What is the option worth?** (price the optionality)
2. **Where is the market mispricing it?** (option value vs. capex/ask → undervalued screen)
3. **How do we capture it?** (link model value to realized capture in bess-map / mengxi ops)

**The decisions that motivate this (2026-10-24, broadened 2026-10-25):** three deal types land on the same desk, and all three are the same economic object — an option on a provincial revenue stack:

1. **Greenfield development permits (开发指标)** — BESS, wind, solar. Pay a permit price today for the right to build within a validity window. Intrinsic value today often doesn't cover build cost; what may justify the permit is extrinsic value from price dynamics plus **policy-regime optionality** (capacity payments / FR access / 机制电价 rolling out province by province under the 136号 reform path).
2. **Distressed-asset M&A** — operating wind/solar/BESS assets offered below replacement cost by forced sellers. The question: is the ask below the option value of the revenue stack, anchored on the asset's *actual* operating history where we have it, plus turnaround levers we control (trading-strategy uplift toward champion-model capture, co-location retrofit options like adding BESS to a wind farm)?
3. **Hold/optimize** of owned assets — already served by Pillar 3; the option engine gives it a fundamental-value benchmark.

The system's job: price the option honestly per province per asset type, so the permit bid or M&A offer is a number, not a feeling.

BESS first (the cleanest spread option; data and LP already in production). Coal-fired plants second (dark-spread options with unit commitment — phase 2, interface designed now, built later).

Method source: KYOS plant/storage valuation as digested in power-academy (`power-academy/inventory/plant_hedging_and_trading_strategies_kyos_20110530.md`, `worldpower2009_realisticpowerplantvaluation.md` — PCA in commodity simulation, Monte Carlo of spot prices, per-path dispatch optimization, intrinsic/extrinsic split, delta by price-shock-and-revalue). Teaching implementations: `power-academy/labs/intrinsic_vs_extrinsic_value/`, `monte_carlo_and_lsm_for_generation_valuation/`, `dispatch_optimisation_and_delta_hedging/`.

## 2. Core definitions (the Jensen split)

For a dispatchable asset and a rolling 12-month window of hourly RT prices:

- **Expected price curve** — the month × hour-of-day average curve (288 points expanded to 8,760 h). This is the forward-curve analog: all day-level and realization-level variance averaged out.
- **Intrinsic value** `V_int` — optimal deterministic dispatch value against the expected curve (LP for BESS; DP-with-commitment for coal). Contains *no* optionality by construction.
- **Total value** `V_tot` — mean over N simulated price paths of per-path optimal dispatch value.
- **Extrinsic value** `V_ext = V_tot − V_int ≥ 0` — the value of price volatility itself (Jensen gap; academy concept `intrinsic_vs_extrinsic_value.md`).

Design property to state in UI: for storage, extrinsic dominates — most of a battery's value is purchased volatility, and the split quantifies exactly how much. This is the investment-relevant insight.

**Third layer — the build/acquisition option.** The asset's option value `V_asset = V_int + V_ext + policy_net` is the *underlying*; the development permit or the M&A ask is an option *on* it (§5.6).

**Wind/solar differ fundamentally from BESS — the model must say so.** A non-dispatchable renewable is *short* volatility through cannibalization: output concentrates in hours it depresses. Its revenue = Σ price_t × volume_t, so extrinsic = the price-volume **covariance term** — typically negative (solar most negative; wind mildly negative, e.g. observed price-wind correlations 蒙西 −0.38 / 山东 −0.13 / 安徽 +0.26 in `dispatch_tab.py:40-47`). Negative extrinsic is a feature to report, never floored to zero. Wind/solar's *positive* optionality comes from: (a) the **curtailment floor** — revenue = max(price, 0) × volume when curtailment is free, a strip of strike-0 calls insuring negative prices （山东 already prints negatives); (b) the policy stack (136号 机制电价 guaranteed-volume premium, green certificates); (c) the build/acquisition option itself; (d) **retrofit optionality** — adding BESS to a wind/solar site converts short-vol into long-vol, and that conversion value is computable in the engine (hybrid asset = wind revenue + BESS dispatch option sharing one grid connection).

Two calibration anchors already in production:

- **Historical PF benchmark** `V_hist` — `marketdata.bess_capture_daily.theoretical_profit_per_mwh_day` summed over the same window (daily perfect-foresight LP). A well-calibrated simulator gives `V_tot ≈ V_hist`; the ratio is the calibration report (§8). Reader note: theoretical rows are duplicated across models in that table — filter to one model (champion `ols_rt_time_v1`) or dedup. V_hist is energy-only (theoretical dispatch runs without compensation), consistent with the energy-only V_tot.
- **Capture rate** — `bess_capture_daily.capture_rate` (champion model `ols_rt_time_v1`, trailing 90 d, repo convention Σrealized/Σtheoretical) applied as haircut to the energy value only → **capture-adjusted value** `V_cap = capture × (V_int + V_ext) + policy_net` (policy streams are contracted/regulated, not forecast-dependent). The honest "what we actually expect to bank" number. Directly links Pillar 2 realized performance into the valuation.

## 3. Goals / non-goals / anti-goals

**Goals (v1):**
1. Rolling-12-month intrinsic value per province × asset type (BESS 2h/4h; wind; solar), computed from `marketdata.spot_prices_hourly` (+ fundamentals for renewable volumes).
2. Extrinsic value via upgraded PCA Monte Carlo: vectorized per-path DP dispatch for BESS; joint price-volume copula simulation with curtailment floor for wind/solar (negative extrinsic reported, never floored).
3. Policy-lever revenue stack as scenario-adjustable inputs, with a **per-asset-type lever registry**: BESS → capacity price 容量电价, capacity compensation 容量补偿, FR 调频, discharge compensation, minus sysopfee 系统运行费; wind/solar → 136号 机制电价 guaranteed-volume premium, green certificates 绿证, sysopfee treatment per provincial rule.
4. Undervaluation screen: total value annuitized vs. capex/ask benchmark → margin ranking across provinces × asset types.
5. **Permit & M&A (build/acquisition option) valuation**: scenario-weighted decision tree over the decision window — policy scenarios with probability + effective date, capex-at-COD with decline rate (permits) or ask price + actuals anchor + turnaround levers (M&A) → GO/NO-GO.
6. Two new Deal Structurer tabs + 3 Structurer agent tools; snapshot table + batch CLI.
7. Calibration report: `V_tot` vs `V_hist` (BESS, vs `bess_capture_daily`); simulated vs empirical capture-rate discount (wind/solar, vs price×volume observations in `spot_fundamentals_hourly`).

**Non-goals (v1):** coal valuation (interface stub only); nodal-granularity option value (provincial v1; nodal path via `md_mengxi_nodal_price_96` later); LSM/realistic-policy valuation (capture haircut is the v1 substitute); forward-curve term structure (rolling-average curve *is* the curve); any web/external data; any trading-execution linkage.

**Anti-goals (failure even if it "works"):** a simulator whose `V_tot` cannot be reconciled to `V_hist` within tolerance; a screen that takes minutes to load; new unit-mix bugs (¥/kWh vs ¥/MWh — the repo has been bitten); anything that writes to production tables from the interactive app path.

## 4. What already exists (reuse map — do not rebuild)

| Need | Existing asset | Location |
|---|---|---|
| LP dispatch (validation oracle) | PuLP/CBC MILP `optimise_window`, hourly + 15-min | `services/bess_map/optimisation_engine.py:42` |
| Daily PF intrinsic series, 29 provinces | `bess_capture_daily` + pipeline | `services/bess_map/run_capture_pipeline.py` |
| Policy lever math + DB loaders | capacity/FR/sysopfee/discharge-comp → ¥/MWh/yr | `services/bess_map/capacity_stack.py` (loaders lines 211–293) |
| Price history reader (unit normalize; whole-column DA fallback only) | `fetch_price_history` | `services/deal_engine/price_data.py:26` |
| PCA simulator v1 | `fit_pca`/`simulate_pca` (eigh + outer-product — **OpenBLAS segfault workaround, keep this pattern**) | `libs/deal_models/price_simulator.py:42-102` |
| Shape-PCA + fundamentals score models (reference) | ridge on PC scores | `services/bess_map/price_lab/pca_shapes.py` |
| Closed-form BESS strip (screen cross-check) | Kirk/Margrabe, intrinsic/time-value split | `libs/decision_models/bess_spread_call_strip.py` |
| B76/Greeks machinery (coal later) | Black-76, SVI | `libs/options/` |
| MC orchestration + tornado | `run_monte_carlo` | `libs/deal_models/monte_carlo.py` |
| Empirical comp rates from settlements | register pattern | `services/deal_committee/economics.py:44-74` |
| Owned-asset realized benchmarks | scenario P&L ladder | `reports.bess_asset_daily_scenario_pnl`, `asset_registry` |
| IRR revenue-stream defaults + hurdle 8% (capex is a UI input, not in this file) | `build_cashflows`, `_irr_defaults_for_province` | `apps/bess-map/irr_helpers.py:48,124` |
| Wind price×volume copula (existing MC dispatch for wind) | `_dispatch_wind` Gaussian copula | `libs/deal_models/dispatch_valuation.py:53-89` |
| Empirical price–wind correlation per province | `fetch_price_wind_correlation` （蒙西 −0.38 / 山东 −0.13 / 安徽 +0.26) | `services/deal_engine/price_data.py:69-163` |
| Renewable hourly volumes (wind_mw/solar_mw) for empirical capture rates | `spot_fundamentals_hourly` (coverage gaps known — Aug 2025+, 23 provinces) | `services/bess_map/run_fundamentals_ingest.py:251-283` |
| 136号 mechanism policy context （机制电价/绿证） | mechanism docs | `knowledge/interconnectors/mechanism_136.md`, `knowledge/policy/` |
| KYOS delta method | finite-difference revalue | `power-academy/labs/dispatch_optimisation_and_delta_hedging/compute.py:97` |

## 5. Architecture

Three new components, same layering rule as deal_models (libs = pure computation, zero I/O; services = DB + orchestration; app = UI):

```
libs/option_value/            # pure math, no I/O
  curves.py                   # expected curve construction
  pca_simulator.py            # PCA MC simulator v2
  dp_dispatch.py              # vectorized per-path DP dispatch (BESS)
  value_stack.py              # intrinsic/extrinsic/policy decomposition + Greeks
  asset_models.py             # AssetOptionModel protocol; BESS/wind/solar impls; coal stub
  build_option.py             # §5.6 permit tree + M&A mode + Dixit-Pindyck cross-check
  tests/
services/option_engine/
  readers.py                  # DB: prices (via deal_engine.price_data), levers (capacity_stack),
                              #     capture rates (bess_capture_daily), asset benchmarks
  snapshot.py                 # marketdata.option_value_snapshot DDL + read/write
  batch.py                    # CLI runner: one province/duration or full screen
  calibration.py              # V_tot vs V_hist report
apps/deal_structurer/
  option_tab.py               # Tab 8 · 期权价值 (single-asset detail)
  screen_tab.py               # Tab 9 · 价值筛选 (undervaluation matrix)
libs/deal_models/adapters/agent_tools.py   # +3 tools (§7)
db/ddl/marketdata/option_value_snapshot.sql
```

**Asset plugin (generalized):** `AssetOptionModel` protocol — `expected_curve(inputs)`, `value_path(path, params) -> revenue` (dispatch DP for BESS; price×volume with curtailment floor for wind/solar), `intrinsic(curve, params)`, `policy_stack(levers, params)`, `applicable_levers: list[str]`. v1 implements `BESSDispatchOption` (2h/4h), `WindRevenueOption`, `SolarRevenueOption`; `CoalDarkSpread` is a documented stub (dark spread = price − coal_price×heat_rate − var_OM from `province_fuel_fleet`; DP state (on/off, uptime), start cost — academy dispatch lab is the method reference). Raises `NotImplementedError` with pointer. Hybrid retrofit (wind+BESS co-location) = composition of the two models sharing a grid-connection cap — v1.5, noted not built.

### 5.1 Expected curve (`curves.py`)

- Input: 12 months hourly RT (unit-normalized to ¥/MWh on read: if series median < 5 → ×1000; **per-hour** RT=0/missing → DA fallback — `fetch_price_history` only switches columns when the *entire* column is zero, so `readers.py` must reimplement the per-hour fallback from `run_capture_pipeline.py:249`; LingFeng zero-fills are a known recurring data issue).
- Month × hour-of-day mean (288 pts) → tiled to 8,760 h. Provinces with < 11.5 months coverage flagged `insufficient_history` and excluded from screen (known gaps: pre-2025-09 old-pipeline gap; 福建 LingFeng stale past 2026-09-15; sub-provincial series per `_RANKING_EXCLUDE`).

### 5.2 PCA simulator v2 (`pca_simulator.py`)

Upgrades over `libs/deal_models/price_simulator.py` (which stays untouched for existing tabs):

1. **Seasonality removed before PCA**: residual = daily profile − month×hour mean curve. PCA on (days × 24) residual matrix.
2. **AR(1) score dynamics per PC** (v1 scores are iid — misses volatility clustering): φ fit by OLS on the daily score series.
3. **Fat-tailed innovations**: Student-t with per-PC fitted df (moment estimator on kurtosis; fall back to empirical bootstrap residuals if df fit unstable). Simulated path = mean curve + Σ_p score_p(t)·loading_p, via the existing eigh + outer-product pattern (Fargate OpenBLAS segfault — do not introduce SVD or big BLAS matmuls; see `docs/handoff-2026-07-25-deal-structurer.md`).
4. **Price bounds**: clip at [floor, cap], defaults = observed rolling-window [min, max], overridable (province price-cap rules are a params input, not hardcoded).
- n_components=4 default; seed fixed per run (reproducibility is a hard requirement — snapshot params record it).
5. **Joint price–volume simulation for wind/solar**: CF paths via Gaussian copula against simulated price residuals (the `_dispatch_wind` pattern), correlation per province from `fetch_price_wind_correlation`, CF shape from `spot_fundamentals_hourly` wind_mw/solar_mw where covered, parametric seasonal hour-of-day shape elsewhere (documented fallback). Curtailment floor applied per hour: revenue = max(price, 0) × volume.

### 5.3 Vectorized DP dispatch (`dp_dispatch.py`) — the engine core

Why not LP-per-path: `optimise_window` MILP ≈ 50–200 ms per 24 h day → 500 paths × 365 d ≈ 3–10 h per province/duration. Too slow for both interactive and nightly batch.

Instead: **backward dynamic programming on an SOC grid, vectorized across all paths with numpy** (academy DP lab pattern, lifted to (paths × steps × soc_states) tensor ops):

- State: SOC on grid (default 0.1·E_max step → 11–41 states); actions: charge/discharge/idle bounded by power; efficiency η_c=η_d=√RTE; terminal SOC free.
- No binary no-simultaneous flag needed (DP never charges and discharges in the same step at η<1 — economically dominated); ramp and degradation caps omitted in v1 (documented; validation gate below bounds the error).
- Cost: 8,760 steps × ~21 states × 1,000 paths vectorized ≈ seconds per province/duration.

**Validation gate (hard test):** DP vs LP `optimise_window` on 30 random historical days × 3 provinces （山东， 蒙西， 广东）: revenue gap must be < 2% (median) — else grid is refined or LP-fast-mode renegotiated. This test is the engine's license to operate.

### 5.4 Value stack + Greeks (`value_stack.py`)

Per (province, asset, duration, window):
- `V_int` = DP dispatch on expected curve (deterministic; DP and LP agree here by gate).
- `V_tot` = mean path revenue; also report P10/P50/P90.
- `V_ext = V_tot − V_int` (assert ≥ −ε).
- **Policy stack** (scenario-adjustable, reported ¥/kW/yr — `capacity_stack` returns ¥/MWh_installed/yr, the conversion is pure unit math): capacity_price, cap_comp, FR (mileage actuals > pool-share > 0 precedence per `capacity_stack.fr_component`, incl. its 500 MW fleet floor), discharge_comp, minus sysopfee cost. Rates/default lever values from the DB loaders; **volume-based levers** (discharge_comp, sysopfee — both take cycles/volume, not just rates) use the option engine's own MC-mean annual discharge volume from the DP paths (self-consistent with V_tot; discharge volume is far more model-stable than revenue; cross-checked against capture-pipeline realized cycles in the calibration report). Scenario = named override set {lever: multiplier|absolute} **plus, for permit valuation, an effective date and a probability** (e.g. "容量电价 doubles from 2027-07, p=0.6") stored in snapshot params. Presets: `current_policy`, `capacity_x1.5`, `fr_pool_halved`, custom.
- `V_total = V_int + V_ext + policy_net`; `V_cap = capture_rate × (V_int+V_ext) + policy_net` (policy streams are not capture-discounted).
- **Greeks** (KYOS price-shock-and-revalue, cheap under vectorized DP): delta = V(curve+10 ¥/MWh) − V(curve); vega = V(score σ ×1.1) − V; theta = V(window −1 month) − V.
- Units: internal ¥/kW/yr; screen also shows ¥/kWh/yr = ÷duration_h for capex comparison.

### 5.5 Undervaluation screen (`screen_tab.py`)

- NPV per kWh energy capacity = Σ_yr V_total/(1+h)^yr − capex_¥kWh − PV(O&M); life 15 yr, hurdle 8% (`_compute_npv` convention), capex 600 ¥/kWh default (bess-map UI default — it is UI text, not in irr_helpers; all user-adjustable). Screen IRR is **unlevered project IRR** from the same simple annuity formula — leverage is deal-specific and pollutes a cross-province ranking; the levered equity view stays in the existing Tab 3 cashflow for single-asset deep-dives.
- Matrix: province × asset type (BESS 2h/4h, wind, solar) rows; columns intrinsic, extrinsic (signed), policy, total, capture-adj, capex/ask, **margin ¥/kWh**, **IRR-uplift**, freshness flag. `IRR_uplift = IRR(V_cap) − IRR(V_int + policy_net)` — how many IRR points the optionality adds over the static-curve case (the committee one-liner; for wind/solar it can be negative — that's the cannibalization story). Sortable; stale/excluded provinces visibly marked, not silently dropped.
- Cross-links: province rows deep-link to bess-map (Pillar 2 detail); 蒙西 rows show owned-asset realized benchmark from `reports.bess_asset_daily_scenario_pnl` (Pillar 3 actuals vs model).

### 5.6 Permit & M&A — the acquisition-option wrapper (`build_option.py` in `libs/option_value/`)

Two decision modes over the same engine output `V_asset`:

**Mode A — greenfield permit (开发指标).** Inputs: permit cost (¥, user-entered — no permit-price table exists; observed asks are manual input), validity window, asset params (province, MW, duration/type), capex-at-COD with annual decline rate (waiting buys cheaper capex as well as information), policy scenario set from §5.4 (probability + effective date each), price-regime drift (default 0 — conservative). Method: scenario-weighted decision tree over the window — at each resolution date the policy state is revealed; exercise (build) iff `NPV_asset(state, τ) > 0`; V_permit = probability-weighted discounted exercise payoffs − permit cost. GO iff V_permit > 0 vs. ask. Cross-check: Dixit-Pindyck perpetual build option (academy `real_options_framework_for_generation_assets/compute.py` — β1, trigger V*, wait multiplier); >30% divergence = review flag.

**Mode B — distressed M&A.** Inputs: ask price, asset params, and — where the asset is in our perimeter (`asset_registry`, settlement/dispatch history, e.g. IM fleet) — its **actuals anchor**: realized revenue/capture vs. province model value (replaces haircut with measured performance). Turnaround levers as scenario rows: trading-strategy uplift (gap between seller's realized capture and champion-model capture is directly measurable from `strategy_experiments`), co-location retrofit (add BESS → hybrid, v1.5 composition noted above; v1 shows the standalone BESS option value at that node as a proxy line). Output: V_asset (actuals-anchored) vs. ask → margin, distress discount vs. replacement capex, GO/NO-GO. External targets without data fall back to the provincial model value with a wider uncertainty band shown.

**Both modes** output the value decomposition by policy scenario — the committee sees *which policy bet* carries the value. **Anti-goal:** no full American-option lattice in v1; policy reveals land on government schedules, so discrete dated scenarios are the honest model.

### 5.7 Persistence (`snapshot.py`)

`marketdata.option_value_snapshot` (DDL `db/ddl/marketdata/option_value_snapshot.sql`, idempotent `CREATE TABLE IF NOT EXISTS`, applied via ensure-pattern like `deal_committee.sql`):

```
province TEXT, asset_type TEXT, duration_h DOUBLE, asof_date DATE,
window_start DATE, window_end DATE,
intrinsic_yuan_kw DOUBLE, extrinsic_yuan_kw DOUBLE, total_yuan_kw DOUBLE,
capture_adj_yuan_kw DOUBLE, policy_json JSONB, greeks_json JSONB,
mc_params JSONB (model, n_paths, seed, n_components, clip bounds, git sha),
created_at TIMESTAMPTZ DEFAULT NOW(),
PK (province, asset_type, duration_h, asof_date)
```

Only `batch.py` writes. App tabs read latest per key. Numpy paths never persisted (deal_committee lesson).

### 5.8 Batch CLI (`batch.py`)

`python services/option_engine/run_option_batch.py --province 山东 --duration 2 --paths 1000` (single) or `--screen` (29 provinces × 2 durations, fast mode: 200 paths). Full refresh ≈ minutes under vectorized DP. ECS scheduling deferred (nodal-writer terraform pattern when needed); manual/cron first.

## 6. App surface (deal_structurer v-next)

- **Tab 8 · 期权价值 (Option Value)**: province/**asset-type (BESS 2h/4h, wind, solar)**/window pickers → expected-curve chart, simulated-path fan (P10/50/90), value waterfall (intrinsic → +extrinsic → +policy stack → total → capture-adj; **extrinsic shown red when negative** for wind/solar cannibalization), Greeks row, policy-lever sliders (scenario preset dropdown with probability + effective-date per scenario; lever list filtered by asset type), calibration line (V_tot vs V_hist for BESS; simulated vs empirical capture discount for wind/solar). **Acquisition panel** with mode toggle: permit mode (permit cost, validity, capex-at-COD + decline) / M&A mode (ask price, actuals anchor where available, turnaround levers) → V_option, breakeven price, GO/NO-GO, value decomposition by policy scenario (§5.6).
- **Tab 9 · 价值筛选 (Value Screen)**: the matrix of §5.5 across provinces × asset types + margin ranking bar chart. Permit-relevant sort: ascending intrinsic vs. (extrinsic + policy-upside) ratio — permits are cheapest precisely where intrinsic is low but optionality is high; that ordering is the hunt list. M&A mode overlay: rows annotated with known distressed-asking context where a deal brief exists in `deal_briefs` (linkage to Tab 0/6 flow).
- Tab numbering continues existing convention (`app.py:41-48` nav list); 💬 Structurer unchanged otherwise.
- **Agent tools (+3)** in `libs/deal_models/adapters/agent_tools.py`: `get_option_value(province, asset_type, duration_h)` (latest snapshot + on-demand fast recompute), `screen_option_values(sort_by, min_freshness)`, `get_policy_levers(province)`. Same grounding-rule system prompt; tool results cite `option_value_snapshot` asof_date.
- Portal card copy update only (no Quick Ask change).

## 7. Linkage to existing apps (read-only, no cross-writes)

| App | What option engine reads | For |
|---|---|---|
| bess-map (P2) | `bess_capture_daily` (V_hist, capture), `capacity_stack` levers, IRR defaults | calibration, haircut, policy, capex |
| mengxi-dashboard (P3) | `reports.bess_asset_daily_scenario_pnl`, `asset_registry` | owned-asset realized benchmark in Tab 8/9 |
| asset-risk / retail-risk | `rm_settlement_items` empirical comp rates (economics.py:36-74 pattern) | policy default verification |
| spot-market (P1) | nothing in v1 | (P4: fundamentals-informed PCA score models via price_lab ridge) |
| portal | — | card copy |

## 8. Verification

**Unit (pytest, `libs/option_value/tests/` + `tests/option_engine/`):**
- Curve: month×hour means exact on synthetic series; kWh-series normalized ×1000.
- Simulator: simulated 12m moments (mean, std, daily-spread distribution) match history within tolerance; seed → identical paths.
- **DP-vs-LP gate** (§5.3): < 2% median gap on 30 days × 3 provinces.
- Jensen: V_ext ≥ −1e-6 on all provinces in fast screen.
- Closed-form cross-check: `bess_spread_call_strip` value within 15% of fast-MC V_tot for 1-cycle-constrained BESS (different models, same economics — flags calibration bugs).
- Policy: each lever's sign and magnitude vs `capacity_stack` hand-computed province （山东）.
- Snapshot round-trip; ensure-DDL idempotent.
- Permit tree: deterministic 2-state fixture reproduces hand-computed V_permit; DP perpetual cross-check matches academy lab numbers on the lab's own parameters (`real_options_framework_for_generation_assets/compute.py`); GO iff V_permit > 0; probability weights must sum ≤ 1 (validated input).
- Wind/solar: simulated capture-rate discount matches empirical (Σ price×volume / Σvolume vs mean price, from `spot_fundamentals_hourly`) within tolerance per covered province; **solar extrinsic is negative in cannibalization provinces (assert sign, never floored)**; curtailment-floor contribution ≥ 0 and material in 山东 (negative-price hours); M&A mode with actuals anchor reproduces the owned-asset's realized revenue band as sanity.

**Integration:** 山东 BESS 2h end-to-end batch → snapshot → Tab 8 render locally (`streamlit run apps/deal_structurer/app.py --server.port 8522`, AUTH_MODE n/a — app has no Cognito locally); screen tab loads < 60 s cold (fast mode pre-computed by batch).

**Calibration report** (deliverable of P1): table of V_tot/V_hist per province; provinces outside [0.7, 1.4] listed with cause (data gap vs model miss).

## 9. Build order

1. `libs/option_value/` contracts + curves + tests
2. pca_simulator v2 + moment tests
3. dp_dispatch (BESS) + **DP/LP gate**
4. wind/solar value models (copula + curtailment floor) + calibration tests
5. value_stack + Greeks + per-asset-type policy registry + tests
6. build_option (permit tree + M&A mode + DP cross-check) + tests
7. `services/option_engine/` readers + DDL + batch CLI + calibration report (run on real data, review numbers before UI)
8. Tabs 8/9 (incl. acquisition panel) + 3 agent tools, local run verification
9. Commit per unit; deploy only on explicit confirmation (standard protocol: read LATEST-IMAGES.md, next tag, --platform linux/amd64, jq-swap from current tdArn — deal-structurer deploys bypass terraform)

## 10. Explicitly deferred (recorded so they aren't silently dropped)

Coal DP valuation (interface ready); LSM realistic-policy (academy `monte_carlo_and_lsm` lab + syllabus `lsmc_for_dispatch` spec the method); nodal option value via `md_mengxi_nodal_price_96`; Sobol tornado (spec'd 2026-07-09, still unbuilt); scenario comparison tab (spec'd, never wired); `scenario_store.py` is dead code — leave or remove in a separate cleanup, not this build.

## 11. Key calls made in design (review these)

1. **Intrinsic = dispatch on the averaged curve**, not on replayed history — this is what makes extrinsic = pure volatility value (Jensen-clean). `V_hist` is kept as calibration anchor, not as intrinsic.
2. **New vectorized DP engine** instead of LP-per-path — the only way MC option value is computable at fleet scale; gated against the production LP.
3. **New `libs/option_value/` + upgraded simulator** rather than editing `libs/deal_models/price_simulator.py` in place — existing tabs 1–5 keep their behavior; option engine is additive.
4. **Capture rate as haircut** instead of LSM policy in v1 — reuses the Pillar 2 champion model's measured performance; honest and already trusted.
5. **Policy levers enter as ¥/kW/yr stack**, scenario-adjustable, sourced from the production `capacity_stack` loaders — no parallel policy data model.
6. **Provincial v1, nodal deferred** — nodal 15-min data only covers 蒙西/山东 well; provincial screen is the investment question.
7. **Screen IRR is unlevered; IRR-uplift = IRR(V_cap) − IRR(V_int+policy_net)** — leverage is deal-specific and pollutes a province ranking; uplift isolates what optionality adds to returns. (Settled at builder check; flag if you want levered instead.)
8. **Volume-based policy levers use the engine's own MC-mean discharge volume**, not capture-pipeline realized cycles — self-consistent with V_tot; realized cycles are the calibration cross-check. (Settled at builder check.)
9. **Permit valuation = scenario decision tree, not an American lattice** — policy reveals land on government schedules, so discrete dated scenarios with probabilities are the honest model; Dixit-Pindyck perpetual closed form is a cross-check only. Price-regime drift default 0 (conservative; upside is the policy bet, not assumed market kindness).
10. **Wind/solar extrinsic may be negative (short-vol cannibalization) — reported with sign, never floored.** Their positive optionality = curtailment floor + policy + build option + retrofit. This is the honest economics and the screen's key differentiator vs. naive "renewables NPV" models.
11. **M&A = same engine, different wrapper** — no separate valuation stack; actuals anchor (where the asset is in our data perimeter) replaces the model haircut; turnaround levers are scenario rows, not a parallel model.
