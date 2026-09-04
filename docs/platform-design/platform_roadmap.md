# Platform Build Roadmap
*Refined version — organized by platform layer, not just by build sequence*

---

## Platform Architecture: Four Layers

```
┌─────────────────────────────────────────────────────────────────┐
│  LAYER 4: AGENT LAYER                                           │
│  Autonomous reasoning loops; natural language I/O; L1–L4        │
│  AS1–AS8; daily orchestrator; decision handoff interface        │
├─────────────────────────────────────────────────────────────────┤
│  LAYER 3: ASSET OPTIMIZATION LAYER                              │
│  Physical asset valuation; embedded optionality; dispatch       │
│  OPTIONALITY_DECOMP; BESS/tolling models; optimal exercise      │
├─────────────────────────────────────────────────────────────────┤
│  LAYER 2: RISK LAYER                                            │
│  Attribution; monitoring; hedge recommendation; stress          │
│  PNL_ATTRIBUTION; REALIZED_IMPLIED_MONITOR; SIGNAL_RADAR;       │
│  SCENARIO_ENGINE; HEDGE_RECOMMENDATION; POSITION_LIFECYCLE       │
├─────────────────────────────────────────────────────────────────┤
│  LAYER 1: CORE INTELLIGENCE LAYER                               │
│  Pricing; surfaces; Greeks; workflow                            │
│  VOL_SURFACE; PRICING_ENGINE; RISK_DECOMPOSITION;               │
│  WORKFLOW_ENGINE                                                │
└─────────────────────────────────────────────────────────────────┘
```

**Dependency rule:** Each layer depends on all layers below it. Layer 1 has no dependencies — it operates on raw market data. No higher layer can function correctly without Layer 1. The agent layer (Layer 4) orchestrates all lower layers.

**Build rule:** Build bottom-up. Each layer delivers standalone value while enabling the next.

---

## Layer 1: Core Intelligence Layer

**Purpose:** Provide accurate, consistent, arbitrage-free pricing and risk computation as a service to all other layers. This layer is entirely deterministic — no judgment, no recommendations.

**Modules:**

| Module | Core function | Universal? | Build effort |
|---|---|---|---|
| `VOL_SURFACE` | Calibrate no-arbitrage IV surface from quotes | Yes | Medium (code exists; data pipeline needed) |
| `PRICING_ENGINE` | Price vanilla and spread options with full Greeks | Yes | Low (code exists; needs API wrapping) |
| `RISK_DECOMPOSITION` | Aggregate Greeks across heterogeneous book | Yes | Low–Medium (code exists; needs generalization) |
| `WORKFLOW_ENGINE` | Orchestrate daily ops loop; manage data + state | Yes | Medium (code exists; needs productionization) |

**Key architectural decisions at this layer:**

1. **Market content is configuration, not code.** Forward curve, contract specs, data sources, correlation assumptions — all are parameters, not hardcoded. The engines are market-agnostic; a new market requires a data adapter and a contract registry, not a new engine.

2. **Vol surface is the pricing backbone.** All downstream valuations must query `VOL_SURFACE` rather than using ad-hoc vol inputs. This ensures consistency across all Greeks, attribution, and scenario computations.

3. **Position state is versioned.** Every end-of-day snapshot is preserved. This enables attribution (requires T−1 state), backtesting (requires historical states), and audit (requires every state ever).

**Minimum viable Layer 1:**
- `PRICING_ENGINE`: Kirk + Margrabe + Black-76 — fully implemented in archive, needs wrapping
- `RISK_DECOMPOSITION`: `Book_Management_v1.py` logic, generalized input interface
- `VOL_SURFACE`: SVI calibration from `SVI.py` + `Vol_Calib.py`, connected to any vol quote source
- `WORKFLOW_ENGINE`: trade CSV ingestion + daily snapshot file management (simple version in hours)

**What Layer 1 unlocks immediately:** Consistent derivatives pricing, daily Greek computation, and position state management — enough to run a derivatives book analytically without depending on a third-party risk system.

---

## Layer 2: Risk Layer

**Purpose:** Transform raw Greeks and market data into actionable risk intelligence: explain P&L, monitor for deteriorating conditions, stress-test positions, and recommend hedges.

**Modules:**

| Module | Core function | Depends on | Universal? | Build effort |
|---|---|---|---|---|
| `PNL_ATTRIBUTION` | Decompose P&L into causal factors | L1 | Yes | Low (code exists in `BookManagement4.py`) |
| `SCENARIO_ENGINE` | Revalue book under price/vol/time grid | L1 | Yes | Low–Medium |
| `REALIZED_IMPLIED_MONITOR` | Track RVol vs IVol divergence; breakeven distance | L1 + price history | Yes | Low |
| `SIGNAL_RADAR` | Monitor upstream market vol signals | L1 + multi-market data | Energy/universal | Medium |
| `HEDGE_RECOMMENDATION` | Prescribe hedges under instrument constraints | L1 + L2 | Yes | Medium–High |

**Key architectural decisions at this layer:**

1. **Attribution is the anchor for all risk communication.** Every risk module should be able to express its finding in terms of the attribution components: "this is a delta problem", "this is a vega problem". This creates a consistent language across the platform.

2. **Monitoring modules fire continuously; decision-support modules fire on-demand.** `REALIZED_IMPLIED_MONITOR` and `SIGNAL_RADAR` run on every price update. `SCENARIO_ENGINE` and `PNL_ATTRIBUTION` run at configurable intervals or on user request.

3. **Hedge recommendation explicitly handles incomplete instrument sets.** The most dangerous recommendation is one that assumes a perfect hedge exists when it doesn't. The module must always output residual risk alongside the recommendation.

**Minimum viable Layer 2:**
- `PNL_ATTRIBUTION`: port `BreakDown()` function, add attribution sentence template
- `REALIZED_IMPLIED_MONITOR`: rolling RVol + ATM IV comparison + breakeven distance — trivial to build
- `SCENARIO_ENGINE`: extend `Scenarios()` price sweep to include vol dimension
- `SIGNAL_RADAR`: start with manually collected upstream IV data; automate scraping later

**What Layer 2 unlocks:** Daily risk narrative, realized vs implied surveillance, and scenario analysis — enough to replace manual Excel-based risk reporting and provide management with causal risk narratives.

---

## Layer 3: Asset Optimization Layer

**Purpose:** Value and manage the embedded optionality in physical assets and structured contracts; translate physical dispatch decisions into financial Greeks; optimize dispatch and hedge strategies jointly.

**Modules:**

| Module | Core function | Depends on | Market scope | Build effort |
|---|---|---|---|---|
| `OPTIONALITY_DECOMP` | Decompose physical assets into equivalent options | L1 + L2 | Energy/asset-backed | Medium–High |
| Asset-specific models | BESS strip model; thermal spark model; PPA decomp | L1 + `OPTIONALITY_DECOMP` | Asset-backed | Medium per asset type |
| Dispatch optimizer | Optimal exercise policy given Greeks and constraints | L1 + L2 + L3 | Asset-backed | High |

**Key architectural decisions at this layer:**

1. **Each asset type has a canonical decomposition.** Once defined, it is reused. Adding a new asset type requires defining its decomposition rule — not rebuilding the pricing layer.

2. **Physical constraints enter as option modifications, not as separate systems.** Ramp constraints, minimum run, degradation, and contract minimums all modify the effective strike or available volume of the equivalent option. They do not require a separate dispatch model.

3. **The financial Greeks of a physical asset are the bridge to the risk layer.** Once a BESS is expressed as a strip of spread calls, its Greeks feed directly into `RISK_DECOMPOSITION` and `PNL_ATTRIBUTION` — the same system used for financial derivatives positions.

**Canonical decompositions:**

| Asset type | Equivalent option structure | Key parameters |
|---|---|---|
| BESS (1 cycle/day) | Strip of N daily spread calls | K = (1/η − 1)×P_charge + O&M; vol = spread vol; N = cycle life |
| Thermal plant | Strip of daily spark spread calls | K = heat_rate × gas + carbon_cost + VOM |
| Tolling agreement | Same as thermal (toller holds the calls) | Same parameters; toller is long, capacity provider is short |
| PPA with floor | Short put strip at floor price | K = floor price; vol = power vol at each expiry |
| PPA collar | Short put + long call (or vice versa) | Two strikes; net premium determines economics |
| Swing/take-or-pay | Compound option (right to vary volume) | Minimum and maximum quantity bounds |
| Cross-border arbitrage | Exchange option between two node prices | K = transmission cost; ρ = node price correlation |

**Minimum viable Layer 3:**
- BESS spread call strip model: 20–30 lines of code on top of `PRICING_ENGINE`
- PPA floor/cap decomposition: Black-76 strip on `PRICING_ENGINE`
- These two cover the majority of current asset portfolio value

**What Layer 3 unlocks:** Investment-grade valuation of physical assets (replacing DCF guesses), consistent hedging of physical and financial exposure, and structured product pricing grounded in the same surface used for financial derivatives.

---

## Layer 4: Agent Layer

**Purpose:** Provide autonomous reasoning loops that monitor, explain, recommend, and (with appropriate permissions) execute — operating on all lower layers and surfacing outputs to humans in natural language.

**Agent skills:**

| Agent skill | Trigger | L-level | Primary modules |
|---|---|---|---|
| AS1: P&L Causal Explainer | Daily close | L1 | `PNL_ATTRIBUTION` |
| AS2: Divergence Detector | Price update / daily | L1–L2 | `REALIZED_IMPLIED_MONITOR` |
| AS3: Vol Surface Integrity | Post-calibration | L1 | `VOL_SURFACE` |
| AS4: Short-Vol Fragility Monitor | Daily / on price move | L1–L2 | `RISK_DECOMPOSITION`, `REALIZED_IMPLIED_MONITOR` |
| AS5: Cross-Market Signal Alert | Daily data refresh | L1 | `SIGNAL_RADAR` |
| AS6: Hedge Adjustment Recommender | Trigger breach / on-demand | L2–L3 | `HEDGE_RECOMMENDATION` |
| AS7: Embedded Optionality Identifier | Asset input / revaluation | L1–L2 | `OPTIONALITY_DECOMP` |
| AS8: Daily Operations Orchestrator | Market open | L4 | All modules |

**Key architectural decisions at this layer:**

1. **Agents are thin wrappers over modules.** The intelligence is in the modules (deterministic computation). The agent layer adds: triggering logic, output formatting, natural language generation, and routing to the appropriate human.

2. **Natural language outputs are template-driven first, LLM-enhanced later.** Start with structured templates: "P&L was X. Of that: Y came from Z." Upgrade to LLM-generated narrative in Phase 3. This keeps the agent reliable from day one.

3. **Permission levels are explicit and configurable per agent per context.** A hedge recommendation agent on a live book must be L2 (human approves). The same agent on a backtesting simulation can be L4. Permission levels are not hardcoded — they are configuration.

4. **AS8 (Daily Orchestrator) is the primary user interface.** In the fully deployed platform, most users interact with the orchestrator, not with individual modules. It compiles everything into a single daily briefing with ranked next-action items.

**Minimum viable Layer 4:**
- AS1 (P&L Explainer): template-driven, requires only `PNL_ATTRIBUTION` output → buildable immediately
- AS3 (Vol Integrity): simple threshold check on surface diagnostics → buildable immediately
- AS2 (Divergence): rule-based alert on realization ratio and breakeven distance → buildable immediately
- AS8 (Orchestrator): scheduled script calling modules in sequence, collecting outputs, generating report → buildable immediately without LLM

---

## Build Sequence

### Phase 1: Core Intelligence Layer — Functional pricing and daily risk

**Goal:** Platform can price any option or spread consistently, compute daily Greeks, and produce a causal P&L narrative.

**Build list (in order):**

| Step | Deliverable | Effort | Code source |
|---|---|---|---|
| 1.1 | `PRICING_ENGINE` API: Kirk + Margrabe + Black-76, full Greeks | 1–2 days | Port `Kirk.py`, `Margrabe2Assets.py`, `VanillaOption.py` |
| 1.2 | `RISK_DECOMPOSITION` service: book input → Greeks table | 2–3 days | Port `Book_Management_v1.py` `AggregatedTrades()` + `Scenarios()` |
| 1.3 | `VOL_SURFACE` service: quote input → calibrated SVI + query API | 3–5 days | Port `SVI.py`, `Vol_Calib.py`; build data adapter |
| 1.4 | `PNL_ATTRIBUTION` service: two-snapshot input → attribution table | 1–2 days | Port `BookManagement4.py` `BreakDown()` |
| 1.5 | `WORKFLOW_ENGINE` v1: CSV trade book + daily snapshot management | 2–3 days | Port `StartOfDay.py`, `Trade_Recorder_Func.py` |
| 1.6 | AS1 P&L Explainer: template-driven narrative over attribution table | 1 day | New: output template + trigger logic |
| 1.7 | AS3 Vol Integrity checker: surface diagnostic alerts | 1 day | New: threshold logic |

**Phase 1 timeline estimate:** 2–3 weeks for a working system  
**Phase 1 output:** Consistent derivatives pricing, daily Greek computation, daily P&L narrative. Replaces manual Excel-based risk tracking.

---

### Phase 2: Risk Layer — Monitoring, stress, and hedge recommendations

**Goal:** Platform monitors conditions continuously, runs stress scenarios, and recommends hedges.

**Build list:**

| Step | Deliverable | Effort | Code source |
|---|---|---|---|
| 2.1 | `REALIZED_IMPLIED_MONITOR`: rolling RVol, alert thresholds, breakeven distance | 1–2 days | Port from `Dynamic_Hedging_Simulator_v1.py` logic + `Breakeven_Cal19_*.csv` pattern |
| 2.2 | `SCENARIO_ENGINE`: price × vol grid; historical stress scenarios | 3–4 days | Extend `Scenarios()` with vol dimension; add stress scenario loader |
| 2.3 | `SIGNAL_RADAR` v1: upstream IV data + divergence alerts | 3–5 days | Port `ImpliedVol_Scraper_*` scrapers; build cross-market correlation; new market data adapters |
| 2.4 | `HEDGE_RECOMMENDATION` v1: delta-only (futures), simple optimization | 2–3 days | Port delta hedge logic from `Trade_Recorder_Editor.py`; add residual reporting |
| 2.5 | AS2 Divergence Detector: alert narrative over monitor output | 1 day | New: alert threshold logic + narrative template |
| 2.6 | AS4 Fragility Monitor: gamma profile vs price; proximity scoring | 2 days | New: requires Greeks scenario profile from `RISK_DECOMPOSITION` |
| 2.7 | AS5 Cross-Market Signal: regression alert over `SIGNAL_RADAR` | 2–3 days | New: simple linear regression model on historical cross-IV data |
| 2.8 | AS8 Daily Orchestrator v1: scheduled run, alert queue, daily briefing | 3–4 days | New: orchestration script; email/notification output |

**Phase 2 output:** Fully automated daily risk cycle with continuous monitoring, alerts, hedge guidance, and management-ready reporting narrative.

---

### Phase 3: Asset Optimization and Agent Intelligence Layer

**Goal:** Decompose and value physical asset optionality; upgrade agents to use LLM-based narrative generation and learned signals.

**Build list:**

| Step | Deliverable | Effort | Notes |
|---|---|---|---|
| 3.1 | `OPTIONALITY_DECOMP`: BESS strip model; PPA floor/cap | 3–5 days | New: decomposition rules + `PRICING_ENGINE` calls |
| 3.2 | `OPTIONALITY_DECOMP`: thermal/tolling spark spread model | 2–3 days | Extension of 3.1 |
| 3.3 | `HEDGE_RECOMMENDATION` v2: multi-Greek optimization; imperfect instruments | 5–7 days | Extends Phase 2 delta-only version |
| 3.4 | AS7 Embedded Optionality Identifier: asset spec → decomposition → hedge | 3–4 days | Wraps `OPTIONALITY_DECOMP` |
| 3.5 | AS6 Hedge Recommendation Agent: execution-ready L2/L3 | 2–3 days | Wraps `HEDGE_RECOMMENDATION` with confirmation interface |
| 3.6 | LLM narrative layer: replace templates with structured LLM prompts | 3–5 days | Claude API integration; structured output schema |
| 3.7 | `SIGNAL_RADAR` v2: learned cross-market model (HMM or regression ensemble) | 5–10 days | Requires historical IV dataset; offline model training |
| 3.8 | Vol regime classifier: classify current vol regime; condition strategy on regime | 5–10 days | HMM or clustering on historical IV data |
| 3.9 | AS8 Orchestrator v2: full agentic loop with LLM reasoning and tool use | 5–7 days | Upgrade from scripted to agentic; Claude API with tools |

**Phase 3 output:** A platform that autonomously runs the full daily risk cycle, values physical assets correctly, identifies embedded optionality, and communicates findings at every level of the organization in natural language.

---

## Capability Ranking Summary

### By transferability (market-agnostic score)

| Rank | Capability | Reasoning |
|---|---|---|
| 1 | `PNL_ATTRIBUTION` + AS1 | Identical logic in any derivatives market |
| 2 | `PRICING_ENGINE` (vanilla + spread) | Two-forward payoff is universal |
| 3 | `RISK_DECOMPOSITION` | Greek aggregation is market-agnostic |
| 4 | `REALIZED_IMPLIED_MONITOR` + AS2 | RVol vs IVol comparison is universal |
| 5 | `VOL_SURFACE` | Works wherever options are quoted |
| 6 | `SCENARIO_ENGINE` | Scenario logic is market-agnostic |
| 7 | `HEDGE_RECOMMENDATION` | Imperfect-instrument framework is universal |
| 8 | `SIGNAL_RADAR` | Causal chain is market-specific; detection logic is universal |
| 9 | `OPTIONALITY_DECOMP` | High-value in energy; lower outside physical assets |
| 10 | `WORKFLOW_ENGINE` | Infrastructure layer; universal but not differentiating |

### By commercial value

| Rank | Capability | Reasoning |
|---|---|---|
| 1 | `OPTIONALITY_DECOMP` + AS7 | Prices what competitors guess; direct investment/structuring advantage |
| 2 | `VOL_SURFACE` | Enables all consistent pricing; without it, all derivatives marks are suspect |
| 3 | `PRICING_ENGINE` (spread) | Core valuation for BESS, tolling, spark — direct revenue-enabling |
| 4 | `PNL_ATTRIBUTION` + AS1 | Governance value; prevents P&L surprises; narrative for management |
| 5 | `REALIZED_IMPLIED_MONITOR` | Early warning; prevents large losses |
| 6 | `HEDGE_RECOMMENDATION` | Reduces hedge cost; makes rationale auditable |
| 7 | `SIGNAL_RADAR` | Informational edge in trading and cross-commodity structuring |
| 8 | `SCENARIO_ENGINE` | Required for governance; raises counterparty confidence |

### By ease of initial build

| Rank | Capability | Reasoning |
|---|---|---|
| 1 | `PRICING_ENGINE` | Code fully implemented in archive; needs wrapping |
| 2 | `RISK_DECOMPOSITION` | Code implemented; needs interface generalization |
| 3 | `PNL_ATTRIBUTION` | `BreakDown()` function is complete; needs productionization |
| 4 | `REALIZED_IMPLIED_MONITOR` | Simple time-series computation; 1–2 days |
| 5 | AS1, AS2, AS3 | Template-driven agents over existing module outputs |
| 6 | `VOL_SURFACE` | Code complete; data pipeline is the effort |
| 7 | `SCENARIO_ENGINE` | Extends existing `Scenarios()` function |
| 8 | `OPTIONALITY_DECOMP` | Conceptual framework clear; asset parsers are new work |
| 9 | `SIGNAL_RADAR` | Scrapers exist for Europe; new market adapters needed |
| 10 | `HEDGE_RECOMMENDATION` v2 | Multi-Greek optimization is the most complex new build |

### By strategic differentiation

| Rank | Capability | Reasoning |
|---|---|---|
| 1 | `OPTIONALITY_DECOMP` | Systematic advantage in asset valuation where competitors use DCF |
| 2 | `PNL_ATTRIBUTION` + AS1 | Most platforms show numbers; none explain them causally, automatically |
| 3 | `REALIZED_IMPLIED_MONITOR` | Proactive risk surveillance vs reactive loss discovery |
| 4 | `VOL_SURFACE` (no-arbitrage) | Rigorous surface vs flat vol or interpolated approximation |
| 5 | AS8 Daily Orchestrator | Fully automated risk cycle replaces manual ops; operational moat |
| 6 | `SIGNAL_RADAR` | Cross-commodity informational edge before it appears in target market |
| 7 | `HEDGE_RECOMMENDATION` (imperfect instruments) | Explicit residual risk quantification — rare in practice |

---

## Europe-Specific vs Cross-Market Content

| Component | Europe-specific content | Cross-market reusable logic |
|---|---|---|
| `VOL_SURFACE` data pipeline | Aligne, Spectron, EEX quotes | SVI calibration algorithm; arbitrage removal logic |
| `PRICING_ENGINE` | EEX contract specs (8760h/year, EUR/MWh) | Black-76, Kirk, Margrabe formulas |
| `SIGNAL_RADAR` causal chain | EUA carbon + TTF gas + API2 coal → DE power | Lead-lag detection; divergence alert framework |
| `OPTIONALITY_DECOMP` | CCGT spark spread parameters | Strip of spread calls decomposition |
| `RISK_DECOMPOSITION` | Cal19/Cal20 correlation (0.85) | Greek aggregation; correlation-adjusted portfolio |
| `WORKFLOW_ENGINE` | Aligne scraper, Reuters CPD | Data normalization; snapshot management; orchestration |

**Rule for new markets:** Replace the data adapter and causal chain parameters. Keep all engine logic.

**For China specifically:**
- `VOL_SURFACE`: use DCE coal options, CBEEX carbon quotes → same SVI algorithm
- `SIGNAL_RADAR`: replace gas/coal/EUA with DCE coal + CBEEX carbon + LNG → same detection logic
- `OPTIONALITY_DECOMP`: BESS in provincial day-ahead markets (Guangdong, Inner Mongolia) → same strip model with local peak-offpeak spread curves
- `PRICING_ENGINE`: unit change (¥/MWh), calendar change (Chinese holiday calendar) → parameter update only
