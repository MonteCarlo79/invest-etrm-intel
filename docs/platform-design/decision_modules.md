# Platform Decision Modules
*Refined version — merged, typed, and classified for system building*

---

## Module Architecture

Modules are organized by engine type. Each module has a single responsibility, a defined interface, and a known position in the dependency graph. Overlapping concerns from the prior version have been merged.

```
ANALYTICAL ENGINES          DECISION-SUPPORT ENGINES
  VOL_SURFACE ────────────┐     PNL_ATTRIBUTION
  PRICING_ENGINE ─────────┼───► SCENARIO_ENGINE
  RISK_DECOMPOSITION ─────┘
                                MONITORING ENGINES
                                  REALIZED_IMPLIED_MONITOR
                                  SIGNAL_RADAR
                                  POSITION_LIFECYCLE

                                RECOMMENDATION ENGINES
                                  HEDGE_RECOMMENDATION
                                  OPTIONALITY_DECOMP

                                EXECUTION-SUPPORT ENGINE
                                  WORKFLOW_ENGINE
```

**Dependency rule:** Analytical engines have no upstream module dependencies. All other engine types consume analytical engine outputs. Monitoring and recommendation engines may also consume decision-support outputs. The workflow engine orchestrates all others.

---

## Module Scope Classification

Each module is tagged:
- `[U]` Universal — logic is market-agnostic; works with any instrument set
- `[E]` Energy/power specific — logic holds broadly but calibration and data are energy-market-oriented
- `[A]` Asset-backed — especially useful for systems with physical assets or structured physical contracts

---

## M1: Vol Surface Engine `[U][E]`

**Engine type:** Analytical
**Purpose:** Construct and serve a no-arbitrage implied vol surface from sparse option quotes. Acts as the pricing backbone for all downstream valuation, risk, and structuring modules.

**Inputs:**
- Option quotes: strike, expiry, mid price or mid vol, type (call/put)
- Forward/futures curve for the underlying (per expiry)
- Risk-free rate; carry cost if applicable
- Prior-day surface parameters (warm-start)

**Outputs:**
- Calibrated SVI or spline parameters per expiry slice
- Query interface: IV(K, T), local vol(K, T), risk-neutral density(K, T)
- Delta-to-strike and strike-to-delta mappings
- Arbitrage diagnostics: butterfly violation magnitude per slice, calendar violation per slice pair
- Surface diff vs prior day: ATM vol change, slope change, curvature change per slice

**Core logic:**
1. Convert prices to IVs: bisection on Black-76, bounds [5%, 60%]
2. Clean: remove out-of-bounds IVs
3. Per slice: initialize SVI from five-interval piecewise regression
4. Per slice: least-squares fit on IVs → initial parameters
5. Iterative: refit with penalty terms on butterfly + calendar arbitrage violations; double penalties until residuals below threshold (default 0.001)
6. Export: parametric form + flat query API

**Typical users:** Quant (build), Trader (query), Structurer (query), Risk manager (audit), Asset optimizer (query)
**Mode:** Batch (daily, at market open) + Interactive (on-demand query at any point)
**Type:** Descriptive ("what is the market pricing?") + Diagnostic ("is the surface consistent?")

**Commercial value:** Without an arbitrage-free surface, all option pricing produces inconsistent marks across strikes — mispricing risk in structured products, model risk in risk management, and unreliable Greeks. This is the pricing foundation.

**Minimum data to build now:**
- At minimum: 5+ option quotes per expiry (call + put at 3–5 strikes, 1–2 expiries)
- Degrades gracefully: with fewer quotes, switch to cubic spline or flat vol; SVI requires at least 5 points per slice to be well-conditioned
- Can start with historical vol estimates and flat surface; upgrade to calibrated SVI as quote data pipeline is built

**AI agent path:** Agent can run surface calibration autonomously at market open, detect arbitrage violations, and alert: *"Butterfly arbitrage detected in the Q3 slice — check the 45-strike quote before pricing."*

---

## M2: Derivatives Pricing Engine `[U][E][A]`

**Engine type:** Analytical
**Purpose:** Price vanilla and spread options consistently; compute full per-leg Greeks. Serves as the valuation layer for all risk, attribution, structuring, and asset optimization modules.

*Merges prior modules: Spread/Exchange Option Valuation Engine + vanilla pricing aspects of Vol Surface Engine.*

**Inputs:**
- Instrument specification: type (vanilla call/put, spread call/put, exchange option), legs, strikes, weights, expiry, volume/notional
- Forward prices F1, F2 (one for vanilla, two for spread/exchange)
- Vol inputs: σ1, σ2 from `VOL_SURFACE` query or direct input; correlation ρ12
- Risk-free rate r; carry cost b

**Outputs (per instrument):**
- Price, intrinsic value, time value, moneyness
- Greeks: delta1, delta2, gamma1, gamma2, vega1 (∂P/∂σ1), vega2 (∂P/∂σ2), ρ-vega (∂P/∂ρ), theta
- Breakeven forward level(s)
- Scenario table: price × vol grid of values and Greeks

**Core logic:**
- Vanilla: Black-76 via QuantLib with surface-interpolated vol; Greeks from QuantLib engine
- Spread (K≠0): Kirk approximation — treats (F2+K) as quasi-numeraire, reduces to Black-76 on adjusted forward ratio
- Exchange (K=0): Margrabe — vol = √(σ1² + σ2² − 2ρσ1σ2), standard normal CDF pricing
- All Greeks via numerical finite difference: dF = 0.01, dσ = 0.01, dT = 1/365
- Weighted multi-leg: sum price and Greeks across legs with signs and weights

**Typical users:** Structurer, Quant, Trader, Asset optimizer
**Mode:** Interactive (real-time pricing) + Batch (book mark)
**Type:** Descriptive (valuation)

**Commercial value:** Direct revenue-enabling: correctly prices what competitors guess. In BESS, tolling, PPA structuring, and spread trading, pricing accuracy is margin.

**Minimum data to build now:**
- Forward price (1 number per leg per expiry)
- Vol estimate (historical, quoted, or surface-derived)
- Correlation (historical or assumed)
- Can operate on zero market data using historical vol estimates and flat correlation — imprecise but functional

**AI agent path:** Agent receives a deal specification in natural language → parses into instrument inputs → returns price + Greeks + plain-language risk summary.

---

## M3: Risk Decomposition Engine `[U][E][A]`

**Engine type:** Analytical
**Purpose:** Aggregate delta, gamma (asymmetric), vega, SkewVega (per leg), and theta across a heterogeneous book; apply cross-instrument correlation adjustments; produce scenario profiles.

*Merges prior modules: Greeks & Risk Factor Decomposition Engine + Portfolio Greeks aggregation.*

**Inputs:**
- Trade book: each trade with type, legs, strikes, weights, expiry, volume, premium paid/received
- Current forward price mark(s)
- Vol surface (from `VOL_SURFACE`) or vol parameters
- Inter-contract correlation matrix

**Outputs:**
- Per-trade Greeks table: PnL, delta, gamma-up, gamma-down, vega, SkewVega-per-leg, theta
- Portfolio-level Greeks: raw sum + correlation-adjusted (adjusted vega = vol_vec @ cor_matrix @ vega_vec)
- Scenario profile: all Greeks as function of underlying price over configurable range
- Skew exposure: per-leg vol differential and its sensitivity
- ATM vol, strike vol, breakeven daily move (= ATM_vol × F / √252)

**Typical users:** Trader (daily risk check), Risk manager (limit monitoring), PM (portfolio view)
**Mode:** Batch (daily mark) + Interactive (intraday)
**Type:** Descriptive (what is the risk?) + Diagnostic (what drives it?)

**Commercial value:** Without this, risk is managed by intuition. With it, every position's risk contribution is visible, comparable, and manageable. Prerequisite for P&L attribution and hedge recommendation.

**Minimum data to build now:**
- Trade book (any format: CSV, JSON, database table)
- One forward price per contract
- One vol estimate per contract (flat is acceptable to start)
- Correlation matrix: can use 1.0 (uncorrelated) initially; refine with historical data

**AI agent path:** Agent monitors Greeks intraday; alerts when a threshold is breached: *"Net delta has moved outside the ±50 MW band — rehedge candidate identified."*

---

## M4: P&L Attribution Engine `[U][E][A]`

**Engine type:** Decision-support
**Purpose:** Decompose daily MtM P&L change into causal risk-factor components; reconcile against actual; surface unexplained residuals. Converts P&L from a number into a narrative.

**Inputs:**
- Trade book state at T−1 and T (snapshots)
- Forward price at T−1 and T
- Vol surface at T−1 and T
- Days elapsed T−1 → T
- Greeks at T−1 (from `RISK_DECOMPOSITION`)

**Outputs:**
- Attribution table:
  - Hedge P&L: flat/forward positions
  - Delta P&L: `delta₀ × ΔF`
  - Gamma P&L: `½ × gamma₀ × ΔF²`
  - SkewVega P&L: `Σ [SkewVega_leg1 × Δσ_leg1 − SkewVega_leg2 × Δσ_leg2]` per spread option
  - VanillaVega P&L: `Σ [vega_vanilla × Δσ_vanilla]`
  - Theta P&L: `days_elapsed × theta₀`
  - Unexplained residual = Total − Σ(components)
- Residual flag: if |unexplained| > 2% of total, raise for review
- Vol change per leg: Δσ_leg for each spread option leg
- Plain-language attribution sentence (template-driven)

**Typical users:** Trader (daily), Risk manager (daily audit), Management (weekly narrative)
**Mode:** Batch (automated post-close daily)
**Type:** Diagnostic ("why did P&L move?")

**Commercial value:** Without attribution, P&L is a black box — losses are unexplained, gains are unclear. Attribution is the foundation of risk governance, hedge effectiveness measurement, and management reporting. It also separates model error (residual) from real market moves.

**Minimum data to build now:**
- Two consecutive trade book snapshots (T−1, T)
- Two forward prices
- Two vol surface parameters (or two ATM vol estimates — reduced precision)
- Greeks at T−1: can be estimated if not stored, but daily snapshot storage is strongly recommended

**AI agent path:** Agent runs attribution automatically post-close; generates narrative: *"Today's P&L was +€42K. Gamma contributed +€28K (price moved €1.5), vega added +€18K (vol surface widened 0.8 pts), theta cost −€5K. Residual is €1K (2.4%) — within normal range."*

---

## M5: Scenario & Stress Engine `[U][E]`

**Engine type:** Decision-support
**Purpose:** Revalue the full book or any subset under a user-defined or standard stress scenario grid; produce loss distributions and narrative scenario summaries.

*Merges prior modules: Scenario Engine + stress-testing aspects of Realized Monitor.*

**Inputs:**
- Trade book (current)
- Scenario specification: price range, vol range, correlation range, time horizons
- Historical stress episodes (optional): replay price/vol paths from defined events
- Monte Carlo parameters: number of paths, vol assumption, seed

**Outputs:**
- PnL surface: P&L as function of (price, vol) at each requested horizon
- Risk profiles: delta(price), gamma(price), vega(vol) at each horizon
- Scenario table: all positions revalued under each scenario, contribution breakdown
- Max loss estimate: worst case in defined scenario space
- Probability-weighted loss: if distributional assumption provided
- Narrative: plain-language description of each scenario outcome

**Typical users:** Risk manager (limit-setting, stress reporting), Structurer (product design sanity check), Management (risk appetite review)
**Mode:** Batch (scheduled weekly stress run) + Interactive (ad-hoc what-if)
**Type:** Predictive (what could happen?) + Prescriptive (what hedge reduces worst-case loss?)

**Commercial value:** Mandatory for risk governance and counterparty confidence. In structured products, showing counterparties scenario analysis is a commercial differentiator. Internally, it pre-empts surprises.

**Minimum data to build now:**
- Trade book + current mark
- Scenario grid requires only forward price and vol estimates — no surface needed for basic version
- Historical stress episodes: need price history (freely available) + vol history (require scraping or broker quotes)

**AI agent path:** Agent runs stress scenarios automatically when market moves exceed a threshold: *"Price has moved >3% today — running stress test. Worst case over the remaining 30 days is −€X under vol+10 scenario."*

---

## M6: Realized vs Implied Monitor `[U][E][A]`

**Engine type:** Monitoring
**Purpose:** Continuously track whether realized market conditions are converging or diverging from the assumptions embedded in current valuations; trigger alerts before divergence becomes material loss.

**Inputs:**
- Rolling price history (configurable window: 10d, 20d, 60d)
- Current implied vol (ATM from `VOL_SURFACE`)
- Vol-at-inception for each structured position or trade
- Breakeven forward levels (from `PRICING_ENGINE`)

**Outputs:**
- Realized vol (rolling): per window length
- Vol realization ratio: RVol / IVol (>1 = realized > implied; <1 = realized < implied)
- Breakeven distance: current market vs breakeven level, in both currency units and σ-units
- DH P&L simulation (lite): expected remaining P&L under current realized vol (30–50 Monte Carlo paths)
- Alert queue: flagged conditions with severity level
- Time series: realization ratio and breakeven distance history

**Alert conditions (configurable):**
- `WARN`: realization ratio > 1.15 or < 0.75
- `ALERT`: realization ratio > 1.30 or breakeven distance < 1.5σ
- `CRITICAL`: breakeven distance < 1.0σ

**Typical users:** Trader (intraday monitoring), Risk manager (daily limits), PM (portfolio health)
**Mode:** Continuous (event-driven on each price update) + Batch (daily summary)
**Type:** Monitoring (is anything wrong?) + Predictive (where does this go?)

**Commercial value:** The earliest possible warning system for structured positions and vol-selling strategies. Catching divergence at the WARN level allows orderly adjustment; catching it at CRITICAL is too late.

**Minimum data to build now:**
- Price history (20–60 trading days): publicly available for most markets
- ATM implied vol: need at least one broker quote per day (can manually enter if no scraper)
- Breakeven and inception vol: internal trade data

**AI agent path:** Agent monitors continuously; surfaces relevant alerts in natural language at user-defined cadence or on-demand: *"Realized vol (20d) is now 34%. The position was sold at 37% implied. Realization ratio is 0.92 — within normal range. Breakeven is 2.8σ away."*

---

## M7: Cross-Market Signal Radar `[U][E]`

**Engine type:** Monitoring
**Purpose:** Monitor implied vol and price signals from markets causally upstream of the target market; detect divergences that historically precede moves in the target; flag trading implications.

**Inputs:**
- Daily implied vol time series: target market + all upstream/correlated markets
- Historical cross-market IV correlation matrix (rolling, 15d/30d)
- Z-score thresholds for alert generation
- Optional: price level data for the same markets

**Outputs:**
- Cross-commodity vol dashboard: all IVs indexed to baseline, on one view
- Divergence signal: upstream IV moved but target IV has not → alert with historical predictive strength
- Implied correlation: back-derived from spread vol; compare to historical
- Lead-lag analysis: which upstream market leads the target, and by how many days
- Regime signal: all-markets-up, all-markets-down, diverging-markets

**Typical users:** Trader (market intelligence), Quant (model input), Structurer (cross-commodity product pricing), Risk manager (macro risk context)
**Mode:** Continuous/daily refresh + Interactive drill-down
**Type:** Monitoring + Predictive

**Commercial value:** Informational edge: see moves coming in the target market before they arrive. In structured products: correctly price cross-commodity correlation. In risk management: identify systemic risk build-up.

**Minimum data to build now:**
- Minimum: ATM IV for 2–3 correlated markets (can be manually collected initially)
- Full version: daily IV time series going back 1–2 years for correlation calibration
- For China: DCE coal options IV, CBEEX carbon, LNG spot implied vol — data is sparse but growing

**AI agent path:** Agent monitors cross-market divergences and generates alerts: *"Gas IV has risen 6 points in 5 days. Based on 30-day rolling correlation of 0.71, power IV is expected to increase ~4 points within 5–10 days. Power IV has not yet moved."*

---

## M8: Hedge Recommendation Engine `[U][E]`

**Engine type:** Recommendation
**Purpose:** Given current Greeks and available hedge instruments, compute the portfolio of hedges that minimizes residual risk subject to instrument constraints; quantify the unavoidable residual.

**Inputs:**
- Current Greeks: delta, gamma, vega per contract/tenor (from `RISK_DECOMPOSITION`)
- Target risk profile: desired net delta band, gamma limit, vega limit
- Available instruments: futures tenor list, option strikes/expiries available, bid-ask estimates
- Rehedge trigger rule: delta band, gamma threshold, calendar rule, or event-driven
- Hedge vol assumption (may differ from position vol)

**Outputs:**
- Recommended hedge: trade type, instrument, volume, direction
- Residual Greeks after recommended hedge
- Hedge cost: estimated bid-ask and premium impact
- Imperfect hedge flag: when target Greeks cannot be fully achieved, show minimum achievable residual and which risk factor remains
- Mismatch simulation: if hedge vol ≠ position vol, run 20-path Monte Carlo, show expected P&L leakage distribution

**Typical users:** Trader (execution support), Risk manager (limit enforcement)
**Mode:** Interactive (on-demand) + Agentic (auto-trigger when rehedge rule fires)
**Type:** Prescriptive ("trade X to achieve Y, accepting residual Z")

**Commercial value:** Reduces hedge cost by optimizing across available instruments; makes hedge rationale explicit and auditable; handles the common real-world case where the perfect hedge doesn't exist.

**Minimum data to build now:**
- Current Greeks (from `RISK_DECOMPOSITION`)
- Available instrument list with prices (even approximate)
- Target risk profile (configurable, simple delta band to start)
- Imperfect hedge logic requires: vol assumption input and Monte Carlo engine (already in archive)

**AI agent path:** Agent monitors delta band trigger; when breached, automatically generates: *"Delta has moved to −125 MWh. Target band is ±50. Recommended: BUY 75 MW Cal19 forward at €52.15. After hedge: delta −50, within band. Gamma unaffected (no options available for hedge)."* Agent can optionally execute via `WORKFLOW_ENGINE`.

---

## M9: Physical Asset Optionality Decomposition `[E][A]`

**Engine type:** Analytical + Decision-support
**Purpose:** Decompose a physical asset or structured contract into equivalent financial option structures; price and risk-manage the embedded optionality using `PRICING_ENGINE` and `RISK_DECOMPOSITION`.

**Inputs:**
- Asset specification: type (BESS, hydro, thermal, tolling, PPA), capacity, efficiency η, degradation, operational constraints
- Contract terms for structured products: floor, cap, collar, must-take, swing provisions
- Forward price curves for each relevant leg
- Vol surface or vol estimates per leg; correlation between legs

**Outputs:**
- Option decomposition: list of equivalent financial options (strip of spread calls, put strip, etc.) with effective strikes, expiries, and volumes
- Aggregate option value and Greeks
- Dominant risk factor: which input (price level, spread width, vol, correlation, efficiency) contributes most to option value
- Sensitivity analysis: how does value change as each input varies?
- Optimal exercise rule: simplified decision rule for when to exercise

**Core logic — key decompositions:**
- BESS (1 cycle/day): N daily spread calls, strike K = (1/η − 1) × P_charge + O&M per MWh, vol = spread vol, ρ = peak-offpeak correlation
- Thermal plant: daily spark spread calls, strike K = heat_rate × gas_price + carbon_cost + VOM
- Tolling: same as thermal; the optionality is held by the toller
- PPA floor: put strip at floor price; PPA cap: short call strip at cap price; collar = put − call
- Swing/take-or-pay: compound option on the right to vary volume

**Typical users:** Asset optimizer, Structurer, Investment analyst, Finance/CFO
**Mode:** Interactive (asset valuation) + Batch (portfolio-level mark)
**Type:** Descriptive (what is the embedded option worth?) + Decision-support (what risk does it create?)

**Commercial value:** Most asset managers and developers do not price embedded optionality rigorously — they use DCF or rule-of-thumb capacity factors. Optionality decomposition creates systematic advantage in: BESS investment decisions, PPA pricing and negotiation, tolling/route-to-market contract structuring, and portfolio mark-to-market.

**Minimum data to build now:**
- Asset specification: known at time of investment or contract
- Forward price curves: available from exchange data or broker quotes
- Vol: flat historical vol is sufficient to start; surface calibration adds precision
- Correlation: historical from price data (free); can assume initially

**AI agent path:** Agent receives asset spec → decomposes automatically → returns: *"This 100 MW / 200 MWh BESS is equivalent to 365 spread calls per year at a net strike of €12/MWh. At current vol (28%), annual embedded option value is €X. Primary risk: peak-offpeak spread vol (vega = €Y per 1 vol point). Recommended hedge: sell a portion of the spread forward to lock in realized value above €15/MWh."*

---

## M10: Workflow & Execution Engine `[U]`

**Engine type:** Execution-support
**Purpose:** Orchestrate the daily risk operations loop; record all trades with full metadata; maintain versioned position state; manage the data pipeline from external feeds to engine inputs.

*Merges prior modules: Position Lifecycle Monitor + Workflow Automation Engine.*

**Inputs:**
- External data: market price feeds, vol quote sources, trade confirmations
- Internal: trade book updates, engine outputs, alert queue

**Outputs:**
- Normalized trade book (current + versioned history)
- Daily position snapshot archive
- Triggered engine runs (scheduled + event-driven)
- Alert queue: all monitoring module alerts, prioritized
- Daily report package: Greek summary, attribution, alerts, next-action items

**Core logic:**
- Start-of-day: ingest market data → run `VOL_SURFACE` calibration → update position marks → run `RISK_DECOMPOSITION` → run `PNL_ATTRIBUTION` → run monitoring modules → generate alert queue
- Event-driven: on new trade → update position → re-run `RISK_DECOMPOSITION` → check triggers → if rehedge trigger → call `HEDGE_RECOMMENDATION`
- End-of-day: snapshot position state → archive → generate daily report

**Typical users:** Quant/operations (setup), Trader (daily use), Risk manager (audit trail)
**Mode:** Automated (daily scheduled) + Event-driven (on trade/price update) + Agentic (can run with no human intervention once configured)
**Type:** Execution-support (makes all other modules operational)

**Commercial value:** This is the operational glue. Without it, all the analytical engines are tools that require manual orchestration. With it, the platform runs as a system.

**Minimum data to build now:**
- Any trade book format (CSV is sufficient to start)
- Any price feed (manual entry is a valid starting point)
- The orchestration logic is independent of data quality

**AI agent path:** The workflow engine is the host environment for all agents — it provides the event triggers (market open, price update, trade executed) that cause agents to fire.
