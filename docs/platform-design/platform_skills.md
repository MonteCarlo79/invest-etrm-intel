# Core System Skills: Reusable Capabilities for an Investment–Trading–Assets Intelligence Platform
*Refined version — architectural framing*

---

## Framing Principle

Every skill listed below is extracted from operational evidence in the options trading archive: code that ran daily, analytical loops that were repeated hundreds of times, and infrastructure that was built and used under live market conditions. The skills are framed as system capabilities — not as descriptions of activity, but as definitions of what a platform component must be able to do.

Each skill is typed by its function in the decision architecture:

| Type | Role |
|---|---|
| **Analytical** | Computes values, surfaces, sensitivities — no judgment, pure computation |
| **Decision-support** | Structures information to support a human decision |
| **Monitoring** | Watches conditions continuously and signals deviation |
| **Recommendation** | Prescribes a specific action given current state and objectives |
| **Execution-support** | Facilitates, records, and manages the execution of a decision |

---

## Skill 1: No-Arbitrage Vol Surface Construction

**Type:** Analytical
**System name:** `VOL_SURFACE`

**What the system must do:**
- Ingest sparse option quotes (price or vol form) at discrete strikes and expiries
- Convert prices to implied vols via numerical inversion (Black-76 bisection)
- Fit a continuous, arbitrage-free surface using SVI parametrization per expiry slice
- Enforce no-butterfly (positive risk-neutral density) and no-calendar (monotone total variance) constraints via iterative penalty fitting
- Expose the surface as: IV at any (K, T), local vol, risk-neutral density, ATM vol, delta-to-strike mapping
- Support daily refresh and versioned surface archive; compare surface to prior day

**Archive evidence:** `SVI.py` (full implementation with `butterfly()`, `calendar()`, `RND()`, iterative penalty loop); `Vol_Calib.py` (calibration driver, SVI/spline switching); 200+ daily `Aligne skews DE - *.xlsx`; `quotes_Cal19_*.csv`; `StartOfDay.py` (vol calibration as first automated step each day)

**Market scope:**
- **Universal:** any market where options are quoted; surface shape changes, model does not
- **Power/energy specific:** thin strike ladders and single-expiry structures require heavier regularization; SVI degrades gracefully to spline or flat vol under data scarcity
- **Asset-backed:** physical asset valuation requires a surface, not a point estimate, to price optionality across a range of exercise conditions

---

## Skill 2: Two-Asset Derivatives Pricing with Full Greeks

**Type:** Analytical
**System name:** `PRICING_ENGINE`

**What the system must do:**
- Price European vanilla options (Black-76) using either a flat vol or a surface-interpolated vol
- Price spread options on two correlated forwards using Kirk's approximation (strike K ≠ 0) and Margrabe's formula (K = 0, exchange option)
- Compute full sensitivities: per-leg delta, per-leg gamma, per-leg vega, correlation sensitivity (ρ-vega), theta — numerically via finite difference
- Support weighted multi-leg structures (ratio spreads: w1 × payoff1 − w2 × payoff2)
- Accept vol from surface query, historical estimate, or direct input
- Return moneyness, intrinsic value, and time value alongside price and Greeks

**Archive evidence:** `Kirk.py` (Kirk class with `Pricer()` + `Greeks()`); `Margrabe2Assets.py` (exchange option with quantity and efficiency scaling); `VanillaOption.py`, `Vol_Pricer.py` (QuantLib-backed Black-76 with full Greeks); `SpreadOption2.py`, `SpreadPricing.py` (multi-leg production pricing); earlier `Margrabe2Assets.py` versions from CSL oil spread option work

**Market scope:**
- **Universal:** two-forward spread payoffs exist in every commodity and financial market
- **Power/energy specific:** spark spread (power − gas − carbon = clean spark), dark spread (power − coal − carbon = clean dark), seasonal spreads, peak–offpeak spread, cross-border congestion spread
- **Asset-backed:** battery dispatch = strip of spread calls; tolling = spark spread calls; physical optionality decomposes into spread/exchange option chains

---

## Skill 3: Multi-Factor Greeks Aggregation

**Type:** Analytical
**System name:** `RISK_DECOMPOSITION`

**What the system must do:**
- Aggregate delta, up-gamma, down-gamma, vega, SkewVega (per-leg vega for each spread option leg), and theta across a heterogeneous book: spread options + vanilla options + flat/physical positions
- Apply correlation adjustments across tenors and instruments: `AdjVega = vol_vector @ cor_matrix @ vega_vector`
- Produce per-trade, per-contract, and portfolio-level Greeks tables
- Sweep across a price range to produce scenario profiles of all Greeks (payoff diagram generalization)
- Flag: asymmetric gamma (up ≠ down), skew exposure (spread between individual leg vols changes), cross-tenor correlation risk

**Archive evidence:** `Book_Management_v1.py` `AggregatedTrades()` (per-trade Greek computation for `OptSpread`, `Vanilla`, `Flat` types); `Portfolio_Management_v1.py` (cross-contract correlation adjustment with `VegaCoef = [[1, 0.85], [0.85, 1]]`); `BookManagement4.py` `Scenarios()` (price sweep); `Kirk.py` (per-leg Greeks via finite difference)

**Market scope:**
- **Universal:** Greek aggregation is market-agnostic; the aggregation logic is identical for power, gas, oil, equity, or rates books
- **Power/energy specific:** SkewVega per leg is especially important in power because individual leg vols (e.g., peak vs offpeak, summer vs winter) move independently, unlike equity where the vol surface shift is approximately parallel
- **Asset-backed:** converts physical positions into equivalent financial Greeks; enables a unified risk view across physical assets and financial hedges

---

## Skill 4: Causal P&L Attribution

**Type:** Decision-support
**System name:** `PNL_ATTRIBUTION`

**What the system must do:**
- Decompose the change in mark-to-market value between two dates into: Delta P&L, Gamma P&L, SkewVega P&L (per spread option leg), Net Vega P&L, Theta P&L, Hedge P&L, Unexplained residual
- Use start-of-period Greeks for all attribution (avoids path-dependency)
- Reconcile the sum of components against actual MtM change; surface residuals >threshold for review
- Support per-trade, per-contract, and portfolio-level attribution

**Archive evidence:** `BookManagement4.py` `BreakDown()` (exact implementation: Delta P&L = `delta0 × ΔF`; Gamma P&L = `gamma0 × ΔF² / 2`; SkewVega P&L = `ΣSkewVega_leg × Δvol_leg × 100`; Theta P&L = `days × theta0`); daily `DE_Cal_Qrt_ddMmmyy.xlsm` snapshots (100+ days of attributable data)

**Market scope:**
- **Universal:** the attribution identity holds for any book where Greeks exist; market label is a parameter
- **Power/energy specific:** SkewVega attribution is disproportionately important; power vol surfaces are highly skewed and individual leg vol changes drive most spread book P&L
- **Asset-backed:** allows separation of "dispatch/operational P&L" from "market value change" — critical for BESS and tolling performance monitoring

---

## Skill 5: Realized-vs-Implied Divergence Tracking

**Type:** Monitoring
**System name:** `REALIZED_IMPLIED_MONITOR`

**What the system must do:**
- Continuously compare rolling realized volatility (10d, 20d, 60d) to current implied vol (ATM from surface)
- Track the vol realization ratio: RVol/IVol; classify regime (vol seller winning / losing)
- Maintain breakeven forward level(s) and compute market distance in € and σ-units
- Simulate expected remaining P&L under current realized vol (lite Monte Carlo, 30–50 paths)
- Trigger alerts when: realization ratio crosses threshold (e.g., >1.2 or <0.7), breakeven distance falls below 1.5σ

**Archive evidence:** `Dynamic_Hedging_Simulator_v1.py` (Monte Carlo, 100 paths, tests DH P&L under 25%, 34%, 37% vol); `DH_PnL_25vol_match.csv`, `37vol_mismatch.csv`, etc. (actual simulation outputs testing vol mismatch scenarios); `Breakeven_Cal19_*.csv` (daily breakeven tracking, Aug–Sep 2018)

**Market scope:**
- **Universal:** the realized-vs-implied framework is foundational to any derivatives trading or structured products business
- **Power/energy specific:** power vol is highly event-driven (weather, fuel, outages) → realized vol spikes frequently; monitoring divergence is operationally critical
- **Asset-backed:** for BESS or tolling: "are spreads realizing as forecast when the dispatch strategy was designed?" is the same question

---

## Skill 6: Hedge Design Under Instrument Constraints

**Type:** Recommendation
**System name:** `HEDGE_RECOMMENDATION`

**What the system must do:**
- Given current Greeks and a target risk profile, solve for the hedge portfolio that minimizes residual risk within available instruments
- Explicitly handle incomplete hedge sets: when gamma or vega cannot be hedged (futures only), quantify and report minimum achievable residual
- Simulate hedge performance under price paths (Monte Carlo) for the candidate hedge, including vol mismatch scenarios
- Output: trade recommendation, residual Greeks after hedge, hedge cost estimate

**Archive evidence:** `Delta Hedging Instructions 30Jul-03Aug.xlsx` (written operating procedures with explicit rehedge triggers); `Dynamic_Hedging_Simulator_v1.py` (simulate DH P&L under hedge vol ≠ realized vol); `Order_Placing.py` (execution layer); `Trade_Recorder_Editor.py` (delta-based rehedge logic: `hedgevol = round(delta)` → execute hedge when integer delta changes)

**Market scope:**
- **Universal:** hedge design under imperfect instruments is the dominant practical problem in any illiquid derivatives market
- **Power/energy specific:** power options are often hedged with forward contracts only (no gamma hedge available) → the vol mismatch simulation is particularly important
- **Asset-backed:** physical assets have operational constraints on hedging (minimum dispatch, ramp, contract commitments) → the incomplete-instruments framework applies directly

---

## Skill 7: Cross-Market Causal Signal Monitoring

**Type:** Monitoring
**System name:** `SIGNAL_RADAR`

**What the system must do:**
- Ingest implied vol data from multiple causally-related markets simultaneously
- Maintain rolling cross-market correlations and detect divergences between upstream vol moves and target market vol
- Alert: "upstream IV moved; target IV has not yet responded; historical lag suggests move within N days"
- Track spread vol: σ_spread ≈ √(σ₁² + σ₂² − 2ρσ₁σ₂); use to back-derive implied correlation

**Archive evidence:** `ImpliedVol_Scraper_v1.py`, `ImpliedVol_Scraper_Carbon_v1.py`, `ImpliedVol_Scraper_Coal_v1.py` (three simultaneous scrapers); `Vol Models/cross commodities.xlsx`; archived `Carbon/Coal/Gas Implied Vol/` directories (cross-commodity IV history); `VIX Hist Data.py` (macro vol awareness)

**Market scope:**
- **Universal:** cross-market signal logic (lead-lag, divergence detection) generalizes to any multi-market setup
- **Power/energy specific:** the specific causal chains are well-defined: coal/gas/carbon → marginal cost → power price → power vol; the framework is not European-specific, it is energy-market-specific
- **Asset-backed:** upstream vol signals affect the value of embedded optionality in physical assets before the asset's own market moves

---

## Skill 8: Physical Asset Optionality Decomposition

**Type:** Analytical / Decision-support
**System name:** `OPTIONALITY_DECOMP`

**What the system must do:**
- Parse a physical asset specification or structured contract into a decomposition of equivalent financial option structures
- Price each component using `PRICING_ENGINE`; aggregate into total option value and portfolio Greeks
- Compute sensitivity of asset value to: spread level, spread vol, correlation, efficiency parameter, time
- Identify the dominant risk factor and the appropriate hedge instrument

**Archive evidence:** `Shell Flexibility Workshop 18-4-18.pptx` (flexible power asset as option); `Spread Options/General/Managing_the_spark_spread.pdf` (power plant as spark spread strip); Margrabe applied to two-forward payoffs throughout the archive; ratio-call/put-spread notebooks (structuring viewpoint applied to financial positions that mirrors the physical-to-financial translation)

**Market scope:**
- **Power/energy specific:** most directly applicable in energy where physical flexibility (dispatch, storage, conversion) has well-defined optionality equivalents
- **Asset-backed:** this is the core skill for BESS, tolling, hydro, PPA structuring — essentially any capital asset whose value depends on the right but not obligation to act
- Not universal in the same sense as other skills — requires domain knowledge of how specific assets generate optionality

---

## Skill 9: Risk Operations Workflow Automation

**Type:** Execution-support
**System name:** `WORKFLOW_ENGINE`

**What the system must do:**
- Orchestrate the daily operational loop: data ingestion → vol calibration → position mark → Greeks computation → attribution → alert queue → report generation
- Record all trades at execution with full metadata; maintain versioned end-of-day position state
- Support rollback and comparison to any prior date
- Manage data pipeline: multiple source feeds → normalized internal format → engine inputs

**Archive evidence:** `StartOfDay.py` (automated morning workflow); `Trade_Recorder_Func.py`, `Trade_Recorder_Editor.py`, `Trade_Recorder_inception.py` (full trade blotter with metadata); `Order_Placing.py`; `Historical_Data_Builder_v1.py`, `Volatility_File_Creator.py`; `BLM Scripts.py` (Bloomberg); Reuters CPD workbook; 140+ dated `Cal19_trades_*.csv` (daily state snapshots demonstrating operational discipline for 8 consecutive months)

**Market scope:**
- **Universal:** the workflow pattern (data → compute → risk → report) is identical across all markets; only the data sources and engine calls change
- Foundational: without this layer, all other skills are ad-hoc rather than operational
