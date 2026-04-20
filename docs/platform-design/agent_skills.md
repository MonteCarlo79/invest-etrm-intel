# AI Agent Skill Map
*Refined version — each skill is a discrete, composable agent capability grounded in platform module outputs*

---

## Framing Principles

Agent skills are not chatbot responses. They are autonomous reasoning loops that:
1. Are triggered by an event (time, price move, threshold breach, user query)
2. Pull data from one or more platform modules
3. Apply deterministic or learned logic
4. Produce a specific output: an alert, a recommendation, a narrative, or an action
5. Hand off to a human or execute autonomously (depending on permission level)

Each skill is mapped to the modules it depends on and the human decision it supports.

**Agent permission levels used below:**
- `L1 — Alert`: agent surfaces a finding; human decides what to do
- `L2 — Recommend`: agent proposes a specific action; human approves or rejects
- `L3 — Execute`: agent executes after human confirmation
- `L4 — Autonomous`: agent acts without human intervention (high-trust, well-defined context only)

---

## AS1: P&L Causal Explainer

**Trigger:** End of trading day (batch) or on-demand query
**Permission level:** L1 (alert / inform)

**Question answered:** *What drove today's P&L change, how much came from each risk factor, and is the attribution clean?*

**Data required:**
- Trade book snapshots T−1 and T
- Forward prices T−1 and T
- Vol surfaces T−1 and T
- Greeks at T−1 (from `RISK_DECOMPOSITION`)

**Logic:**
1. Call `PNL_ATTRIBUTION` → get component table
2. Rank components by absolute contribution
3. Check residual: if |unexplained| > 2% of total, flag model integrity concern
4. Check vol change per spread leg: is SkewVega the dominant term? (Unusual if so — flag)
5. Generate plain-language attribution sentence from template

**Output:** *"P&L today was +€42K. Breakdown: Gamma +€28K (price moved €1.5, book is long gamma), Vega +€18K (vol surface widened 0.8pts on the ATM slice), Theta −€5K (time decay), Hedge +€1K. Unexplained: €0.4K (0.9%) — within normal range."*

**Human decision supported:** Trader confirms book behaved as expected; risk manager verifies model; management receives daily narrative instead of a number.

**Modules consumed:** `PNL_ATTRIBUTION`, `RISK_DECOMPOSITION`, `VOL_SURFACE`

---

## AS2: Realized Conditions Divergence Detector

**Trigger:** Daily (batch) or on each price update exceeding threshold
**Permission level:** L1 (alert) → L2 (recommend) if WARN level breached

**Question answered:** *Is the market realizing in a way that materially differs from the assumptions embedded in current positions? How urgent is the divergence?*

**Data required:**
- Rolling price history (20d, 60d)
- Implied vol from `VOL_SURFACE` (ATM, current and at inception)
- Breakeven forward levels for each position

**Logic:**
1. Compute rolling realized vol (20d, 60d)
2. Compute realization ratio = RVol / IVol (inception and current)
3. Compute breakeven distance in σ units
4. Run lite DH simulation (30 paths) under current realized vol → estimate remaining P&L
5. Apply alert thresholds: NORMAL / WARN / ALERT / CRITICAL
6. If WARN or above: generate recommendation narrative

**Output (NORMAL):** *"Realized vol (20d): 31%. Inception implied vol: 37%. Realization ratio: 0.84 — vol seller winning. Breakeven is 2.6σ away. No action required."*

**Output (ALERT):** *"Realized vol (20d) has reached 42% — 5 points above inception implied vol of 37%. Realization ratio: 1.14. Breakeven is now 1.4σ away. Expected remaining DH P&L has deteriorated to −€X under current path. Recommend reviewing delta hedge frequency and considering partial position reduction."*

**Human decision supported:** Trader decides whether to rehedge or reduce; risk manager tracks limits.

**Modules consumed:** `REALIZED_IMPLIED_MONITOR`, `VOL_SURFACE`, `PRICING_ENGINE`

---

## AS3: Vol Surface Integrity Checker

**Trigger:** Immediately after each vol surface calibration run (daily, or on new quote ingestion)
**Permission level:** L1 (alert on failure)

**Question answered:** *Is the calibrated vol surface arbitrage-free? Have any quotes moved anomalously? Is the surface consistent with yesterday's?*

**Data required:**
- Freshly calibrated surface parameters
- Prior-day surface parameters
- Raw option quotes used in calibration

**Logic:**
1. Check butterfly residuals per slice: any > threshold (0.001)?
2. Check calendar residuals across slice pairs: any violations?
3. Compare ATM vol shift per slice to prior day: flag shifts > 2 vol points
4. Compare surface slope (skew) and curvature to prior day: flag large deviations
5. Identify which quotes are driving largest residuals (candidate for data error)
6. Report: clean / warning / failure

**Output (clean):** *"Surface calibrated cleanly. Maximum butterfly residual: 0.0003. No calendar arbitrage. Largest ATM shift: +0.8 vols on Cal19 slice vs yesterday."*

**Output (warning):** *"Butterfly arbitrage detected in Cal20 slice at 45-strike. Penalty was required; residual is 0.004 (above threshold). Likely cause: the 45-strike call quote may be stale. Recommend checking this quote before pricing Cal20 positions."*

**Human decision supported:** Quant/trader validates surface before running risk; ops team checks for bad data.

**Modules consumed:** `VOL_SURFACE`

---

## AS4: Short-Vol Fragility Monitor

**Trigger:** Daily (batch) + intraday if price moves exceed 1σ
**Permission level:** L1 (monitor) → L2 (recommend) at ALERT level

**Question answered:** *Is the net short-volatility exposure approaching conditions where it becomes structurally fragile? Where exactly is the fragility?*

**Data required:**
- Current Greeks from `RISK_DECOMPOSITION` (especially gamma profile vs price)
- Position structure: short strike levels, weights, expiry
- Realized conditions divergence score (from AS2)
- Days to expiry per option leg

**Logic:**
1. Extract gamma profile: compute gamma at current price and at ±1σ, ±2σ from short-strike cluster
2. Detect gamma spike zone: price level at which short-option gamma acceleration peaks
3. Compute gamma acceleration remaining: gamma of short options accelerates as 1/TTM → estimate "gamma weeks" remaining
4. Check realized conditions (AS2 score): is vol seller losing and fragility is also building?
5. Combined fragility score: LOW / MEDIUM / HIGH / CRITICAL

**Output (LOW):** *"Short-gamma fragility is LOW. Current price is 3.1σ from the short-strike cluster (€50). Net gamma is −€X per €1 move. TTM gamma acceleration will peak in 47 days. Breakeven: 2.8σ away."*

**Output (HIGH):** *"SHORT-VOL FRAGILITY: HIGH. Price has moved within 1.2σ of the short-call cluster (€50). Net gamma has increased 3× over the past 10 days (TTM effect). Realized vol is running above inception implied (ratio: 1.18). If price reaches €49, estimated daily PnL sensitivity is ±€Y. Recommend: (1) tighten delta hedge band from ±50 to ±25 MW, (2) consider buying back 20% of short wing, (3) review scenario table at €50 strike."*

**Human decision supported:** Trader decides when and how to defend the position; risk manager monitors against stop-loss thresholds.

**Modules consumed:** `RISK_DECOMPOSITION`, `PRICING_ENGINE`, `REALIZED_IMPLIED_MONITOR`, `SCENARIO_ENGINE`

---

## AS5: Cross-Market Vol Signal Alert

**Trigger:** Daily (batch) after upstream IV data refresh
**Permission level:** L1 (alert) → L2 (trading implication note)

**Question answered:** *Are causally upstream markets signaling a likely move in the target market vol that has not yet occurred? What is the predicted direction, magnitude, and timing?*

**Data required:**
- Daily ATM IV time series: target market + all monitored upstream/correlated markets
- Rolling cross-market correlation matrix (30-day)
- Historical lead-lag structure (calibrated offline)

**Logic:**
1. Detect recent upstream IV move: flag if any upstream market IV moved > 2 vol points in past 3 days
2. Check if target market IV has responded: if not, compute expected response = upstream_move × regression_coefficient
3. Compute z-score of current divergence vs historical distribution
4. Apply lead-lag model: estimate days until target typically responds
5. Cross-check correlation regime: is this a high-correlation or low-correlation period?
6. Generate alert with confidence qualifier

**Output:** *"Gas IV has increased 6 points in 5 days (TTF front year). Historical 30-day correlation between gas IV and power IV: 0.71. Predicted power IV increase: ~4 points within 5–10 days (based on regression, R²=0.52). Current power IV has not yet moved. Confidence: medium (correlation is stable but not high). Implication: this may represent a trading opportunity (buy power vol) or a risk alert for existing short-vol positions."*

**Human decision supported:** Trader considers whether to act on the signal; structurer updates cross-commodity pricing assumptions; risk manager flags potential vol risk buildup.

**Modules consumed:** `SIGNAL_RADAR`, `VOL_SURFACE`

---

## AS6: Hedge Adjustment Recommender

**Trigger:** Delta band breach event OR daily review OR on-demand
**Permission level:** L2 (recommend, human approves) → L3 (execute after confirmation)

**Question answered:** *Given current Greeks and available instruments, what hedge trades should be executed to restore the target risk profile? What residual risk will remain?*

**Data required:**
- Current Greeks (from `RISK_DECOMPOSITION`)
- Target risk profile (pre-configured per strategy)
- Available instrument list with current prices
- Rehedge trigger status

**Logic:**
1. Compute Greek residuals vs target
2. Identify which residuals exceed trigger threshold
3. For each breached residual: find available instrument that reduces it most efficiently
4. Solve for hedge volumes (linear problem for delta; constrained optimization for multi-Greek)
5. Where perfect hedge unavailable: minimize residual, flag uncloseable risk
6. Simulate hedge performance: 20-path Monte Carlo, show P&L distribution with and without hedge
7. Estimate execution cost

**Output:** *"Delta has moved to −125 MW equivalent. Target band: ±50. Recommended: BUY 75 MW Cal19 forward at current market (€52.15). Estimated cost: €2K (bid-ask). Post-hedge: delta −50, within band. Gamma and vega unchanged (no option instruments available to hedge). Residual gamma risk: if price moves >€3, expect P&L swing of ±€X beyond delta-hedge coverage."*

**Human decision supported:** Trader executes with quantified rationale; risk manager confirms hedge fits within strategy parameters.

**Modules consumed:** `RISK_DECOMPOSITION`, `HEDGE_RECOMMENDATION`, `PRICING_ENGINE`

---

## AS7: Embedded Optionality Identifier

**Trigger:** On asset/contract specification input OR on periodic portfolio revaluation
**Permission level:** L1 (inform/value) → L2 (risk and hedge recommendations)

**Question answered:** *What optionality is embedded in this physical asset or structured contract? What is it worth? How should the risk be managed?*

**Data required:**
- Asset or contract specification (structured input or parsed from document)
- Forward price curves for relevant legs
- Vol surface or vol estimates per leg
- Correlation estimate between legs

**Logic:**
1. Parse specification → identify asset type (BESS, thermal, tolling, PPA, swing, etc.)
2. Apply decomposition rule for asset type (see `OPTIONALITY_DECOMP` module)
3. Call `PRICING_ENGINE` for each option component
4. Aggregate: total embedded option value + portfolio Greeks
5. Identify dominant risk factor (vol sensitivity vs price sensitivity vs correlation)
6. Generate optimal exercise rule heuristic
7. Suggest hedge: what financial instrument most efficiently hedges dominant risk

**Output:** *"This 100 MW / 200 MWh BESS (η=85%, O&M=€3/MWh) is equivalent to 365 daily spread call options per year. Effective strike: €9.4/MWh (efficiency + O&M). At current peak-offpeak spread vol (26%), annual embedded option value is €X. Dominant risk: spread vol (long vega, €Y per 1 vol point). The asset benefits from wider spreads AND higher spread vol. Recommended risk management: sell a portion of forward peak-offpeak spread at current levels to lock in value above the effective strike; retain upside optionality above €20/MWh."*

**Human decision supported:** Asset manager values and hedges embedded optionality; structurer prices products correctly; investor assesses option-adjusted return on capital.

**Modules consumed:** `OPTIONALITY_DECOMP`, `PRICING_ENGINE`, `RISK_DECOMPOSITION`, `SCENARIO_ENGINE`

---

## AS8: Daily Operations Orchestrator

**Trigger:** Market open (scheduled) + event-driven (price update, new trade, alert)
**Permission level:** L4 (autonomous within configured parameters)

**Question answered:** *All systems nominal? All positions marked? Any alerts to surface? What needs human attention today?*

**Logic (start-of-day):**
1. Ingest market data (prices, vol quotes)
2. Run `VOL_SURFACE` calibration → run `AS3` (integrity check)
3. Mark all positions → run `RISK_DECOMPOSITION`
4. Run `PNL_ATTRIBUTION` (vs prior day) → run `AS1` (explainer)
5. Run `REALIZED_IMPLIED_MONITOR` → run `AS2` (divergence check)
6. Run `AS4` (fragility check), `AS5` (signal check)
7. Run `SCENARIO_ENGINE` (daily stress)
8. Compile alert queue: ranked by severity
9. Generate daily briefing: Greeks summary, P&L attribution, alerts, next-action items

**Output:** Structured daily briefing delivered to trader/PM:
- Market summary: prices, vol surface shift
- Greeks snapshot: delta, gamma, vega vs limits
- P&L attribution narrative (AS1)
- Alert queue: any WARN/ALERT/CRITICAL items
- Next-action items: ordered by priority

**Human decision supported:** Trader starts day with full situational awareness; risk manager confirms daily sign-off; management receives concise summary.

**Modules consumed:** All modules

---

## Agent Skill Dependency Map

```
Market data feed
      │
      ▼
VOL_SURFACE ──────────────────────────────────────────►  AS3 (surface integrity)
      │
      ├──── PRICING_ENGINE
      │           │
      │           ├──── RISK_DECOMPOSITION ────────────►  AS4 (fragility)
      │           │              │                         AS6 (hedge recommend)
      │           │              ▼
      │           │      PNL_ATTRIBUTION ──────────────►  AS1 (P&L explainer)
      │           │
      │           └──── OPTIONALITY_DECOMP ─────────►    AS7 (asset optionality)
      │
      ├──── REALIZED_IMPLIED_MONITOR ─────────────────►   AS2 (divergence)
      │
      ├──── SIGNAL_RADAR ──────────────────────────────►  AS5 (cross-market signal)
      │
      └──── SCENARIO_ENGINE ──────────────────────────►  stress outputs
                                                           (consumed by AS4, AS7)

All of the above ──────────────────────────────────────►  AS8 (daily orchestrator)
```
