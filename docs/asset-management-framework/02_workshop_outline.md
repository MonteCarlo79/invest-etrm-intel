# Workshop: Asset Management — Session Outline & Talk Track

*Format: 60–90 min, modular. Each block = ~10 min story + ~10 min open discussion. Rules honored:
interactive, no China-specific focus, no dashboards without real data and credible sources,
practical over conceptual, everything adaptable overseas.*

**Anonymization rules (apply throughout):** the operating fleet is "a fleet of four grid-scale
BESS assets in a liberalized spot market"; no province/market names for that fleet; counterparties
unnamed; ratios and percentages preferred over absolute currency. GB/international market data
(Modo Energy, Elexon — named, credible) provides the international grounding.

---

## Cold open (5 min)

> "On a merchant BESS fleet, the gap between theoretical revenue — perfect foresight dispatch —
> and what you actually realize is where asset management lives. We measure that gap per asset,
> per day. Today I'll walk through where it leaks, in four places, with real numbers — and each
> time I'd rather hear your version than finish mine."

**Frame slide:** the four layers — Physical Asset / Data & Settlement / Market & Dispatch /
Commercial & Lifecycle. "Value is won or lost in all four. The hardware is the smallest part of
the variance."

---

## Block 1 · Your P&L is wrong until proven otherwise (L2 — Data & Settlement)

**Story (~10 min):**
- ~80% of asset-management work is data plumbing. Nobody believes this until they own a bill.
- Real case: a mis-signed charge component — a sign-convention mismatch between counterparty
  formats — persisted for months: 79 charge records had to be rebuilt, plus duplicate discharge
  records removed. Found only when we started re-deriving every settlement from first principles.
  End state: eight months of books reconciled to the cent.
- Settlement PDF formats change without notice. Parsers drift silently. If your recon is manual,
  your detection latency is a month; if it's automated per-bill re-derivation, it's a day.

**Exhibit:** one anonymized bill re-derivation walkthrough (printed charge → stored sign →
reconciled monthly total). No market names.

**Discussion prompt:** *"Who in your organization re-derives the settlement statement before it
gets paid? And what is your detection latency when a counterparty changes format?"*

**Expected pushback:** "Our market's settlement is simpler." → "Then your version of this failure
is in the metering and telemetry layer — same question: who re-derives it?"

---

## Block 2 · The capture gap is the job (L3 — Market & Dispatch Intelligence)

**Story (~10 min):**
- Metric first: capture rate = realized ÷ theoretical (perfect-foresight LP) revenue, per asset
  per day. Everything in dispatch quality lives in that number.
- Real case: a legacy forecast model used day-ahead prices that are only published *after*
  real-time trading closes. Apparent capture inflated ~100%. It survived until we ran multiple
  models side-by-side and one stuck out. Lesson: model governance — never one unchallenged model.
- Second real case: AI forecast/intelligence feeds have real unit costs. One of ours carries a
  weekly credit quota (measured ~36–40 calls/week against a 140-call wish). We now rotate
  question cadence to budget, and abort runs on the first quota error instead of burning
  failures. AI operations are an economics problem, not a demo.

**Exhibit:** capture-by-model comparison table (theoretical vs realized, per model — one model
visibly inflated). Percentages only.

**Discussion prompt:** *"What do you measure dispatch quality against — and has anyone audited
the benchmark itself?"*

---

## Block 3 · Attribution is the trust layer (L4 — Commercial & Lifecycle)

**Story (~10 min):**
- "The market was bad" is not an explanation; it's a mood. Per-asset daily P&L attribution
  decomposes performance into price / dispatch / degradation drivers. That decomposition is the
  only thing that makes a performance-linked contract verifiable.
- Real practice: daily per-asset attribution, strategy-comparison workflow (actual dispatch vs
  alternative strategies, P&L waterfall), portfolio roll-up, IRR re-run on operating data —
  acquisition models go stale fast.
- Commercial consequence: performance contracts (capture floor + upside share) are only sellable
  on top of an attribution both parties trust. Contract design and measurement design are the
  same document.

**Exhibit:** one anonymized daily P&L waterfall for a single asset (price effect / dispatch
effect / other, decomposed). No currency labels, indexed units.

**Discussion prompt:** *"What would your owners accept as proof of outperformance — and does your
current reporting survive that standard?"*

---

## Block 4 · Small team, big stack (cross-cutting — the AI-leverage operating model)

**Story (~10 min):**
- Staffing reality: a small senior team covers all four layers because four domain agents do the
  analyst legwork — a market strategist, an economics quant, a trading-ops analyst, and an
  investment aggregator.
- The disciplines that make agents trustworthy in production: strict grounding (answers only
  from data returned by tools — never from the model's general knowledge), persistent domain
  memory across sessions, and human review loops. And the honest limits: quotas, grounding
  failures, and the day a vendor silently changed their login flow and a nightly intelligence
  feed started spamming magic-link emails instead of data. Resilience engineering is part of
  AI operations.

**Exhibit:** architecture sketch — four agents over one data platform, with the grounding/memory
loop. Optionally a live read-only walk if the room allows (real data, no credentials).

**Discussion prompt:** *"Where would an agent break first in your data environment — and who
catches it?"*

---

## Close (10 min)

The take-home checklist, four questions:
1. Do you re-derive your settlement, or accept it? (L2)
2. Do you measure dispatch against a counterfactual, and has the counterfactual been audited? (L3)
3. Does your P&L decompose into drivers your owners would accept? (L4)
4. What in your stack is instrumented, attributed, *proven* — and what is still asserted? (all)

> "The hardware is the smallest part of the variance. The stack is the job."

---

## Speaker prep notes

- **Timing flex:** 60-min version = drop Block 4 discussion (keep story) and compress close.
- **Evidence pack to refresh before presenting:**
  1. Anonymized bill re-derivation exhibit (from the settlement audit trail).
  2. Capture-by-model table (`marketdata.bess_capture_daily`, last quarter, percentages).
  3. Daily P&L attribution waterfall for one asset (trading-ops dashboard export, indexed).
  4. Agent architecture sketch (4 agents, grounding rules, memory loop).
  5. Quota/economics numbers for the AI-ops anecdote (measured weekly call budget).
- **Sources named in the room:** Modo Energy + Elexon for GB market context (credible, public/
  subscription); fleet data stays anonymized as above.
- **Do NOT:** name the fleet's market/provinces, counterparties, or show any credential-adjacent
  screen. If asked "which market is the fleet in?" — "a liberalized spot market; the failure
  modes are the point, they're market-agnostic."
