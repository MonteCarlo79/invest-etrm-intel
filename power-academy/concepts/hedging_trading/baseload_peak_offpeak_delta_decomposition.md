---
id: baseload_peak_offpeak_delta_decomposition
track: hedging_trading
level: intermediate
prerequisites:
- forward_curve_structure_and_products
- power_plant_economics_and_dispatch
markets:
- EU
- GB
status: drafted
sources:
- id: dispatch_and_delta_hedging_aom
  use: practice_example
- id: incremental_peak_delta2
  use: practice_example
- id: the_caveats_in_aom_peak_delta_calculation
  use: practice_example
- id: offpeakdeltas
  use: practice_example
originality: synthesized
translations:
  zh:
    status: drafted
    en_hash: 89ee27d6ed9e66bf
---
## Learning objectives

- Decompose a plant's total price sensitivity into baseload, peakload, and offpeak deltas, and prove the identity $\Delta_{base} = \Delta_{peak} + \Delta_{offpeak}$.
- Distinguish production delta (average run MWh per block) from finite-difference delta (value sensitivity to that block's price), and explain where and why they differ.
- Quantify the incremental peak delta: the part of peak sensitivity coming from hours at the dispatch boundary, not from hours already running.
- Reproduce the AOM caveat: hedging with the average-production delta mis-sizes the position and leaves measurable residual risk.

## Intuition

A plant's total delta is one number, but you cannot trade "one number" — you trade baseload and peakload contracts. So the delta must be split across products. The naive split is by production: "I run X MWh in peak hours, so my peak delta is X." It feels right and it is wrong in exactly the cases that matter.

The reason is the dispatch boundary. A plant that runs whenever the price clears its cost has some hours *certain* to run (deep in the money) and some hours *at the boundary* — they run only if prices firm a little. The boundary hours carry no production in the base case but carry *sensitivity*: a €1 rise in the peak product switches them on. Value sensitivity, not production, is the hedge ratio. The finite-difference delta captures both parts — production already running plus incremental conversion at the boundary; the production delta captures only the first. The difference between the two, concentrated at the boundary, is the **incremental peak delta** — and the **AOM caveat** is the documented pitfall of computing peak deltas as averages of per-scenario production (average-of-marginals): it mis-weights boundary scenarios and hands the desk a hedge that is systematically off, in a direction that shows up exactly when prices move.

## Formal treatment

**Decomposition.** Shock each block's price separately (peak hours only, offpeak only, all hours). Since the shocks add linearly,

$$\Delta^{fd}_{base} = \Delta^{fd}_{peak} + \Delta^{fd}_{offpeak}$$

where $\Delta^{fd}_{B} = \frac{V(F + \epsilon \mathbb{1}_B) - V(F - \epsilon \mathbb{1}_B)}{2\epsilon}$ per product block $B$ (dispatch re-optimised in each shocked scenario). The identity is a consistency check every implementation should run.

**Two deltas.**

- **Production delta:** $\Delta^{prod}_B = \mathbb{E}[\text{run MWh in } B]$ — expected generation inside the block.
- **Finite-difference delta:** $\Delta^{fd}_B$ as above — value sensitivity including the dispatch response.

For a must-run plant (never at a boundary) the two coincide and equal block hours. For a marginal plant they differ; the sign and size of $\Delta^{fd} - \Delta^{prod}$ depend on where the price density sits relative to the cost strike — hours just below the strike inflate $\Delta^{fd}$ above $\Delta^{prod}$, hours just above do the reverse.

**Incremental peak delta** $= \Delta^{fd}_{peak} - \Delta^{prod}_{peak}$ — the boundary contribution. It is the quantity the AOM-style average misses, and the source of the residual risk in the caveat case.

## Worked example

Synthetic month of 20 weekdays (seed 17), hourly profile peaking at 75 €/MWh midday, offpeak 35; peaker $K = 60$, must-run $K = 20$.

- Must-run: production and FD deltas both 12.00 (peak) / 11.86 (offpeak, noise asymmetry) — no boundary, no difference; identity 23.86 = base.
- Peaker: production delta 5.40 peak / 0.00 offpeak; FD delta 5.26 peak. Identity holds: 5.26 + 0.00 = base 5.26. The ~3% gap between 5.40 and 5.26 is the boundary effect at hours priced 58–59.
- AOM caveat: hedge the peak block with the production delta (5.40) vs the FD delta (5.26) and compare residual PaR95 of daily P&L: **11.40 vs 11.08** — the production-delta hedge leaves more tail risk. Small here by construction; in practice the gap concentrates exactly in the high-price scenarios the hedge exists for.

`labs/baseload_peak_offpeak_delta_decomposition/compute.py` reproduces every figure; its test asserts the identity, the must-run exactness, the boundary gap, and the residual-PaR ordering.

## Market variants

- **EU/GB:** block products are peak/base (EEX) or peak/base/weekend (GB); the decomposition discipline is what a desk runs daily before the 15:00 curve snapshot; EFA-block products in GB make the split three-way.
- **US:** 5x16/7x24 products with the same logic; the boundary effect is strongest in evening ramp hours.
- **CN:** 中长期 contracts split into 尖峰/峰/平/谷 buckets by province; the decomposition must use the *provincial* bucket definitions, and the incremental delta question becomes "which 峰谷 boundary hours flip when the 现货 price moves" — directly relevant to placing 中长期 volume across buckets.

## Common errors

1. **Production delta as hedge ratio** — the AOM caveat; use value (FD) deltas.
2. **Forgetting the identity check** — if $\Delta_{peak} + \Delta_{offpeak} \ne \Delta_{base}$, the implementation is wrong, full stop.
3. **Double counting peak hours** — peak hours sit inside baseload; selling base *and* the full peak delta hedges them twice. The tradable split is base + *incremental* peak (the part the base position doesn't already cover).
4. **Static decomposition** — the boundary moves with the curve and with volatility; deltas recomputed monthly are stale by construction.
5. **Ignoring noise asymmetry** — weekday/holiday counts and profile noise make offpeak deltas non-trivial even for "peak" plants.

## Assessable questions

1. Prove $\Delta_{base} = \Delta_{peak} + \Delta_{offpeak}$ from the linearity of the block shocks.
2. Why do production and FD deltas coincide for a must-run plant but not for a peaker?
3. What is the incremental peak delta, and where in the price distribution does it live?
4. State the AOM caveat in one sentence and its practical consequence.
5. A desk sells baseload equal to total expected generation and then sells the full peak FD delta as well. What is wrong, and what is the correct second leg?
