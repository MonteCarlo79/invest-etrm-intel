---
id: rolling_intrinsic_strategy
track: hedging_trading
level: intermediate
prerequisites:
- baseload_peak_offpeak_delta_decomposition
- forward_curve_structure_and_products
markets:
- EU
- GB
status: drafted
sources:
- id: plant_hedging_and_trading_strategies_kyos_20110530
  use: derivation_reference
- id: dipeng_gas_storage
  use: practice_example
- id: dipeng_models
  use: practice_example
originality: synthesized
translations:
  zh:
    status: none
    en_hash: null
---
## Learning objectives

- Define the rolling intrinsic strategy: periodically re-dispatch against the current forward curve and adjust the hedge to the new intrinsic schedule.
- Explain what the strategy locks in (the intrinsic value path as the curve moves) and what it cannot capture (extrinsic value, and the final curve-to-spot gap).
- Decompose the tracking error between strategy P&L and realised intrinsic, and identify its main driver.
- State when rolling intrinsic is the right benchmark strategy and when a delta-driven or option-based approach beats it.

## Intuition

Rolling intrinsic is the workhorse strategy of asset-backed trading, and the simplest honest answer to "how should I hedge a plant?" The rule: every day (or week), recompute the intrinsic schedule — run the plant against *today's* forward curve — and trade the hedge to match it. If the curve rises and new hours come into the money, sell the extra expected volume; if it falls, buy back. Every adjustment locks in a bit of the intrinsic value at current prices, so as the curve wanders, the strategy harvests the intrinsic value of the path it actually took.

The name tells you what you get: **intrinsic** value, realised *rolling*. Two things it does not give you. First, extrinsic value — the optionality above intrinsic stays unmonetised (you'd need option sales or a smarter dynamic strategy for that). Second, a perfect lock: the hedge is set to the curve *at each rebalance*, but settlement happens at delivery spot. The difference between the volume-weighted average of your entry prices and the final spot is the **tracking error**, and its size is driven by how much the curve moves (curve volatility), not primarily by how often you rebalance. Daily rebalancing is standard not because it eliminates tracking error — it doesn't — but because it keeps each individual adjustment small and liquid.

## Formal treatment

**The strategy.** At each rebalance date $t$: compute the intrinsic schedule $q^{int}(F_t)$ (run MWh per product from the current curve); set the hedge position to it: $H_t = q^{int}(F_t)$; trade the adjustment $H_t - H_{t-1}$ at current prices.

**P&L decomposition.** Final P&L = plant margin (realised spot) + hedge P&L:

$$\text{P&L} = \underbrace{\sum_m \pi(\text{spot}_m)}_{\text{realised dispatch}} + \underbrace{\sum_m H_m \big(\bar F^{entry}_m - \text{spot}_m\big)}_{\text{tracking error}}$$

with $\bar F^{entry}_m$ the volume-weighted average entry price of month $m$'s position, built up over the rebalances. The first term is what the plant physically earned; the second is the strategy's implementation slippage, with $\text{std} \propto H \times \sigma_{curve}$.

**What it captures.** On average, the strategy's mean P&L ≈ mean realised intrinsic — it is an unbiased harvester of the intrinsic value of the realised curve path. Its outcome variance is lower than unhedged because curve moves are monetised progressively rather than all at delivery. What remains unhedged: the convex (option) part of the margin and the final curve-to-spot gap.

## Worked example

Synthetic year (500 paths, seed 19): 12 monthly products, per-month forward curve vol 4 €/MWh, daily rebalancing, plant 100 MW at $K = 52$ €/MWh with intra-month daily dispersion 8 €/MWh (Bachelier margin).

- Strategy mean P&L 7.41 M€ vs realised intrinsic mean 7.42 M€ — the strategy is an unbiased intrinsic harvester (<1% gap).
- Outcome std: 1.60 M€ strategy vs 2.21 M€ unhedged (−28%).
- Tracking error scales with curve vol: std 1.73 M€ at vol 4, 0.85 M€ at vol 2, 0.42 M€ at vol 1. The share of paths landing within 5% of realised intrinsic rises from 18% to 48% as curve vol halves — at realistic curve vols, single-path tracking is *not* tight; the strategy's promise is unbiasedness plus variance reduction, not per-path precision.
- Rebalance frequency (weekly vs daily) changed the tracking error by only a few percent in this toy — the curve vol dominates.

`labs/rolling_intrinsic_strategy/compute.py` reproduces all figures; its test asserts the unbiasedness, the variance reduction, the vol scaling, and the low-vol convergence.

## Market variants

- **EU/GB:** the documented KYOS benchmark; daily re-hedging against EEX/ICE closing curves is the desk standard for CCGT and storage (the practice sources include gas-storage rolling intrinsic).
- **US:** PJM/ERCOT versions run against the monthly strips; in ERCOT, intraday vol makes the tracking error the dominant consideration.
- **CN:** the strategy reads as 滚动更新现货敞口: place 中长期 volume to cover the current optimal dispatch schedule, and roll it as the curve and the dispatch model update — with the caveat that CN 中长期 trades in windows, so the "daily rebalance" becomes "each placement window".

## Common errors

1. **Expecting per-path precision** — rolling intrinsic is unbiased with reduced variance; judging it on one path's gap to realised intrinsic misunderstands what it sells.
2. **Blaming rebalance frequency** — tracking error is curve-vol driven; rebalancing more often helps marginally, not structurally.
3. **Confusing it with delta hedging** — rolling intrinsic hedges the *intrinsic schedule*, not the FD delta ladder; near the money the two diverge and the delta ladder is the better position.
4. **Ignoring transaction costs** — every adjustment is a trade; in thin far-dated products, the cost of the roll can eat the variance benefit.
5. **Calling the leftover "alpha"** — the residual extrinsic value is a known, priced quantity (see the valuation track), not free money the strategy failed to catch by accident.

## Assessable questions

1. Write the rolling-intrinsic rule as a three-line algorithm.
2. Decompose the strategy P&L into its two terms and name the driver of the second.
3. Why is the strategy unbiased for intrinsic value but not for total (intrinsic + extrinsic) value?
4. Your desk reports a 1.7 M€ tracking-error std on a 7.4 M€ book. Curve vol halves next year. What do you report?
5. A province only opens 中长期 placement windows monthly. What changes in the strategy, and what grows?
