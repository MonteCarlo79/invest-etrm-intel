---
id: delta_sensitivity_and_hedge_volume
track: hedging_trading
level: intermediate
prerequisites:
- baseload_peak_offpeak_delta_decomposition
- forward_curve_structure_and_products
markets:
- EU
- GB
- US
status: drafted
sources:
- id: energy_risk_2012___kyos_power_plant_hedging_strategies
  use: derivation_reference
- id: plant_hedging_and_trading_strategies_kyos_20110530
  use: derivation_reference
- id: powerhedging
  use: background
originality: synthesized
translations:
  zh:
    status: drafted
    en_hash: 3d874a53e223b09e
---
## Learning objectives

- Convert a per-product finite-difference delta ladder into hedge volumes in MWh per contract.
- Adjust for product overlap (peak hours live inside baseload) so the position doesn't double-hedge.
- Measure what the hedge did: residual earnings distribution and residual PaR after the position settles.
- Define rebalance triggers: when and why the ladder — and therefore the volumes — must be recomputed.

## Intuition

A delta ladder is a table of sensitivities per product per period. A hedge is an order: "sell 240 MWh of March peak, 480 MWh of March base." This concept is the arithmetic between the two — and the one place the arithmetic goes wrong in practice.

The trap is overlap. Peak hours are a subset of base hours. If the ladder says $\Delta_{base} = 24$ and $\Delta_{peak} = 12$ and you sell both in full, the peak hours are hedged twice — you are short the same volume twice, and a price rise hits you twice. The correct structure: sell the **baseload volume for the whole curve**, then sell only the *incremental* peak sensitivity that the base position doesn't already cover. The decomposition identity from the previous concept is what makes the adjustment exact.

Finally: a hedge is not done when it is placed. The delta ladder moves with the forward curve, volatility, outages, and the season. The position must be measured (residual PaR against the objective from *profit_at_risk_and_hedging_objective*) and rebalanced on defined triggers, not on hunches.

## Formal treatment

**Volumes from the ladder.** For period $m$ with $D_m$ delivery days and per-day FD delta $\delta^{fd}_{B,m}$ (hours-equivalent per day at unit capacity), the hedge volume in MWh is

$$V_{B,m} = \bar q \cdot D_m \cdot \delta^{fd}_{B,m}$$

with $\bar q$ the plant capacity. (Hours-equivalent delta × capacity × days = MWh.)

**Overlap adjustment.** The base leg hedges every hour, peak included. So the peak leg should carry only the part of peak sensitivity *not* already inside the base delta:

$$V^{trade}_{base,m} = \bar q D_m \delta^{fd}_{base,m}, \qquad V^{trade}_{peak,m} = \bar q D_m \big(\delta^{fd}_{peak,m} - \delta^{prod}_{peak,m}\big)$$

where the second leg is the incremental peak delta from the previous concept. (Conventions vary — some desks trade base+peak with the peak leg netting the base-covered peak hours; what matters is that peak hours end up hedged exactly once.)

**Measuring the hedge.** Settle the position against the realised block price: hedged P&L $= \pi_{plant} + V \cdot (F_{entry} - F_{settle})$. Compare the residual PaR against the unhedged figure and against the tolerance from the risk objective.

**Rebalance triggers.** Curve move beyond a band (e.g. ±5 €/MWh), delta drift beyond a fraction of volume, scheduled frequency (weekly near delivery, monthly further out), and events (outage, fuel switch, new quotes).

## Worked example

Continuing the previous concept's synthetic month (20 weekdays, per-day figures): peaker FD deltas base 6.08, peak 6.08, production peak 6.00 h-equiv/day; incremental peak $6.08 - 6.00 = +0.08$.

- Tradeable volumes at 100 MW: base $100 \times 20 \times 6.08 = 12{,}160$ MWh; incremental peak $100 \times 20 \times 0.08 = 160$ MWh.
- Residual PaR95: unhedged 6 020 € → hedged **1 999 €** (−67%), when the plant's risk is dominated by block-level moves.
- The same hedge applied when the plant's P&L variance is dominated by *idiosyncratic hourly noise* (block average barely correlates with the plant's few running hours) cuts variance only slightly and can even worsen the left tail: the block forward hedges block-level moves, not your profile or volume risk. Hedge effectiveness is a correlation statement before it is a volume statement.

`labs/delta_sensitivity_and_hedge_volume/compute.py` reuses the decomposition machinery; its test asserts volume conversion, the overlap adjustment, and the residual-PaR reduction.

## Market variants

- **EU/GB:** volumes are traded in MW (rate) per product — a 10 520 MWh month position ≈ 14.6 MW round-the-clock; EEX/OTC tickets are in MW with block definitions from the curve concept.
- **US:** same conversion with 5x16/7x24 products; weekend products carry their own leg.
- **CN:** 中长期 volumes are placed in 分时段 buckets per province; the "incremental peak" leg is the 峰段 extra beyond the 平段 base; settlement is against the province's spot block averages, and 偏差考核 changes the residual-PaR calculation.

## Common errors

1. **Double-hedging peak hours** — full base plus full peak; the classic desk error the incremental leg exists to prevent.
2. **Delta drift** — placing the right volume on a stale ladder; recompute triggers must be defined before the position, not after a loss.
3. **Ignoring contract size conventions** — tickets in MW vs MWh; a "24" that means 24 MW for the month is 17 280 MWh, not 24 MWh.
4. **Never measuring the residual** — the hedge is judged by residual PaR vs objective, not by whether the market moved your way afterwards.
5. **Hedging expected production instead of delta** — the recurring theme of this track; volumes come from value sensitivities.

## Assessable questions

1. A plant's per-day FD deltas for March (20 delivery days) are base 6.2, incremental peak 1.1 h-equiv at 250 MW. Compute the two tradeable volumes.
2. Explain why the peak leg uses the *incremental* delta and what goes wrong otherwise.
3. Write the settlement P&L of a forward hedge and identify each term.
4. Name three rebalance triggers and justify each.
5. Your residual PaR after hedging equals the unhedged PaR. List two distinct explanations to check first.
