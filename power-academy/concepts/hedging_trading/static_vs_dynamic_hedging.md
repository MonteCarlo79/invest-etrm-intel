---
id: static_vs_dynamic_hedging
track: hedging_trading
level: intermediate
prerequisites:
- delta_sensitivity_and_hedge_volume
markets:
- EU
- GB
- US
status: drafted
sources:
- id: likron_powerplants
  use: background
- id: worldpower2009_realisticpowerplantvaluation
  use: background
- id: energy_risk_2012___kyos_power_plant_hedging_strategies
  use: derivation_reference
originality: synthesized
translations:
  zh:
    status: drafted
    en_hash: 560cd6df0d3c4406
---
## Learning objectives

- Define static hedging (set once, hold to delivery) and dynamic hedging (rebalance as the delta moves).
- Show that in a diffusion, rebalancing materially reduces hedge error — and quantify it.
- Show that through price gaps/jumps, delta rebalancing does *not* rescue you, and explain why: you are short the tail.
- Choose between the two for a plant book, given its optionality and the market's jumpiness.

## Intuition

A plant is long optionality (it holds $\max(\text{spread}, 0)$, not the spread), so hedging it with linear instruments makes the desk **short delta**. The hedge-error experiment is the cleanest way to feel what that means: short an option and hedge it with the underlying.

**Static** hedging sets the delta once and holds. Every wiggle of the price away from the hedge point creates error, but the errors of a smooth random walk partly cancel. **Dynamic** hedging re-sets the delta as the price moves — in a diffusion this is strictly better: you are always hedged at the *current* delta, so the residual error per step shrinks. Textbook result, and the lab reproduces it: hedge-error std falls by more than half with intra-week rebalancing.

**Then reality adds jumps.** When the price gaps, there is no price at which to rebalance "on the way through" — the move happens discontinuously, and the short-delta book pays the gap in full. Worse: the plant's payoff is convex, so the gap's effect on your short-delta position is asymmetric — you lose on spikes, and no rebalance frequency fixes that. The correct instrument for the tail is an option, not more rebalancing. This is why desks hedge the smooth part dynamically and buy the tail explicitly.

## Formal treatment

**The experiment.** Short a weekly ATM call (value $C_0$), delta-hedged. Hedge error after one week:

$$\text{err} = -C_T + \sum_i \Delta_i\,(S_{i+1} - S_i) + C_0$$

**Static:** $\Delta_i = \Delta_0$ for all $i$ (hedged once at week start). **Dynamic:** $\Delta_i = \Delta(S_i, \tau_i)$ re-computed each step.

In a diffusion, $\mathbb{E}[\text{err}] \approx 0$ for both, but $\text{std}(\text{err}_{dyn}) < \text{std}(\text{err}_{stat})$ — the residual is the gamma term $\sum \frac12 \Gamma_i (\Delta S_i)^2$, and finer steps make each step's gamma error smaller. With jumps, the dominant term is the gap itself: $-\big[\text{payoff}(S^+) - \text{payoff}(S^-)\big] + \Delta\,(S^+ - S^-)$, which is negative when short convexity, and independent of step size.

## Worked example

Weekly ATM calls ($S_0 = K = 55$ €/MWh, weekly vol 3 €/MWh) re-issued over 26 weeks, 4 000 paths (seed 23); dynamic = 5 rebalances/week.

- Diffusion: hedge-error std 5 → 2 with rebalancing (**−60%**), mean ≈ 0 both.
- With jumps (8% chance per step, mean size 10): mean error turns **negative** (−42 static, −37 dynamic over the 26 weeks — you are short the tail and it shows up), std rises to ~20 for both regimes. Rebalancing does not rescue it.

`labs/static_vs_dynamic_hedging/compute.py` reproduces all four numbers; its test asserts the diffusion improvement and the jump failure.

## Market variants

- **EU/GB:** intraday liquidity makes genuine dynamic hedging feasible for the front weeks; further out, hedges are effectively static blocks (monthly products) and the discussion is about rebalance *dates* rather than continuous adjustment.
- **US:** ERCOT's gap-like moves are the canonical case where delta-only hedges fail; desks hold explicit spike options/scarcity products for the tail.
- **CN:** 限价 caps bound the gap size (the tail is administratively truncated), but 中长期 placement windows make *all* hedging quasi-static anyway — the dynamic/static question becomes "how often can I adjust 签约量".

## Common errors

1. **Assuming rebalancing always helps** — it helps in diffusions, not through gaps; know which regime you're in before paying the transaction costs.
2. **Delta-hedging the tail with the underlying** — a linear instrument cannot cover a convex payoff through a discontinuity; buy the option for the tail.
3. **Ignoring the cost of the roll** — every rebalance crosses the spread; in thin far-dated products the cost eats the error reduction.
4. **Static means set-and-forget forever** — even a static hedge needs re-measurement against the current ladder; "static" describes rebalancing frequency, not measurement.
5. **Comparing regimes on one path** — the comparison is distributional (std of hedge error over many paths), not a single week's outcome.

## Assessable questions

1. Write the hedge-error decomposition for a short-option delta-hedged book.
2. Why does finer rebalancing reduce hedge error in a diffusion but not through a gap?
3. In the worked example, why is the mean hedge error ≈ 0 in the diffusion but negative with jumps?
4. Your plant book is short delta in a market that gaps weekly. What is the right instrument for the tail, and why not just rebalance more often?
5. 限价 caps both spike size and crash size. How does that change the static/dynamic calculus for a CN plant book?
