---
id: piecewise_replication_and_ccgt_intrinsic_modelling
track: asset_valuation
level: advanced
prerequisites:
- intrinsic_vs_extrinsic_value
- spread_option_pricing_models
- heat_rate_and_plant_parameters
markets:
- EU
- GB
status: drafted
sources:
- id: ccgt_replication
  use: derivation_reference
- id: dipeng_valuation_reports
  use: practice_example
- id: valuing_generation_assets___overview___spark_spread_option_v
  use: background
originality: synthesized
translations:
  zh:
    status: none
    en_hash: null
---
## Learning objectives

- State the replication idea: any piecewise-linear dispatch payoff is exactly a portfolio of standard call options.
- Build the option decomposition of a multi-unit station's hourly payoff and identify each option's strike and volume.
- Replicate a smooth (continuous heat-rate curve) CCGT payoff with $N$ linear segments and bound the error.
- Explain what replication buys in practice: instant intrinsic values, Greeks by summation, and portfolio-level aggregation.

## Intuition

A single-unit plant with a fixed heat rate has the simplest payoff in this track: $\max(q(S - K), 0)$ — one call option. Real stations are messier: several units of different efficiency, or one turbine whose heat rate varies continuously with load. But there is a structural fact that tames all of it — dispatch profit as a function of the spread is always **convex and piecewise linear** (or well-approximated by such). And every convex piecewise-linear function is a finite sum of hockey sticks, i.e. of call options with strikes at the kinks and volumes given by the slope increments.

This is the replication trick, and it turns plant valuation into portfolio arithmetic. Once the payoff is written as $\sum_i w_i \max(S - K_i, 0)$, the value under *any* price model is the same weighted sum of option prices, and the Greeks come out by summing the option Greeks. It also exposes the economics: a station *is* a ladder of options, one per efficiency tier.

## Formal treatment

**Two-unit station (exact).** Unit A: $q_A = 300$ MW at $HR_A = 2.0$ (cost $K_A = 60$ €/MWh at $G = 30$); unit B: $q_B = 200$ MW at $HR_B = 2.4$ ($K_B = 72$). Merit order runs A first, B only when the spread covers B's cost:

$$\pi(S) = 300\max(S - 60, 0) + 200\max(S - 72, 0)$$

The payoff is piecewise linear with kinks at 60 and 72; slopes 0 → 300 → 500. This is not an approximation — it is the exact payoff, and the strikes/volumes read directly off the merit order: **one call option per unit, strike at the unit's marginal cost, volume at the unit's capacity.**

**Smooth heat-rate curve (approximate).** With $HR(q)$ varying continuously, the optimal output $q^*(S)$ rises smoothly and $\pi(S) = \max_q\, qS - C(q)$ is smooth and convex. Replicate with segments: choose spreads $K_0 < K_1 < \dots < K_N$, and set weights $w_i$ as slope increments $\pi'(K_i^+) - \pi'(K_i^-)$. Then

$$\hat\pi_N(S) = \sum_{i=0}^{N} w_i \max(S - K_i, 0) \;\xrightarrow[N \to \infty]{}\; \pi(S)$$

with uniform error shrinking like $O(1/N)$ on a bounded spread range. The replication's intrinsic value is $\sum_i w_i \max(F - K_i, 0)$ per forward price $F$ — no dispatch optimisation needed at valuation time.

**Segments and time buckets.** Practice material decomposes further by time-of-use segment (weekday peak, off-peak, weekend): fit the option regression per bucket because the price distribution and the plant's competitive position differ by bucket.

## Worked example

Part 1 (exact): on a price grid $S \in \{40, 55, 60, 65, 72, 80, 100\}$, direct dispatch of the two-unit station vs the two-option formula — identical to the cent (the lab asserts this).

Part 2 (approximate): a CCGT with quadratic cost $C(q) = 30\,(q + 0.0008\,q^2)$ for $q \in [0, 500]$ MW (rising marginal heat rate: $C'(q) = 30 + 0.048q$). Optimal dispatch gives a smooth convex $\pi(S)$; an 8-segment replication with strikes concentrated in the bending region (30–60 €/MWh, where $q^*$ is interior) is built from secant-slope increments plus the constant $\pi(K_0)$ — the interpolation lies above a convex payoff, with error $\sim \pi''h^2/8$. The lab asserts max *absolute* error below 50 € — well under 1% of the full-load payoff at $S = 100$ (29 000 €), and notes that *relative* error is meaningless near $\pi \approx 0$.

The payoff for the smooth case: marginal cost $C'(q) = 30 + 0.048q$… note the units in the lab: at $q^*$ where $S = C'(q^*)$, so $q^* = (S - 30)/0.048$ capped at 500; e.g. $S = 54$ → $q^* = 500$, full load.

## Market variants

- **EU/GB:** replication is how trading desks quote station intrinsic daily across portfolios — per-unit option ladders summed over countries, with clean (carbon-in) strikes; time-of-use buckets match the peak/offpeak block structure of EEX/GB forward products.
- **US:** same ladder with heat-rate call options as the quoted instrument; PJM/MISO offer curves are literally submitted as piecewise-linear segments, so the market *is* the replication.
- **CN:** hourly price discovery is younger and 中长期 blocks dominate, but the structure carries: a 火电 fleet is a ladder of options on 现货−煤耗价差 with strikes at each unit's 煤耗, and the replication view is the cleanest way to see how much of the fleet is in-the-money at a given spot level.

## Common errors

1. **Forcing a single strike** — valuing a two-unit station with one "average" heat rate; the value error is exactly the optionality between the two kinks, which is where peaking value lives.
2. **Slope/volume mismatch** — assigning the slope increment to the wrong kink, which silently shifts value between strikes.
3. **Replicating on spot, valuing on forwards** — strikes fitted to a spot grid but priced with a forward curve of a different shape.
4. **Too few segments at the kink region** — error concentrates near the money; put more strikes where the payoff bends, not evenly spaced by habit.
5. **Re-fitting daily** — the ladder is a property of the plant, not the prices; re-fit when the plant changes (outage, upgrade, fuel switch), not because prices moved.

## Assessable questions

1. Write the option decomposition of a 3-unit station: 250 MW at 55 €/MWh, 150 MW at 68, 100 MW at 85.
2. Why is the dispatch payoff always convex in the spread? (Think about what $\max$ does to a family of linear functions.)
3. For the smooth-cost CCGT above, derive $q^*(S)$ and hence $\pi(S)$ in closed form on the interior range.
4. Where should replication strikes be concentrated and why does even spacing waste segments?
5. A desk reports station delta as the sum of option deltas weighted by segment volumes. Justify this in one line.
