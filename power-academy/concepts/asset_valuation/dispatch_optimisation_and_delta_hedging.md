---
id: dispatch_optimisation_and_delta_hedging
track: asset_valuation
level: intermediate
prerequisites:
- real_options_framework_for_generation_assets
- spread_option_pricing_models
- heat_rate_and_plant_parameters
markets:
- EU
- GB
- US
status: drafted
sources:
- id: dispatch_and_delta_hedging_v2
  use: practice_example
- id: dispatch_and_delta_hedging_v3
  use: practice_example
- id: dispatch_and_delta_hedging_v4
  use: practice_example
- id: energy_risk_2012___kyos_power_plant_hedging_strategies
  use: derivation_reference
- id: plant_hedging_and_trading_strategies_kyos_20110530
  use: derivation_reference
originality: synthesized
translations:
  zh:
    status: drafted
    en_hash: eb83d1e7e08f2c96
---
## Learning objectives

- Formulate plant dispatch as a constrained optimisation (DP/LP) with start costs and min up/down times, and compute the optimal profit.
- Show how much a naive heuristic leaves on the table and *where* (start-cost timing, running through small losses).
- Define plant delta two ways — average-production delta and finite-difference delta — and explain why delta is not simply expected generation.
- Turn a delta ladder into a forward hedging schedule.

## Intuition

Everything so far priced flexibility; this concept is about *running* it. Dispatch is the daily problem: given tomorrow's prices and the plant's constraints (start costs, min up/down, ramp), which hours do we run? It is a small dynamic program — the same state machine as the valuation concepts, but the output is a schedule, not a value.

Why do it properly instead of "run when the spread is positive"? Because the constraints make the locally right answer globally wrong: starting for two good hours and paying a cold start can be worse than staying off; running through one bad hour can be cheaper than a stop-start pair. The gap between optimal and greedy dispatch is real money, and it is the *same* money as the extrinsic value from earlier concepts — captured or lost in operation.

Once dispatch is a model, hedging becomes a sensitivity question: if tomorrow's price curve moves by 1 €/MWh, how much does my profit change, per period? That sensitivity is the plant's **delta** — the volume to sell forward — and it is *not* simply the expected run volume, because when prices rise the plant also runs more hours.

## Formal treatment

**Dispatch as DP.** State $(u, d)$ = consecutive hours on/off, value function $V_t(u, d)$ = max profit from hour $t$ on. Transitions encode the rules: online with $u < U$ must run; online with $u \ge U$ may stop; offline with $d \ge D$ may start (pay $c_{start}$). Backward induction gives the optimal schedule in $O(T \times U \times D)$ — trivially fast. (LP/MIP formulations solve the same problem when plants couple — shared fuel, portfolio limits.)

**Greedy benchmark.** Run when spread > 0, stop when < 0, constraints permitting. The optimal-greedy gap concentrates around sign-flip regions, where constraint costs live.

**Two deltas.** For a price product (day, week, month) with forward $F$:

1. **Average-production delta:** $\Delta_{prod} = \mathbb{E}[\text{run MWh in the period}]$ from Monte Carlo dispatch — the volume you expect to produce.
2. **Finite-difference delta:** $\Delta_{fd} = \frac{V(F + \epsilon) - V(F - \epsilon)}{2\epsilon}$ — the sensitivity of *value* to the product's price.

For a flexible plant $\Delta_{fd} \ge \Delta_{prod}$ in general: a price rise converts marginal offline hours into run hours, so value rises faster than current production volume. The hedging literature (KYOS) is explicit that the *value* delta is the hedge ratio, and that confusing it with expected volume leaves the portfolio under- or over-hedged exactly when prices move.

**Delta ladder → hedge.** Compute $\Delta_{fd}$ per forward period (per day for the near curve, per month further out): sell that volume of each product; rebalance as prices and the ladder move.

## Worked example

100 MW plant, min up 4h / min down 2h, start cost 2 000 €, synthetic week (seed 9: seasonal base 10 + 20·sin + OU noise):

- DP dispatch: profit 237 277 €, 118 run hours, 6 starts.
- Greedy: 215 334 €, 121 run hours — greedy runs *more* hours yet earns 21 943 € (−9%) less: it starts for hours that don't cover the start cost and stops where running through would be cheaper.
- Finite-difference deltas per day (ε = 1 €/MWh): 18.0, 10.3, 15.9, 17.0, 21.0, 15.8, 20.0 hours-equivalent vs actual run hours 18, 10, 16, 17, 21, 16, 20 — tracking closely, and slightly *above* run hours on marginal days (price rises add hours).

`labs/dispatch_optimisation_and_delta_hedging/compute.py` reproduces every number; its test asserts the gap, the constraint compliance, and that deltas track run hours within ±2h.

## Market variants

- **EU/GB:** day-ahead auction granularity makes the DP hourly (or 15-min in DE); intraday markets let the operator re-dispatch as the curve moves — the delta ladder is rebalanced intraday, not once.
- **US:** ERCOT/PJM require offer curves, not schedules — the DP output is converted into incremental offer blocks (the replication ladder from earlier).
- **CN:** 现货 + 中长期 means the DP runs on the *residual* merchant volume after contracted blocks; start-cost recovery rules (启动成本补偿) exist in some provinces and change the objective — model the compensation explicitly, as the platform's own BESS practice shows (放电量补偿).

## Common errors

1. **Greedy dispatch** — running when positive and calling it optimal; the gap is a measurable percentage of monthly margin.
2. **Delta = expected volume** — under-hedges exactly on the days the plant matters most; use the value (finite-difference) delta.
3. **Ignoring start-cost amortisation in the run decision** — comparing an hour's spread to zero instead of to the marginal start economics.
4. **Static hedge** — computing the ladder once a month; deltas move with price level, volatility, and outages.
5. **Modelling start costs but not min up/down** (or vice versa) — the two interact; one without the other misstates both the schedule and the deltas.

## Assessable questions

1. Why can greedy dispatch run *more* hours and still earn less than optimal dispatch?
2. State the two delta definitions and explain when they diverge most.
3. In the worked example, why are day-5 deltas (21.0) slightly above run hours (21)? What does that imply for hedging that day?
4. Formulate the dispatch DP's state and transition rules in four lines.
5. A province introduces start-cost compensation for thermal plants. How does the optimal schedule change, and what happens to the delta ladder?
