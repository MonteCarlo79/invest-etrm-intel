---
id: power_plant_economics_and_dispatch
track: hedging_trading
level: foundation
prerequisites: []
markets:
- EU
- GB
- US
status: drafted
sources:
- id: plant_hedging_and_trading_strategies_kyos_20110530
  use: background
- id: energy_risk_2012___kyos_power_plant_hedging_strategies
  use: background
originality: synthesized
translations:
  zh:
    status: none
    en_hash: null
---
## Learning objectives

- Write down the plant's hourly profit as a function of the spread, output, and start costs.
- Derive the run/stop decision rule with and without start costs, and the role of min up/down.
- Compute expected run hours and expected margin per period — the two quantities the hedging track hedges.
- Explain why "expected generation" is a random variable, not a parameter, and why that drives the whole hedging problem.

## Intuition

Before you can hedge a plant you must know *what the plant will do* — and the honest answer is: it depends on prices. A thermal plant is not a factory that produces a fixed volume you can simply sell forward. It is a decision-maker that runs when running pays and sits out when it doesn't. Tomorrow's volume is itself a function of tomorrow's prices.

This has two consequences that organise the whole track. First, the quantity to hedge (expected generation) is stochastic and *correlated with the price* you hedge it at — selling your expected volume forward is not a clean hedge, because in the scenarios where prices are high you also produce more. Second, the plant's profit splits into a part you can lock in today (the forward-margin structure) and a part that remains floating (the price-responsive, option-like part). The hedging problem is deciding how much of which part to sell, when, and in which products.

## Formal treatment

**Hourly economics.** Per hour, a unit with capacity $\bar q$ (MW) and marginal cost $K$ (€/MWh, fuel + carbon + VOM at the relevant load point) facing power price $P$ earns, if it runs, margin $P - K$ per MWh. Ignoring inter-temporal constraints, the optimal rule is

$$\text{run} \iff P > K \quad\Rightarrow\quad \pi = \bar q \cdot \max(P - K, 0).$$

**Start costs and min up/down.** Starting costs $c_{start}$ and must-run commitments change the rule: run through hours with $P < K$ if the loss is smaller than the stop-start cost, and start only when the expected run covers the start (see *dispatch_optimisation_and_delta_hedging* in the valuation track for the full DP). For hedging purposes the practical output of the dispatch model is a **run profile** $q_t(P)$: expected run MWh per period conditional on the price path.

**The two hedging quantities.** Per delivery period (month, quarter):

$$\text{Expected generation } Q = \mathbb{E}\Big[\sum_t q_t\Big], \qquad \text{Expected margin } M = \mathbb{E}\Big[\sum_t q_t (P_t - K_t)\Big].$$

Both are expectations over the *joint* distribution of prices and the dispatch response. Note the covariance problem: $Q$ and the average price $\bar P$ are positively correlated for a flexible plant, so $\mathbb{E}[Q \bar P] > \mathbb{E}[Q]\,\mathbb{E}[\bar P]$ — selling $\mathbb{E}[Q]$ forward leaves this covariance unhedged. That gap is the subject of the delta concepts in this track.

## Worked example

A 100 MW unit, $K = 55$ €/MWh, no constraints, 24h toy path with prices (€/MWh): night hours 1–6 at 40, morning ramp 7–9 at 58, midday dip 10–15 at 48, evening peak 16–21 at 75, late 22–24 at 52.

- Run hours (rule $P > K$): hours 7–9 and 16–21 → 9 h. Gross margin: $3 \times 100 \times (58-55) + 6 \times 100 \times (75-55) = 900 + 12{,}000 = 12{,}900$ €.
- With a 5 000 € start cost and the unit offline at hour 0, evaluate *blocks*: the morning block nets $900 - 5{,}000 < 0$ — don't start for it. The evening block nets $12{,}000 - 5{,}000 = 7{,}000$ € — start at 16:00. Final: 6 run hours, 1 start, 7 000 €.
- Would extending the evening run *backward* through the dip to also catch the morning help? Extra margin $900 - 4{,}200 = -3{,}300$ € (dip loss $6 \times 100 \times 7 = 4{,}200$) — no. The chaining question only arises once a block *already* justifies its start; the comparison is then dip loss vs a *second* start cost.

`labs/power_plant_economics_and_dispatch/compute.py` reproduces this rule and the numbers; its test asserts the run hours, the chained decision, and the margin.

## Market variants

- **EU/GB:** the run/stop rule applies to clean spreads (carbon in $K$); negative midday prices from solar make the "run through the dip" calculation a daily reality for conventional fleets.
- **US:** unit commitment is centralised (PJM/MISO) — the plant submits costs and the ISO optimises; the economics are the same but the decision is partially outsourced.
- **CN:** 现货 + 中长期 means the run decision applies only to the merchant share; contracted 中长期 volume runs regardless, and the floating part is the residual — hedge analysis must separate the two shares.

## Common errors

1. **Hedging expected volume as if it were fixed** — the covariance between volume and price is precisely what a plant hedge must manage.
2. **Ignoring start economics in the run rule** — the $P > K$ rule is the limiting case; real plants run through small losses to avoid stop-start cycles.
3. **Using nameplate cost at partial load** — the marginal $K$ rises at low load; dispatch and hedge both need the curve.
4. **Mixing contracted and merchant volume** — in markets with PPAs or 中长期 blocks, only the residual is price-responsive.
5. **Treating the run profile as static** — it moves with the forward curve, fuel prices, and outages; yesterday's profile is not today's hedge.

## Assessable questions

1. Derive the run/stop rule without constraints and state exactly which assumption each part of it needs.
2. In the worked example, why is running through the midday dip better than two starts? At what dip price would two starts win?
3. Show that for a flexible plant $\mathbb{E}[Q\bar P] > \mathbb{E}[Q]\mathbb{E}[\bar P]$. What does this imply for a naive volume hedge?
4. Why does expected generation rise when the forward curve rises?
5. A 售电公司 holds a 中长期 contract covering 80% of a plant's expected output. Which share of the plant's economics should the hedging analysis focus on?
