---
id: intrinsic_vs_extrinsic_value
track: asset_valuation
level: foundation
prerequisites:
- spark_dark_spread_fundamentals
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
- id: valuing_generation_assets___overview___spark_spread_option_v
  use: background
- id: clewlow
  use: background
originality: synthesized
translations:
  zh:
    status: drafted
    en_hash: 321d84b78b6ff05d
---
## Learning objectives

- Define intrinsic value, hourly intrinsic value, and extrinsic (real option) value for a generation asset.
- Compute intrinsic value from a forward spread curve and explain exactly which assumptions it makes.
- Show that total plant value = intrinsic + extrinsic, and that extrinsic value is always non-negative for a free option.
- Explain why perfect-foresight dispatch is *not* what intrinsic value assumes — the two are different mistakes in opposite directions.

## Intuition

Suppose you locked in today's forward prices for power and fuel and dispatched the plant against those fixed numbers: run when the spread is positive, stay off when it is negative. The profit of that schedule is the plant's **intrinsic value** — what the plant is worth if the future turns out exactly as today's forward curve says, with no uncertainty left.

Real prices move. A flexible plant gains from that movement: when spreads blow out it runs harder, when they collapse it sits still. It holds, in effect, a strip of options on the spread — and options are worth *more* when volatility rises. The extra value above intrinsic, earned purely from flexibility in the face of uncertainty, is the **extrinsic value** (or real option value). The decomposition

$$\text{Plant value} = \text{Intrinsic} + \text{Extrinsic}$$

is the backbone of every plant valuation in this track: intrinsic is what you can hedge today by selling forwards, extrinsic is what you can only capture by dispatching well (or by selling the flexibility, e.g. via a toll).

## Formal treatment

For delivery hours $t = 1..T$ with forward power price $F_t$ and fuel-adjusted cost $K_t$ (fuel + carbon at the plant's heat rate), define the forward spark spread $S_t = F_t - K_t$.

**Intrinsic value** (switching model, per unit capacity):

$$V_{int} = \sum_t \max(S_t, 0)$$

i.e. the plant runs in every hour whose forward spread is positive. **Hourly intrinsic** refines this by shaping monthly/quarterly forward quotes to hourly granularity before taking the max — it captures peak/off-peak shape but still treats the curve as certain.

**Extrinsic value.** Under a stochastic model of the spread, let $\tilde S_t$ be the random future spread. A flexible plant earns $\mathbb{E}[\max(\tilde S_t, 0)]$ per hour, so

$$V = \sum_t \mathbb{E}\big[\max(\tilde S_t, 0)\big] \ge \sum_t \max(\mathbb{E}[\tilde S_t], 0) = V_{int}$$

where the inequality is Jensen's: $\max(\cdot, 0)$ is convex, so uncertainty *adds* value to a flexible plant. The gap $V - V_{int}$ is the extrinsic value, and it grows with volatility, correlation structure between power and fuel, and time to expiry.

**What intrinsic is not.** Intrinsic value fixes the *price path*, not the dispatch clairvoyance. Perfect-foresight dispatch — re-optimising each realised day with full knowledge of that day's prices — ignores start costs and constraints you cannot undo, and *overstates* achievable value; intrinsic with forward prices and static commitment *understates* it. Real operations sit between: decide under uncertainty, honour inter-temporal constraints.

## Worked example

A 1 MW (per-unit) plant, four delivery hours, forward spreads $S = (-10, +30, -5, +50)$ €/MWh.

- Intrinsic: $0 + 30 + 0 + 50 = 80$ €/MWh of capacity.
- Now let each hour's realised spread move ±20 around the forward value with 50/50 probability. Expected per-hour profit: hour 1: $(10+0)/2 = 5$; hour 2: $(50+10)/2 = 30$; hour 3: $(15+0)/2 = 7.5$; hour 4: $(70+30)/2 = 50$. Total $V = 92.5$.
- Extrinsic: $92.5 - 80 = 12.5$ €/MWh — created entirely in hours 1 and 3, where uncertainty turned a negative-spread hour into a 50% chance of profit. Hours already deep in/out of the money gain nothing from small symmetric uncertainty, which is why extrinsic value concentrates around the money.

`labs/intrinsic_vs_extrinsic_value/compute.py` reproduces this table; the test asserts intrinsic 80, total 92.5, extrinsic 12.5.

## Market variants

- **EU/GB:** high renewable penetration has made intraday spread shapes spikier, raising the extrinsic share of flexible-asset value; clean spreads (with EUA/UKA) are the correct underlying.
- **US:** gas-price basis volatility adds a second source of extrinsic value; heat-rate call options traded OTC are literally packaged extrinsic value.
- **CN:** the intrinsic/extrinsic split maps to 中长期 vs 现货 thinking — margin locked via 中长期 contracts is intrinsic-like; the residual captured through spot and balancing is extrinsic-like. Because CN spot prices are capped and floors exist (限价), extrinsic value is bounded differently than in uncapped markets.

## Common errors

1. **Reporting intrinsic as "the plant's value"** — it omits exactly the component (flexibility) that justifies owning the physical asset over a forward strip.
2. **Perfect-foresight backtests** — re-optimising with known prices and calling the result achievable; constraints and start costs make yesterday's optimum unattainable.
3. **Adding extrinsic value to deep ITM/OTM hours** — extrinsic concentrates near the money; a blanket "volatility uplift %" on all hours double-counts.
4. **Using spot instead of forward spreads for intrinsic** — intrinsic is defined against the tradable forward curve; yesterday's spot spread is not hedgable today.
5. **Ignoring sign of convexity** — a plant that *must* run (must-take PPA, heat-led CHP) has negative extrinsic value in bad states; flexibility is what makes extrinsic non-negative.

## Assessable questions

1. Compute intrinsic value for spreads $S = (-20, +15, +40, -10)$ €/MWh.
2. In the worked example, which two hours create the extrinsic value and why do hours 2 and 4 contribute none?
3. State Jensen's inequality and use it to show extrinsic value ≥ 0 for a flexible plant.
4. Why does higher correlation between power and gas prices *reduce* extrinsic value? (Think about the volatility of the spread.)
5. Your colleague backtests a dispatch strategy with perfect foresight and reports 120% of the plant's option value. Name two modelling errors that make this number unreachable.
