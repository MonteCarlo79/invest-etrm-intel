---
id: spread_option_pricing_models
track: asset_valuation
level: intermediate
prerequisites:
- spark_dark_spread_fundamentals
- intrinsic_vs_extrinsic_value
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
- id: erratumtoelectricityderivativeschapterspreads_66_70
  use: background
originality: synthesized
translations:
  zh:
    status: none
    en_hash: null
---
## Learning objectives

- Define a spread (exchange) option and explain why a thermal plant is a strip of them.
- Apply Margrabe's formula for $\max(F_1 - F_2, 0)$ and state each of its assumptions.
- Apply Kirk's approximation when a fixed strike $K$ enters the payoff, and know when the approximation is acceptable.
- Cross-check a closed form against Monte Carlo and read off the hedge ratios (deltas).

## Intuition

The previous concepts established that a plant earns $\max(\text{spread}, 0)$ per hour and that uncertainty makes this option-like payoff valuable. To *price* it we need an option model. The cleanest starting point is the exchange option: the right to swap one asset for another. A gas plant holds, for each delivery hour, the right to exchange fuel-plus-carbon (leg 2) for power (leg 1) — that is exactly Margrabe's setting.

One obstacle: electricity is not storable, so the classic "buy the underlying and wait" replication fails. The fix is to price off **forward** contracts, which *are* tradable: work with the forward prices $F_1, F_2$ for the delivery period and discount the expected payoff. This is why everything in this track is anchored to the forward curve rather than the spot price.

## Formal treatment

**Margrabe (strike zero).** For payoff $\max(F_1 - F_2, 0)$ at maturity $T$, with both legs lognormal, volatilities $\sigma_1, \sigma_2$, correlation $\rho$, and discount factor $DF$:

$$\sigma = \sqrt{\sigma_1^2 + \sigma_2^2 - 2\rho\sigma_1\sigma_2}, \qquad d_1 = \frac{\ln(F_1/F_2) + \tfrac12 \sigma^2 T}{\sigma\sqrt{T}}, \quad d_2 = d_1 - \sigma\sqrt{T}$$

$$V = DF \cdot \big[ F_1 N(d_1) - F_2 N(d_2) \big]$$

The whole two-asset problem collapses into one number — the *spread volatility* $\sigma$ — which is why correlation enters value so strongly (recall the previous concept: high $\rho$ compresses $\sigma$).

**Kirk (fixed strike $K$).** For $\max(F_1 - F_2 - K, 0)$ — e.g. the spread minus VOM and amortised start costs — approximate the shifted leg $F_2 + K$ as lognormal with rescaled volatility $\tilde\sigma_2 = \sigma_2 \cdot F_2 / (F_2 + K)$, then apply Margrabe to $\max(F_1 - (F_2 + K), 0)$ with $\tilde\sigma = \sqrt{\sigma_1^2 + \tilde\sigma_2^2 - 2\rho\sigma_1\tilde\sigma_2}$. The approximation is accurate for small $K$ relative to $F_2$ and short-to-medium maturities; verify against Monte Carlo when $K$ is large.

**Deltas (hedge ratios).** For Margrabe: $\Delta_1 = DF \cdot N(d_1)$ units of the power forward, $\Delta_2 = -DF \cdot N(d_2)$ of the fuel leg. These feed the hedging track directly.

**Assumptions to state out loud:** both legs lognormal (no jumps, no mean reversion), constant vol/correlation over $T$, forwards traded and frictionless, no intrahour shape. Each is wrong for power to some degree — later concepts (spike models, Monte Carlo with hourly dispatch) relax them.

## Worked example

One delivery month ($T = 0.25$), $F_1 = 80$ €/MWh (power forward), $F_2 = 60$ €/MWh (fuel + carbon at heat rate), $\sigma_1 = 0.50$, $\sigma_2 = 0.30$, $\rho = 0.40$, $DF = 0.99$.

- Spread vol: $\sigma = \sqrt{0.25 + 0.09 - 0.12} = 0.469$.
- Margrabe: $d_1 = 1.344$, $d_2 = 1.110$ → $V = 0.99 \times (80 \times 0.9105 - 60 \times 0.8664) = 20.65$ €/MWh.
- With VOM-type strike $K = 10$: $\tilde\sigma_2 = 0.30 \times 60/70 = 0.257$, $\tilde\sigma = 0.462$ → Kirk value $= 12.88$ €/MWh.
- Monte Carlo (200 000 paths, seed 11) reproduces Margrabe within 2%, and Kirk-with-$K$=0 reduces to Margrabe exactly.

`labs/spread_option_pricing_models/compute.py` implements all three pricers; the test asserts these numbers and the cross-checks.

## Market variants

- **EU:** the fuel leg itself has two components (gas + EUA × emission factor) — either model three factors or fold carbon into an effective fuel price with adjusted correlation; clean-spread options are the quoted OTC product.
- **GB:** same with UKA; liquidity in spark-spread options is thin, so these models price the *plant*, not a tradeable quote.
- **US:** heat-rate options are an established OTC product (e.g. ERCOT/PJM heat-rate call options) — Margrabe/Kirk with a fixed heat-rate strike is the market standard reference.
- **CN:** no traded spread options exist — this gap is precisely the curriculum's derivatives argument. The models here price the flexibility a 售电公司 or storage owner currently cannot hedge with instruments, only with physical assets.

## Common errors

1. **Spot instead of forward** — pricing off today's spot prices ignores that the option settles on delivery-period prices; use the forward for that period.
2. **Spread vol as $\sigma_1 - \sigma_2$** — variances add (with the correlation term), they never subtract.
3. **Forgetting the discount factor** — Margrabe with forwards still needs $DF$; at short maturities the error is small but systematic.
4. **Kirk with a large $K$** — the approximation degrades as $K \to F_2$; cross-check with MC instead of trusting the closed form.
5. **Ignoring mean reversion and spikes** — lognormal legs underprice near-the-money options in spiky markets; the next concepts add the missing dynamics.

## Assessable questions

1. Compute the spread volatility for $\sigma_1 = 0.6$, $\sigma_2 = 0.25$, $\rho = 0.5$.
2. What happens to the Margrabe value as $\rho \to 1$? Explain via the spread volatility.
3. State Kirk's approximation in one sentence and name the parameter region where it is least reliable.
4. Why can a storable-commodity argument (cost of carry) not be used to price power options?
5. Using the worked example numbers, what are the Margrabe deltas $\Delta_1$ and $\Delta_2$, and what forward position do they imply per MWh of option?
