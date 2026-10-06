---
id: option_greeks_for_power_derivatives
track: hedging_trading
level: intermediate
prerequisites:
- delta_sensitivity_and_hedge_volume
- baseload_peak_offpeak_delta_decomposition
markets:
- EU
- GB
- US
status: drafted
sources:
- id: erratumtoelectricityderivativeschapterspreads_66_70
  use: background
- id: valuing_generation_assets___overview___spark_spread_option_v
  use: derivation_reference
- id: dipeng_put_options
  use: practice_example
originality: synthesized
translations:
  zh:
    status: drafted
    en_hash: ae4335650165e2fa
---
## Learning objectives

- Compute the Greeks of a spark-spread option: delta on both legs, gamma, vega (spread-vol).
- Cross-check any analytic Greek against a finite difference of the same pricer — and make it a habit.
- Read the deltas as the hedge volumes of the power leg and the fuel leg, and gamma/vega as the risks a linear hedge leaves behind.
- Explain how a plant desk uses the Greeks: delta to size hedges, gamma to know when rebalancing pays, vega to know what a vol move costs.

## Intuition

The delta ladder told you *how much* to hedge. The Greeks tell you *how the answer changes*. A spark-spread option (the plant, per hour, from the valuation track) has two underlyings — power and fuel — so it has **two deltas**: long the power forward, short the fuel leg. Those are the hedge volumes: sell $\Delta_1$ MWh of power, buy $\Delta_2$ MWh-equivalent of fuel.

The other two Greeks are the honest residuals. **Gamma** measures how fast delta moves when prices move — it is the reason a delta hedge needs rebalancing, and the quantity that decides whether rebalancing is worth its transaction costs (see *static_vs_dynamic_hedging*). **Vega** measures sensitivity to spread volatility — for a plant book, this is often the dominant unhedged risk, because you can hedge price levels with forwards but you cannot hedge the vol with them. When implied vol re-rates, the option value of the plant book moves with no help from your linear hedges.

## Formal treatment

From Margrabe ($V = DF[F_1 N(d_1) - F_2 N(d_2)]$, spread vol $\sigma$):

$$\Delta_1 = \frac{\partial V}{\partial F_1} = DF\, N(d_1), \qquad \Delta_2 = \frac{\partial V}{\partial F_2} = -DF\, N(d_2)$$

$$\Gamma = \frac{\partial \Delta_1}{\partial F_1} = \frac{DF\, \varphi(d_1)}{F_1 \sigma \sqrt{T}}, \qquad \text{Vega} = \frac{\partial V}{\partial \sigma} = DF\, F_1\, \varphi(d_1)\, \sqrt{T}$$

Note the discount factor caps $|\Delta|$ below 1 — deep ITM does not mean delta 1. And note vega is taken with respect to the *spread* vol (both legs' vols scaled together, correlation fixed), not to the power leg alone.

**Verification discipline.** Any analytic Greek should be checked once against a finite difference of the same pricer: $\partial_x \approx \frac{V(x+h) - V(x-h)}{2h}$. The lab does this for all four Greeks — if your analytic formula and your FD disagree, the formula is wrong, the pricer is wrong, or the bump is inconsistent with the model's correlation structure (the classic vega bug: bumping one leg's vol changes the spread vol in a way the formula didn't intend).

## Worked example

The valuation track's Margrabe case ($F_1 = 80$, $F_2 = 60$, $\sigma_1 = 0.5$, $\sigma_2 = 0.3$, $\rho = 0.4$, $T = 0.25$, $DF = 0.99$; value 20.65 €/MWh):

- $\Delta_1 = 0.9014$, $\Delta_2 = -0.8577$ — per MWh of option, hedge with 0.90 MWh power sold and 0.86 MWh-equivalent fuel bought. (Note both are below the naïve 1 in magnitude, discount factor included.)
- $\Gamma = 0.00853$ per €/MWh — a 10 € move in the power forward shifts the power delta by ~0.085: the size of the rebalance that move requires.
- Vega $= 6.40$ €/MWh per unit of spread vol — spread vol rising from 0.469 to 0.569 adds ~0.64 €/MWh of option value, unreachable by any forward hedge.

All four analytic values match finite differences of the same pricer to 4+ digits — verified in `labs/option_greeks_for_power_derivatives/compute.py`.

## Market variants

- **EU/GB:** clean-spread options have a three-leg delta (power, gas, EUA) — either model three factors or fold carbon into the fuel leg with adjusted vol/correlation and accept the approximation.
- **US:** heat-rate options quote the strike heat rate explicitly; the fuel leg delta is the heat-rate exposure desks actually trade (gas forwards at the delivery hub).
- **CN:** with no traded spread options, the Greeks describe *physical* hedges: delta 1 = 中长期 power volume, delta 2 = coal procurement (长协煤). The framework maps directly onto 电煤联动管理.

## Common errors

1. **One delta for a two-legged option** — hedging only the power leg leaves the fuel leg's delta unhedged; the position is then long fuel risk, not hedged.
2. **Delta 1 assumption** — forgetting the discount factor (and for hourly strips, the intra-period shape) caps the hedge ratio.
3. **Vega on the wrong vol** — bumping power-leg vol only and calling it vega; the plant's vega is on the *spread* vol, which correlation can shrink or amplify.
4. **Trusting analytics unverified** — always FD-check once; the correlation-structure bug is the most common Greek error in spread books.
5. **Ignoring gamma until it hurts** — gamma is the rebalance schedule; a book with high gamma and a monthly rebalance is a book with a hidden short-vol position.

## Assessable questions

1. Compute $\Delta_1$ and $\Delta_2$ for the worked example and state the hedge they imply.
2. Why does the discount factor prevent delta from reaching 1?
3. What does a gamma of 0.0085 mean for your rebalance schedule after a 10 € price move?
4. Explain the vega bug that arises from bumping only one leg's volatility. How does correlation enter?
5. A 售电公司 wants to hedge a spark-spread book physically. Name the two real-world legs corresponding to $\Delta_1$ and $\Delta_2$ in China.
