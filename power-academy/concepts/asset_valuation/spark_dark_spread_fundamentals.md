---
id: spark_dark_spread_fundamentals
track: asset_valuation
level: foundation
prerequisites: []
markets:
- EU
- GB
- US
status: drafted
sources:
- id: energy_risk_2012___kyos_power_plant_hedging_strategies
  use: background
- id: plant_hedging_and_trading_strategies_kyos_20110530
  use: background
- id: valuing_generation_assets___overview___spark_spread_option_v
  use: derivation_reference
- id: value_ccgt
  use: background
- id: power_european_toll
  use: practice_example
originality: synthesized
translations:
  zh:
    status: none
    en_hash: null
---
## Learning objectives

- Define the spark spread, dark spread, and their "clean" (carbon-adjusted) variants.
- Convert between efficiency (η) and heat rate (HR) conventions, including US BTU/kWh.
- Compute all four spreads from power price, fuel price, heat rate, and carbon price.
- Explain why a thermal plant's marginal margin per MWh is a spread, not the power price.
- Explain why the spread is the underlying variable of every plant valuation and hedging model that follows in this track.

## Intuition

A gas-fired plant is a machine that converts gas into electricity. What it earns per MWh of output is not the power price — it is the power price **minus the cost of the fuel it burned**. That difference is the *spark spread*. A coal plant does the same conversion with coal; its margin is the *dark spread*.

This reframing matters because it turns a two-price problem (power and fuel) into a one-number problem. Dispatch ("is running this hour worth it?"), valuation ("what is the plant worth?"), and hedging ("what am I exposed to?") are all statements about the spread, not about either price alone.

In carbon-priced markets the plant also buys emission allowances for every tonne of CO₂ it emits. Subtracting that cost gives the *clean* spark spread and *clean* dark spread. The clean spreads — not the plain ones — determine which technology actually runs: at high carbon prices a more efficient gas plant outranks a less efficient coal plant even when coal itself is cheaper per unit of energy. This "fuel switching" logic is the daily reality of EU and GB merit orders.

## Formal treatment

Let, for a given delivery period:

- $P$ — power price (€/MWh)
- $G$ — gas price (€/MWh_th), $C$ — coal price (€/MWh_th)
- $\eta_g, \eta_c$ — net electrical efficiency of the gas / coal plant (MWh_e per MWh_fuel)
- $HR = 1/\eta$ — heat rate (MWh_fuel per MWh_e)
- $E$ — carbon price (€/tCO₂), e.g. EUA or UKA
- $ef_g \approx 0.202$ tCO₂/MWh_th (natural gas), $ef_c \approx 0.341$ tCO₂/MWh_th (coal)

Then the four spreads (€/MWh_e):

$$\text{Spark} = P - HR_g \cdot G$$
$$\text{Clean spark} = P - HR_g \cdot (G + E \cdot ef_g)$$
$$\text{Dark} = P - HR_c \cdot C$$
$$\text{Clean dark} = P - HR_c \cdot (C + E \cdot ef_c)$$

**Convention pitfalls.** Efficiency and heat rate express the same thing; always check which one a document uses, and in which units. US practice quotes heat rate in BTU/kWh: $HR_{\text{MWh}_{th}/\text{MWh}_e} = HR_{\text{BTU/kWh}} / 3412$. Fuel may be quoted in €/MWh_th, $/MMBtu, or p/therm — convert before subtracting from a power price. Carbon cost scales with *fuel* input, not power output, which is why efficiency appears twice in the clean spreads.

**Fuel switching.** The gas plant outranks the coal plant exactly when

$$\text{Clean spark} > \text{Clean dark} \iff HR_c(C + E \cdot ef_c) > HR_g(G + E \cdot ef_g).$$

At $E = 0$ this is pure fuel-cost comparison; as $E$ rises, the switch point moves in favour of the lower-emission technology — carbon pricing is, mechanically, a fuel-switching price.

## Worked example

Inputs (one delivery hour, GB-like levels): $P = 80$ €/MWh, $G = 30$ €/MWh_th, $C = 15$ €/MWh_th, $E = 80$ €/t, $\eta_g = 50\%$, $\eta_c = 38\%$.

Heat rates: $HR_g = 2.0$, $HR_c = 2.632$.

| Spread | Calculation | Value (€/MWh) |
|---|---|---|
| Spark | $80 - 2.0 \times 30$ | $+20.00$ |
| Clean spark | $80 - 2.0 \times (30 + 80 \times 0.202)$ | $-12.32$ |
| Dark | $80 - 2.632 \times 15$ | $+40.53$ |
| Clean dark | $80 - 2.632 \times (15 + 80 \times 0.341)$ | $-31.26$ |

Reading: both plants look profitable on plain spreads, and both are under water on clean spreads — at these input levels the carbon cost, not the fuel cost, decides. Coal earns more per MWh than gas in every variant here because its fuel is much cheaper, despite worse efficiency and higher emissions. `labs/spark_dark_spread_fundamentals/compute.py` reproduces every number in the table; its test asserts them to the cent.

## Market variants

- **EU (EEX/EPEX):** clean spreads with EUA are the standard quoted margins; fuel-switching between coal and gas is an active trading thesis.
- **GB:** same structure with UKA carbon since 2021; the UK Carbon Price Support adds a fixed £18/t floor on top for GB generators, so GB clean spreads can diverge from EU ones at identical fuel prices.
- **US:** most regions have no carbon price, so plain spreads dominate; heat rates are quoted in BTU/kWh (a 7,000 BTU/kWh combined cycle ≈ 49% efficiency). Regional gas basis (Henry Hub vs citygate) matters as much as the heat rate.
- **CN:** spreads are computed in 元/MWh with no carbon cost in the power price (the national ETS covers the power sector with free allocation, so it does not yet drive hourly dispatch); the relevant margin for coal units is 电价 − 标煤单价 × 供电煤耗 (gce/kWh), i.e. a dark spread quoted through coal consumption rate rather than heat rate.

## Common errors

1. **Mixing conventions mid-calculation** — efficiency in one line, heat rate in the next; or BTU/kWh treated as MWh_th/MWh_e. Convert first, compute once.
2. **Forgetting carbon in a "clean" comparison** — comparing clean spark to plain dark spread and concluding gas beats coal (or vice versa).
3. **Carbon on output instead of input** — applying the emission factor per MWh_e instead of per MWh_th; the error is exactly the efficiency factor.
4. **Currency/unit mismatch** — gas in $/MMBtu subtracted from power in €/MWh. The spread is a subtraction; both legs must share a unit.
5. **Reading the spread as the plant's profit** — it is the *marginal* margin before start-up costs, minimum-run constraints, fixed O&M, and (in clean form) allowance surrender logistics. Later concepts (dispatch, tolling) add these back explicitly.

## Assessable questions

1. A plant has η = 55%. What is its heat rate in MWh_th/MWh_e and in BTU/kWh?
2. Given P = 95 €/MWh, G = 40 €/MWh_th, E = 85 €/t, η_g = 52%: compute the spark and clean spark spreads. Is the hour clean-spread positive?
3. Holding fuel prices fixed, above what carbon price does a η_g = 50% gas plant outrank a η_c = 38% coal plant when G = 30, C = 15 €/MWh_th? (Set clean spark = clean dark and solve for E.)
4. Why does the emission factor multiply by heat rate rather than being a fixed cost per MWh of power?
5. A US report quotes a CCGT margin of "$28/MWh over a 6,800 BTU/kWh heat rate at $3.50/MMBtu gas". Reconstruct the implied power price.
