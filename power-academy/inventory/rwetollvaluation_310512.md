# RWETollValuation_310512

- id: `rwetollvaluation_310512` · class: library · type: pptx
- topic: Valuation of a toll agreement structured as a strip of monthly spark spread options on a gas-fired power plant · level: advanced · market: GB · year: 2012
- worked examples: True · code: False

## Concepts
- **Tolling Agreement** — A contractual arrangement where the buyer acquires the right to dispatch a generation asset and pays a capacity fee plus variable costs in lieu of owning the plant outright.
- **Clean Spark Spread (CSS) Option** — A monthly option on the margin of converting gas to electricity net of carbon costs, representing the optionality embedded in a gas-fired plant.
- **Strip of Monthly Options** — A sequence of individually valued monthly option contracts covering multiple EFA seasons used to decompose a multi-period tolling deal.
- **Peak vs Baseload Optionality** — The distinction between options exercisable only during peak EFA blocks and those covering full baseload hours, reflecting different dispatch profiles.
- **Plant Efficiency (Heat Rate)** — The thermal conversion factor (here 51.5%) linking gas input to electricity output, determining the gas leg quantity in the spark spread.
- **Emission Rate** — The carbon dioxide output per MWh of electricity generated (here 0.358 t/MWhe), used to compute the carbon cost leg of the clean spread.
- **Variable O&M Costs** — Season-differentiated per-MWh costs deducted from the spread payoff to reflect fuel handling, maintenance and other dispatch-dependent expenses.
- **Capacity Fee** — A fixed monthly payment made by the toll buyer to the plant owner for reserving dispatch rights regardless of actual generation.
- **EFA Season** — The UK electricity market's standard seasonal periods (Winter, Summer) used to define contract delivery windows and variable cost schedules.
- **Mark-to-Market Valuation** — Snapshot valuation of the toll as-of a specific date using prevailing forward curves for power, gas and carbon to determine current fair value.

## Methods
- Spark spread option pricing
- Strip decomposition of multi-period options
- Forward curve construction for power (UK baseload/peak) and NBP gas
- Carbon cost adjustment using EUA forward prices
- Heat rate conversion of gas to power quantities
- Monthly option intrinsic and extrinsic value separation
- Capacity fee present value discounting

## Implied prerequisites
- Energy commodity forward curves
- Options pricing fundamentals
- Spark spread mechanics
- UK electricity market structure (EFA blocks, seasons)
- Carbon markets (EU ETS, EUAs)
- Discounted cash flow analysis
- Physical power and gas market conventions
