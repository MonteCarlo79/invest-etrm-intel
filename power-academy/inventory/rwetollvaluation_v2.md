# RWETollValuation_v2

- id: `rwetollvaluation_v2` · class: library · type: ppt
- topic: Tolling agreement valuation for a strip of monthly coal-spark-spread options on UK power versus NBP gas · level: advanced · market: GB · year: 2012
- worked examples: True · code: False

## Concepts
- **Tolling Agreement** — A contractual arrangement where the buyer acquires the right to use generation capacity in exchange for a monthly capacity fee plus variable costs.
- **Coal-Spark Spread (CSS) Option** — An option whose payoff depends on the spread between power prices and the fuel cost of generating power from gas or coal.
- **Strip of Monthly Options** — A sequence of independently exercisable options, one per calendar month, together constituting the full deal tenor.
- **Peak vs Baseload Election** — The choice available each month between delivering power on a peak or baseload profile, each with its own variable cost.
- **Variable Cost (Heat Rate Strike)** — The per-MWh cost applied at exercise to convert fuel exposure into a net power spread payoff, differing by season and profile.
- **Capacity Fee** — The fixed monthly premium paid by the buyer to the seller for reserving generation capacity regardless of dispatch.
- **EFA Season** — A UK electricity market seasonal period (Winter or Summer) used to organise forward contracts and capacity schedules.
- **NBP Gas Price** — The National Balancing Point benchmark gas price used as the fuel-side reference in UK spark-spread valuation.
- **Physical Delivery** — Settlement mode requiring actual transfer of electricity megawatt-hours rather than cash difference at expiry.
- **Mark-to-Market Valuation** — Repricing of the option strip using prevailing forward curves as of a specified trade date.

## Methods
- Spread option pricing
- Forward curve construction (power and gas)
- Season-specific variable cost adjustment
- Peak/baseload profile decomposition
- Monthly option strip aggregation
- Capacity fee present-value calculation
- Repricing/bid-price adjustment

## Implied prerequisites
- Energy derivatives pricing theory
- UK power market structure and EFA conventions
- NBP gas market and forward curves
- Spark spread and dark spread mechanics
- Options on commodity spreads
- Discount curve construction
- Physical vs financial settlement distinction
