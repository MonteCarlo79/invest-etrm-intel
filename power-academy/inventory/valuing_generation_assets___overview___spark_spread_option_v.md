# Valuing_generation_assets_-_overview_&_spark_spread_option_valuation

- id: `valuing_generation_assets___overview___spark_spread_option_v` · class: library · type: pdf
- topic: Valuation of thermal generation assets as real options, with focus on analytic spark spread option pricing · level: intermediate · market: US · year: 2009
- worked examples: True · code: False

## Concepts
- **Spark spread option** — An option on the spread between the power price and the heat-rate-adjusted fuel cost of a generation unit.
- **Real options for generation assets** — The treatment of operational flexibility in running a power plant as optionality amenable to financial option pricing techniques.
- **Heat rate** — A measure of generation unit fuel efficiency expressed as fuel input per unit of electricity output, which varies with operating level and ambient conditions.
- **Intrinsic value** — The discounted in-the-money portion of the spark spread option value based on current forward prices alone.
- **Extrinsic (time) value** — The portion of option value attributable to price uncertainty beyond the intrinsic value.
- **Black's formula** — A closed-form pricing model for European call options on futures contracts used to value the spark spread option analytically.
- **Kirk approximation** — A technique that reduces a two-factor spread option to a single-factor option with a combined effective volatility dependent on power volatility, gas volatility, and their correlation.
- **Forward volatility term structure** — The time-dependent implied volatility surface for power and gas forward prices, exhibiting seasonal patterns and mean-reversion.
- **Implied correlation** — The market-derived correlation between power and gas forward prices used as an input to spread option pricing.
- **Start cost** — A fixed charge per plant start covering fuel consumed during start-up and wear-and-tear, incorporated into the option strike or via heat rate adjustment.
- **Variable operation and maintenance (VOM) cost** — Non-fuel variable costs of running a generation unit, treated as a fixed adder to the option strike.
- **Minimum stable generation** — The lowest output level at which a unit can operate and still deliver power to the grid.
- **Ramp rate** — The rate at which a generation unit can increase or decrease its output level, limiting instantaneous dispatch flexibility.
- **Minimum up/down time** — Operational constraints requiring a unit to remain on or off for a minimum number of hours once switched.
- **Forced and scheduled outages** — Unplanned and planned reductions in unit availability, approximated in the analytic model by de-rating the volume of MWh generated.
- **Baseload, mid-merit, and peaking unit classification** — A taxonomy of thermal generators by operating regime, moneyness of the associated spark spread option, and proportion of intrinsic versus extrinsic value.
- **Monte Carlo simulation for dispatch valuation** — Hourly simulation of spot power prices combined with a dispatch algorithm to value generation assets while incorporating operational constraints.
- **Perfect foresight bias** — The overvaluation arising when simulated spot prices are used both to dispatch the plant and to calculate profit and loss in a Monte Carlo framework.
- **Least squares Monte Carlo (LSMC)** — A method combining backward induction via regression with Monte Carlo price simulation to value generation assets without assuming perfect foresight.
- **Trinomial tree with backward induction** — A lattice-based dynamic programming approach that steps backward through time to optimally dispatch a generation asset at each node.
- **Emissions costs** — CO2, NOx, and SO2 charges incorporated as deterministic strike adjustments or, when stochastic, requiring Monte Carlo treatment.
- **Heat rate curve** — A step or continuous function describing fuel efficiency as a function of output level between minimum stable generation and maximum capacity.
- **Dark spread** — The spread between power price and the cost of coal used to generate it, analogous to the spark spread for gas-fired units.

## Methods
- Analytic spark spread option pricing (Kirk approximation)
- Black's formula for options on futures
- Heat rate adjustment to absorb stochastic start gas cost
- Volume de-rating to approximate outage effects
- Strike decomposition into VOM, heat-rate gas adder, and amortised start cost
- Implied volatility extraction from liquid option markets
- Monte Carlo simulation with hourly price granularity
- Heuristic dispatch algorithms
- Linear programming dispatch optimisation
- Dynamic programming on trinomial trees (backward induction)
- Least squares Monte Carlo (LSMC)

## Implied prerequisites
- Black-Scholes and Black 1976 option pricing theory
- Forward and futures curve construction for power and gas
- Spread option pricing concepts
- Basic stochastic processes for commodity prices
- Discounted cash flow and present value
- Correlation and covariance in multi-asset settings
- Dynamic programming fundamentals
- Monte Carlo simulation methods
- Linear regression
