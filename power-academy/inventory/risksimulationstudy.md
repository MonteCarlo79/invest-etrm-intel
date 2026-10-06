# RiskSimulationStudy

- id: `risksimulationstudy` · class: library · type: doc
- topic: Clean Spark Spread simulation and extreme price impact analysis · level: intermediate · market: EU · year: None
- worked examples: True · code: False

## Concepts
- **Clean Spark Spread (CSS)** — The net margin from generating electricity with gas after accounting for fuel and carbon costs.
- **Extreme price events in Monte Carlo simulation** — Occurrence of very high or low simulated commodity prices that lie far outside the typical distribution range.
- **Price correlation across commodity legs** — The degree to which power, gas, and carbon prices move together within a simulation, affecting spread volatility.
- **Percentile-based risk metrics (P5/P95)** — Statistical quantiles used to characterise the lower and upper tails of a simulated price or value distribution.
- **Impact of outlier iterations on average valuation** — How a single extreme simulation path can shift the mean CSS estimate, scaled by the total number of iterations.
- **Asset valuation horizon sensitivity** — The dependence of valuation accuracy on the time period modelled, with extreme tail events accumulating over longer horizons.

## Methods
- Monte Carlo simulation (Risk software)
- Percentile extraction (P5, P95, min, max)
- Spread arithmetic (CSS = power price − gas cost − carbon cost)
- Iteration-weighted average impact calculation

## Implied prerequisites
- Commodity spread mechanics
- Probability distributions and percentiles
- Basic Monte Carlo simulation concepts
- Power plant economics and heat rate concepts
- Carbon cost pass-through in electricity pricing
