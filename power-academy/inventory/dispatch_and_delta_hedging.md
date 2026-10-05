# Dispatch and Delta Hedging

- id: `dispatch_and_delta_hedging` · class: library · type: ppt
- topic: Dispatch and Delta Hedging · level: intermediate · market: GB · year: 2012
- worked examples: True · code: False

## Concepts
- **Plant dispatch** — The decision to generate at full capacity or zero based on whether the market spread exceeds the variable cost threshold.
- **Clean spark spread (CSS)** — The net margin from generating power after accounting for fuel and carbon costs, used as the effective price signal for dispatch.
- **Optionality value of a generation asset** — The additional value captured by dispatching conditionally on each simulated price path rather than at a fixed average volume.
- **Average dispatch fallacy** — The error of computing plant value by multiplying average dispatch volume by average price, which systematically understates expected value.
- **Stochastic price simulation** — Monte Carlo simulation of forward price paths used to value generation assets and origination deals under uncertainty.
- **Delta hedging** — Hedging strategy based on the sensitivity of expected asset value to a parallel shift in simulated forward prices.
- **Delta (generation asset)** — The change in expected dispatch value per unit change in the forward spread, computed by bumping simulated prices and revaluing.
- **Variable cost** — The per-MWh cost of generation that sets the strike level determining whether the plant runs on each simulated path.
- **Bump-and-revalue** — Numerical method for estimating delta by shifting all simulated prices by a small increment and comparing the resulting expected values.
- **Expected value under simulation** — The arithmetic mean of path-wise revenues computed after applying the optimal dispatch decision on each simulated price path.

## Methods
- Monte Carlo simulation
- Threshold-based dispatch optimisation
- Bump-and-revalue delta estimation
- Path-wise valuation
- Backcasting

## Implied prerequisites
- Forward curve construction
- Basic options theory (intrinsic vs time value)
- Spark spread mechanics
- Fundamentals of hedging
- Probability and expectation
