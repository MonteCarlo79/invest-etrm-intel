# Dispatch and Delta Hedging v4

- id: `dispatch_and_delta_hedging_v4` · class: library · type: ppt
- topic: Dispatch and Delta Hedging for Generation Assets · level: intermediate · market: GB · year: 2012
- worked examples: True · code: False

## Concepts
- **Average Dispatch** — Expected megawatt output of a generation asset averaged across simulated price scenarios, conditional on clean spark spread exceeding variable cost.
- **Delta Hedging** — Sensitivity of a generation asset's expected value to a parallel shift in the price distribution, expressed in MW-equivalent hedge quantity.
- **Clean Spark Spread (CSS)** — Net margin per MWh from generating electricity after subtracting fuel and carbon costs from the power price.
- **Dispatch Decision** — Binary operating choice to run or idle a plant in each scenario based on whether the simulated spread exceeds variable cost.
- **Price Simulation** — Set of Monte Carlo paths for the clean spark spread used to estimate expected value and sensitivities of a generation asset.
- **Delta–Dispatch Equivalence** — Mathematical result showing that, for infinitesimal price shifts, delta equals the probability that price exceeds variable cost, which equals expected dispatch.
- **AOM Dispatch Bias** — Empirically observed divergence between AOM-modelled expected dispatch and delta, and investigation of its drivers.
- **Probability of Exercise** — Probability that the spark spread exceeds variable cost, which under continuous price distributions equals both delta and expected dispatch.
- **Variable Cost** — Marginal cost of generation per MWh used as the strike threshold in the plant dispatch optimisation.

## Methods
- Monte Carlo simulation of spark spread paths
- Finite-difference delta estimation via price shift
- Expected value calculation across simulated scenarios
- Indicator-function decomposition of max() payoff

## Implied prerequisites
- Options delta definition
- Clean spark spread construction
- Basic Monte Carlo valuation
- Generation asset dispatch optimisation
- Probability and expectation
