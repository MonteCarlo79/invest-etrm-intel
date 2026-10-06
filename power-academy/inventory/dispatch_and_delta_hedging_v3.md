# Dispatch and Delta Hedging v3

- id: `dispatch_and_delta_hedging_v3` · class: library · type: ppt
- topic: Dispatch and Delta Hedging for Power Generation Assets · level: intermediate · market: GB · year: 2012
- worked examples: True · code: False

## Concepts
- **Clean Spark Spread** — The net margin per MWh from generating electricity after accounting for fuel and carbon costs, used as the key dispatch trigger.
- **Average Dispatch** — The capacity-weighted mean generation output across simulated price scenarios, representing expected plant utilisation.
- **Delta of a Generation Asset** — The sensitivity of a plant's expected value to a small shift in the forward spread price, numerically equal to average dispatched MW.
- **Monte Carlo Price Simulation** — A set of simulated spread price paths used to evaluate dispatch decisions and expected value under uncertainty.
- **Variable Cost Threshold** — The per-MWh operating cost below which generation is uneconomic and the plant is dispatched off.
- **Expected Value (EV)** — The probability-weighted average hourly revenue from optimal dispatch across all simulated price scenarios.
- **Bump-and-Reprice Delta** — A finite-difference method that estimates delta by shifting the forward price by a small increment and recomputing expected value.

## Methods
- Monte Carlo simulation of spread prices
- Threshold-based binary dispatch rule
- Finite-difference (bump-and-reprice) delta calculation
- Scenario averaging for expected value

## Implied prerequisites
- Concept of financial delta and hedging
- Power plant economics and variable cost structure
- Spark spread and clean spark spread definitions
- Basic probability and expected value
- Forward curve construction
