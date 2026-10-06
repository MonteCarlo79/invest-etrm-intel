# KyHydroPumpInput

- id: `kyhydropumpinput` · class: library · type: txt
- topic: Hydro pump storage valuation model input configuration · level: advanced · market: mixed · year: 2017
- worked examples: False · code: False

## Concepts
- **Hydro pump storage** — Parametric setup for a pumped-hydro storage asset including capacity, efficiency, and operational horizon.
- **Storage efficiency (round-trip)** — Ratio capturing energy losses when pumping and generating, here set at 0.8.
- **EEX price curve** — Forward or spot price schedule from the European Energy Exchange used as the underlying price process.
- **Least-Squares Monte Carlo (LSMC)** — Simulation-based dynamic-programming method for valuing the optimal dispatch of a storage asset.
- **Operational date range** — Start and end dates (encoded as Excel serial numbers) defining the asset's active dispatch window.
- **Reservoir capacity and inventory** — Maximum storage level and initial fill state constraining feasible pump/generate decisions at each time step.
- **Simulation paths** — Number of Monte Carlo scenarios (50) drawn to approximate the value function via LSMC regression.

## Methods
- Least-Squares Monte Carlo (LSMC)
- Dynamic programming / backward induction
- Monte Carlo simulation
- Forward curve bootstrapping

## Implied prerequisites
- Stochastic processes for energy prices
- Dynamic programming and Bellman equation
- Monte Carlo simulation techniques
- Options pricing fundamentals
- Energy market microstructure (EEX)
- Excel date serial number convention
