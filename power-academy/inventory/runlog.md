# RunLog

- id: `runlog` · class: library · type: txt
- topic: Pump-storage hydro asset valuation · level: intermediate · market: mixed · year: 2017
- worked examples: False · code: False

## Concepts
- **Pump-storage hydro asset** — A two-reservoir hydroelectric facility that can both generate and consume electricity by pumping water between an upper and lower storage.
- **Top/bottom storage configuration** — The physical representation of upper and lower reservoirs, including initial volumes and maximum capacities.
- **Initial volume assumption** — A fallback rule setting the lower reservoir's starting volume as the upper reservoir's maximum capacity minus its initial storage when the lower reservoir is undefined.
- **Intrinsic valuation** — Deterministic valuation of the storage asset based on known or forward price spreads without simulation of price uncertainty.
- **Simulation valuation** — Stochastic valuation of the storage asset using Monte Carlo or equivalent price path simulation to capture optionality.
- **Valuation horizon** — The start and end dates over which the asset is valued, here a single calendar month.

## Methods
- Intrinsic optimisation
- Monte Carlo simulation

## Implied prerequisites
- Hydro dispatch optimisation
- Energy storage modelling
- Forward curve construction
- Stochastic price modelling
- Real-options valuation
