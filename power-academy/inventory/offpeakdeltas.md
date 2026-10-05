# OffpeakDeltas

- id: `offpeakdeltas` · class: library · type: txt
- topic: Offpeak price deltas by month · level: intermediate · market: DE · year: 2012
- worked examples: False · code: True

## Concepts
- **Offpeak delta** — The price spread or difference associated with offpeak hours for a given calendar month.
- **Calendar month granularity** — Decomposition of power market data into individual year-month periods for analysis.
- **Offpeak hours** — Non-peak delivery hours in a power contract, typically nights and weekends, forming a distinct pricing segment.
- **Monthly hour count** — Total number of offpeak hours in a given month, used as a weighting or normalisation factor.

## Methods
- Tabular time-series data construction
- Monthly aggregation of hourly power data
- Delta computation between two price or volume series

## Implied prerequisites
- Power market product structure (peak vs offpeak)
- Hourly spot price data handling
- Calendar arithmetic for power hours
