# power/Imbalancing Calc

- id: `power_imbalancing_calc` · class: practice · type: folder
- topic: Imbalance Cost and Settlement Calculation for Renewable and CHP Assets · level: advanced · market: mixed · year: None
- worked examples: True · code: True

## Concepts
- **Imbalance settlement** — Covers the financial settlement of deviations between metered generation and contracted position under national balancing regimes.
- **Cashout price** — Covers the per-unit price applied to imbalance volumes at the time of settlement, including single and dual cashout price structures.
- **Wind forecast error** — Covers the difference between predicted and metered wind output used as the primary source of imbalance exposure.
- **Solar generation shape** — Covers the intraday profile of solar PV output used to construct forecast and metered position for settlement purposes.
- **Portfolio netting** — Covers the aggregation of multiple generation assets within a single balancing responsible party position to reduce net imbalance exposure.
- **Bootstrap resampling for imbalance risk** — Covers statistical resampling of historical imbalance price and volume data to estimate tail-risk metrics such as P99 imbalance cost.
- **Balancing Mechanism Reporting Service (BMRS) data** — Covers the use of National Grid's BMRS data feed as the source for settlement prices and system-length signals in GB imbalance calculations.
- **Day-ahead forecast versus actual dispatch** — Covers the comparison of day-ahead market scheduling against real-time metered output to compute imbalance volumes.
- **Intraday forecast updating** — Covers the revision of generation forecasts within the trading day to reduce imbalance exposure before gate closure.
- **Demand-side management imbalance** — Covers the calculation of imbalance arising from industrial and commercial demand response not matching contracted curtailment volumes.
- **Waste CHP imbalance** — Covers the settlement exposure specific to combined heat and power plants dispatched on thermal load rather than market signal.
- **Balancing responsible party (BRP) position** — Covers the aggregated nominated schedule that a BRP submits to the TSO against which imbalance is measured.
- **CO2 price pass-through in price modelling** — Covers the incorporation of carbon allowance costs into power price models used as inputs to imbalance revenue calculations.
- **TTF gas price as power price driver** — Covers the use of Dutch TTF natural gas prices to derive marginal cost benchmarks within power imbalance price models.
- **Cross-border asset comparison** — Covers the evaluation of the same physical asset under different national imbalance regimes to identify the optimal contracting jurisdiction.
- **P99 imbalance cost metric** — Covers the 99th-percentile worst-case imbalance cost derived from bootstrapped or simulated price-volume distributions.
- **Half-hourly settlement period** — Covers the GB-specific 30-minute settlement interval used to align generation metering with imbalance price windows.
- **Simulated future market scenarios** — Covers forward-looking price and volume scenarios (e.g. 2025, 2030) used to project imbalance costs under anticipated market conditions.

## Methods
- Bootstrap resampling
- Historical simulation
- Time-series aggregation of metered vs forecast volumes
- Portfolio netting of multi-asset positions
- Scenario analysis across forward years
- Statistical percentile estimation (P99)
- Regression-based power price modelling from gas and CO2 inputs
- Monte Carlo-style price path simulation

## Implied prerequisites
- Electricity market settlement mechanics
- Balancing responsible party obligations
- Wind and solar generation forecasting fundamentals
- Python data manipulation (pandas, numpy)
- Probability and statistics (resampling, percentiles)
- European power market structure (GB, NL, BE, DE, FR)
- Gas and carbon market price linkages
