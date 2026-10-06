# power/Live Power Valuation Tool

- id: `power_live_power_valuation_tool` · class: practice · type: folder
- topic: Power derivatives valuation tool suite · level: advanced · market: DE · year: None
- worked examples: True · code: True

## Concepts
- **Power outright option valuation** — Pricing of plain-vanilla options on German baseload and peakload power forwards across multiple model versions.
- **Power spread valuation** — Mark-to-market and pricing of spread options (e.g. dark/spark spreads) on power and fuel combinations.
- **Baseload vs peakload price databases** — Structured daily time-series storage of NL baseload and peakload forward price data for model calibration and valuation.
- **Forward curve construction** — Building and maintaining power forward price curves used as inputs to option pricing models.
- **Volatility surface calibration** — Fitting implied volatility surfaces to observed market quotes for power options across strikes and tenors.
- **Mark-to-market (MtM) valuation** — Daily revaluation of power derivative positions against live or end-of-day market data.
- **Model versioning in valuation tools** — Iterative refinement of pricing model logic across successive spreadsheet releases to improve accuracy or scope.
- **Data download and templating** — Automated retrieval and standardised formatting of market data feeds for downstream valuation models.

## Methods
- Black-76 option pricing
- Spread option pricing (Margrabe / Kirk approximation)
- Historical volatility estimation
- Implied volatility extraction
- Forward curve interpolation
- Excel VBA automation
- Scenario and sensitivity analysis

## Implied prerequisites
- Futures and forwards pricing
- Black-Scholes / Black-76 framework
- European option Greeks
- Energy commodity market structure
- Spreadsheet-based financial modelling
- Basic stochastic processes for commodity prices
