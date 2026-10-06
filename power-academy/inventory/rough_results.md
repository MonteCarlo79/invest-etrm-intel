# rough results

- id: `rough_results` · class: library · type: doc
- topic: Unit root testing of UK electricity forward/baseload price ratios · level: intermediate · market: GB · year: None
- worked examples: True · code: False

## Concepts
- **Augmented Dickey-Fuller (ADF) test** — Tests whether a time series has a unit root (non-stationarity) against the alternative of stationarity.
- **Stationarity** — Property of a time series whose statistical moments do not change over time, required for many econometric models.
- **EFA (Electricity Forward Agreement) price** — Standardised UK electricity forward contract price used as the time series under analysis.
- **Baseload price** — Flat around-the-clock electricity price used here as a denominator to form a spread or ratio.
- **Peak/baseload ratio** — Ratio of peak-period forward prices to baseload prices, tested for stationarity as a potential cointegrating relationship.
- **Cointegration via ratio stationarity** — Empirical approach that checks whether a price ratio is stationary as a proxy for a long-run equilibrium between two price series.
- **Lag order selection in ADF** — Choice of the number of lagged difference terms included in the ADF regression to correct for serial correlation.
- **Weekend/weekday seasonality segmentation** — Splitting electricity price data into weekend and weekday sub-samples to account for systematic intra-week demand patterns.
- **p-value truncation warning** — Numerical artefact where the reported p-value is floored at 0.01 when the true p-value is smaller, relevant when interpreting ADF results.

## Methods
- Augmented Dickey-Fuller unit root test
- Price ratio construction (EFA / baseload)
- Weekend/weekday data segmentation
- Monthly subsample analysis
- Lag order specification (lag = 9)

## Implied prerequisites
- Time series analysis fundamentals
- Electricity market structure and contract types
- Understanding of forward curve products (EFA blocks)
- Basic hypothesis testing and p-value interpretation
- R programming (adf.test function usage)
