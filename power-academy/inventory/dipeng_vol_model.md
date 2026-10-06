# dipeng/Vol model

- id: `dipeng_vol_model` · class: practice · type: folder
- topic: Power and gas volatility modelling: electricity–gas spread volatility and correlation across UK, NL and DE markets · level: intermediate · market: mixed · year: 2015
- worked examples: True · code: False

## Concepts
- **Baseload volatility** — Annualised implied or realised percentage volatility of baseload power forward prices for a given seasonal contract.
- **Peak volatility** — Annualised percentage volatility of peak-hour power forward prices for a given seasonal contract.
- **Gas price volatility (NBP/TTF)** — Annualised percentage volatility of the corresponding natural-gas forward price used as the fuel reference leg.
- **Spark-spread volatility** — Volatility of the differential between a power forward price and its gas-price reference, derived from the individual legs and their correlation.
- **Power–gas correlation** — Linear correlation coefficient between daily log-returns of a power forward and its associated gas forward over the same delivery period.
- **Seasonal contract segmentation (1SA / 2SA)** — Decomposition of volatility and correlation estimates by near and far seasonal forward contracts to capture term-structure effects.
- **Baseload–peak volatility differential** — Comparison of volatility levels between baseload and peak power contracts for the same delivery period, reflecting shape risk.
- **Forward contract tenor** — Classification of delivery periods (day-ahead, month-ahead, quarter-ahead, season-ahead) used to organise vol output files.
- **Volatility term structure** — Pattern of how volatility estimates change across successive forward maturities from day-ahead to multi-season horizons.
- **Sampling frequency in vol estimation** — Choice of intraday (5-minute) versus daily return intervals used to compute volatility, affecting noise and bias in estimates.

## Methods
- Historical volatility estimation from daily log-returns
- Intraday high-frequency volatility estimation (5-minute returns)
- Spread volatility derivation via two-asset volatility formula
- Pearson correlation estimation between power and gas return series
- Seasonal and tenor stratification of volatility outputs
- Cross-market comparison (UK NBP, NL TTF, DE power)

## Implied prerequisites
- Forward and futures market structure in European power and gas
- Log-return calculation and annualisation of volatility
- Basic statistics: variance, covariance, correlation
- Understanding of baseload and peak power product definitions
- Familiarity with NBP, TTF gas price benchmarks
