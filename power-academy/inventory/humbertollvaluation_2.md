# HumberTollValuation

- id: `humbertollvaluation_2` · class: library · type: ppt
- topic: Tolling Agreement Valuation (Spark Spread Options) · level: advanced · market: GB · year: 2012
- worked examples: True · code: False

## Concepts
- **Tolling Agreement** — A contractual arrangement giving the holder the right to convert gas into power at a physical plant for a capacity fee plus variable costs.
- **Spark Spread Option (CSS)** — A daily option on the spread between power prices and gas cost (converted via heat rate), with physical delivery.
- **Peak / Off-Peak Product Structure** — Decomposition of the tolling strip into separate weekday peak, weekday off-peak, and weekend baseload option legs.
- **Intrinsic Value** — The value of the option strip computed from current forward prices without accounting for future price uncertainty.
- **Shape Value** — Additional intrinsic value captured by using monthly-shaped curves derived from month-to-quarter, season-to-quarter, and weekday-to-weekend ratios.
- **Extrinsic Value** — The optionality value of the tolling strip arising from volatility and correlation of the underlying power, gas, and carbon forward prices.
- **Local Volatility Term Structure** — The time-to-maturity-dependent volatility surface where near-maturity contracts exhibit higher volatility than longer-dated ones.
- **Correlation Term Structure** — The time-to-maturity-dependent structure of pairwise correlations between commodities, typically increasing for longer-dated contracts.
- **Variable Cost / Strike** — The per-MWh variable operating cost of the plant that acts as the effective strike of the spark spread option.
- **Delta** — Sensitivity of the tolling strip value to changes in the underlying baseload, peak, or off-peak forward prices, computed on a monthly basis.
- **Capacity Fee** — A fixed monthly payment per MW made by the buyer for the right to dispatch the plant, representing the cost of optionality.
- **BSUoS and NTS SO Exit Charges** — UK-specific balancing and transmission charges modelled as additional variable costs borne by the option buyer.

## Methods
- Forward curve construction with seasonal shaping (M/Q, S/Q, WD/WE ratios)
- Intrinsic valuation from shaped forward curves
- Extrinsic valuation via portfolio of daily spark spread options
- Term structure modelling of local volatilities
- Term structure modelling of pairwise commodity correlations
- Monthly delta computation via forward price bumping

## Implied prerequisites
- Options pricing theory (Black-76 or equivalent)
- Spread option pricing (multi-asset)
- Power and gas forward curve construction
- UK power market structure (peak/off-peak definitions, BSUoS, NTS charges)
- Volatility surface calibration
- Correlation modelling for multi-commodity portfolios
- Basic tolling/virtual power plant concepts
