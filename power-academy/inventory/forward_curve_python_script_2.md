# Forward Curve Python Script

- id: `forward_curve_python_script_2` · class: library · type: doc
- topic: Forward Curve Construction and Shaping · level: intermediate · market: GB · year: 2012
- worked examples: False · code: True

## Concepts
- **Forward Curve Shaping** — The process of decomposing longer-tenor forward contracts into shorter-tenor constituent prices using ratio-based methods.
- **Contract Breakdown Hierarchy** — The structured mapping from larger-period contracts (seasonal, annual) down to their smaller constituent contracts (quarterly, monthly).
- **Historical Ratio Calculation** — Deriving the average price ratio of a smaller contract to its parent contract over a defined historical lookback window.
- **Normalised Monthly Ratios** — Scaling sub-period ratios so that the weighted average of shaped prices recovers the original parent contract price.
- **Weekend/Weekday Price Ratio** — The empirically derived ratio of weekend spot prices to weekday spot prices, applied to disaggregate monthly prices into daily prices.
- **Daily Shaping** — Applying weekend/weekday ratios to monthly shaped prices to produce distinct weekday and weekend price series.
- **Off-Peak Price Derivation** — Computing off-peak power prices as a residual from baseload and peak power prices using load-weighted arithmetic.
- **Last Trading Day Identification** — Determining the final traded date for each contract to define the boundary of usable historical price data.
- **End-of-Trading-Period Exclusion** — Removing a buffer of trading days prior to contract expiry from the lookback sample to avoid price distortion near expiry.
- **Value Date** — The specific date for which the forward curve is extracted from historical data and subsequently shaped.
- **Overlapping Contract Resolution** — The rule by which shorter-tenor contracts are discarded in favour of longer-tenor contracts when both cover the same delivery period.
- **EFA Calendar** — The Electricity Forward Agreement calendar used for UK power markets, distinct from the Gregorian calendar used for gas contracts.
- **Bid-Offer Mid Price** — The arithmetic mid-point between bid and offer quotes used as a single representative price for a contract.
- **Nested Price Dictionary** — A hierarchical data structure indexed by commodity, contract, and date used to store and retrieve historical price series.

## Methods
- Historical lookback ratio averaging
- Contract hierarchy decomposition
- Ratio normalisation (unweighted, flagged as incorrect)
- Weekend/weekday ratio estimation from spot data
- Daily price disaggregation via ratio application
- Off-peak price back-calculation from baseload and peak
- CSV export of shaped curves and ratios
- End-of-period buffer exclusion from estimation sample

## Implied prerequisites
- Energy forward market structure (seasonal, quarterly, monthly contracts)
- UK gas and power market conventions
- Python programming (dictionaries, file I/O, loops)
- Spreadsheet data management (Excel tabs, CSV)
- Basic time-series date arithmetic
- Concept of bid-offer spread and mid-price
