# Forward Curve Python Script

- id: `forward_curve_python_script` · class: library · type: doc
- topic: Forward Curve Construction and Shaping · level: intermediate · market: GB · year: 2012
- worked examples: False · code: True

## Concepts
- **Forward Curve Shaping** — The process of disaggregating longer-tenor contract prices (seasonal, quarterly, annual) into shorter-tenor prices (monthly, daily) using historical ratio relationships.
- **Contract Breakdown Hierarchy** — The structured mapping of larger period contracts (e.g. Winter season) into their smaller constituent contracts (e.g. Q1, Q2) for cascading price decomposition.
- **Historical Price Ratios** — The average ratio of a shorter-tenor contract price to its parent longer-tenor contract price, estimated over a lookback window of traded days.
- **Normalised Monthly Ratios** — Ratio adjustments applied to ensure that the weighted average of disaggregated shorter-period prices is consistent with the parent contract price.
- **Last Trading Day Effects** — The distortion in price ratios near contract expiry, mitigated by excluding a buffer of trading days before the last trading day from the historical sample.
- **Weekend/Weekday Price Ratio** — The empirical ratio of weekend (WEND) to day-ahead (DA) prices by calendar month, used to split monthly average prices into weekday and weekend daily prices.
- **Daily Shaping** — The application of weekend/weekday ratios to monthly average prices to produce distinct weekday and weekend price estimates for each day in the curve.
- **Off-Peak Power Price** — The implied price for off-peak power hours, derived algebraically from baseload and peak power prices.
- **Value Date** — The reference date for which the forward curve is constructed, determining which snapshot of market prices is extracted from the historical data file.
- **Overlapping Contract Discarding** — The rule whereby, when granular and broader contracts cover the same period, the smaller explicitly quoted contracts take precedence and the implied breakdown of the larger contract is discarded.
- **Gregorian vs EFA Calendar** — The distinction between the standard Gregorian calendar assumed for power contracts in this implementation and the Electricity Forward Agreement calendar conventionally used in GB power markets.
- **Bid-Offer Mid Price** — The arithmetic mid-point between bid and offer quotes used as the representative price for ratio and curve calculations.

## Methods
- Historical ratio averaging over rolling lookback window
- Cascading contract disaggregation (season → quarter → month → day)
- Weekend/weekday ratio estimation from historical spot and forward data
- Algebraic off-peak price derivation from baseload and peak
- Weekday and weekend day counting per contract month
- CSV export of shaped daily price curves and ratio tables
- End-of-data detection for dynamic historical file parsing

## Implied prerequisites
- Energy commodity forward market conventions (gas and power)
- Python programming (dictionaries, file I/O, loops)
- Basic calendar arithmetic and date handling
- Understanding of seasonal, quarterly and monthly power/gas contract structures
- GB power market structure (baseload, peak, off-peak)
- Spreadsheet data management (Excel tabs, CSV formats)
