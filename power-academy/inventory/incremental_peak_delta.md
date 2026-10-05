# Incremental Peak Delta

- id: `incremental_peak_delta` · class: library · type: pdf
- topic: Incremental Peak Delta · level: intermediate · market: mixed · year: None
- worked examples: True · code: False

## Concepts
- **Peak Delta** — The price differential between peak and off-peak power products expressed as an incremental scalar or spread.
- **Baseload Price** — The flat around-the-clock reference price used as the foundation for deriving peak and off-peak valuations.
- **Peak Price** — The price applicable to on-peak delivery hours, derived by applying a scalar to the baseload price.
- **Off-Peak Price** — The price applicable to off-peak delivery hours, implied residually from baseload and peak prices given their volumetric weights.
- **Peak Scalar** — A multiplicative factor applied to the baseload price to obtain the peak price, capturing the ratio of peak to flat value.
- **Around-the-Clock (ATC) Averaging** — The volumetric weighting of peak and off-peak hours to reconcile component prices back to the baseload flat price.
- **Shape Value** — The incremental value attributable to the hourly load shape relative to a flat baseload position.

## Methods
- Scalar multiplication of baseload price to derive peak price
- Volumetric hour-weighting (e.g. 10 MWh off-peak, 15 MWh peak) to back out implied off-peak price
- Algebraic decomposition of around-the-clock price into peak and off-peak components
- Delta calculation as difference between peak and baseload price levels

## Implied prerequisites
- Basic power market product definitions (baseload, peak, off-peak)
- Hour-count conventions for peak and off-peak periods
- Arithmetic price weighting and averaging
- Understanding of MWh volumetric settlement
