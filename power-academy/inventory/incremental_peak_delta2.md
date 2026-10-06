# Incremental Peak delta2

- id: `incremental_peak_delta2` · class: library · type: pdf
- topic: Incremental peak delta sensitivity decomposition · level: advanced · market: mixed · year: None
- worked examples: True · code: False

## Concepts
- **Peak scalar (PK scalar)** — A weighting factor that allocates a contract's exposure between peak and off-peak periods.
- **Delta (AV / delta-V)** — First-order price sensitivity of a position's value to a small shift in market price.
- **Incremental shift (0.01 shift)** — A standardised one-cent or one-unit bump applied to isolate marginal price sensitivity.
- **BI (block instrument / base instrument notional)** — The notional quantity or block size of the instrument used to scale delta calculations.
- **SPK (super-peak or shaped-peak quantity)** — The portion of the block instrument quantity attributable to peak-shaped exposure.
- **OBI (off-peak block instrument component)** — The residual block instrument quantity after removing the peak-shaped component.
- **Price scalar** — A multiplier converting a raw price shift into a market-price-equivalent delta in MWh terms.
- **CEBL (contract or exposure base level)** — A base-level contract exposure term combined with scaled peak contributions to form total delta.
- **Delta decomposition** — Separation of total value sensitivity into peak and off-peak components using scalar weighting.
- **SAK term** — An additive adjustment factor applied alongside the CEBL to compute the full incremental delta.

## Methods
- Finite-difference bumping (0.01 shift)
- Linear delta decomposition by peak/off-peak allocation
- Scalar-weighted sensitivity aggregation
- Algebraic rearrangement of block-instrument delta into peak and residual components

## Implied prerequisites
- Power market product structures (peak, off-peak, base)
- Greeks and first-order sensitivity analysis
- MWh-based position sizing and notional calculations
- Block instrument contract mechanics
