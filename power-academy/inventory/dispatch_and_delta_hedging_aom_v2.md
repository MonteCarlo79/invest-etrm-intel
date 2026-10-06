# Dispatch and Delta Hedging AOM v2

- id: `dispatch_and_delta_hedging_aom_v2` · class: library · type: ppt
- topic: Dispatch and Delta Hedging for Power Assets · level: advanced · market: mixed · year: None
- worked examples: True · code: False

## Concepts
- **AOM Baseload Delta** — The sensitivity of an asset or option model (AOM) value to changes in baseload power prices, used as a hedging quantity.
- **Dispatch Comparison** — Side-by-side evaluation of physically dispatched generation volumes against model-derived delta positions to assess hedging accuracy.
- **Peak Incremental Delta** — The marginal price sensitivity of a peaking asset's value with respect to peak-period power prices, isolating the peak component.
- **Composite Baseload Delta** — An aggregated delta measure combining baseload and peak incremental exposures into a single equivalent baseload hedge quantity.
- **Delta Corrections** — Adjustments applied to raw model delta outputs to reconcile discrepancies between theoretical hedge ratios and observed dispatch behaviour.
- **Negative Composite Baseload Delta** — A scenario in which the net aggregated baseload delta becomes negative, indicating a short baseload exposure after combining peak and baseload components.

## Methods
- Delta hedging
- Dispatch vs delta comparison
- Pre- and post-correction delta analysis
- Peak/baseload decomposition

## Implied prerequisites
- Power option pricing fundamentals
- Greeks and delta computation
- Baseload and peak contract structures
- Physical asset dispatch modelling
- Spark spread and heat rate concepts
