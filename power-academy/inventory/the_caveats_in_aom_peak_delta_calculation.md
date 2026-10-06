# The Caveats in AOM Peak Delta Calculation

- id: `the_caveats_in_aom_peak_delta_calculation` · class: library · type: doc
- topic: AOM Peak Delta Calculation Methodology and Caveats · level: advanced · market: GB · year: 2011
- worked examples: True · code: False

## Concepts
- **Baseload Delta** — The full average baseload delta of a CCGT asset as produced directly by AOM optimisation output.
- **Incremental Peak Delta** — The marginal delta attributable to peak hours, derived by decomposing the full baseload delta using scalar sensitivity runs in AOM.
- **Composite Baseload Delta** — The residual baseload delta remaining after subtracting the incremental peak delta from the full CCGT delta, used for hedging off-peak exposure.
- **Peak Scalar** — A scalar applied to peak-hour prices within AOM to shift peak price levels for sensitivity calculations without altering overall price.
- **Scalar Sensitivity Run** — An AOM run in which peak scalars are shifted by a small increment and off-peak scalars adjusted inversely, used to isolate peak price sensitivity.
- **Station Value Change** — The difference in optimised station value between the scalar sensitivity run and the base run, used as the numerator in delta calculations.
- **Peak Price Change** — The correctly computed change in peak price, defined as the change in peak scalar multiplied by the baseload price rather than the peak price.
- **7-Day Peak vs 5-Day Peak** — The distinction between AOM's monthly-granularity optimisation covering all seven days and market-traded peak forward contracts covering only the five working days.
- **EFA Blocks** — UK electricity market settlement periods (EFA blocks 3, 4, and 5) that define peak hours within AOM's optimisation framework.
- **Delta Decomposition** — The process of splitting a CCGT's full delta into mutually exclusive peak and baseload components for front-office hedging purposes.
- **AOM (Asset Optimisation Model)** — An in-house optimisation model operating at monthly granularity that produces station values and baseload deltas but does not distinguish weekday from weekend.
- **ISM Line** — An internal benchmark based on dispatch volumes from optimisation output, used as the basis for trading decisions and transfer pricing.
- **Transfer Pricing between Desks** — The internal financial settlement between Power Generation and Midstream desks based on incremental peak and composite baseload delta figures.

## Methods
- Scalar perturbation sensitivity analysis
- Two-run AOM comparison (base run vs scalar sensitivity run)
- Delta decomposition via finite difference approximation
- Adjustment scaling of 7-day to 5-day peak delta

## Implied prerequisites
- Options and delta hedging fundamentals
- Power market products: baseload and peak forward contracts
- UK electricity market structure and EFA periods
- Asset optimisation modelling for thermal generation
- CCGT dispatch and valuation concepts
- Mark-to-market PnL attribution
- Price scalar construction in optimisation models
