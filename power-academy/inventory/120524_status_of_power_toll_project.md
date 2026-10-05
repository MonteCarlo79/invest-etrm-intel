# 120524 Status of power toll project

- id: `120524_status_of_power_toll_project` · class: library · type: docx
- topic: Power tolling agreement valuation system architecture · level: advanced · market: mixed · year: 2012
- worked examples: False · code: False

## Concepts
- **Forward curve shaping** — Deriving granular forward price curves from liquid traded products using historical ratio-based shaping across month-quarter, quarter-season, and workday-weekend dimensions.
- **Product ratio method** — Estimating intra-period price shape by averaging historical ratios between overlapping or nested contract prices.
- **No-arbitrage curve construction** — Ensuring that shaped forward curves are consistent with observable market prices so no risk-free profit can be extracted across overlapping contracts.
- **Overlapping contract handling** — Resolving conflicts or redundancies when multiple traded contracts cover the same delivery period within a forward curve.
- **Local volatility** — Point-in-time instantaneous volatility of an energy price process calibrated to market data for a specific tenor.
- **Terminal volatility** — Integrated volatility of an energy price process over the full time horizon to expiry, used for option pricing.
- **Correlation surface** — Estimated co-movement structure between energy commodities—peak power, off-peak power, gas, and carbon—used as input to spread option pricing.
- **Generation cost (GenCost) volatility** — Direct statistical estimation of the volatility of the variable cost of generation, combining fuel and carbon price dynamics.
- **Kirk approximation** — Closed-form analytical approximation for pricing spread options on two or more underlyings, applied here as a three-legged variant for power tolling payoffs.
- **Spark spread / clean spark spread (CSS) option** — Option on the spread between power price and generation cost (fuel plus carbon), representing the optionality in a power tolling agreement.
- **Strip of daily options** — Decomposition of a tolling agreement into a sequence of daily peak and off-peak spread options covering the entire deal period.
- **Intrinsic versus extrinsic value split** — Decomposition of a tolling option's total value into the deterministic in-the-money component and the time-value component arising from price uncertainty.
- **Greeks / Delta** — Price sensitivities of the tolling valuation to movements in underlying forward prices and volatilities, used for hedging.
- **Peak / off-peak power price distinction** — Separation of power forward curves and volatilities into on-peak and off-peak delivery periods reflecting intraday demand patterns.
- **Adaptive pricer calling** — Computational efficiency technique that reduces option pricer invocations by grouping homogeneous days or using iterative refinement where value changes slowly.
- **Backtesting engine** — Framework for retrospectively evaluating valuation model performance against historical market outcomes.
- **Object-oriented (OO) valuation wrapper** — Software design pattern encapsulating deal inputs, pricing logic, and output for a strip of energy options within a reusable class structure.

## Methods
- Historical ratio averaging for forward curve shaping
- Three-legged Kirk spread option approximation
- Monte Carlo simulation (referenced for benchmark testing)
- Direct numerical integration (referenced for benchmark testing)
- Local and terminal volatility calibration
- Direct statistical volatility estimation
- Loop-based daily option strip valuation with weekday/weekend separation
- CSV-based data input/output

## Implied prerequisites
- Energy commodity forward curve construction
- Options pricing theory (Black-76 or equivalent)
- Spread option pricing concepts
- Stochastic process and volatility modelling
- Power market structure (peak/off-peak, seasons, contract conventions)
- Carbon and gas market fundamentals
- Object-oriented programming
- Basic Greek computation and hedging
