# CCGT Replication

- id: `ccgt_replication` · class: library · type: ppt
- topic: CCGT Replication · level: advanced · market: GB · year: 2012
- worked examples: True · code: False

## Concepts
- **CCGT intrinsic value** — Value of a combined-cycle gas turbine expressed as a weighted sum of spark-spread call options across operating levels.
- **Spark spread** — The margin between power price and gas cost (adjusted by heat rate) that determines CCGT dispatch profitability.
- **Heat rate** — The ratio of fuel input to electrical output used to convert gas prices into power-equivalent cost at different load levels.
- **SEL and MEL** — Stable Export Limit and Maximum Export Limit defining the minimum and maximum generation output levels of the plant.
- **Replication by time-of-use segment** — Decomposition of plant cash flows into weekday peak, weekday off-peak, weekend daytime and weekend night-time buckets for separate calibration.
- **Piecewise call-option regression** — Fitting multiple call-option terms with distinct volume, heat-rate and strike parameters to replicate non-linear dispatch cash flows.
- **R-squared goodness of fit** — Proportion of variance in modelled cash flows explained by the replicating portfolio across each time-of-use segment.
- **Explained value (hour-weighted)** — Aggregate replication accuracy measured as the percentage of total plant value captured by the portfolio, weighted by settlement hours.

## Methods
- Spark-spread option decomposition
- Piecewise linear regression with call-option basis functions
- Time-of-use segmentation
- Ordinary least squares calibration of replication parameters
- R-squared and hour-weighted explained-value diagnostics

## Implied prerequisites
- Energy commodity derivatives pricing
- Spark-spread option valuation
- Power plant dispatch economics
- Regression analysis
- Electricity market structure (peak/off-peak settlement periods)
