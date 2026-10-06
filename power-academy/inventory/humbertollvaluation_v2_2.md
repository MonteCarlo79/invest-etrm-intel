# HumberTollValuation_v2

- id: `humbertollvaluation_v2_2` · class: library · type: ppt
- topic: Tolling Agreement Valuation – Clean Spark Spread Options · level: advanced · market: GB · year: 2012
- worked examples: True · code: False

## Concepts
- **Tolling agreement** — A contractual arrangement giving the buyer the right to dispatch a generation asset by paying variable costs and a capacity fee in exchange for the spark spread payoff.
- **Clean Spark Spread (CSS) option** — A daily option on the spread between power prices and the sum of gas and carbon costs net of variable generation costs.
- **Unshaped intrinsic value** — Option value computed using season-level forward prices without applying intra-season price shape adjustments.
- **Shaped intrinsic value** — Option value obtained after disaggregating seasonal forward curves into monthly, quarterly and workday/weekend granularity using historical shape ratios.
- **Extrinsic value** — The component of option value attributable to volatility and correlation of the underlying commodities beyond the forward-price intrinsic.
- **Volatility term structure** — The dependence of local commodity price volatility on time-to-maturity of the underlying forward contract.
- **Correlation term structure** — The dependence of pairwise correlations between power, gas and carbon on time-to-maturity of the relevant forward contracts.
- **Capacity fee** — A monthly fixed payment made by the option buyer to the asset owner as compensation for reserving generation capacity.
- **Option delta** — The sensitivity of option value to a unit change in the underlying base, peak or off-peak forward price, computed on a monthly basis.
- **Peak vs off-peak option decomposition** — Separation of the total toll value into contributions from weekday peak, weekday off-peak and weekend baseload option strips.
- **BSUoS charge** — Balancing Services Use of System cost treated as an additional variable cost in the tolling valuation.
- **NTS SO Exit charge** — National Transmission System System Operator exit charge modelled as an incremental variable cost borne by the option buyer.
- **Dirty spark spread** — The spread between power price and gas cost at a given heat rate before netting carbon costs, used as a diagnostic time-series.
- **Parameter sensitivity** — The change in total option value for a one-percentage-point shift in key model inputs such as power/gas correlations and individual commodity volatilities.

## Methods
- Forward curve shaping using Month/Quarter, Season/Quarter and Workday/Weekend historical ratios
- Strip of daily European spread options for intrinsic and extrinsic valuation
- Local volatility term-structure estimation from one-year historical window
- Correlation term-structure estimation between power (peak/off-peak) and gas/carbon
- Extrinsic value decomposition by option type (peak, off-peak, baseload weekend)
- Capacity fee conversion from £/MWh value to £/MW/month
- Delta calculation as finite-difference change in value to underlying forward price moves

## Implied prerequisites
- Spark spread and clean spark spread mechanics
- European option pricing theory
- Forward curve construction for power and gas
- Volatility surface and term-structure concepts
- Correlation modelling between energy commodities
- GB power market structure (peak/off-peak/baseload settlement)
- BSUoS and NTS charge frameworks
- Options Greeks – delta interpretation
