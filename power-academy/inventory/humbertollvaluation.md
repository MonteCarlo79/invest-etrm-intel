# HumberTollValuation

- id: `humbertollvaluation` · class: library · type: ppt
- topic: Tolling Agreement Valuation (CSS Options on UK Power/Gas Spread) · level: advanced · market: GB · year: 2012
- worked examples: True · code: False

## Concepts
- **Tolling Agreement** — A contractual structure giving the holder the right to convert gas into power at a physical plant for a variable cost (toll fee), modelled as a strip of spark-spread options.
- **Commodity Spread Swap (CSS) Option** — A daily option on the spread between UK power and NBP gas prices, with physical delivery, forming the building block of the tolling strip.
- **Intrinsic Value** — The value of the tolling strip based solely on current forward prices without accounting for future price uncertainty.
- **Unshaped Intrinsic Value** — Intrinsic value computed using only seasonal (quarterly/seasonal) forward price granularity before any intra-period shaping is applied.
- **Shape Value** — Incremental value gained by moving from seasonal to monthly forward curves using market-derived month/quarter, season/quarter, and weekday/weekend ratio shapes.
- **Extrinsic (Time) Value** — The component of option value attributable to volatility and correlation of the underlying commodities beyond what is captured in current forward prices.
- **Local Volatility Term Structure** — The modelled relationship between implied volatility and time-to-maturity for each commodity, capturing the stylised fact that near-dated contracts are more volatile than far-dated ones.
- **Correlation Term Structure** — The modelled relationship between pairwise commodity correlations (power peak, off-peak, gas, carbon) and time-to-maturity, capturing increasing correlation at longer tenors.
- **Peak/Off-Peak/Baseload Decomposition** — Splitting the tolling option strip into separate weekday peak, weekday off-peak, and weekend baseload option components for valuation and risk purposes.
- **Delta** — First-order sensitivity of the tolling value to changes in the underlying base, peak, or off-peak forward power prices, computed on a monthly bucket basis.
- **Capacity Fee** — A fixed monthly payment per MW made by the buyer to the seller under the tolling agreement, benchmarked against the calculated extrinsic value per unit of capacity.
- **BSUoS and NTS Charges** — Balancing Services Use of System and National Transmission System SO Exit charges modelled as additional variable costs borne by the buyer within the tolling structure.

## Methods
- Strip-of-options valuation for tolling agreements
- Forward curve shaping using month/quarter, season/quarter, and weekday/weekend ratios
- Term-structure modelling of local volatilities
- Term-structure modelling of pairwise commodity correlations
- Spark-spread option pricing incorporating power, gas, and carbon
- Monthly delta calculation via forward price bumping

## Implied prerequisites
- Options pricing fundamentals (Black-Scholes / spread option models)
- UK power market structure (peak, off-peak, baseload definitions)
- NBP gas market and spark-spread mechanics
- Forward curve construction for power and gas
- Volatility surface concepts
- Correlation modelling in multi-commodity settings
- Basic energy risk management (delta hedging)
