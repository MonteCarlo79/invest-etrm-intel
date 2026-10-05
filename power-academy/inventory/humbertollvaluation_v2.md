# HumberTollValuation_v2

- id: `humbertollvaluation_v2` · class: library · type: ppt
- topic: Tolling Agreement Valuation (Gas-to-Power Spark Spread Options) · level: advanced · market: GB · year: 2012
- worked examples: True · code: False

## Concepts
- **Tolling Agreement** — A contractual arrangement granting the right to convert gas into power at a fixed efficiency, modelled as a strip of spark spread options with variable costs.
- **Clean Spark Spread (CSS) Option** — An option on the spread between power price and gas cost (adjusted for carbon and efficiency), forming the underlying building block of the toll valuation.
- **Intrinsic Value (Unshaped)** — The value of the strip of options computed using quoted seasonal forward prices without intra-seasonal price shaping.
- **Intrinsic Value (Shaped)** — The incremental option value arising from applying month/quarter and workday/weekend forward curve shape ratios to seasonal quotes.
- **Extrinsic Value** — The time-value component of the option strip reflecting volatility and correlation of power, gas, and carbon beyond the shaped forward curve.
- **Forward Curve Shaping** — The process of disaggregating seasonal forward prices into monthly and daily granularity using historically estimated ratios for peak, off-peak, and weekend periods.
- **Volatility Term Structure** — The empirical relationship between local volatility of power and gas contracts and their time-to-maturity, typically showing higher volatility for near-dated contracts.
- **Correlation Term Structure** — The empirical relationship between pairwise correlations of power, gas, and carbon and their time-to-maturity, typically increasing for longer-dated contracts.
- **Daily Option Portfolio** — The decomposition of the tolling strip into individual daily peak (weekday), off-peak (weekday), and baseload (weekend) options for granular valuation.
- **Delta (Option Sensitivity)** — The rate of change of toll value with respect to underlying base, peak, or off-peak forward prices, computed monthly and reflecting in/out-of-the-moneyness.
- **Capacity Fee** — A monthly fixed payment by the buyer expressed per MW per month, used as the metric for converting total option value into deal economics.
- **Variable Cost (Heat Rate / VOM)** — Strike-equivalent costs per MWh including fuel efficiency, O&M, BSUoS, and NTS charges that determine the effective strike of each spread option.
- **BSUoS (Balancing Services Use of System)** — A GB-specific uplift charge modelled as an additional variable cost borne by the option buyer, affecting the effective option strike.
- **NTS SO Exit Charge** — A National Transmission System charge modelled as an additional variable cost that reduces effective spark spread value.
- **Dirty Spark Spread** — The spread between power price and the combined cost of gas and carbon at a given plant efficiency, before deducting variable operating costs.

## Methods
- Strip-of-options valuation (daily granularity)
- Forward curve shaping using seasonal/monthly/workday ratios
- Historical ratio estimation for curve disaggregation
- Volatility term structure calibration from historical time series
- Correlation term structure calibration from historical time series
- Intrinsic value calculation (unshaped and shaped)
- Extrinsic value calculation via multi-commodity option model
- Delta computation via finite difference on forward prices
- Sensitivity analysis (vega and correlation sensitivity)
- Capacity fee conversion from total value

## Implied prerequisites
- Options pricing theory (Black-76 or equivalent)
- Spark spread option mechanics
- GB power market structure (peak/off-peak/baseload products, seasons)
- GB gas market (NBP pricing)
- Carbon market basics (EU ETS)
- Forward curve construction for power and gas
- Time series analysis for volatility and correlation estimation
- Basic financial Greeks (delta, vega)
