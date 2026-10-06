# dipeng/Volatility model

- id: `dipeng_volatility_model` · class: practice · type: folder
- topic: UK power volatility modelling for seasonal and spread contracts · level: advanced · market: GB · year: 2017
- worked examples: True · code: False

## Concepts
- **Seasonal contract volatility** — Implied or realised volatility measured on forward seasonal (Summer/Winter) power contracts.
- **Spread volatility** — Volatility of the price spread between two related forward contracts, e.g. power vs gas.
- **Relative volatility** — Volatility of a contract expressed relative to a benchmark such as the day-ahead contract.
- **Time-to-maturity term structure** — How volatility or correlation changes as a forward contract approaches its delivery period.
- **Correlation modelling** — Estimation and forecasting of correlations between seasonal power contracts or between power and gas.
- **Moving-average volatility** — A rolling historical volatility estimate used as a smoothing and forecasting input.
- **Seasonality in volatility** — Systematic calendar patterns in forward contract volatility levels across Summer and Winter periods.
- **Volatility term structure forecast** — Out-of-sample projection of the volatility curve for a specific seasonal contract as a function of time to maturity.
- **Power-gas spread** — The price differential between UK baseload power and NBP gas used to decompose power volatility.
- **Volatility curve model** — A parametric or empirical model that maps implied or realised volatility across maturities for power contracts.
- **ATM volatility** — At-the-money volatility extracted from option prices or approximated from forward price data.
- **Skew** — The asymmetry of the implied volatility smile across strikes for power options.
- **Day-ahead (DA) volatility** — Short-dated spot-proxy volatility used as an anchor to derive longer-dated seasonal volatilities.

## Methods
- Historical realised volatility estimation
- Rolling moving-average volatility
- Relative volatility ratio modelling
- Volatility term-structure regression
- Seasonal decomposition of volatility
- Spread volatility decomposition
- Correlation estimation from historical returns
- Out-of-sample volatility forecasting
- Power volatility derivation from gas and spark-spread volatilities

## Implied prerequisites
- Forward curve mechanics for power and gas
- Options pricing fundamentals
- Statistical time-series analysis
- Implied and realised volatility concepts
- UK power and NBP gas market structure
- Spread option pricing basics
