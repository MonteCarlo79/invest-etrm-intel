# Energy_Risk_2012_-_KYOS_Power_plant_hedging_strategies

- id: `energy_risk_2012___kyos_power_plant_hedging_strategies` · class: library · type: pdf
- topic: Power plant delta hedging strategies and delta calculation methods · level: advanced · market: mixed · year: 2012
- worked examples: True · code: False

## Concepts
- **Delta sensitivity** — The change in power plant value with respect to a small change in a forward commodity price, used to quantify hedging exposure per traded product period.
- **Spark spread** — The margin between the power price and the cost of gas plus carbon required to generate one unit of electricity, determining plant dispatch profitability.
- **Dark spread** — The margin between the power price and the cost of coal plus carbon required to generate electricity from a coal-fired plant.
- **Spark spread option** — A financial option whose payoff equals the spark spread when positive and zero otherwise, capturing the optionality inherent in power plant dispatch.
- **Margrabe's formula** — A closed-form spread option pricing model that values the right to exchange one asset for another, applied here to value power plants as spread options on power versus fuel-plus-carbon.
- **Intrinsic value** — The plant value calculated from current forward price levels only, treating the plant as fully dispatched or idle with no recognition of price uncertainty or hourly flexibility.
- **Hourly intrinsic value** — An extension of intrinsic value that shapes the forward curve to hourly granularity but still ignores forward price variability and optionality.
- **Cointegration** — A long-run equilibrium relationship between commodity prices (e.g. power and fuel) that constrains spark spread dynamics and is absent from the standard Margrabe framework.
- **Gradual linear hedging strategy** — A benchmark approach that sells equal expected production volumes along the forward curve over the hedging horizon, balancing price and liquidity risk.
- **Hedging benchmark** — The target forward sale volume defined as a fraction of expected future production, against which actual trading positions are monitored.
- **Geometric Brownian Motion (GBM)** — The price process assumed by Margrabe's formula for the combined fuel-plus-carbon leg of the spread, implying log-normally distributed prices.
- **Take-or-pay constraint** — A contractual obligation requiring a plant operator to pay for a minimum fuel volume regardless of actual dispatch, creating inflexibility in hedging and dispatch.
- **Oil-indexed gas contract** — A long-term gas supply contract whose price is linked to oil indices rather than gas market quotes, creating additional commodity exposure for plant operators.
- **Asset-backed trading** — Trading activity directly linked to and constrained by physical plant positions, distinguished from speculative positions for risk attribution and capital allocation purposes.
- **Power purchasing agreement (PPA)** — A structured long-term contract for the forward sale of power output, used as an alternative or supplement to exchange-traded hedging instruments.

## Methods
- Margrabe's spread option formula (closed-form)
- Monte Carlo simulation of forward and spot prices
- Dynamic programming for optimal plant dispatch
- Delta calculation by average simulated production per period
- Delta calculation by finite-difference price shocking
- Hourly forward curve shaping
- Volatility and correlation term structure estimation
- Gradual linear hedging schedule construction

## Implied prerequisites
- Options pricing theory (Black-Scholes framework)
- Spread option valuation
- Forward and futures markets mechanics
- Power and gas market structure
- Monte Carlo simulation techniques
- Dynamic programming
- Stochastic processes and GBM
- Cointegration and time-series econometrics
- Value-at-risk concepts
- Physical power plant dispatch mechanics
