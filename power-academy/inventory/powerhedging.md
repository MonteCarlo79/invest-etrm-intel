# PowerHedging

- id: `powerhedging` · class: library · type: pdf
- topic: Power plant delta-hedging strategies and delta calculation methods · level: intermediate · market: EU · year: 2012
- worked examples: True · code: False

## Concepts
- **Delta sensitivity** — The change in power plant value with respect to a small change in a forward commodity price, used to determine hedge volumes.
- **Spark spread** — The margin between the power price and the cost of gas and carbon required to produce one unit of power at a given efficiency.
- **Dark spread** — The margin between the power price and the cost of coal and carbon required to produce one unit of power.
- **Spark spread option** — A call option on the spark spread representing the plant owner's right to dispatch only when the spread is positive.
- **Margrabe's formula** — A closed-form spread-option pricing model used to value power plants and compute delta hedges by treating the plant as an exchange option on two commodities.
- **Intrinsic value approach** — A static valuation method that dispatches the plant based solely on current forward spread levels, ignoring price uncertainty and hourly optionality.
- **Hourly intrinsic value** — A refined intrinsic approach that shapes the forward curve to hourly granularity before computing plant value, still ignoring price variability.
- **Cointegration** — A long-run equilibrium relationship between commodity prices that constrains spark spread dynamics and is absent from standard GBM-based spread option models.
- **Geometric Brownian Motion (GBM)** — The continuous-time price process assumed for commodity prices in Margrabe's formula, characterised by constant drift and volatility.
- **Gradual linear hedging strategy** — A benchmark approach that builds hedge volume incrementally over the hedging horizon by selling equal forward volumes on average across time.
- **Hedging benchmark** — A target hedge volume schedule defined as a product of a gradual time-based ramp and expected future production, against which actual hedge positions are monitored.
- **Asset-backed trading** — Trading activity directly linked to and bounded by the physical asset position, kept separate from speculative proprietary positions for performance attribution.
- **Dynamic programming dispatch model** — An optimisation technique used to determine the economically optimal dispatch schedule for a power plant subject to its physical operating constraints.
- **Plant dispatch constraints** — Physical operating restrictions on a power station such as start costs, minimum run-times, heat delivery obligations, and take-or-pay commitments.
- **Volatility term structure** — The schedule of forward price return volatilities across delivery periods used as input to spread option models.
- **Correlation term structure** — The schedule of return correlations between commodity forward prices across delivery periods used in multi-commodity hedging models.
- **Oil-indexed gas contracts** — Long-term gas supply agreements where the gas price is linked to oil price indices rather than traded gas market quotes.
- **Power purchasing agreement (PPA)** — A structured long-term contract for the sale of power output from a plant, used as an alternative to exchange-traded forward hedging.
- **Take-or-pay contract** — A fuel supply contract obligating the buyer to pay for a minimum volume regardless of actual consumption, creating a floor on fuel cost exposure.
- **Value-at-risk (VaR)** — A statistical measure of the potential loss on a portfolio over a given horizon and confidence level, used for capital allocation and risk reporting.

## Methods
- Margrabe's closed-form spread option formula
- Monte Carlo simulation of forward and spot prices
- Dynamic programming for optimal plant dispatch
- Delta calculation by average simulated production per traded period
- Delta calculation by finite-difference price shocking
- Hourly power price curve shaping
- Volatility and correlation term structure calibration
- Gradual linear hedging schedule construction

## Implied prerequisites
- Options pricing theory
- Forward and futures markets mechanics
- Commodity price modelling
- Stochastic calculus and GBM
- Monte Carlo simulation methods
- Power plant dispatch economics
- Energy market structure and liquidity
- Cointegration and time-series econometrics
- Greeks and delta hedging concepts
