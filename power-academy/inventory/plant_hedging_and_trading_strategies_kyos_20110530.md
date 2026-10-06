# Plant hedging and trading strategies - KYOS - 20110530

- id: `plant_hedging_and_trading_strategies_kyos_20110530` · class: library · type: pdf
- topic: Power plant hedging and delta calculation strategies · level: intermediate · market: mixed · year: 2011
- worked examples: True · code: False

## Concepts
- **Delta hedging** — The strategy of trading forward contracts in quantities equal to the expected production (delta) of a power plant to reduce price exposure.
- **Delta sensitivity** — The partial derivative of plant value with respect to a forward price, used to determine the correct hedge volume per commodity and period.
- **Dark spread** — The profit margin of a coal-fired power station, defined as the difference between the power price and the coal cost of generation.
- **Spark spread** — The profit margin of a gas-fired power station, defined as power price minus fuel cost minus carbon cost adjusted for plant efficiency.
- **Spark spread option** — The optionality of a gas-fired plant to run only when the spark spread is positive, valued as a call option on the spread.
- **Intrinsic value approach** — A static dispatch method that treats the plant as fully in-the-money or fully out-of-the-money based solely on current forward spread levels, ignoring price uncertainty.
- **Real option value** — The additional expected profit arising from the plant's flexibility to dispatch only when profitable, above the intrinsic forward margin.
- **Margrabe's formula** — A closed-form spread option pricing model that treats each leg of the spread as a geometric Brownian motion to derive option value and delta sensitivities.
- **Geometric Brownian Motion (GBM)** — A continuous-time stochastic process with log-normally distributed returns, used as the price dynamics assumption in Black-Scholes-type models.
- **Cointegration** — A long-run equilibrium relationship between power and fuel prices that keeps spark spreads within a narrow bandwidth and is ignored by Margrabe's formula.
- **Gradual linear hedging strategy** — A benchmark strategy that builds up forward hedge volumes uniformly over the hedging horizon until expected production is fully covered before delivery.
- **Risk premium** — The discount producers may accept when selling power forward relative to expected spot realisation, arising from asymmetric hedging demand between producers and consumers.
- **Expected production (hedge volume)** — The probability-weighted average output of the plant used as the target forward sale quantity to minimise profit variance.
- **Power purchasing agreement (PPA)** — A structured long-term contract for the sale of power output, used as an alternative to liquid forward market hedging.
- **Take-or-pay contract** — A structured gas supply contract imposing minimum offtake obligations that affect the effective marginal fuel cost and dispatch decision.
- **Oil-indexed gas contract** — A gas supply agreement where the price is linked to oil rather than a gas market hub, creating additional commodity cross-exposures for the plant.
- **Hourly dispatch flexibility** — The plant's ability to vary output on an hourly basis, which determines the realistic delta profile and is not captured by monthly spread option models.
- **Monte Carlo simulation for plant valuation** — A full simulation approach that generates many price scenarios and optimises dispatch in each to compute expected production and delta sensitivities accurately.
- **Dynamic programming for dispatch optimisation** — An optimisation technique used to find the plant's optimal dispatch schedule subject to physical constraints across simulated price paths.
- **Value-at-Risk (VaR)** — A risk metric quantifying potential portfolio losses at a given confidence level, used in capital allocation and trading book oversight.
- **Forward curve shaping** — The decomposition of monthly or quarterly forward prices into hourly granularity to capture intraday price structure in valuation.
- **Asset-backed trading** — Trading activity directly linked to physical plant positions and hedging benchmarks, kept separate from speculative positions for accurate profit attribution.

## Methods
- Margrabe's spread option formula
- Black-Scholes option pricing framework
- Monte Carlo simulation of forward and spot prices
- Dynamic programming for dispatch optimisation
- Delta calculation by price shocking and revaluation
- Delta calculation by average simulated production
- Hourly intrinsic valuation
- Monthly peak/offpeak spread option decomposition
- Volatility and correlation term structure calibration
- Scenario analysis for hedge effectiveness

## Implied prerequisites
- Forward and futures market mechanics
- Basic options theory (calls, puts, payoff profiles)
- Commodity price modelling (GBM, log-normal returns)
- Power market fundamentals (dispatch, merit order)
- Carbon market basics (EU ETS, CO2 pricing)
- Stochastic calculus fundamentals
- Portfolio risk management concepts
- Electricity market structure and liquidity
