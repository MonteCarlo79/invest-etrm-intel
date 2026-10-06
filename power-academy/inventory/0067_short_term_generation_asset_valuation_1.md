# 0067_Short-Term_Generation_Asset_Valuation[1]

- id: `0067_short_term_generation_asset_valuation_1` · class: library · type: pdf
- topic: Short-term power plant valuation under physical operating constraints using stochastic optimisation · level: advanced · market: US · year: 1999
- worked examples: True · code: False

## Concepts
- **Generation asset valuation** — Determination of the expected profit value of a power plant over a short operating horizon under price uncertainty.
- **Spark spread** — The difference between the electricity price and the product of the heat rate and fuel price, representing per-unit operating margin.
- **Spark spread call option** — A financial option whose payoff equals the positive part of the spark spread, used in the financial-options approach to plant valuation.
- **Multi-stage stochastic optimisation** — A sequential decision framework in which commitment choices are made before prices are revealed at each hourly stage.
- **Unit commitment** — The binary on/off scheduling decision for a generation unit, subject to minimum uptime, downtime, and lead-time constraints.
- **Minimum uptime constraint** — The requirement that once a unit is started it must remain online for at least a specified number of periods.
- **Minimum downtime constraint** — The requirement that once a unit is shut down it must remain offline for at least a specified number of periods.
- **Startup cost** — The fuel and labour cost incurred when bringing an offline unit back online, modelled as a function of boiler cooling time.
- **Commitment lead time** — The advance notice period required before a unit can become operational, capturing boiler heat-up dynamics.
- **Heat rate** — The thermal input per unit of electrical output of a generator, used to convert fuel cost into electricity production cost.
- **Input-output characteristic** — A quadratic function relating total fuel consumption (MMBtu/h) to generator output level (MW).
- **State variable** — An integer recording how long a unit has been continuously online or offline, encoding the relevant operational history for constraint checking.
- **Indifference locus** — A curve in electricity–fuel price space at which the expected value of turning the unit on equals the expected value of turning it off.
- **Cost-to-go function** — The optimal expected profit from a given state at time t to the end of the operating horizon, computed by backward dynamic programming.
- **Mean-reverting price process** — An Itô process for log-prices in which prices are pulled back toward a seasonal long-run mean at a rate proportional to displacement.
- **Lognormal price distribution** — The distributional assumption for electricity and fuel prices implied by modelling log-prices as a mean-reverting Gaussian diffusion.
- **Value at Risk (VaR)** — The loss threshold at a specified confidence level over the operating period, derived from the simulated profit distribution.
- **Maximum likelihood estimation** — Calibration of mean-reversion speed, long-run mean, and volatility parameters of the price processes from historical price data.
- **Downside risk** — The probability and magnitude of negative profits arising from physical operating constraints that prevent the operator from responding optimally to adverse prices.

## Methods
- Monte Carlo simulation (forward and backward)
- Backward dynamic programming
- Bisection root-finding for indifference locus
- Curve-fitting for indifference locus approximation
- Maximum likelihood estimation of Itô process parameters
- Stochastic dynamic programming recursion

## Implied prerequisites
- Stochastic calculus and Itô processes
- Dynamic programming
- Monte Carlo methods
- Financial option pricing theory
- Power system unit commitment
- Optimisation under constraints
- Statistical estimation (maximum likelihood)
