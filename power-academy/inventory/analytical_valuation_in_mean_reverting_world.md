# Analytical Valuation in Mean-Reverting World

- id: `analytical_valuation_in_mean_reverting_world` · class: library · type: pdf
- topic: Analytical Valuation of Full Requirements Contracts in Mean-Reverting Electricity Markets · level: advanced · market: mixed · year: 2001
- worked examples: False · code: False

## Concepts
- **Full Requirements Contract** — A power supply agreement where the seller is sole provider at a fixed price per unit and the customer cannot resell received power.
- **Geometric Mean-Reverting Process** — A stochastic process for positive-valued variables such as power prices or temperature where the log-price reverts to a time-dependent equilibrium mean.
- **Mean-Reverting Process (arithmetic)** — An Ornstein-Uhlenbeck-type process for real-valued variables such as temperature reverting to a time-dependent equilibrium mean.
- **Risk-Neutral Dynamics** — The probability measure under which discounted asset prices are martingales, used here to price contingent claims by discounted expectation.
- **European Contingent Claim on Power and Temperature** — A derivative whose payout at expiry depends jointly on the spot power price and a temperature index.
- **Effective Volatility** — A time-aggregated volatility parameter that summarises the integrated variance of a mean-reverting process over a valuation interval.
- **Effective Correlation** — A time-aggregated correlation parameter between two mean-reverting processes over a valuation interval.
- **Load Structure** — A family of time-indexed load functions mapping a random state variable (temperature or load) to customer power demand at each delivery time.
- **Load Function** — A deterministic function mapping the random state variable to the quantity of power demanded by a customer at a given delivery time.
- **Strip of European Contingent Claims** — Representation of a full requirements contract as a sum of European claims with payouts at each delivery date in the contract's delivery schedule.
- **Eigenclaim / Spectral Method** — An analytical pricing technique that expands contingent claim values in terms of eigenfunctions of the pricing operator to obtain closed-form solutions.
- **Power Function Payout** — A payout structure that is a power (monomial) function of the state variable, enabling analytical reduction of joint claims to univariate temperature claims.
- **Equilibrium Mean** — The time-dependent level to which a mean-reverting process is attracted, entering the long-run drift of the stochastic differential equation.
- **Mean-Reversion Rate** — The speed parameter governing how quickly a mean-reverting process returns toward its equilibrium mean after a deviation.
- **Fair Fixed Price (Break-even Strike)** — The fixed price per unit of power at which the inception value of the full requirements contract equals zero.
- **Correlated Wiener Processes** — A pair or triple of Brownian motions with time-dependent instantaneous correlations used to model joint dynamics of power price and temperature.
- **Power Price with Spikes** — An extension of the mean-reverting price model that incorporates non-Markovian spike behaviour observed in electricity spot prices.

## Methods
- Discounted risk-neutral expectation for European claim pricing
- Spectral (eigenclaim) method for closed-form valuation
- Reduction of joint power-temperature claims to univariate temperature claims
- Analytical integration for effective volatility and correlation parameters
- Linear combination / polynomial payout decomposition
- Exponential payout decomposition for arithmetic mean-reverting temperature
- Break-even fixed-price solving from zero-NPV condition
- Limiting argument (mean-reversion rate to zero) to recover Brownian motion / GBM cases

## Implied prerequisites
- Stochastic differential equations and Ito calculus
- Risk-neutral pricing and change of measure
- Ornstein-Uhlenbeck and geometric Ornstein-Uhlenbeck processes
- Lognormal and Gaussian distribution properties
- Basic derivative pricing theory (European options, discounted expectations)
- Electricity market structure and power derivatives
- Linear algebra and functional analysis (operator/spectral methods)
