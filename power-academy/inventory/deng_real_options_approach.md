# Deng Real options approach

- id: `deng_real_options_approach` · class: library · type: pdf
- topic: Real options valuation of electricity tolling agreements · level: advanced · market: US · year: 2006
- worked examples: True · code: False

## Concepts
- **Tolling agreement** — A supply contract granting the buyer the right to take output from a generation asset by paying a predetermined premium, subject to operational and contractual constraints.
- **Spark spread** — The margin between the electricity price and the heat-rate-adjusted fuel cost, representing the instantaneous payoff of operating a thermal generator.
- **Heat rate** — A measure of generator fuel efficiency expressed as units of fuel (MMBtu) consumed per unit of electricity (MWh) produced, varying with output level.
- **Operational option** — The right but not obligation of a plant operator to generate electricity, whose payoff at each time step equals the positive spark spread.
- **Real option** — A managerial flexibility right on a physical asset valued using financial option-pricing theory, here applied to the decision to start or stop electricity generation.
- **Restart constraint** — A contractual limit on the maximum number of plant restarts permitted over the tolling contract's life, reducing operational flexibility and contract value.
- **Ramp-up delay** — The time period required for a generating unit to reach minimum output after starting from the off state, during which fuel costs are incurred but no electricity is produced.
- **Startup and shutdown costs** — Fixed costs borne by the tolling contract holder whenever the plant transitions from off to on or from on to off.
- **Stochastic dynamic programming (SDP)** — A backward-induction optimisation framework that determines the value function of the tolling contract by maximising expected discounted payoffs over all admissible operating actions.
- **Hamilton–Jacobi–Bellman (HJB) equations** — The recursive optimality conditions characterising the value function of the tolling contract across all plant operational states and price scenarios.
- **Value function approximation** — Approximation of the conditional expected continuation value in the dynamic programme by a truncated polynomial basis expansion whose coefficients are fitted by least-squares regression.
- **Least-squares Monte Carlo (LSM)** — A simulation-based method that estimates continuation values in American-style option problems by regressing realised future values onto basis functions of current state variables.
- **Mean-reverting process (Ornstein–Uhlenbeck)** — A continuous-time diffusion model for log-commodity prices that pulls the process back toward a long-run mean at a speed proportional to the current deviation.
- **Mean-reverting jump-diffusion process** — An extension of the mean-reverting diffusion model that superimposes a compound Poisson jump component to capture the spikes observed in electricity prices.
- **Correlated commodity price processes** — Joint modelling of electricity and natural gas log-prices as bivariate stochastic processes with a specified instantaneous correlation coefficient.
- **On-peak / off-peak price scaling** — A discretisation device that multiplies simulated daily prices by different scaling factors for peak and off-peak hours to reflect the intra-day price pattern in power markets.
- **No-arbitrage pricing** — The principle that the discount rate in the SDP equals the risk-free rate when operating payoffs can be perfectly replicated by traded forward contracts on electricity and fuel.
- **Incomplete market risk premium** — An additional spread added to the risk-free discount rate when traded instruments cannot perfectly hedge the real option payoff, reflecting unhedgeable market risk.
- **Price regime and stationarity** — The assumption that the parameters of the commodity price processes remain constant over the contract horizon, with non-stationarity leading to systematic mis-valuation.
- **Euler discretisation of SDEs** — A numerical scheme that approximates continuous-time stochastic differential equations by first-order finite-difference updates used to simulate price paths.

## Methods
- Stochastic dynamic programming (backward induction)
- Monte Carlo simulation of correlated mean-reverting and jump-diffusion price paths
- Least-squares regression for value function approximation (Longstaff–Schwartz approach)
- Polynomial basis function expansion (up to third-order monomials in exponentiated log-prices)
- Euler–Maruyama discretisation of continuous-time SDEs
- Heuristic jump-filtering parameter estimation for MRJD processes
- OLS regression of log-returns on lagged price level for mean-reversion parameter estimation
- On-peak/off-peak price scaling for intra-day price structure

## Implied prerequisites
- Stochastic calculus and Itô processes
- Financial option pricing theory (Black–Scholes, martingale pricing)
- Dynamic programming and Bellman equations
- Monte Carlo simulation methods
- Regression analysis and least squares
- Electricity market structure and power plant operations
- Commodity derivatives and forward/futures markets
- Poisson processes and compound jump processes
