# tseng barz - short term generation asset valuation a real options approa...

- id: `tseng_barz_short_term_generation_asset_valuation_a_real_opti` · class: library · type: pdf
- topic: Short-term generation asset valuation with unit commitment constraints using real options · level: advanced · market: US · year: 2002
- worked examples: True · code: False

## Concepts
- **Spark spread option** — The payoff of a power plant modelled as a call option on the spread between electricity price and heat-rate-weighted fuel price.
- **Unit commitment constraints** — Physical operating rules—minimum uptime, minimum downtime, startup/shutdown lead times—that restrict when a thermal generator can change its on/off state.
- **Minimum uptime and downtime** — Requirements that once a thermal unit is started or stopped it must remain in that state for a specified minimum number of periods.
- **Startup and shutdown costs** — Economic costs incurred when switching a unit between offline and online states, including fuel and labour components that vary with boiler cooling state.
- **State variable for unit commitment** — A signed integer tracking whether a unit is online or offline and how many periods it has been in that mode.
- **Indifference locus (IL)** — The boundary in the (electricity price, fuel price) plane at which the expected profit from committing versus not committing a unit is equal.
- **Multistage stochastic dynamic programming** — A backward-recursive optimisation framework that embeds price uncertainty into sequential unit-commitment decisions over a planning horizon.
- **Forward Monte Carlo simulation** — Price-path generation moving forward in time used within the backward dynamic programming to evaluate expected future values.
- **Geometric mean-reverting price process** — A continuous-time Ito process for electricity or fuel prices with mean reversion and lognormally distributed levels used to capture observed market dynamics.
- **Heat rate** — The conversion ratio (MMBtu/MWh) between fuel consumed and electricity generated, represented as a quadratic function of output level.
- **Optimal dispatch** — Real-time selection of generation output to maximise instantaneous profit subject to capacity bounds, solved analytically given current prices.
- **Ramp rate constraint** — A limit on how quickly a generator can change its output level between consecutive online periods.
- **Analytical upper bound on plant value** — A relaxed formulation with no minimum uptime/downtime or startup costs that yields a closed-form overestimate of true plant value.
- **Financial options approach to plant valuation** — Prior literature method that values a power plant as a strip of European spark-spread call options, ignoring operational constraints.
- **Spinning reserve market** — An ancillary-service capacity market where an online unit can sell residual capacity below its rated maximum.
- **No-load cost** — The fixed fuel cost component required to keep a thermal unit online at zero power output.
- **Risk-neutral vs actual price processes** — Distinction between market-implied risk-neutral distributions used in derivatives pricing and historical statistical distributions used for operational valuation.
- **Maximum likelihood estimation for price processes** — Statistical procedure for fitting mean-reversion rate, volatility and seasonality parameters of log-price processes to historical data.
- **State-transition diagram** — A graphical representation of feasible transitions between unit operating states governed by uptime, downtime and lead-time constraints.
- **Boiler cooling function** — An exponential model describing how startup cost varies with the duration a unit has been offline as the boiler loses heat.
- **Real options valuation** — Treating generation asset operational flexibility as financial options to derive economic value.
- **Spark spread options** — Options on the spread between electricity prices and fuel costs used to value and hedge power plant output.
- **Stochastic differential equations** — Continuous-time probabilistic models governing the dynamics of electricity and fuel prices.
- **Unit commitment** — Short-term scheduling problem determining which generating units to commit subject to operational constraints.
- **Ramp constraints** — Physical limits on the rate of change of generator output incorporated into short-term scheduling.
- **Monte Carlo simulation for path-dependent options** — Simulation-based pricing method extended to handle options whose payoffs depend on the full price path.
- **Two-factor price lattices** — Discrete lattice framework incorporating two stochastic price factors for generation asset valuation.
- **Nodal pricing and transmission rights** — Locational marginal pricing and associated financial rights capturing the value of transmission capacity.
- **Electricity derivatives pricing and risk management** — Valuation and hedging of derivative instruments written on electricity prices in competitive markets.
- **Options on futures spreads** — Financial options whose underlying is the spread between two futures contracts, applied to energy commodities.

## Methods
- Backward stochastic dynamic programming
- Forward Monte Carlo simulation
- Combined forward-MC / backward-DP algorithm
- Root-finding for indifference locus construction
- Geometric mean-reverting Ito process simulation
- Maximum likelihood parameter estimation
- Quadratic heat-rate function fitting
- Analytical upper-bound derivation via constraint relaxation
- Heuristic ramp-constrained dispatch
- State-space discretisation for ramp constraints
- Monte Carlo simulation
- Real options analysis
- Stochastic differential equation modelling
- Lattice (binomial/two-factor) methods
- Dynamic programming
- Financial options theory applied to physical assets
- Unit commitment optimisation

## Implied prerequisites
- Stochastic calculus and Ito processes
- Dynamic programming and Bellman equations
- Monte Carlo simulation methods
- Option pricing theory
- Unit commitment problem formulations
- Electricity market structure and spot pricing
- Statistical estimation and maximum likelihood
- Mixed-integer optimisation
- Commodity price modelling
- Stochastic calculus
- Financial derivatives theory
- Electricity market structure
- Optimisation and operations research
- Power systems engineering fundamentals
- Numerical methods for option pricing
