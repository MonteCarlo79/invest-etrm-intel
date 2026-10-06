# Gav

- id: `gav` · class: library · type: pdf
- topic: Short-term power plant valuation with unit-commitment constraints using real options · level: advanced · market: US · year: 2002
- worked examples: True · code: False

## Concepts
- **Real options for power plant valuation** — Treating the operator's hourly on/off decision as a sequence of real options on the spark spread to determine asset value.
- **Spark spread** — The net payoff per MWh defined as electricity price minus the product of heat rate and fuel price.
- **Unit commitment constraints** — Physical intertemporal constraints including minimum uptime, minimum downtime, and startup/shutdown lead times that restrict when a generator may switch states.
- **Minimum uptime and downtime** — Requirements that once a thermal unit is switched on or off it must remain in that state for a minimum number of periods.
- **State-transition dynamics** — A finite state-space representation tracking how long a unit has been continuously online or offline to enforce commitment constraints.
- **Startup and shutdown costs** — Costs incurred when toggling a thermal unit, where startup cost depends on boiler cooling state following an exponential cooling model.
- **Heat rate function** — A quadratic input-output characteristic relating fuel consumption in MMBtu to generation output in MW.
- **No-load cost** — The fixed fuel cost component of the heat rate function representing fuel consumed to keep the boiler running at zero net output.
- **Multistage stochastic programming** — Formulation of the sequential commitment decision problem under price uncertainty across multiple hourly stages.
- **Indifference locus** — The set of electricity-fuel price pairs at which the expected value of committing the unit on equals the expected value of keeping it off for a given state and time.
- **Geometric mean-reverting price process** — An Ito process for commodity prices incorporating mean reversion, lognormality, and seasonal drift used to model both electricity and fuel prices.
- **Mean reversion** — The tendency of commodity prices to be pulled back toward a long-run seasonal level, parameterised by a speed-of-reversion coefficient.
- **Ramp rate constraints** — Limits on the rate of change of generator output between successive online periods that restrict dispatch flexibility.
- **Optimal dispatch** — Real-time selection of generation level within capacity bounds to maximise hourly profit given current prices, yielding an analytical closed-form solution.
- **Backward dynamic programming recursion** — Bellman-equation-based backward induction over time periods to embed optimal future decisions into the current valuation.
- **Forward Monte Carlo simulation** — Forward-in-time simulation of price paths used to evaluate expected payoffs and locate indifference loci for each state.
- **Analytical upper bound** — A relaxed version of the valuation problem with no commitment constraints that yields a tractable European spark-spread call formula serving as an upper bound.
- **Financial options approach to generation valuation** — Existing literature methods that value a plant as a strip of European call options on the spark spread, ignoring physical operational constraints.
- **Spinning reserve market** — An ancillary-service market where residual online capacity below maximum rating can be sold as standby generation, augmenting energy revenue.
- **Risk-neutral vs. actual price processes** — Distinction between market-implied risk-neutral dynamics used for derivative pricing and historical actual dynamics used here for operational valuation.
- **Maximum likelihood estimation for price processes** — Statistical procedure for fitting mean-reversion speed, long-run mean, and volatility parameters of log-price processes to historical data.
- **Correlated commodity prices** — Modelling of instantaneous correlation between electricity and fuel price innovations through correlated Wiener processes.
- **Spark spread options** — Options whose payoff depends on the spread between electricity and fuel prices, used to value generation assets.
- **Path-dependent options** — Options whose value depends on the history of the underlying price path, requiring simulation-based pricing methods.
- **Generation asset valuation** — Techniques for determining the economic value of power generation units using financial and operational models.
- **Unit commitment** — Operational optimisation problem determining which generating units to bring online over a scheduling horizon subject to technical constraints.
- **Ramp constraints** — Technical limits on the rate at which a generation unit can increase or decrease output, affecting short-term scheduling.
- **Nodal prices and transmission rights** — Locational marginal pricing framework and associated financial instruments for managing congestion in power networks.
- **Two-factor price lattices** — Discrete-time tree models incorporating two stochastic factors to value generation assets under electricity price uncertainty.
- **Options on futures spreads** — Derivative instruments written on the difference between two futures prices, used for hedging and speculation in commodity markets.
- **Stochastic differential equations** — Continuous-time mathematical framework for modelling the random evolution of commodity prices underpinning derivative valuation.
- **Natural gas power asset hedging** — Application of financial options theory to quantify and manage the price risk embedded in gas-fired generation assets.

## Methods
- Backward stochastic dynamic programming
- Forward Monte Carlo simulation
- Root-finding / curve-fitting for indifference locus identification
- Quadratic programming for optimal dispatch
- Maximum likelihood estimation
- Heuristic ramp-constrained dispatch
- State-space discretisation for exact ramp-constraint treatment
- Analytical upper-bound derivation via constraint relaxation
- Monte Carlo simulation
- Lattice/tree pricing models
- Financial options theory
- Stochastic differential equations
- Dynamic programming for unit commitment
- Short-term resource scheduling optimisation

## Implied prerequisites
- Stochastic calculus and Ito processes
- Dynamic programming and Bellman equations
- Monte Carlo simulation techniques
- Financial options theory (European and American options)
- Power system operations and unit commitment
- Commodity price modelling
- Mixed-integer and convex optimisation
- Statistical estimation (maximum likelihood)
- Stochastic calculus
- Derivative pricing theory
- Commodity futures markets
- Power systems operations
- Numerical methods in finance
- Optimisation and operations research
