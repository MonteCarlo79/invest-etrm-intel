# Decide_when_to_start_your_power_plant_v2

- id: `decide_when_to_start_your_power_plant_v2` · class: library · type: pdf
- topic: Power plant start-stop optimisation and valuation using least-squares Monte Carlo · level: advanced · market: GB · year: 2011
- worked examples: True · code: False

## Concepts
- **Spark spread** — The margin between power revenue and fuel cost for a gas-fired generator, used as the primary value driver for dispatch decisions.
- **Intrinsic value** — The plant value derived from locking in current forward spark spreads, ignoring price uncertainty.
- **Extrinsic value** — The additional value above intrinsic value arising from price volatility and operational flexibility, also called real option value.
- **Real option value** — The economic value embedded in the operational flexibility of a physical asset to respond to future price movements.
- **Perfect foresight assumption** — The modelling simplification that treats future price paths as fully known when making dispatch decisions, leading to overstatement of plant value.
- **Non-perfect foresight** — A modelling framework in which dispatch decisions are made using only currently observable price information rather than future realised prices.
- **Least-squares Monte Carlo (LSMC)** — A simulation-based method that uses cross-sectional regressions of simulated payoffs on state variables to estimate continuation values at each decision node.
- **Dynamic programming (backward valuation)** — A recursive optimisation technique that solves for optimal decisions by working backwards from the terminal period through all states and time steps.
- **State space** — The set of all variables the optimiser must track simultaneously, including current operational status, hours online/offline, and cumulative starts.
- **Short-history constraint** — An operational constraint determined by recent plant history, such as minimum run-time or minimum down-time requirements.
- **Long-history constraint** — An operational constraint that depends on accumulated actions over a longer horizon, such as an annual cap on the number of starts.
- **Shadow cost** — An iteratively adjusted implicit penalty applied to starts to enforce a hard start-count limit within a dynamic programming framework without expanding the state space.
- **Minimum run-time / minimum down-time** — Operational constraints requiring the plant to remain online or offline for a minimum number of hours after each state transition.
- **Start cost** — An explicit or implicit cost incurred each time the plant is restarted, covering fuel consumption and wear, which may vary by cold, warm, or hot start type.
- **Cold/warm/hot start classification** — A categorisation of restart costs based on the duration the unit has been offline, reflecting the thermal state of the plant.
- **Hard start-count limit** — A contractual or engineering constraint capping the total number of plant starts permitted within a defined period, common in VPP contracts.
- **Virtual Power Plant (VPP) contract** — A contractual arrangement granting a counterparty dispatch rights over a physical plant, often including explicit start-number limitations.
- **Continuation value** — The expected present value of future cash flows from holding an asset in its current state, used to compare against immediate-action payoffs in dynamic programming.
- **Cointegration (power and fuel prices)** — A statistical relationship linking power prices to fuel costs through merit-order fundamentals, keeping spark spreads mean-reverting within plausible bounds.
- **Monte Carlo price simulation** — A stochastic simulation method generating many possible future price paths for power and fuel, capturing volatility and dependence structures.
- **Mixed-integer linear programming (MILP)** — An alternative deterministic optimisation approach that formulates dispatch as a set of linear constraints and binary on/off variables.
- **Minimum stable generation** — The lowest output level at which a thermal plant can operate continuously without shutting down, associated with a lower efficiency than full load.
- **Dispatch optimisation** — The process of determining the optimal hourly production schedule for a plant to maximise profit subject to technical and contractual constraints.
- **Regression basis functions (LSMC)** — The set of observable price variables, such as current spark spread and recent average spread, used as regressors to estimate continuation values in LSMC.

## Methods
- Least-squares Monte Carlo (LSMC)
- Backward dynamic programming
- Shadow-cost iterative calibration
- Expanded state-space dynamic programming
- Multi-commodity multi-factor forward curve simulation
- Regime-switching spot price model
- Cointegration-based price modelling
- Monte Carlo scenario generation
- Cross-sectional linear regression for continuation value estimation
- Mixed-integer linear programming (MILP, discussed as comparator)

## Implied prerequisites
- Stochastic calculus and price process modelling
- Fundamentals of options pricing and real options
- Dynamic programming and Bellman equation
- Monte Carlo simulation techniques
- Linear regression and ordinary least squares
- Power market structure and spark spread mechanics
- Gas and electricity forward curve construction
- Cointegration and time-series econometrics
- Thermal generation technology basics (CCGT)
