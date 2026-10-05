# The_value_of_starting_up_a_power_plant_WorldPower_2010_Dijken_Abbema_Los_de_Jong

- id: `the_value_of_starting_up_a_power_plant_worldpower_2010_dijke` · class: library · type: pdf
- topic: Power plant valuation with start-stop constraints using Least-Squares Monte Carlo · level: advanced · market: GB · year: 2010
- worked examples: True · code: False

## Concepts
- **Spark spread** — The margin between power revenue and gas fuel cost for a gas-fired plant, used as the primary value driver.
- **Intrinsic value** — Plant value computed from the deterministic forward spark spread curve assuming optimal dispatch.
- **Extrinsic (flexibility) value** — Additional value above intrinsic value arising from price uncertainty and the optionality to respond to realised spot prices.
- **Real options** — The framework treating plant operational flexibility (run, stop, start) as options whose value depends on future price uncertainty.
- **Perfect foresight assumption** — The modelling simplification that the operator knows all future prices when making dispatch decisions, which overstates plant value.
- **Non-perfect foresight** — The realistic condition where operators can only estimate future prices, reducing the achievable value from flexible assets.
- **Least-Squares Monte Carlo (LSMC)** — A simulation-based method that estimates continuation values via regression to optimise sequential decisions without perfect foresight.
- **Dynamic programming** — A backward-induction optimisation framework that finds the optimal dispatch policy by solving sub-problems across a state-space grid.
- **State-space representation** — The set of variables (online/offline duration, cumulative starts) the plant model must track to determine feasible actions and costs.
- **Short-history constraint** — Operational restrictions based on recent plant status such as minimum run-time or minimum down-time requirements.
- **Long-history constraint** — Restrictions that depend on cumulative plant behaviour over a longer period, such as a hard annual cap on the number of starts.
- **Shadow cost for starts** — An implicit penalty per start iteratively calibrated so that the optimal dispatch respects a maximum annual start limit.
- **Start cost (hot/warm/cold)** — Explicit costs incurred when restarting a plant that vary with how long the unit has been offline.
- **Minimum run-time** — A constraint requiring the plant to remain online for a minimum number of hours before it may be shut down.
- **Maximum annual starts constraint** — A hard upper bound on the number of times a plant may start within a year, common in Virtual Power Plant contracts.
- **Virtual Power Plant (VPP)** — A contractual arrangement that grants a counterparty rights over a plant's flexible capacity, typically including constraints on start frequency.
- **Cointegration of power and fuel prices** — A statistical relationship anchoring power prices to fuel fundamentals so that spark spreads remain within economically plausible bounds over time.
- **Continuation value** — The expected future value of holding a position or keeping a plant in a given state, used in backward dynamic programming and LSMC.
- **Mixed-integer linear programming (MILP)** — An alternative deterministic optimisation approach that formulates dispatch as a set of linear constraints with binary on/off variables.
- **Monte Carlo price simulation** — Stochastic generation of many possible price paths for power, gas, and carbon used to value plant flexibility in expectation.
- **Multi-commodity multi-factor forward curve model** — A price model that jointly captures movements in power, gas, and carbon forward curves using multiple stochastic factors.
- **Regime-switching spot price model** — A spot price model that allows abrupt changes between distinct price regimes to capture spikes and low-price periods.
- **Plant degradation cost** — An implicit cost reflecting reduced asset life or increased maintenance expenditure caused by frequent start-stop cycling.
- **Minimum stable generation level** — The lowest output at which a thermal plant can operate continuously, below which it must shut down.

## Methods
- Least-Squares Monte Carlo (LSMC) with regression-based continuation value estimation
- Backward dynamic programming with state-space enumeration
- Shadow cost iteration for start-limit enforcement
- Monte Carlo simulation with cointegrated multi-commodity price model
- Regime-switching spot price modelling
- Mixed-integer linear programming (MILP) for dispatch (discussed as benchmark)
- Sensitivity analysis across start cost levels, minimum run-times, and annual start caps

## Implied prerequisites
- Stochastic calculus and Ito processes
- Options pricing theory
- Dynamic programming and Bellman equation
- Monte Carlo simulation techniques
- Cointegration and time-series econometrics
- Power market fundamentals and merit order
- Linear regression
- Energy commodity markets (power, gas, carbon)
