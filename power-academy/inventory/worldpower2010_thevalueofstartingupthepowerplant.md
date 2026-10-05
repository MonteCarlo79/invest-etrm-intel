# WorldPower2010_TheValueofStartingUpthePowerPlant

- id: `worldpower2010_thevalueofstartingupthepowerplant` · class: library · type: pdf
- topic: Gas-fired power plant valuation with start-stop constraints using Least-Squares Monte Carlo · level: advanced · market: GB · year: 2010
- worked examples: True · code: False

## Concepts
- **Spark spread** — The margin between power price and fuel cost scaled by plant efficiency, used as the key profitability signal for dispatch decisions.
- **Intrinsic value** — Plant value computed from current forward spark spread curves assuming deterministic future prices.
- **Extrinsic (flexibility) value** — Additional value above intrinsic arising from price uncertainty and the optionality of flexible dispatch.
- **Real option value** — The economic worth embedded in the plant operator's ability to choose production hours in response to evolving market prices.
- **Strip of European call options** — A simplified plant valuation model treating each hour as an independent call option on the spark spread, ignoring inter-temporal constraints.
- **Start costs** — Explicit and implicit costs incurred each time a plant is restarted, including fuel consumption and wear-dependent maintenance penalties.
- **Hot/warm/cold start classification** — Temperature-dependent categorisation of restart costs based on the duration the unit has been offline.
- **Minimum run-time constraint** — An operational restriction requiring the plant to remain online for a minimum number of hours before being allowed to shut down.
- **Hard start-number limit** — A contractual or mechanical cap on the total number of starts permitted within a given period, common in VPP contracts.
- **Short-history state** — A dynamic-programming state variable tracking recent consecutive on/off hours to enforce minimum up/down time constraints.
- **Long-history state** — A dynamic-programming state variable accumulating total starts since period start to enforce annual start-count limits.
- **Shadow cost for starts** — An iteratively calibrated implicit penalty per start used to enforce a start-count limit within a standard dynamic programming framework.
- **Perfect foresight assumption** — The modelling simplification that the operator knows all future price realisations when making dispatch decisions, which overstates plant value.
- **Least-Squares Monte Carlo (LSMC)** — A simulation-based backward-induction method that uses cross-sectional regressions to estimate continuation values without assuming perfect price foresight.
- **Continuation value regression** — A polynomial regression of discounted future payoffs on current state variables used in LSMC to approximate the value of keeping the plant in a given state.
- **Cointegration of power and fuel prices** — A long-run equilibrium relationship linking electricity prices to fuel fundamentals that bounds spark spread behaviour in simulation models.
- **Multi-factor forward curve model** — A stochastic model with multiple risk factors driving movements in the forward price curve for power and fuel.
- **Regime-switching spot price model** — A spot price specification that allows the price process to switch between distinct statistical regimes to capture spikes and mean reversion.
- **Virtual Power Plant (VPP) contract** — A contractual arrangement granting a counterparty dispatch rights over a physical plant, often including explicit start-number restrictions.
- **Dynamic programming for plant dispatch** — A backward-induction optimisation that computes the value-maximising dispatch decision at each time-state node over the plant's operating horizon.
- **State-space expansion** — The increase in the number of model states when the long-history start-count variable is added as an extra dimension in dynamic programming.
- **Mixed-integer linear programming (MILP)** — An alternative dispatch optimisation formulation using binary commitment variables and linear constraints, noted for ignoring price uncertainty.
- **Minimum stable generation level** — The lowest output at which a CCGT plant can operate continuously, below which a full shutdown is required.

## Methods
- Least-Squares Monte Carlo (Longstaff-Schwartz algorithm applied to energy assets)
- Backward-induction dynamic programming
- Shadow-cost iterative calibration for start-count constraints
- State-space dynamic programming with long-history start counter
- Monte Carlo price simulation with cointegration and regime switching
- Multi-commodity multi-factor forward curve simulation
- Polynomial cross-sectional regression for continuation value estimation
- Mixed-integer linear programming (benchmarked, not primary method)

## Implied prerequisites
- Stochastic calculus and risk-neutral pricing
- Real options theory
- Dynamic programming and Bellman equation
- Monte Carlo simulation techniques
- Forward curve modelling for power and gas
- Cointegration and time-series econometrics
- Spark spread mechanics and CCGT plant economics
- Regression analysis (OLS)
- Energy market structure and VPP contracts
