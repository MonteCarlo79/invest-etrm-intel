# dipeng/valuation reports

- id: `dipeng_valuation_reports` · class: practice · type: folder
- topic: Power plant and spread option valuation across European electricity markets · level: intermediate · market: DE, GB, FR · year: 2017
- worked examples: True · code: False

## Concepts
- **Intrinsic value** — The deterministic value of a power plant or spread option derived from the current hourly shaped price forward curve.
- **Extrinsic (time) value** — The component of plant or option value beyond intrinsic, arising from price volatility and the optionality of future dispatch decisions.
- **Clean dark spread** — The margin of a coal-fired plant after deducting coal fuel costs and carbon allowance costs from the power price.
- **Clean spark spread** — The margin of a gas-fired plant after deducting gas fuel costs and carbon allowance costs from the power price.
- **Spread option strip** — A series of hourly European-style options on the clean spark or dark spread, representing the upper bound of plant optionality with no start costs.
- **Real plant product** — A power station model that incorporates start costs, part-load efficiency penalties, and variable operating costs to reflect actual dispatch constraints.
- **Hourly shaped price forward curve** — A forward curve disaggregated to hourly resolution to capture intraday price seasonality for plant dispatch valuation.
- **UK Carbon Price Floor** — A UK-specific carbon tax that supplements EU ETS allowance prices, effectively raising the carbon cost faced by UK generators.
- **Delta hedging** — A dynamic replication strategy that locks in a portion of extrinsic option value by continuously hedging the price sensitivity of the plant position.
- **Monte Carlo simulation for dispatch optimisation** — Stochastic simulation of commodity prices used to estimate expected plant value by averaging payoffs over many price paths.
- **Least-squares Monte Carlo (LSMC)** — A regression-based dynamic programming method applied within Monte Carlo simulation to determine the optimal hourly dispatch strategy.
- **Plant efficiency** — The heat rate or conversion ratio linking fuel input to power output, which determines the fuel cost per MWh generated.
- **Part-load operation** — Running a plant at reduced capacity with a lower efficiency to avoid incurring start costs when margins are thin.
- **Start cost** — A fixed cost incurred each time a power plant is brought from offline to generating, penalising frequent cycling in dispatch optimisation.
- **Historical calibration of volatility and correlation** — Estimating stochastic model parameters from observed market price data over a defined lookback window for use in simulation.

## Methods
- Monte Carlo price simulation
- Least-squares Monte Carlo (LSMC) dispatch optimisation
- Intrinsic valuation using hourly price forward curves
- Historical calibration of volatility and correlation parameters
- Delta hedging of extrinsic value

## Implied prerequisites
- Electricity market fundamentals
- Commodity spread option pricing
- Power plant dispatch economics
- EU ETS and carbon pricing mechanisms
- Stochastic processes for energy prices
- Options pricing theory
- Dynamic programming and backward induction
