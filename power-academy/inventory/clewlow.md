# Clewlow

- id: `clewlow` · class: library · type: pdf
- topic: Monte Carlo valuation of generation assets with operational constraints · level: intermediate · market: US · year: 2009
- worked examples: True · code: False

## Concepts
- **Generation asset real option valuation** — Treating a power plant as a spark-spread call option whose value depends on satisfying physical operational constraints.
- **Extended mean-reverting jump-diffusion model (MRJDx)** — A stochastic spot-price process capturing mean reversion, jumps, seasonality, and consistency with the forward curve.
- **Term structure of forward volatility** — A function defining how volatility attenuates with maturity, parameterised by spot volatility, long-term volatility, and speed of attenuation.
- **Hybrid power price model** — A two-stage modelling approach that derives power prices from simulated fundamental drivers via temperature-load and load-price functional relationships.
- **Temperature-load relationship** — A bi-quadratic functional mapping between ambient temperature and system electricity demand used within hybrid price models.
- **Load-price relationship** — A functional curve linking system load to power price, adjusted for fuel prices and generation outages.
- **Forced outage modelling** — Representing time-to-failure and time-to-repair as exponential distributions to capture random unplanned plant unavailability.
- **Path-dependent dispatch algorithm** — A simulation-based dispatch logic that conditions current operating decisions on prior-period states to respect operational constraints.
- **Generator state machine** — A discrete set of mutually exclusive operational states (e.g., maximum generation, ramping, forced outage) with permissible transitions governing plant dispatch.
- **Minimum up and down time constraints** — Requirements that a plant remain online or offline for at least a specified number of hours before changing status.
- **Ramp rate constraints** — Physical limits on the rate at which a generator can increase or decrease output, varying by hot, warm, and cold start conditions.
- **Heat-rate curve** — A function expressing fuel input per unit of electricity output as a function of generation level, reflecting efficiency variation across the dispatch range.
- **Start-up costs** — Fixed or variable costs triggered each time a unit is brought online, differentiated by hot, warm, and cold start classifications.
- **Spark-spread cashflow** — The net revenue per period from operating a gas-fired unit, computed as generation times the difference between power price and heat-rate-adjusted fuel cost.
- **Look-forward dispatch logic** — A forward-looking heuristic that compares projected cashflows over minimum commitment periods against start-up costs to decide whether to commit the unit.
- **Earnings-at-risk (EaR)** — A distributional risk metric capturing the downside spread of periodic earnings from a generation asset across Monte Carlo scenarios.
- **Expanded forward curve** — A forward price curve extended from monthly block quotes to hourly resolution using historical intra-day price shapes.
- **Emission costs** — Variable costs proportional to generation level reflecting the cost of regulated pollutants such as SOx, NOx, CO2 produced during combustion.

## Methods
- Monte Carlo simulation
- Mean-reverting jump-diffusion process calibration
- Bi-quadratic temperature-load curve fitting
- Exponential distribution sampling for outage modelling
- Path-dependent state-machine dispatch simulation
- Forward curve bootstrapping to hourly resolution
- Sensitivity analysis across constraint scenarios

## Implied prerequisites
- Stochastic calculus and Itô processes
- Spark-spread option pricing
- Power market structure and spot price dynamics
- Basic Monte Carlo simulation techniques
- Forward curve construction
- Real options theory
- Statistical distributions (exponential)
