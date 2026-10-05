# WorldPower2009_RealisticPowerPlantValuation

- id: `worldpower2009_realisticpowerplantvaluation` · class: library · type: pdf
- topic: Power plant valuation using cointegrated energy price simulation · level: advanced · market: DE · year: 2009
- worked examples: True · code: False

## Concepts
- **Intrinsic value** — The discounted value of a power plant computed from static forward spark-spread curves without any optionality.
- **Extrinsic / flexibility value** — The incremental value above intrinsic value arising from the ability to adapt dispatch decisions dynamically to realised price outcomes.
- **Spark spread** — The gross margin per MWh of a gas-fired plant, equal to the power price minus fuel cost adjusted for plant heat rate, plus CO2 cost.
- **Dark spread** — The gross margin per MWh of a coal-fired plant, equal to the power price minus coal fuel cost adjusted for heat rate and CO2 cost.
- **Cointegration** — An econometric relationship capturing stable long-run price-level linkages between power, fuel and carbon prices, preventing unrealistic spread extremes in simulation.
- **Merit order** — The cost-ranked dispatch sequence of generation technologies that fundamentally anchors power prices to fuel and carbon costs.
- **Real options approach** — Treating a power plant as a portfolio of spark-spread call options to capture the value of operational flexibility under price uncertainty.
- **Monte Carlo price simulation** — Stochastic simulation of joint commodity price paths used to estimate the distribution of power plant cash flows and option values.
- **Correlated returns model** — A Monte Carlo approach using a correlation matrix of daily price returns that captures short-horizon co-movement but fails to bound long-horizon spread levels.
- **Multi-factor forward curve model** — A parameterised stochastic model capturing level shifts, contango-to-backwardation transitions and seasonal spread dynamics across the forward curve.
- **Regime-switching spot prices** — A price model component that generates infrequent large spikes in spot power or gas prices with mean reversion back to forward curve levels.
- **Dynamic programming dispatch optimisation** — A backward-induction algorithm that derives the optimal hourly start/stop and output decisions for a plant subject to technical and contractual constraints.
- **Minimum run-time and minimum off-time constraints** — Technical restrictions requiring a thermal plant to remain on or off for a minimum number of consecutive hours, reducing theoretical flexibility value.
- **Start costs** — Fixed fuel consumption and maintenance expenses incurred each time a thermal plant is started, penalising frequent cycling.
- **Variable O&M costs** — Operating and maintenance expenses that scale with production hours or number of starts, reducing net spark-spread margin.
- **Planned maintenance and plant trips** — Scheduled outage days and unplanned forced outages that reduce available generation capacity and thus plant value.
- **Plant degradation** — Gradual decline in thermal efficiency over time that increases fuel consumption per MWh and reduces plant value.
- **Take-or-pay gas contract** — A contractual minimum gas offtake obligation that constrains dispatch flexibility by imposing a minimum annual operating-hour requirement.
- **Static hedge** — A forward sale of the expected production volume and purchase of fuels and CO2 at the start of the evaluation period, held without subsequent adjustment.
- **Dynamic hedge** — A rolling hedge strategy that re-balances forward positions as expected production volumes change with evolving spark-spread levels, improving risk-return profile.
- **Delta hedge** — A hedge volume equal to the sensitivity of plant value to the underlying commodity price, approximated here by the volume-weighted expected production.
- **Net present value (NPV) analysis** — A static discounted cash flow method applied to power plant investment that ignores the value of operational flexibility.
- **Principal Component Analysis (PCA) in commodity simulation** — A dimensionality-reduction technique applied to the commodity return covariance matrix as an alternative to a full correlation matrix in Monte Carlo models.
- **Contango and backwardation** — Forward curve shapes where prices are respectively above or below current spot, relevant to calibrating multi-factor commodity models.

## Methods
- Cointegration modelling (Engle-Granger framework)
- Multi-factor stochastic forward curve simulation
- Monte Carlo simulation of joint commodity price paths
- Regime-switching spot price modelling
- Dynamic programming for optimal dispatch
- Hourly intrinsic valuation
- Monthly intrinsic valuation
- Static and dynamic hedging analysis
- Scenario analysis for plant constraints
- NPV discounted cash flow analysis

## Implied prerequisites
- Stochastic calculus and Brownian motion
- Forward curve construction for power, gas and carbon
- Time-series econometrics and cointegration theory
- Option pricing fundamentals
- Dynamic programming and Bellman equation
- Energy commodity markets (power, gas, coal, CO2)
- Heat rate and efficiency concepts for thermal generation
- Risk management and hedging principles
- Monte Carlo simulation techniques
- Portfolio delta and Greeks
