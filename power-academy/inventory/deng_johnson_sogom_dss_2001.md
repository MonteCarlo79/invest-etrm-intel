# deng-johnson-sogom-dss-2001

- id: `deng_johnson_sogom_dss_2001` · class: library · type: pdf
- topic: Electricity derivatives pricing and real options valuation of generation and transmission assets · level: advanced · market: US · year: 2001
- worked examples: True · code: False

## Concepts
- **Spark spread option** — A derivative whose payoff depends on the difference between the electricity price and the product of a strike heat rate and fuel price.
- **Locational spread option** — A derivative whose payoff depends on the price difference of electricity between two geographical locations.
- **Heat rate** — A measure of generation efficiency defined as the units of fuel input required to produce one unit of electrical output.
- **Futures-based replication** — A no-arbitrage derivative pricing method that constructs replicating portfolios from electricity futures contracts rather than spot-market storage positions.
- **Non-storability of electricity** — The physical property of electricity that prevents inventory-based arbitrage and invalidates traditional commodity derivative pricing via cost-of-carry.
- **Geometric Brownian motion price process** — A continuous-time stochastic process with constant drift and volatility used as a baseline model for futures price dynamics.
- **Mean-reverting price process** — A stochastic process in which futures prices are pulled toward a long-run mean, capturing the empirically observed reversion in electricity prices.
- **Exchange option (Margrabe formula)** — An option giving the right to exchange one risky asset for another, representing a spread option with zero strike price.
- **Capacity right** — The right to convert a fixed quantity of fuel into electricity using a generation asset at a given point in time.
- **Real options valuation of generation assets** — Treating a power plant's operating flexibility as a portfolio of spark spread options integrated over the asset's remaining useful life.
- **Real options valuation of transmission assets** — Treating a transmission line as a pair of locational spread options permitting power flow in either direction less transmission costs.
- **Put-call parity for spread options** — A no-arbitrage relationship linking spark spread call and put values through the discounted difference of relevant futures prices.
- **No-arbitrage bounds for spread options** — Upper and lower limits on spark spread call values derived from the non-negativity of put prices and the bounded option payoff.
- **Implied market heat rate** — The ratio of the spot electricity price to the spot fuel price, representing the heat rate of the marginal generating unit under market conditions.
- **Volatility term structure** — The dependence of futures price volatility on contract maturity, modelled as a time-varying function calibrated to market data.
- **Discounted cash flow (DCF) valuation** — A traditional asset valuation method using risk-adjusted discounting of expected cash flows, used here as a benchmark against real options values.

## Methods
- Closed-form option pricing via PDE solution
- Dynamic replication with futures contracts
- Margrabe exchange-option formula adapted to futures
- Mean-reverting stochastic process modelling
- Historical volatility estimation from NYMEX futures data
- Implied volatility calibration
- Integration of option values over asset lifetime for capacity valuation
- Comparative statics analysis of option sensitivities
- Discounted cash flow analysis

## Implied prerequisites
- Stochastic calculus and Ito's lemma
- Black-Scholes-Merton option pricing framework
- Futures and forward contract pricing
- No-arbitrage pricing principles
- Partial differential equations for derivative pricing
- Mean-reverting (Ornstein-Uhlenbeck) processes
- Real options theory
- Energy commodity market structure
- Statistical estimation of volatility from time series
