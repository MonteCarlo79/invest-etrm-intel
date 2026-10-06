# us toll

- id: `us_toll` · class: library · type: txt
- topic: US Tolling Agreement Valuation · level: intermediate · market: US · year: None
- worked examples: False · code: False

## Concepts
- **Tolling Agreement** — Contractual right to dispatch a generation asset in exchange for fixed and variable payments, valued as a strip of real options on the spark spread.
- **Hot Start** — Plant operational state allowing rapid dispatch with lower start-up cost and time than a cold start.
- **Generation Dispatch Optionality** — The right but not obligation to generate during each scheduling interval, capturing value when spark spread exceeds variable cost.
- **Hourly Pricing Shape** — Intra-day power price profile used to disaggregate block schedules into hourly dispatch decisions in developed markets.
- **Spark Spread Option** — Financial option on the difference between power price and heat-rate-adjusted fuel cost, forming the canonical building block for tolling valuation.
- **Variable Put** — An option structure capturing the asymmetric payoff of electing not to run when the spark spread is negative.
- **Correlation Sensitivity** — Sensitivity of the spread option value to the assumed correlation between power and fuel price processes.
- **Volatility Curve** — Term structure of implied or historical volatility for power and fuel used to price and risk-manage the tolling option book.
- **Historical Volatility** — Realised price volatility estimated from time-series data, used as a benchmark or input when implied volatility is unavailable.
- **Intrinsic Value** — The discounted value of dispatching the asset optimally against current forward prices, ignoring future price uncertainty.
- **Extrinsic Value** — The additional option premium above intrinsic value attributable to price volatility and scheduling flexibility, typically 10–20 percent of total tolling value.
- **Auxiliary Costs** — Non-fuel variable operating costs such as emissions allowances that reduce effective spark spread and must be embedded in dispatch economics.
- **Emission Costs** — Carbon or NOx/SOx compliance costs treated as a variable generation cost reducing the effective spread captured by the toller.
- **Transmission Costs and Losses** — Costs and energy losses associated with moving power to or from the delivery point, reducing the net spread receivable by the toller.
- **Delta-Flat Hedging** — Hedging strategy that offsets the net price sensitivity of the tolling position in both fuel and power markets simultaneously.
- **Liquidity-Adjusted Pricing** — Incorporation of bid-offer spreads and market depth into forward curve construction to reflect realistic realisable hedge costs.
- **Cost Sensitivity** — Measure of how tolling value responds to changes in variable operating costs including fuel, emissions, and auxiliary expenses.

## Methods
- Spread option closed-form approximation (Margrabe / Kirk)
- Hourly shape disaggregation of block forward prices
- Historical volatility estimation
- Greeks computation for spread options (delta, vega, rho-correlation)
- Intrinsic value calculation against forward curve
- Extrinsic value estimation as residual above intrinsic
- Liquidity adjustment to forward prices
- Delta-flat fuel and power hedging

## Implied prerequisites
- Black-Scholes option pricing theory
- Spread option pricing (Margrabe framework)
- Power and gas forward curve construction
- Basic plant operations and heat-rate concepts
- Options Greeks interpretation
- Electricity market structure and scheduling (US ISO markets)
- Commodity volatility surface construction
- Emissions markets fundamentals
