# dipeng/Tools

- id: `dipeng_tools` · class: practice · type: folder
- topic: Power and energy trading quantitative tools: Monte Carlo simulation, options pricing, and market data automation · level: intermediate · market: mixed · year: None
- worked examples: True · code: True

## Concepts
- **Monte Carlo estimation** — Approximating expectations of functions of random variables by averaging over simulated sample paths.
- **Risk-neutral pricing** — Valuing derivatives as discounted expectations under the equivalent martingale measure rather than the real-world measure.
- **Geometric Brownian Motion (GBM)** — Continuous-time stochastic process used to model asset prices with constant drift and volatility.
- **European call option pricing** — Simulation-based estimation of the Black-Scholes price for a standard European call as a discounted expected payoff.
- **Asian option pricing** — Monte Carlo valuation of path-dependent options whose payoff depends on the average price over a period.
- **Portfolio loss probability** — Estimating via simulation the probability that a multi-asset portfolio value falls below a specified threshold.
- **Multivariate normal distribution** — Joint normal distribution over n variables characterised by a mean vector and covariance matrix.
- **Covariance and correlation matrix** — Symmetric positive semi-definite matrix summarising pairwise linear dependence among random variables.
- **Cholesky decomposition** — Factorisation of a symmetric positive-definite matrix used to generate correlated normal random vectors.
- **Correlated Brownian motions** — Standard Brownian motions with non-zero cross-covariance, used to drive correlated asset price processes.
- **Multivariate lognormal simulation** — Generating correlated lognormal random vectors by exponentiating a correlated multivariate normal draw.
- **Marginal distribution and copula-style construction** — Specifying only marginals and a correlation matrix and using a Gaussian mapping to construct a joint distribution.
- **Strong Law of Large Numbers (SLLN)** — Theoretical guarantee that the Monte Carlo sample mean converges almost surely to the true expectation.
- **Indicator function estimation** — Representing probabilities as expectations of binary indicator random variables for use in Monte Carlo.
- **Power market data products** — Baseload, peak, and hourly electricity forward products across NL, UK, DE, FR, BE markets collected for quantitative analysis.
- **Trade data query and automation** — Systematic retrieval and reformatting of trade records from internal systems into structured spreadsheet outputs.
- **NBP and TTF gas price data** — Historical spot and forward price series for UK NBP and Dutch TTF natural gas hubs used alongside power data.
- **Cubic spline interpolation** — Piecewise polynomial fitting used to interpolate forward curves or volatility surfaces from observed market quotes.
- **Vanilla options pricing model** — Spreadsheet implementation of standard call and put option valuation under Black-Scholes or related frameworks.
- **Binary/digital option valuation** — Pricing of options with discontinuous payoffs that pay a fixed amount if a condition is met at expiry.

## Methods
- Monte Carlo simulation
- Cholesky decomposition for correlated sampling
- Risk-neutral discounted expectation
- Geometric Brownian Motion path simulation
- Indicator function Monte Carlo for probability estimation
- Multivariate normal random vector generation
- Conditional distribution simulation
- Gaussian copula construction for marginal-only-specified dependence
- Cubic spline interpolation
- Excel VBA automation
- Python scripting for data manipulation
- Spreadsheet-based Black-Scholes pricing
- FTP-based market data collection
- Sorting-based Excel row manipulation

## Implied prerequisites
- Probability theory and random variables
- Stochastic calculus and Brownian motion
- Linear algebra (matrix operations, positive definiteness)
- Black-Scholes option pricing theory
- Forward curve construction
- Excel and VBA proficiency
- Basic Python programming
- Energy market product structures (baseload, peak, hourly)
