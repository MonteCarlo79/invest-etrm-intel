# 0109_ELECTRICITY_PRICE_MODELLING_FOR_PROFIT_AT_RISK_MANAGEMENT[1]

- id: `0109_electricity_price_modelling_for_profit_at_risk_manageme` · class: library · type: pdf
- topic: Electricity price modelling for Profit at Risk management · level: intermediate · market: Scandinavia (Nord Pool) · year: None
- worked examples: True · code: False

## Concepts
- **Profit at Risk (PaR)** — A risk measure describing the worst expected operational profit to a specified confidence level over a given period, analogous to VaR but applied to physical-asset portfolios.
- **Value at Risk (VaR)** — A financial-industry risk measure that PaR is derived from, representing the worst expected loss at a given confidence level.
- **Spot price modelling** — Representation of electricity spot price dynamics via stochastic differential equations separating deterministic and stochastic components.
- **Regime-switching price model** — A two-regime framework distinguishing a normal price state from a spike state, governed by a Markov transition matrix.
- **Mean reversion** — A property of commodity price processes whereby prices tend to revert to a long-run mean, captured by a mean-reversion parameter κ.
- **Price spikes** — Short-lived extreme price deviations in electricity markets modelled as a separate regime with its own mean and variance.
- **Seasonal and calendar effects** — Deterministic components of the price model capturing monthly and day-of-week price patterns via dummy variables.
- **Forward price curve** — The term structure of forward electricity prices used for calibration of price scenarios and as hedge instrument pricing.
- **Spark spread option** — A representation of a thermal power plant's operational value as an option on the spread between electricity price and fuel/variable cost.
- **Hedging with forward contracts** — The use of short forward positions to reduce portfolio profit variability and satisfy a PaR constraint.
- **Volume risk** — Uncertainty in power plant output arising from forced outages, modelled as a Markov two-state availability process.
- **Correlation between price spikes and forced outages** — The dependence between simultaneous high-price events and plant unavailability, captured through a joint transition matrix.
- **Financial price models** — Econometric models using historical spot and derivative market data to parameterise stochastic processes for electricity prices.
- **Fundamental price models** — Bottom-up engineering models that derive electricity prices from supply, demand, transmission, and cost data.
- **Combined price modelling approach** — A hybrid methodology that uses fundamental models to generate price scenarios then calibrates them to observed forward prices.
- **Kurtosis-based recursive filter** — An iterative spike-identification procedure that removes data points until sample kurtosis is within a specified ratio of normal-distribution kurtosis.
- **Hydrological risk** — Uncertainty in annual electricity prices driven by variation in hydro reservoir inflows between wet and dry years.
- **Input model risk** — The risk that errors in the parameterisation or structural choices of input models distort the optimal solution of a decision problem.
- **Mixed integer programming** — An optimisation framework used to solve the PaR maximisation and minimisation problems arising from the non-convex structure of the PaR measure.
- **Pay-off diagram** — A graphical representation of portfolio profit as a function of electricity price, used to analyse the effect of hedging on PaR.

## Methods
- Stochastic differential equations (SDE)
- Markov regime-switching model
- Recursive kurtosis-based spike filter
- Maximum likelihood estimation
- Non-linear regression
- Monte Carlo simulation
- Mixed integer programming
- One-dimensional search (for PaR optimisation)
- Forward curve calibration
- Markov chain volume process simulation

## Implied prerequisites
- Stochastic calculus and SDEs
- Financial derivatives pricing (options, forwards)
- Time-series econometrics
- Probability and statistics (distributions, moments, kurtosis)
- Markov chain theory
- Linear and mixed integer programming
- Electricity market fundamentals
- Risk measurement concepts (VaR)
