# 111102_collector

- id: `111102_collector` · class: library · type: pdf
- topic: Practical valuation of power derivatives: energy swaps and swaptions in the Nordic electricity market · level: intermediate · market: Nordic (Nord Pool) · year: None
- worked examples: True · code: False

## Concepts
- **Power swap vs. forward distinction** — Covers why exchange-traded Nordic electricity 'forwards' are structurally swaps with daily financial settlement against spot, not true forwards.
- **Swap present-value discounting** — Covers the correct discounting of power swap cash flows to obtain contract value distinct from the quoted market price.
- **Forward-start interest swap rate** — Covers the use of the forward-starting rate spanning the delivery period as the relevant discount rate for power swap valuation.
- **Geometric Brownian motion applied to power forwards** — Covers the justification for modelling quarterly and annual power swap returns as GBM given empirical return distributions closer to normal than spot returns.
- **Black-76 model for power options** — Covers application of the Black (1976) formula to European options on power forwards/swaps and its role as market standard at Nord Pool.
- **Adjusted delta for swap-hedged power options** — Covers the modification of the Black-76 delta required when the hedging instrument is a power swap rather than a forward expiring at option maturity.
- **Energy swaption (option on power swap)** — Covers the contract structure where exercise delivers the underlying swap at the strike price, generating payoff spread over the delivery period.
- **Energy swaption pricing formula** — Covers the closed-form modification of Black-76 that discounts expected payoff to account for deferred swap delivery cash flows.
- **Put-call parity for energy swaptions** — Covers the no-arbitrage relationship between call and put swaption prices and the implied synthetic swap forward price.
- **Swaption Greeks (delta, gamma, vega, rho)** — Covers analytical sensitivity formulas for energy swaption value with respect to underlying price, volatility, and interest rates.
- **Volatility smile and fat tails in power markets** — Covers the empirical observation of leptokurtic return distributions and the practitioner practice of applying a volatility smile to fudge the Black-Scholes-Merton model.
- **SABR stochastic volatility model** — Covers the SABR model as an extension accommodating stochastic volatility via two additional parameters for use in power derivatives markets.
- **Jump-diffusion models for power prices** — Covers the rationale for incorporating price jumps, potentially combined with stochastic volatility, when modelling electricity price dynamics.
- **Spot price seasonality and mean reversion** — Covers the statistical properties of electricity spot returns including seasonality, mean reversion, and extreme kurtosis and their implications for forward pricing.
- **Strip of forwards vs. annual swap arbitrage** — Covers the no-arbitrage condition requiring a strip of seasonal or quarterly swaps to equal the value of an annual swap after correct discounting.
- **Continuous approximation for swap value** — Covers a continuously-compounded discount-rate approximation that simplifies power swap present-value calculation while maintaining accuracy.

## Methods
- Black-76 closed-form option pricing
- Discounted cash flow analysis of swap settlements
- Delta hedging with adjustment for swap underlying
- Put-call parity arbitrage
- Analytical Greeks computation
- Volatility smile fitting
- Geometric Brownian motion modelling
- Empirical return distribution analysis (kurtosis, skewness)
- SABR model calibration
- Forward-start rate bootstrapping

## Implied prerequisites
- Black-Scholes-Merton option pricing theory
- Ito's lemma and stochastic calculus
- Interest rate discounting and zero-coupon rates
- Futures and forward contract mechanics
- Swap pricing fundamentals
- Risk-neutral pricing and arbitrage arguments
- Basic probability and statistics (distributions, kurtosis)
- Option Greeks interpretation
