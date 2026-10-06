# Understanding

- id: `understanding` · class: library · type: pdf
- topic: Black-Scholes option pricing: interpretation of N(d1) and N(d2) via risk-adjusted probabilities · level: intermediate · market: mixed · year: 1992
- worked examples: True · code: False

## Concepts
- **Black-Scholes formula** — Closed-form expression for the current value of a European call option decomposed into two probability-weighted components.
- **N(d2) as risk-adjusted exercise probability** — The cumulative normal term N(d2) equals the risk-adjusted (risk-neutral) probability that the option finishes in the money.
- **N(d1) as contingent stock-receipt factor** — N(d1) is the factor by which the present value of receiving the stock contingent on exercise exceeds the current stock price.
- **Risk-adjusted (risk-neutral) probabilities** — A probability measure obtained by replacing the drift parameter µ with the riskless rate r, under which discounted asset prices are martingales.
- **Contingent exercise payment** — The component of the call payoff representing payment of the exercise price only if the option finishes in the money.
- **Contingent stock receipt** — The component of the call payoff representing delivery of the stock only if the option finishes in the money.
- **Truncated lognormal expectation** — The expected value of a lognormal random variable conditioned on exceeding a threshold, used to value the contingent stock-receipt component.
- **Lognormal stock price distribution** — The assumption that log(S_T) is normally distributed with drift and variance parameters µ and σ² under the physical measure.
- **One-period binomial option pricing** — A single-step discrete model where the call value is expressed as a discounted risk-neutral expectation with analogues of N(d1) and N(d2).
- **Multi-period binomial option pricing** — An n-step binomial model whose call pricing formula involves complementary binomial distribution functions that correspond to N(d1) and N(d2).
- **Complementary binomial distribution function** — The probability that a binomial random variable equals or exceeds a threshold a, used as the discrete analogue of the Black-Scholes normal probability terms.
- **Hedge ratio (delta)** — The number of shares needed to replicate the option, equal to N(d1) in Black-Scholes but not equal to the analogous factor in the binomial model.
- **Continuously compounded rate of return** — The logarithmic return log(S_t/S)/t, which is normally distributed with mean µ − σ²/2 and variance σ²/t under the physical measure.
- **Risk-neutral drift substitution** — The probability adjustment that replaces µ with r in the lognormal distribution to obtain the risk-neutral pricing measure.

## Methods
- Decomposition of call payoff into two contingent claims
- Risk-neutral (risk-adjusted) expectation pricing
- Truncated lognormal expectation formula with proof
- Standardisation of lognormal variable to derive d1 and d2
- One-period binomial formula restatement
- Multi-period binomial formula restatement using complementary binomial distribution

## Implied prerequisites
- Lognormal distribution and properties
- Standard normal cumulative distribution function
- No-arbitrage pricing principle
- Discounted expected value / present value calculations
- Basic stochastic processes for asset prices
- Binomial option pricing model (Cox-Ross-Rubinstein)
