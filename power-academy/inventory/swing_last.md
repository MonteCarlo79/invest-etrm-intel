# swing-last

- id: `swing_last` · class: library · type: pdf
- topic: Swing option valuation in energy markets · level: advanced · market: US · year: 2003
- worked examples: True · code: False

## Concepts
- **Swing option** — A flexibility-of-delivery contract granting the holder multiple rights to take greater or smaller energy volumes subject to daily and periodic constraints.
- **Take-or-pay contract** — A supply agreement requiring the buyer to pay for a minimum volume of commodity whether or not it is actually taken.
- **Local-effect exercise right** — A swing right whose volume modification applies only on the date of exercise, reverting to base-load thereafter.
- **Global-effect exercise right** — A swing right whose volume modification persists from the exercise date until the next exercise or contract expiry.
- **Refraction period** — A mandatory waiting interval between consecutive exercise of swing rights specified in the contract.
- **Bang-bang exercise** — Optimal swing exercise at extreme allowable volume levels when no penalty on total consumption is present.
- **Penalty function** — A contractual payoff term assessed at expiry for total cumulative delivery falling outside specified minimum or maximum bounds.
- **Dynamic programming for swing options** — Backward-induction valuation over three state dimensions—price, exercise rights remaining, and cumulative usage—to price swing contracts.
- **Trinomial forest** — A multi-layer extension of the trinomial tree in which each layer corresponds to a distinct number of remaining exercise rights and usage level.
- **One-factor mean-reverting price model** — A commodity spot price model driven by a single Ornstein-Uhlenbeck process for the deseasonalized log-price.
- **Ornstein-Uhlenbeck process** — A continuous-time stochastic process with drift toward a long-run mean, used here for the logarithm of the deseasonalized energy spot price.
- **Seasonality factor** — A deterministic multiplicative function of calendar time capturing periodic patterns in energy spot prices.
- **Deseasonalized spot price** — The commodity price series obtained by dividing the spot price by the deterministic seasonality factor to isolate the stochastic component.
- **Risk-neutral measure** — The equivalent martingale measure under which discounted prices of traded instruments are martingales, used as the pricing measure throughout.
- **Convenience yield** — A derived quantity reflecting the benefit of holding physical inventory, linked here to limited substitutability of energy across time.
- **Forward price under mean reversion** — The risk-neutral expectation of the future spot price under the one-factor model, yielding a closed-form term structure expression.
- **Black's formula** — A closed-form pricing expression for European options on futures assuming log-normally distributed futures prices.
- **Implied volatility term structure** — The relationship between option-implied volatility and time to expiration, declining as T^{-1/2} under mean reversion.
- **Hull-White trinomial tree** — A discrete-time lattice construction that matches initial forward curves and accommodates mean reversion via non-standard branching.
- **Model calibration to futures and options** — Estimation of model parameters by minimising pricing errors relative to observed forward curves and implied volatilities.
- **Bermudan option** — An option exercisable on a discrete set of dates, providing the upper bound for a single-right swing option.
- **European option strip** — A collection of European options on individual exercise dates, providing the lower bound for a swing option value.
- **Homogeneity of swing value** — The property that the swing option value scales linearly with price level and with volume quantities under suitable penalty structures.
- **Optimal exercise threshold** — The critical spot price level at or above which immediate exercise of a swing right is optimal, studied for uniqueness under various dynamics.
- **Weak convergence of numerical scheme** — The mathematical guarantee that trinomial-tree swing prices converge to their continuous-time counterparts as the time step tends to zero.

## Methods
- Backward induction dynamic programming
- Trinomial forest (multi-layer trinomial tree)
- Hull-White trinomial tree construction with displacement shifts
- Black's formula for European options on futures
- Ornstein-Uhlenbeck process simulation and analytics
- Least-absolute-deviation calibration to forward curves
- Piecewise-constant seasonality factor estimation
- Induction-based convergence proof

## Implied prerequisites
- Stochastic calculus and Itô's lemma
- Risk-neutral pricing and equivalent martingale measures
- American and Bermudan option theory
- Binomial and trinomial lattice methods
- Futures and forward pricing theory
- Black-Scholes and Black's formula
- Energy market structure and natural gas futures
- Dynamic programming and backward induction
- Convenience yield models
- Numerical methods for PDEs and lattices
