# Pareto Spikes

- id: `pareto_spikes` · class: library · type: pdf
- topic: Non-Markovian power price spike modeling and European contingent claim valuation · level: advanced · market: mixed · year: 2008
- worked examples: False · code: False

## Concepts
- **Non-Markovian power price process** — A power spot price model defined as the product of a spike process and an inter-spike process, yielding a process that requires extended state information beyond the current price.
- **Two-state Markov chain (spike/inter-spike)** — A continuous-time two-state Markov process whose states determine whether the power market is currently in a spike or a regular inter-spike regime.
- **Spike process** — A Markov process equal to a random multiplicative magnitude during spikes and unity between spikes, constructed from the two-state Markov chain and a spike-magnitude distribution.
- **Inter-spike process** — A Markov diffusion process modeling power prices between spikes, typically specified as a geometric mean-reverting process.
- **Geometric mean-reverting process** — A diffusion model for inter-spike power prices featuring drift toward a time-varying equilibrium level with stochastic volatility, used as the baseline price dynamics between spikes.
- **Pareto distribution for spike magnitude** — A heavy-tailed probability distribution on (1, ∞) used to characterise the multiplicative size of power price spikes, parameterised by a shape and minimum-magnitude parameter.
- **Transition probability decomposition** — A splitting of spike-state transition probabilities into a component for remaining continuously in the spike state and a component for re-entering it after an inter-spike period.
- **Kolmogorov-Chapman equation** — A semigroup consistency condition for Markov transition matrices, used here to derive analytical expressions for the two-state transition matrix and its generator.
- **Generator matrix** — A 2×2 matrix characterising the instantaneous rate of change of the two-state Markov transition matrix, with off-diagonal entries controlling spike frequency and duration.
- **Ergodic probabilities** — Long-run stationary probabilities for the spike and inter-spike states of the two-state Markov chain, governing the limiting behaviour of forward prices and option values.
- **European contingent claim on power with spikes** — A claim whose payoff may depend on the power price, the current spike/inter-spike state, and the spike magnitude at expiry, valued as a discounted risk-neutral expectation.
- **Risk-neutral average payoff over spike magnitude** — The conditional expectation of the state-dependent payoff over the spike-magnitude distribution, reducing a spiking-market valuation to a no-spike contingent claim problem.
- **Eigenclaim method** — An analytical technique representing contingent claim payoffs as power functions of the underlying price, enabling closed-form valuation in both Black-Scholes and mean-reverting environments.
- **Asymmetric power option** — A European option whose payoff is a power of the underlying asset price, allowing analytical valuation in both Black-Scholes and mean-reverting settings via moment-generating properties.
- **Power forward price with spikes** — The risk-neutral expected future spot price of power accounting for spikes, expressed as the no-spike forward price multiplied by a risk-neutral average spike magnitude.
- **Characteristic spike lifetime** — A small dimensionless parameter equal to the ratio of mean spike duration to mean inter-spike interval, used to derive first-order spike corrections to option values.
- **Option value spike-smoothing property** — The result that European option values on power do not exhibit spikes when time to expiry is large relative to mean spike duration, because state-dependent values converge exponentially fast.
- **Forward price spike-smoothing property** — The analogous result that power forward prices for distant maturities are insensitive to whether the spot is currently in a spike, differing only by exponentially small corrections.
- **Product integral** — A generalisation of the matrix exponential used when Markov generators do not commute at different times, arising in time-inhomogeneous two-state spike models.
- **Put-call parity for spiking power markets** — Parity relationships between call and put values that hold in both the mean-reverting and Black-Scholes environments, extended to asymmetric power and Pareto-averaged payoffs.
- **Ergodic average spike magnitude** — The long-run stationary expected multiplicative spike factor used to scale inter-spike forward prices when time-to-maturity is large relative to spike lifetime.
- **Power forward price with spikes (delivery period)** — The risk-neutral forward price for electricity delivered over an interval [T1,T2] expressed as the ergodic spike magnitude times the inter-spike forward price up to exponentially small corrections.
- **Short-lived spike approximation** — A perturbative expansion of forward prices and contingent claim values in powers of the characteristic spike-occupation time tch when spike lifetime is much shorter than inter-spike waiting time.
- **European contingent claim on power forwards with spikes** — A derivative whose value equals a Black-Scholes-type value evaluated at a rescaled payoff and forward price, with corrections of order exp(-a(T-t)).
- **Ergodic implied volatility** — The Black-Scholes implied volatility of options on power forwards with spikes, obtained by inverting ergodic-regime call or put pricing formulas.
- **Pareto-distributed spike magnitude** — A heavy-tailed distribution for the multiplicative spike size with tail index gamma, leading to modified call/put pricing formulas with finite-moment restrictions gamma > omega_n.
- **Delta hedging of power contingent claims** — Standard Black-Scholes delta hedging applied to European claims on power forwards when time-to-maturity is large relative to expected spike lifetime.
- **Put-call parity for spiked power options** — The preservation of standard put-call parity relationships for approximate call and put prices on forwards on power with spikes in both ergodic and short-lived spike regimes.
- **Vega of power contingent claims** — The Black-Scholes vega formula used to linearize the implied volatility correction due to spikes in the short-lived spike perturbative expansion.
- **Geometric mean-reverting inter-spike price** — The risk-neutral dynamics of electricity prices outside spike periods, governed by a mean-reverting lognormal process with time-dependent drift.
- **Spike characteristic time (tch)** — The product of spike probability and expected spike lifetime, used as the small expansion parameter in the short-lived spike perturbation series.

## Methods
- Two-state continuous-time Markov chain modelling
- Matrix exponential and product integral for transition matrices
- Geometric mean-reverting diffusion (SDE specification and solution)
- Risk-neutral pricing via discounted expectation over extended state space
- Payoff averaging over Pareto spike-magnitude distribution
- Eigenclaim decomposition for analytical option valuation
- Black-Scholes formula mapping from mean-reverting to log-normal environment
- Asymptotic expansion in spike lifetime (ergodic limit and characteristic lifetime correction)
- Forward price computation for delivery-period contracts
- Ergodic limit / large time-to-maturity asymptotic expansion
- Short-lived spike perturbation expansion in tch
- Black-Scholes formula with rescaled strikes and payoffs
- Pareto-distribution-weighted option pricing (modified BS formulas)
- Implied volatility inversion via vega linearization
- Delta hedging via geometric Brownian motion approximation
- Put-call parity verification for approximate spike-adjusted prices
- Product integration / non-Markovian semigroup methods
- Mixture-of-distributions extension (Pareto plus Dirac delta)

## Implied prerequisites
- Stochastic differential equations and Itô calculus
- Markov process theory and transition semigroups
- Black-Scholes option pricing framework
- Risk-neutral pricing and change of measure
- Mean-reverting (Ornstein-Uhlenbeck / geometric mean-reverting) processes
- Probability distributions: Pareto, Dirac delta, log-normal
- Matrix algebra and matrix exponential
- Power market structure and spot/forward price relationships
- Black-Scholes option pricing (calls, puts, greeks)
- Geometric Brownian motion and risk-neutral pricing
- Forward and futures pricing for commodities
- Markov chain / two-state regime-switching models
- Pareto distribution and heavy-tailed probability distributions
- Perturbation / asymptotic expansion techniques
- Stochastic calculus and risk-neutral measure
- European option put-call parity
- Implied volatility and option Greeks (delta, vega)
