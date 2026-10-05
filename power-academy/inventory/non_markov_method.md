# Non-Markov Method

- id: `non_markov_method` · class: library · type: pdf
- topic: Non-Markovian electricity price modelling with spikes and European contingent claim valuation · level: advanced · market: US · year: 2001
- worked examples: False · code: False

## Concepts
- **Self-reversing jumps** — Spikes modelled as multiplicative or additive jumps that revert autonomously, superimposed on a regular-regime price process.
- **Non-Markovian price process** — A stochastic process for electricity prices where the spike state introduces path-dependence not captured by any Markov process.
- **Two-state continuous-time Markov process** — An underlying Markov chain governing transitions between the regular and spike regimes of electricity prices.
- **Transition matrix** — A 2×2 stochastic matrix whose entries give the probabilities of moving between spike and regular states over a time interval.
- **Generator matrix** — A 2×2 matrix characterising the instantaneous transition rates of the two-state Markov process between spike and regular regimes.
- **Kolmogorov-Chapman equation** — A semigroup consistency condition satisfied by the transition matrix of the underlying Markov regime-switching process.
- **Decomposition of transition probabilities** — A splitting of spike-state transition probabilities into a continuous-residence component and a component involving at least one visit to the regular state.
- **Spike magnitude distribution** — A conditional probability distribution on (1,∞) describing the random multiplicative size of a spike at each point in time.
- **Expected spike lifetime** — The mean duration a price process remains in the spike state, equal to the reciprocal of the spike-to-regular transition rate.
- **Expected inter-spike duration** — The mean time the process spends in the regular state between consecutive spikes, equal to the reciprocal of the regular-to-spike transition rate.
- **Characteristic spike lifetime** — A dimensionless ratio of mean spike lifetime to mean inter-spike lifetime used to parameterise the short-lived, rare-spike asymptotic regime.
- **European contingent claim on electricity without spikes** — A standard derivative whose value is the discounted risk-neutral expected payout under the regular-regime price process alone.
- **European contingent claim on electricity with spikes** — A derivative valued by decomposing the non-Markovian spike dynamics into a portfolio of regular-regime contingent claims weighted by regime-transition probabilities.
- **Payout translation operator** — An operator that rescales a contingent claim's payout function by the spike magnitude, mapping g(s) to g(λs).
- **Short-lived and rare spike approximation** — A first-order perturbative correction to the no-spike contingent claim value, valid when the characteristic spike lifetime is small.
- **Dynamic hedging under spike dynamics** — Replication of a spiked-electricity contingent claim as a portfolio of regular-regime contingent claims, assuming claims are written on forward rather than spot prices.
- **Product integral** — A generalisation of the matrix exponential used to express the transition matrix when the generator matrices at different times do not commute.
- **Time-homogeneous Markov process** — A special case where generator entries are constant, yielding transition probabilities that depend only on the length of the time interval.

## Methods
- Two-state continuous-time Markov chain for regime switching
- Matrix exponential and product integral for transition matrix computation
- Decomposition of transition probabilities into path components
- Risk-neutral pricing via discounted expected payout
- Perturbation expansion in characteristic spike lifetime
- Parametric and non-parametric estimation of spike parameters from historical data
- Portfolio replication of spiked claims using regular-regime claims

## Implied prerequisites
- Continuous-time Markov chains and generator matrices
- Stochastic processes and Itô calculus
- Risk-neutral pricing and equivalent martingale measures
- Matrix exponentials and linear algebra
- European option pricing theory
- Regime-switching models in finance
- Basic statistics and probability distributions
