# out (6)

- id: `out_6` · class: library · type: pdf
- topic: Valuation of power swing options using spike-jump-diffusion models and state-space forest methods · level: advanced · market: EU · year: 2013
- worked examples: True · code: False

## Concepts
- **Swing option** — A derivative bundled with a power forward that grants the holder flexible rights to adjust delivery volume and timing within contractual bounds.
- **Swing right** — An individual option-like entitlement within a swing contract allowing the holder to acquire or deliver an additional volume unit at specified strike prices.
- **Ornstein-Uhlenbeck (OU) process** — A mean-reverting continuous-time stochastic process used to model the core and spike components of power spot prices.
- **Spike-jump-diffusion model** — A spot price model combining a mean-reverting diffusion core with separate Poisson-driven OU processes for positive and negative price spikes.
- **Positive spike process** — A mean-reverting jump process driven by a Poisson subordinator capturing upward price anomalies that revert rapidly.
- **Negative spike process** — A separate mean-reverting jump process capturing downward price anomalies, calibrated independently from positive spikes.
- **Lévy driver** — A pure-jump process, such as a compound Poisson process, used to introduce discontinuous increments into the core OU diffusion.
- **Itô transformation** — A function applied to the latent stochastic process to produce either additive or geometric (log-normal) spot price dynamics.
- **Negative power prices** — Observed market prices below zero, requiring spot price models to move beyond standard exponential OU specifications.
- **Deterministic seasonality** — A decomposed time-varying drift capturing weekly, monthly, quarterly and annual periodic patterns in power prices.
- **State-space forest** — A multi-dimensional grid pricing structure composed of N+1 state-space trees, each representing the swing option value with a given number of remaining exercise rights.
- **State-space tree** — A collection of discretised state spaces over the option lifetime representing all possible process realisations at each time step.
- **Conditional density approximation** — An analytical approximation to the transition density of the underlying processes used to compute node-to-node transition probabilities in the pricing grid.
- **Truncated reversed spike process** — A time-reversed and truncated version of the spike OU process whose distribution is analytically tractable and used to approximate the spike conditional density.
- **Moment-generating function (MGF) of spike process** — A closed-form expression for the MGF of the truncated reversed spike process used to derive its moments and density approximation.
- **Upper incomplete gamma function** — A special function used to express the closed-form density of the truncated spike process under exponential jump-size distributions.
- **Exponential jump size distribution** — A symmetric exponential distribution used to model the heights of positive and negative spiky jumps in the spot price model.
- **Optimal multiple stopping** — The mathematical framework governing the exercise strategy of a swing option as a sequence of optimal stopping decisions.
- **Least squares Monte Carlo (LSMC)** — A simulation-based pricing method for path-dependent derivatives that estimates continuation values via regression, applicable to swing options.
- **Recovery time** — A contractual minimum waiting period that must elapse between consecutive swing exercise actions.
- **Swing payoff structure** — The specification of acquisition and delivery payoffs for swing rights including price boundaries that cap the writer's exposure to large price moves.
- **Marginal value of swing rights** — The incremental option value added by each additional swing exercise right, which is concave and decreasing in the number of rights.
- **Jump risk premium** — The additional option value attributable to the presence of price spikes, quantified as the difference between swing option prices with and without jump components.
- **American option strip** — A benchmark replication strategy for a swing option consisting of a series of identical American options whose aggregate cost exceeds the swing option price.
- **Model calibration via jump filtering** — A sequential procedure that detects and separates positive spikes, negative spikes and non-spiky jumps from historical price data before fitting each process component.
- **Running sample volatility filter** — A statistical tool used during calibration to identify price jumps as outliers beyond a confidence threshold in the transformed price series.
- **Fourier seasonality decomposition** — Calibration of annual and semi-annual periodic price components using Fourier frequencies fitted to the log-transformed price series.
- **Sinh Itô transform** — An alternative transformation that can accommodate negative spot prices within a stochastic spot price model.

## Methods
- State-space forest algorithm
- Trinomial/multinomial tree discretisation
- Analytical conditional density approximation
- Moment-generating function derivation
- Convolution of jump-size and attenuation densities
- Least squares Monte Carlo (LSMC)
- Finite-difference method
- Sequential jump filtering via running volatility
- Linear regression for mean-reversion calibration
- Fourier decomposition for seasonality
- Periodic averaging for intra-period seasonality
- Backward induction dynamic programming on grid

## Implied prerequisites
- Stochastic calculus and Itô's lemma
- Ornstein-Uhlenbeck processes
- Poisson processes and compound Poisson processes
- Lévy processes
- Markov chain and transition probability theory
- American option pricing theory
- Optimal stopping theory
- Monte Carlo simulation
- Time-series econometrics and regression
- Fourier analysis
- Probability distributions (exponential, gamma, normal)
- Power market microstructure and forward contracts
