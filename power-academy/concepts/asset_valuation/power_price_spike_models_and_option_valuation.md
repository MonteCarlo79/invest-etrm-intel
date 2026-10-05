---
id: power_price_spike_models_and_option_valuation
track: asset_valuation
level: advanced
prerequisites:
- spread_option_pricing_models
- real_options_framework_for_generation_assets
markets:
- EU
- GB
status: drafted
sources:
- id: pareto_spikes
  use: derivation_reference
- id: clewlow
  use: background
- id: framework_tseng
  use: background
originality: synthesized
translations:
  zh:
    status: none
    en_hash: null
---
## Learning objectives

- Characterise power-price spikes empirically: heavy tails, clustering, fast decay — and why lognormal models miss all three.
- Model spike sizes with the Pareto (power-law) tail and estimate its exponent from data.
- Explain how the jump specification in a price process changes option values, especially near the money and far out of it.
- Quantify the mispricing that results from valuing a peaking option with a spike-free model.

## Intuition

Power prices spend most of their life in a quiet band and a small fraction in violent excursions: scarcity hours, plant outages, cold snaps. These are not large versions of ordinary fluctuations — they are a different animal. A lognormal (or any Gaussian-tailed) model assigns them probabilities that are wrong by orders of magnitude, and the error lands exactly where flexible assets earn their keep: the tail.

Two facts organise the modelling. First, spike *sizes* follow a power law: the probability of a spike exceeding size $x$ decays like $x^{-\alpha}$ (Pareto tail), with $\alpha$ typically 2–5 in power markets — far fatter than exponential, let alone Gaussian. Second, spikes *cluster in time* (stress comes in days, not hours) and *decay fast* (mean reversion is much stronger in a spike than out of one). The earlier jump-diffusion from this track is the baseline; this concept is about getting the jump *distribution* right and understanding what it does to option values.

## Formal treatment

**Pareto tail.** For spike sizes $X$ above a threshold $u$:

$$\Pr[X > x] = \left(\frac{x}{u}\right)^{-\alpha}, \quad x \ge u,\ \alpha > 0$$

The exponent $\alpha$ is the whole story: $\alpha \le 2$ implies infinite variance (tails so heavy that sample vol is unstable); $\alpha \approx 3$–$5$ is the empirical power-market range. Estimate it with the Hill estimator on the $k$ largest exceedances $x_{(1)} \ge \dots \ge x_{(k)} \ge u$:

$$\hat\alpha = \left( \frac{1}{k} \sum_{i=1}^{k} \ln \frac{x_{(i)}}{u} \right)^{-1}$$

**Process consequences.** The jump-diffusion from earlier becomes: Poisson arrivals $N_t$ with intensity $\lambda(t)$ (higher in stress seasons), jump sizes drawn Pareto with threshold $u$ and exponent $\alpha$, and a jump-decay rate $\kappa_J \gg \kappa$ so spikes revert within hours. Simulation is easy — draw $U \sim \text{Uniform}$, invert the Pareto: $X = u\,U^{-1/\alpha}$.

**Option valuation.** A European call's value decomposes into probability-weighted payoffs of tail events; with power-law tails, far-OTM options are worth dramatically more than under lognormal jumps, because $\Pr[S_T > K]$ decays polynomially, not exponentially. The practical correction: value the diffusion part with your favourite model and add the spike expectation explicitly, $\lambda T \cdot \mathbb{E}[\max(X - K, 0)]$ with the Pareto tail — or simulate the full jump process and let the tails price themselves.

## Worked example

Synthetic spike sample: 400 exceedances over $u = 100$ €/MWh drawn from a true Pareto with $\alpha = 3$ (seed 3).

- The Hill estimator recovers $\hat\alpha = 3.11$.
- Closed form per spike arrival: $\mathbb{E}[\max(X - K, 0)] = \frac{u^\alpha}{(\alpha-1)K^{\alpha-1}}$ — at $K = 150$: $22.2$ €; at $K = 300$: $5.6$ €.
- The Gaussian-jump comparison (same mean and variance): at $K = 150$ the Gaussian actually prices *higher* (34.6 vs 22.2 — Gaussian is fatter near the mean); at $K = 300$ the Gaussian prices 1.5 € vs Pareto 5.6 € — **3.8× underpricing**; at $K = 400$ the ratio is 64×. The mispricing is a *far-tail* phenomenon, invisible near the money.

`labs/power_price_spike_models_and_option_valuation/compute.py` does the estimation and both valuations; its test asserts the Hill estimate, the closed-form value, and the underpricing ratio.

## Market variants

- **EU/GB:** negative spikes (renewable dumps) need a lower-tail Pareto too, or a two-sided jump law; scarcity episodes like the German Q4-2024 Dunkelflaute are textbook cluster events.
- **US:** ERCOT's scarcity adders make the tail administrative as much as physical — model the adder rule, not just the price.
- **CN:** 限价 caps truncate the Pareto tail at the cap — the mass that would exceed it piles up *at* the cap, so options struck near the cap price like digital bets on cap-binding rather than tail options; if caps relax, the tail reappears.

## Common errors

1. **Gaussian jump sizes** — the default in most jump-diffusion code; underprices far-OTM options by orders of magnitude.
2. **Estimating $\alpha$ on all data** — the Pareto law holds only above the threshold; including the bulk biases $\hat\alpha$ upward (tail looks thinner than it is).
3. **Threshold too low** — same bias in the other direction: $u$ must sit in the tail; check with a mean-excess plot.
4. **Ignoring clustering** — i.i.d. jump times spread the spikes evenly; real risk is the *week* with five spikes, not five independent hours.
5. **Calibrating vol to include spikes** — inflating $\sigma$ to match total variance gets the quiet regime wrong (too jumpy) while still missing the tail shape.

## Assessable questions

1. For a Pareto tail with $u = 100$, $\alpha = 3$, compute $\Pr[X > 300]$.
2. Derive $\mathbb{E}[\max(X - K, 0)]$ for the Pareto tail above a strike $K \ge u$.
3. What does $\alpha \le 2$ imply, and why does it break vol-based calibration?
4. Explain why the Hill estimator uses only the top $k$ order statistics.
5. A colleague prices a peaker with a Gaussian-jump model and reports value 30% below yours. Which parameter region makes the gap largest and why?
