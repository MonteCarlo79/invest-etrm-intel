---
id: stochastic_price_processes_for_power_and_fuel
track: asset_valuation
level: intermediate
prerequisites:
- intrinsic_vs_extrinsic_value
markets:
- EU
- GB
- US
status: drafted
sources:
- id: framework_tseng
  use: derivation_reference
- id: tseng_barz_short_term_generation_asset_valuation_a_real_opti
  use: derivation_reference
- id: clewlow
  use: background
- id: pareto_spikes
  use: background
- id: deng_real_options_approach
  use: background
originality: synthesized
translations:
  zh:
    status: drafted
    en_hash: 900651d91817df6a
---
## Learning objectives

- Explain why geometric Brownian motion is the wrong default for power prices, and name the three features any usable power-price process must have.
- Write down the mean-reverting (Ornstein-Uhlenbeck / geometric mean-reversion) process and interpret its parameters.
- Add jumps for price spikes and a deterministic seasonal component; simulate the combined process.
- Explain how power and fuel processes are linked (correlation, cointegration) and why the link matters more than either process alone for spread-dependent assets.

## Intuition

Stock prices wander; power prices *snap back*. A cold snap or an outage can send power to ten times its normal level — but supply responds, demand normalises, and within hours or days the price is pulled back toward its seasonal mean. Any model that lets the price drift away forever (like GBM) will, over months, assign absurd probabilities to absurd prices and misprice the optionality of real assets.

The practical recipe for a power-price process has three ingredients: **mean reversion** (the pull back to normal), **jumps** (the spikes), and **seasonality** (what "normal" even means this week — winter weekday evenings are not summer Sunday nights). Fuel prices (gas, coal) are tamer: mean-reverting with much weaker jumps, but *correlated* with power — and for a plant owner, the correlation is the whole game, because the plant earns the *spread* between the two.

## Formal treatment

**Mean reversion (OU in log-price).** Let $x_t = \ln S_t$:

$$dx_t = \kappa\big(\theta(t) - x_t\big)dt + \sigma dW_t$$

- $\kappa > 0$ — speed of reversion (half-life $\ln 2 / \kappa$);
- $\theta(t)$ — time-varying mean level, set to reproduce the forward curve (calibration, not decoration);
- $\sigma$ — volatility of the log-price.

In price space this is a geometric mean-reversion (GMR) process: log-normal marginals, but pulled back toward $e^{\theta(t)}$.

**Jumps.** Add a compound Poisson term for spikes:

$$dx_t = \kappa(\theta(t) - x_t)dt + \sigma dW_t + J_t dN_t$$

with $N_t$ a Poisson process of intensity $\lambda$ and jump sizes $J_t$ (e.g. normal or exponential, occasionally signed downward for negative-price events). Jumps are what fatten the tails that option prices live in; the MRJDx family (mean-reverting jump-diffusion with parameter-attributed structure) is the reference implementation in the sources.

**Seasonality.** $\theta(t) = \ln F(0,t) - \text{adjustment}$ is chosen so the simulated mean matches the forward curve at every maturity — this embeds annual, weekly, and daily shape without extra factors.

**Two commodities.** Power $S_p$ and fuel $S_f$ each get such a process, driven by correlated Brownian motions $dW_p\, dW_f = \rho\, dt$. The spread's volatility is

$$\sigma_S^2 \approx \sigma_p^2 + \sigma_f^2 - 2\rho\,\sigma_p\sigma_f,$$

so high $\rho$ *compresses* spread volatility and shrinks extrinsic value; weak correlation inflates it. A stronger structural link is cointegration: a shared long-run level so the spread itself mean-reverts. Lattice methods (e.g. trinomial trees with moment-matched branching probabilities, plus a decoupling transformation for the correlation) make the same dynamics usable for dynamic-programming valuation.

## Worked example

Simulate one year of hourly log-prices with $\kappa = 52$ (half-life ≈ 4.8 days), $\sigma = 0.9$ (annualised), $\theta(t)$ flat at $\ln 60$ (€60/MWh), jump intensity $\lambda = 26$/year with mean jump $+0.8$ in log terms, seed 7.

Expected diagnostics the lab checks:
- The long-run sample mean of $x_t$ sits near $\theta + \lambda\mu_J/\kappa = 4.094 + 0.4 \approx 4.49$ — mean reversion pulls toward $\theta$, and the positive jumps lift the stationary mean by exactly $\lambda\mu_J/\kappa$.
- Realised jump count lands within ±30% of $\lambda T = 26$ (a Poisson(26) draw has std ≈ 5).
- A GBM with the same $\sigma$ drifts arbitrarily far; the OU path does not (terminal dispersion bounded by the stationary variance $\sigma^2/2\kappa \approx 0.0078$).

`labs/stochastic_price_processes_for_power_and_fuel/compute.py` runs exactly this simulation; its test asserts each diagnostic.

## Market variants

- **EU/GB:** negative-price jumps (renewable surpluses) require two-sided jump distributions; forward-curve calibration must include the strong intraday shape (peak/off-peak) before intraday valuation.
- **US:** heat waves drive regional spike regimes (ERCOT the extreme case, with scarcity adders); gas-process calibration to Henry Hub plus a basis process for delivery point.
- **CN:** price caps and floors (限价) truncate the jump distribution — simulate then clamp, or model the cap explicitly; seasonality dominated by summer cooling and winter heating peaks; inter-provincial flows add a second mean level for receiving regions.

## Common errors

1. **GBM by default** — unbounded variance growth makes long-horizon plant values a function of model time, not of the asset.
2. **Reversion to a constant mean** — without $\theta(t)$ tracking the forward curve, the model reprices forwards wrongly before any optionality is even considered.
3. **Jump-free tails** — a pure diffusion underprices near-the-money spread options; spikes are not noise, they are where peaking-asset value lives.
4. **Independence between power and fuel** — simulating two uncorrelated processes and then wondering why the spread option looks huge.
5. **Annualised parameters on hourly steps** — forgetting to scale $\sigma$ and $\lambda$ by the time step; $\sigma\sqrt{\Delta t}$ errors of $\sqrt{24}$ appear in hourly simulations.

## Assessable questions

1. What is the half-life of a mean-reversion process with $\kappa = 52$ per year?
2. Why does high power–fuel correlation reduce the value of a spark-spread option?
3. Write the SDE of a mean-reverting jump-diffusion for log price and name every parameter.
4. Your simulation's long-run mean is right but it never spikes. Which component is missing and how would you detect the mispricing it causes?
5. Explain how you would calibrate $\theta(t)$ so the simulated mean reproduces a given monthly forward curve.
