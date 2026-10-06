---
id: real_options_framework_for_generation_assets
track: asset_valuation
level: intermediate
prerequisites:
- spread_option_pricing_models
- intrinsic_vs_extrinsic_value
- heat_rate_and_plant_parameters
markets:
- EU
- GB
- US
status: drafted
sources:
- id: clewlow
  use: background
- id: deng_real_options_approach
  use: derivation_reference
- id: tseng_barz_short_term_generation_asset_valuation_a_real_opti
  use: background
- id: worldpower2010_thevalueofstartingupthepowerplant
  use: background
originality: synthesized
translations:
  zh:
    status: drafted
    en_hash: 69e0b62df60319ea
---
## Learning objectives

- State the real-options viewpoint: operational and investment flexibility is a set of options on physical assets, priced with option logic rather than static NPV.
- Name the standard real options in generation (operate/dispatch, start/stop switching, invest/defer, mothball, expand, fuel switch) and give each a trigger variable.
- Explain why static NPV underprices flexible assets, and compute an option-adjusted investment threshold.
- Solve a small investment-timing problem on a binomial tree and interpret the "wait premium".

## Intuition

Static NPV says: build when expected discounted cashflows exceed cost. That rule treats the decision as now-or-never. But a developer can *wait*: if power prices rise the project gets better, if they fall the developer simply doesn't build. The right to build later — without the obligation — is an American call option on the project's value. Owning land, a grid connection, or a permit is owning that option.

The same logic runs through the plant's life: once built, the operator holds the option to run (dispatch), to stop and restart (switching, at the cost of start-up), to mothball, to expand, or to switch fuels. Every one of these flexibilities has a trigger (a spread, a price level, a demand threshold) and a value that static analysis misses. The real-options framework is the umbrella: enumerate the flexibilities, identify each trigger, value each as an option, add them to the static NPV.

## Formal treatment

**Investment timing (the canonical problem).** Project value $V$ follows a diffusion (e.g. GBM with drift $\mu$, vol $\sigma$); build cost $I$ is fixed; the option to build is perpetual American with payoff $V - I$. Under standard assumptions (Dixit–Pindyck) the optimal rule is: build when $V$ first reaches

$$V^* = \frac{\beta_1}{\beta_1 - 1} \cdot I, \qquad \beta_1 = \tfrac12 - \frac{\mu - \delta}{\sigma^2} + \sqrt{\left(\tfrac12 - \frac{\mu-\delta}{\sigma^2}\right)^2 + \frac{2r}{\sigma^2}} > 1$$

with $\delta$ the convenience yield/cashflow payout rate and $r$ the risk-free rate. The multiplier $\beta_1/(\beta_1 - 1) > 1$ is the **wait premium**: optionality pushes the build trigger *above* the static NPV break-even $V = I$ — often to 1.5–2× cost. Building at static NPV = 0 destroys the option to wait.

**A taxonomy for generation assets:**

| Real option | Trigger variable | Typical holder |
|---|---|---|
| Operate / dispatch | hourly clean spread > 0 (plus start amortisation) | operator |
| Start / stop switching | spread vs start cost and min-run economics | operator |
| Invest / defer | long-run margin vs build cost × wait multiplier | developer |
| Mothball / restart | fixed O&M vs expected margin | owner |
| Expand / repower | margin on incremental capacity | owner |
| Fuel switch | clean spread of fuel A vs fuel B | dual-fuel operator |

Each row is an optionality the real-options framework values and the static model sets to zero.

**Solution methods.** Perpetual problems have closed forms (above); finite-horizon or path-dependent ones use binomial/trinomial trees (matching moments to the price process — see the lattice material in this track's sources), Monte Carlo with regression (LSM, next concept), or dynamic programming on a dispatch model.

## Worked example

Project: build a peaker for $I = 100$ M€. Project value $V$ (NPV of lifetime margins if built today) is GBM with $\mu - \delta = -0.04$, $\sigma = 0.25$, $r = 0.05$.

$$\beta_1 = \tfrac12 - \frac{-0.04}{0.0625} + \sqrt{\left(\tfrac12 - \frac{-0.04}{0.0625}\right)^2 + \frac{2 \times 0.05}{0.0625}} = 1.14 + \sqrt{1.30 + 1.60} = 1.14 + 1.70 = 2.84$$

Wait multiplier: $\beta_1/(\beta_1 - 1) = 2.84/1.84 = 1.54$. Build trigger: $V^* = 154$ M€ — static NPV says build at 100 M€; option logic says wait until the project looks 54% better than cost. At $V = 110$ M€ the static analyst builds and gives up the option; the option to defer is worth the difference between holding it and exercising early, which the lab's binomial tree quantifies.

`labs/real_options_framework_for_generation_assets/compute.py` computes $\beta_1$, the trigger, and a 200-step binomial tree value of the finite-horizon build option; the test asserts the closed-form numbers and that the tree converges toward them as the horizon lengthens.

## Market variants

- **EU/GB:** capacity-market revenues change the payout rate $\delta$ (a steadier cashflow lowers the wait premium); grid-connection queues make the *connection agreement itself* a scarce deferral option that developers trade.
- **US:** ITC/PTC tax credits act like a changing $I$ or $\delta$ with legislated expiry — an option with a known deadline, which compresses the wait premium near expiry ("build by year-end" rushes).
- **CN:** investment is administratively scheduled more than option-driven (核准, 指标), so the framework reads differently: the deferral option belongs partly to the planner, not the developer. For merchant renewables/BESS post-136号文, however, revenue uncertainty is real and the invest/defer logic applies directly to sizing and timing merchant capacity.

## Common errors

1. **Static NPV as the build rule** — ignores that waiting has value; systematically over-builds in volatile markets.
2. **Forgetting the payout stream** — a project that generates cash while you wait (or whose window decays, e.g. subsidy expiry) has a high $\delta$ and a low wait premium; leaving $\delta$ out overstates the premium.
3. **One-size option value** — "the real option is worth 30%" is not a number; each flexibility needs its own trigger and model.
4. **GBM where it doesn't belong** — perpetual closed forms assume GBM; mean-reverting power prices shrink the long-run variance and reduce the wait premium (lattices handle this).
5. **Double counting** — valuing the dispatch option inside the investment NPV *and* again as a separate flexibility line.

## Assessable questions

1. With $\sigma = 0.30$ and the other parameters of the worked example unchanged, does the wait multiplier rise or fall? Why?
2. Name the trigger variable for the mothball option and explain who holds it.
3. A subsidy worth 20 M€ expires in one year. How does its expiry change the build trigger as the deadline approaches?
4. Derive the wait premium intuition from Jensen's inequality instead of the closed form.
5. Your static NPV model says build a CCGT today at V = 1.05 × I. Give two distinct reasons the real-options framework might still say wait.
