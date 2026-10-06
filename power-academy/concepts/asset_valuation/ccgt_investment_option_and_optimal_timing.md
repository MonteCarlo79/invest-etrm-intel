---
id: ccgt_investment_option_and_optimal_timing
track: asset_valuation
level: advanced
prerequisites:
- real_options_framework_for_generation_assets
markets:
- EU
- GB
status: drafted
sources:
- id: ccgt_valuation
  use: background
- id: decide_when_to_start_your_power_plant_v2
  use: derivation_reference
- id: worldpower2010_thevalueofstartingupthepowerplant
  use: background
- id: tseng_barz_short_term_generation_asset_valuation_a_real_opti
  use: background
originality: synthesized
translations:
  zh:
    status: drafted
    en_hash: b93a3544277c89de
---
## Learning objectives

- Apply the real-options framework to the concrete CCGT build decision: what is the underlying, what is the strike, what is the option.
- Derive the build NPV as a linear function of the long-run clean spark spread under mean reversion.
- Compute the optimal build trigger on a mean-reverting (OU) lattice and compare it with the static break-even and the GBM premium.
- Explain why mean reversion *lowers* the wait premium relative to GBM, and what raises it again.

## Intuition

The investment-timing concept gave us the generic rule: don't build at static NPV = 0; wait until the project value clears a premium that pays for the option you give up. This concept makes it concrete for the decision developers actually face: *when do I build a CCGT?* The underlying is not "the project" abstractly — it is the **long-run clean spark spread**, the margin the plant will earn for 25 years. The strike is the build cost (plus connection). The option is the right to wait for a better margin environment without obligation.

The CCGT case has one twist that changes the answer materially: power margins are mean-reverting. High spreads attract entry (including yours), low spreads force exit — so the spread cannot wander off the way a stock price can. Less long-run uncertainty means a smaller wait premium than the GBM formula suggests, and it means the trigger must be computed on a mean-reverting model, not read off Dixit–Pindyck.

## Formal treatment

**Build NPV under OU.** Let the long-run clean spread $S_t$ follow $dS = \kappa(\theta - S)dt + \sigma_S dW$. If the plant is built today, its discounted expected margin stream is an annuity over expected spreads:

$$V_{build}(S) = \frac{H}{1000}\int_0^\infty e^{-rt}\,\mathbb{E}[S_t \mid S_0 = S]\,dt - I = \frac{H}{1000}\left(\frac{S}{r + \kappa} + \frac{\theta\,\kappa}{r(r + \kappa)}\right) - I$$

with $H$ equivalent full-load hours/yr and $I$ the build cost (€/kW). Two things to note: the value is **linear in $S$** (a gift of the OU structure), and the static break-even $S_{static}$ solves $V_{build} = 0$.

**The option.** A perpetual American call on a linear payoff under OU — no closed form, but easy on a trinomial lattice with moment-matched transition probabilities (Kushner-style), run backward until quasi-perpetual. The build trigger $S^*$ is the first grid point where exercising beats continuing.

**Why the OU premium is smaller.** In GBM, variance of $S_T$ grows linearly with $T$ without bound; in OU it converges to $\sigma_S^2 / 2\kappa$. The option to wait is worth less when the future is less uncertain, so $S^*/S_{static}$ shrinks as $\kappa$ grows — and rises with $\sigma_S$, which is why vol assumptions dominate the answer.

## Worked example

Per-kW economics: $\theta = 16$ €/MWh, $\kappa = 0.5$/yr, $\sigma_S = 4$ €/MWh/√yr, $r = 8\%$, $H = 4000$ h, $I = 800$ €/kW.

- Static break-even: $S_{static} = 16.0$ €/MWh — exactly the long-run mean; a static analyst is indifferent today.
- OU lattice (quarterly, 40y): trigger $S^* = 22.5$ €/MWh — **1.41× static**. Build only when the current spread stands 41% above its long-run mean.
- The option at $S = \theta$: worth 23.5 €/kW even though its exercise value is exactly zero — waiting is an asset.
- Contrast with GBM (concept *real_options_framework_for_generation_assets*): premium 1.54×. Mean reversion cut the premium, as predicted.

`labs/ccgt_investment_option_and_optimal_timing/compute.py` implements the lattice; its test asserts the band $S^* \in [20, 25]$, the ratio $[1.25, 1.60]$, and that the trigger rises with $\sigma_S$.

## Market variants

- **EU:** capacity remuneration (FR/BE/IT mechanisms) adds a second, steadier annuity that raises $V_{build}$ at every $S$ and lowers the energy-margin trigger; grid-queue position is itself a deferral option developers hoard.
- **GB:** the T-4 Capacity Market auction converts part of the margin into a 15-year contract for new build — the timing option is partly exercised *through the auction calendar*, not continuously.
- **CN:** for merchant CCGT the analogue is 容量电价 + 电量电价 two-part pricing; the capacity payment truncates downside and compresses the wait premium. But most CN build decisions are administratively scheduled — the option framework applies to *sizing and timing merchant share*, not to whether the project exists.
- **US:** ITC/PTC-style credits with expiry dates force the option toward early exercise as deadlines approach.

## Common errors

1. **GBM by reflex** — reading the trigger off Dixit–Pindyck when the margin mean-reverts; the premium is overstated, sometimes badly.
2. **Pricing the option on static NPV inputs** — using the same $V$ for the NPV and the option's underlying without re-deriving the annuity under OU.
3. **Ignoring capacity revenue** — in markets that pay for capacity, the energy-spread trigger is not the whole decision; omitting the capacity annuity overstates the required margin.
4. **Forgetting the queue** — grid connection lead times mean the "build" decision is really "build in 3–5 years"; the option's horizon and the trigger both shift.
5. **One trigger for all vintages** — $\sigma_S$ and $\theta$ vary by year and region; a 2021-vintage trigger is not a 2026-vintage trigger.

## Assessable questions

1. Derive $V_{build}(S)$ under OU and show it is linear in $S$.
2. Why does the OU build premium shrink as $\kappa$ rises?
3. With the worked-example parameters, what happens to $S^*$ if $\sigma_S$ rises from 4 to 5?
4. A capacity contract pays 50 €/kW/yr for 15 years. Qualitatively, what happens to the energy-margin trigger?
5. Explain to a CFO why "NPV = 0, so let's build" destroys value even when the forecast is unbiased.
