---
id: tolling_and_asset_backed_trading
track: hedging_trading
level: intermediate
prerequisites:
- tolling_agreement_structure_and_valuation
- profit_at_risk_and_hedging_objective
markets:
- EU
- GB
- US
status: drafted
sources:
- id: energy_risk_2012___kyos_power_plant_hedging_strategies
  use: derivation_reference
- id: plant_hedging_and_trading_strategies_kyos_20110530
  use: derivation_reference
- id: power_european_toll
  use: practice_example
originality: synthesized
translations:
  zh:
    status: drafted
    en_hash: b6e25e10b6e8599f
---
## Learning objectives

- Compare the same plant under two regimes — merchant (asset-backed trading) and tolled — on the same simulated paths.
- Show what a toll does to the P&L distribution: mean roughly preserved, variance collapsed.
- Locate the deal zone: seller's floor (fixed cost + risk margin) and buyer's ceiling (strip value minus risk/capital charge).
- Frame asset-backed trading as a choice: run the optionality yourself (and hold the variance) or sell it (and hold almost none).

## Intuition

Own a flexible plant and you face a career-defining choice every few years. **Merchant (asset-backed trading):** run the desk, dispatch and hedge per this track, and earn the full margin distribution — good years, bad years, and the tail. **Tolled:** hand the dispatch rights to someone better equipped (or hungrier for the risk) for a premium, and convert the same physical asset into something close to a bond.

The previous concepts built the buyer's valuation of a toll. This one is the *owner's* view of the same trade. The remarkable fact is how little the mean moves: the toll premium, fairly negotiated, sits near the expected margin (the strip value from the valuation work). What changes is everything around the mean — the variance collapses by ~99%. Whether that trade is attractive depends on things no model prices directly: your cost of capital, your risk limits, your conviction that your desk captures more than the option strip's fair value, and honestly, what keeps you up at night.

## Formal treatment

**Same plant, two P&Ls.** On shared simulated paths of the spread:

- Merchant: $X_m = \sum_t \max(S_t - \text{fees}, 0) \cdot H_t$ — the full distribution: mean $\approx$ strip value, real left tail, real right tail.
- Tolled: $X_t = \text{premium} + \epsilon$ with $\epsilon$ small (availability, minor residuals) — variance ratio vs merchant typically 1–5%.

**The deal zone.** A toll signs only inside:

$$\underbrace{\text{fixed O\&M} + \text{owner's risk margin}}_{\text{seller floor}} \;\le\; \text{premium} \;\le\; \underbrace{\text{strip value} - \text{buyer's risk \& capital charge}}_{\text{buyer ceiling}}$$

If the buyer's desk believes it captures more than the strip (better dispatch, better hedging, portfolio synergies), the ceiling rises; if the owner's alternative is a costly underused desk, the floor falls. The negotiation is about who believes the extrinsic value estimate more.

**Asset-backed trading as the complement.** Choosing merchant means choosing to *be* the buyer of your own flexibility: you keep the strip value and the residual optionality (the extrinsic the buyer would otherwise monetise), and you carry the PaR. The hedging track's entire machinery exists to make that residual manageable rather than accidental.

## Worked example

Same monthly-spread plant as the tolling concept, 4 000 paths (seed 37):

- Merchant: mean 156.8 k€/MW-yr, std 24.6 k€, q5% 116.8 k€ — the full distribution, tail included.
- Tolled at 100% of strip: mean 156.3 k€, std 2.8 k€ (variance ratio 1.3%) — same mean, essentially no variance.
- Tolled at 85% of strip: mean 132.8 k€, std 2.4 k€ — the seller has paid 24 k€/MW-yr of expected margin to remove the tail; whether that's cheap insurance or an expensive desk closure depends on the seller's floor (fixed O&M 94 k€ in the example) and the buyer's ceiling.

`labs/tolling_and_asset_backed_trading/compute.py` reproduces the distributions; its test asserts the variance collapse, the mean alignment, and the floor < ceiling ordering.

## Market variants

- **EU:** the classic CCGT toll market (the practice sources include European and UK tolls); increasingly battery tolls, where the buyer monetises cycling optionality the owner can't trade itself.
- **GB:** toll + Capacity Market stacking is the standard new-build financing structure — the CM annuity covers fixed cost, the toll covers margin, and the lender sees a bankable cashflow shape.
- **US:** heat-rate tolls and "virtual tolls" (purely financial) are standard; the buyer is often a hedge fund or a utility's trading arm.
- **CN:** 共享储能 leasing is the storage version (the lessee is the toll buyer); for thermal and renewables, the analogue is 代运营/委托运营 contracts — and the deal-zone logic applies unchanged: owner floor = 固定成本, buyer ceiling = the merchant value of the flexibility in 现货.

## Common errors

1. **Comparing mean to mean only** — the toll's point is the variance; a premium slightly below strip can still be the right trade if your cost of carrying the tail is high.
2. **Pricing the premium off intrinsic** — the seller's floor must include a share of extrinsic value, or the buyer collects it for free (recurring theme from the tolling concept).
3. **Ignoring availability risk allocation** — who bears outages changes the variance the premium must compensate; an "as-available" toll is worth less to the buyer.
4. **Thinking toll vs merchant is permanent** — the option to re-toll (or take back dispatch) at expiry has its own value; contract length is a real option too.
5. **Forgetting what the buyer does with it** — the buyer runs this entire track's playbook on your plant; understanding their ceiling means understanding your own floor's opportunity cost.

## Assessable questions

1. On the same simulated paths, what changes and what stays roughly the same when a plant moves from merchant to tolled?
2. Write the deal-zone inequality and define both bounds.
3. Why can a premium below the strip value still be rational for the seller?
4. A battery owner's board asks why the lessee pays 110% of your computed strip. Give two buyer-side reasons.
5. Structurally, why does a toll + capacity contract make a new build bankable in GB while pure merchant does not?
