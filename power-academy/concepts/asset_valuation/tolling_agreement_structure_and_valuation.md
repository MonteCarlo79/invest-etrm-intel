---
id: tolling_agreement_structure_and_valuation
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
- id: humbertollvaluation
  use: practice_example
- id: rwetollvaluation_v2
  use: practice_example
- id: power_european_toll
  use: practice_example
- id: power_uk_toll
  use: practice_example
- id: optimal_switching_with_applications_to_energy_tolling_agreem
  use: derivation_reference
originality: synthesized
translations:
  zh:
    status: none
    en_hash: null
---
## Learning objectives

- Define a tolling agreement and name its main structural variants (full toll, revenue toll, virtual toll).
- Value the toll from the buyer's side: option strip minus premium minus fees; and from the seller's side.
- Compute the cost of contractual operating constraints (restart limits, min run, availability) and explain why buyers price them explicitly.
- Explain the economics that make both sides sign: what each sells, buys, and keeps.

## Intuition

A tolling agreement is how a plant owner **sells the optionality without selling the plant**. The buyer pays a premium (and typically supplies or pays for fuel) for the right to dispatch the plant as its own for the contract period — every hour, it earns $\max(\text{spread}, 0)$ minus fees. The owner keeps the asset, the balance sheet, and the residual value; the buyer rents the strip of spread options that this track has been building up to.

This reframing makes valuation immediate: the toll's fair premium *is* the plant's option value over the contract life, computed exactly as in the previous concepts. Everything that reduces the option value — restart limits, minimum-run rules, availability guarantees, fuel pass-through quirks — reduces what a rational buyer pays, and sophisticated buyers price each constraint explicitly before signing. A toll negotiated off intrinsic value alone transfers the extrinsic value from seller to buyer for free.

## Formal treatment

**Structure.** In a *full toll*, the buyer takes dispatch control and pays a fixed premium (€/kW-yr) plus variable fees; fuel is the buyer's problem. In a *revenue toll*, the buyer takes a share of dispatch margin. A *virtual toll* is purely financial — no physical control, just the cashflow $\max(\text{spread}, 0)$ as a swap. Operationally critical clauses: restart limits ($N$ starts over the period), minimum run/down times, availability guarantees (who bears outage risk), and fuel/supply obligations.

**Buyer value.** Over contract months $m$ with forward spreads $F_m$ and monthly option values $O(F_m)$:

$$V_{buyer} = \sum_m O(F_m - \text{fees}) \times H_m - \text{premium}$$

where the option margin is computed net of variable fees, exactly as in *spread_option_pricing_models* and *intrinsic_vs_extrinsic_value*. The breakeven premium is the strip value itself.

**Constraint cost.** A restart limit converts the unconstrained option strip into a constrained switching problem (optimal switching / DP over the operating calendar). The value loss is *not* linear: the first few lost starts are cheap, the binding ones are expensive, because the plant is forced to either burn negative-margin periods staying online or forgo positive ones after stops.

**Seller value.** The seller compares premium + residual coverage of fixed O&M against self-dispatch value minus its own cost of capital and risk — a toll is often the cheaper way to monetise flexibility than running a trading desk.

## Worked example

Per-MW annual toll, monthly blocks, forward spreads $[-5, 0, 8, 15, 25, 40, 60, 35, 20, 10, 5, -2]$ €/MWh, monthly option uncertainty 12 €/MWh, fees 2 €/MWh:

- Unconstrained buyer strip: **156 900 €/MW-yr** (≈157 €/kW-yr) — the breakeven premium.
- Restart-limit cost on a representative volatile month (unconstrained month value 907 €/MW): max 3 starts loses 0.4%, max 2 starts loses **6.2%**, max 1 start loses **12.2%** of month value.
- A contract advertised at "premium 140 €/kW-yr, max 1 start/day-class"… the constraint schedule is the price: the lab shows how to read it.

`labs/tolling_agreement_structure_and_valuation/compute.py` reproduces all figures; its test asserts the strip value, the restart-cost curve, and the breakeven premium.

## Market variants

- **EU:** classic CCGT tolls (the practice sources include Humber and RWE structures) with fuel pass-through and restart clauses; battery tolls are the emerging form, cycling limits playing the role of restart limits.
- **GB:** tolls coexist with Capacity Market obligations — the toll buyer typically prices the CM revenue share explicitly.
- **US:** heat-rate tolls and virtual tolls are standard products; "tolling" of gas transport and storage follows the same option logic.
- **CN:** 共享储能 (shared energy storage) capacity leasing is structurally a storage toll: the lessee buys dispatch rights to a slice of the battery, and cycle limits/availability clauses price exactly like restart limits. For thermal, 灵活性改造 contracts and demand-response aggregations are toll-adjacent.

## Common errors

1. **Pricing the premium off intrinsic** — transfers all extrinsic value to the buyer; the seller's floor should be intrinsic + a share of extrinsic.
2. **Ignoring the constraint schedule** — two tolls at the same premium differ by tens of percent in buyer value once restart and min-run clauses are priced.
3. **Double-counting fuel risk** — if fuel is pass-through, the buyer owns the spread, not the power price; valuing the toll as a power call double-counts.
4. **Misallocating availability risk** — an "as-available" toll is worth materially less to the buyer than a guaranteed-availability one; the difference is an insurance premium.
5. **Treating the breakeven premium as the fair premium** — the buyer needs a margin above breakeven for risk and capital; the seller needs coverage above fixed cost; the deal zone is between them.

## Assessable questions

1. Write the buyer-value formula for a toll and identify every term.
2. Why does the restart-limit cost curve bend (cheap first starts, expensive binding ones)?
3. A seller offers a toll at premium = intrinsic value. What is the seller implicitly giving away, and how would you bound it?
4. Structurally, why is 共享储能 capacity leasing a toll? Which clause plays the role of the restart limit?
5. For the worked example, what premium range leaves both sides better off if the seller's fixed O&M is 60 €/kW-yr?
