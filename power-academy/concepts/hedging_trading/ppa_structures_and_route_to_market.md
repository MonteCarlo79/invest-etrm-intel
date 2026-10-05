---
id: ppa_structures_and_route_to_market
track: hedging_trading
level: intermediate
prerequisites:
- forward_curve_structure_and_products
- power_plant_economics_and_dispatch
markets:
- EU
- GB
status: drafted
sources:
- id: plant_hedging_and_trading_strategies_kyos_20110530
  use: background
- id: power_limejump
  use: practice_example
- id: power_offtake
  use: practice_example
originality: synthesized
translations:
  zh:
    status: none
    en_hash: null
---
## Learning objectives

- Name the main PPA structures (pay-as-produced, baseload, shaped/firm) and what each one transfers between buyer and seller.
- Compute the cashflows of a renewables PPA under merchant, PaP, and baseload routes for the same year.
- Explain capture rate and cannibalisation: why renewables sell below the average price, and why that number belongs in every PPA negotiation.
- Quantify the imbalance cost of a baseload PPA and identify whose problem it becomes.

## Intuition

A renewables plant is the opposite of the flexible plants in this track: it doesn't choose when to run — the weather chooses. That changes what a PPA can sell. A **pay-as-produced (PaP)** PPA sells whatever comes out, when it comes out: volume risk stays with the buyer (or the market), the seller gets a fixed price per MWh actually produced. A **baseload PPA** sells a flat profile: the seller promises the same volume every hour and must settle the difference between its ragged production and the flat promise at spot — the *imbalance* — which is a real, measurable cost with a sign.

The number that makes all of this concrete is the **capture rate**: the volume-weighted price your generation actually earns divided by the plain average price. Renewables fleets produce most when everyone produces most — sunny windy hours — which is exactly when prices sag (cannibalisation). Capture rates below 1 are structural, and they mean a PaP strike at "the expected price" overpays the average hour and underpays your generation's true market value. Every PPA negotiation is, underneath, an argument about capture rate and who carries the imbalance.

## Formal treatment

**Routes to market.** For hourly generation $q_t$ (MWh), price $p_t$, strike $S$:

- **Merchant:** $R_{m} = \sum q_t p_t$ — full price and volume risk.
- **PaP:** $R_{pap} = S \sum q_t$ — no price risk for the seller, volume risk irrelevant to the strike (paid per actual MWh).
- **Baseload:** $R_{bl} = \bar q S \cdot H + \sum (q_t - \bar q)\, p_t$ — flat volume $\bar q$ at strike, imbalance $(q_t - \bar q)$ settled at spot.

**Imbalance cost.** $R_{bl} - R_m = \bar q (S - \bar p_{avg}) H + \text{cov adjustment}$… more usefully, relative to PaP: $R_{bl} - R_{pap} = \sum (q_t - \bar q)(p_t - S)$ — the cost is positive (a cost to the seller) when production is *below* the flat volume in expensive hours and *above* it in cheap hours, which is the renewables norm: cannibalisation makes generation and price anticorrelated.

**Capture rate.** $CR = \frac{\sum q_t p_t}{\sum q_t} \Big/ \frac{\sum p_t}{H}$ — the ratio of volume-weighted to time-weighted price. Structural estimates: solar ~0.8–0.95 in solar-heavy systems, wind ~0.9–1.0, falling as penetration rises.

## Worked example

100 MW wind, synthetic year (seed 31): 393 600 MWh generated, cannibalisation built in (high-wind hours discounted up to 25 €/MWh).

- Merchant: 21.02 M€ (capture rate 0.98 — below 1 despite this being a mild synthetic).
- PaP at 52 €/MWh: 20.47 M€.
- Baseload at 52 (flat 44.9 MW): 20.02 M€ — **1.0 M€ below merchant**: the imbalance settled against the anticorrelated profile. The flat volume sells at strike in hours when the spot is high and the plant isn't producing; the surplus hours sell back cheap.

`labs/ppa_structures_and_route_to_market/compute.py` reproduces every figure; its test asserts PaP's price-independence, the capture rate, and the imbalance identity.

## Market variants

- **EU:** corporate PaP PPAs dominate; baseload PPAs for wind carry explicit imbalance pricing; negative-price hours are excluded from strikes in newer contracts (a capture-rate protection clause).
- **GB:** CFDs are the state-backed PaP-with-two-way-settlement structure — the strike settles against a market reference price, so the capture-rate risk sits with the generator unless the reference is production-weighted.
- **CN:** 绿电/绿证 contracts are PaP-like with certificate value stacked on the energy price; 现货 exposure for renewables is settling into exactly the merchant/PaP/baseload choices above, and 机制电价 (mechanism price) pilot provinces are effectively CFD structures with provincial capture-rate debates attached.

## Common errors

1. **Pricing PaP at the average price** — with capture < 1, the strike at the plain average overpays; price at the capture-weighted expectation.
2. **Ignoring the imbalance sign** — baseload isn't "PaP plus certainty"; it's PaP plus a cost that has a systematic sign for anticorrelated generation.
3. **Treating volume risk as price risk** — PaP removes price risk, not volume risk; annual generation still swings with the weather year.
4. **Comparing strikes without the reference** — a strike is only meaningful with its settlement reference (production-weighted vs time-weighted index).
5. **Forgetting cannibalisation grows** — today's capture rate is not the fleet's at 40% penetration; PPA tenor is a capture-rate bet.

## Assessable questions

1. Write the three route-to-market cashflow formulas and identify the risk each leaves with the seller.
2. Why is the baseload imbalance cost systematically negative for anticorrelated generation?
3. Compute the capture rate from the worked example and explain why it is below 1.
4. A buyer offers a baseload strike equal to the expected average price. Using capture-rate logic, show who wins.
5. How does a CFD with a time-weighted reference price differ from a PaP PPA in what happens to the generator in cannibalised hours?
