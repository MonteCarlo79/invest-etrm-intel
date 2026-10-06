---
id: hedging_under_incomplete_markets_and_spike_dynamics
track: hedging_trading
level: advanced
prerequisites:
- baseload_peak_offpeak_delta_decomposition
- static_vs_dynamic_hedging
markets:
- EU
- GB
- US
status: drafted
sources:
- id: pareto_spikes
  use: background
- id: dipeng_models
  use: practice_example
originality: synthesized
translations:
  zh:
    status: drafted
    en_hash: 548f1aecdf2e3f72
---
## Learning objectives

- Frame market incompleteness concretely: the product you need (peak, weekend, a specific block) doesn't trade — what do you do?
- Build the optimal proxy hedge as a regression of plant P&L on the available product, and quantify the residual basis.
- Show the residual basis grows with the *basis volatility* between your exposure and the proxy — not with price volatility itself.
- Explain how spike dynamics worsen incompleteness (gaps can't be proxied) and what instruments genuinely complete the market.

## Intuition

Every hedge so far assumed the right product exists: you need a peak product, there is one. Real markets are missing most products. If your exposure is a peaker concentrated in peak hours and only baseload trades, you must **proxy**: hedge with the wrong-but-correlated instrument. The craft is doing this with your eyes open — sizing the proxy by regression (how much of my P&L co-moves with the base product?), and measuring what is left behind (the residual basis).

Two results organise the practice. First, the proxy still works: a base-only hedge cuts PaR substantially because most of your exposure co-moves with the base product. Second, the residual is *basis-volatility* driven, not price-volatility driven: it grows exactly when peak and base prices move *differently* (peak-basis moves — scarcity hours, solar dumps hitting midday specifically). And spikes are the extreme of basis: a jump in your running hours that the base product barely registers is unhedgeable by construction — no linear proxy covers a gap it doesn't take part in.

## Formal treatment

**Optimal proxy.** With plant P&L $u$ and the available product's settle price $s_b$, the minimum-variance single-product hedge is the regression coefficient $\beta = \text{cov}(u, s_b)/\text{var}(s_b)$. The residual $\epsilon = u - \beta s_b$ is, by construction, the part of your P&L uncorrelated with the proxy — the number that matters.

**Basis decomposition.** Write your exposure's price driver as level + basis: $s_{exposure} = s_b + b$ where $b$ is the basis (peak-minus-base). Then

$$\text{var}(\epsilon) = \text{var}(u - \beta s_b) \;\propto\; \text{var}(b) \quad \text{(holding level risk hedged)}$$

— the residual grows with **basis volatility**, not with price level volatility. In the lab, adding a peak-only price component with sd 0 → 4 → 8 €/MWh leaves the level hedge untouched but inflates the single-product residual accordingly.

**Two-product benchmark.** Regressing on both base and peak recovers most of the basis component — the gap between single- and two-product residuals is the *price of incompleteness*, measurable in euros per year.

**Spikes.** Spike hours are basis events with extreme size and no duration: a 10× price move in 6 peak hours moves the base average by a fraction of it. Linear proxies under-cover them by the ratio of block sizes, and rebalancing can't help (gaps, from the previous concept). Completing the market for spikes means options or spike-specific products — or, physically, owning flexible capacity, which is why the option value work in the valuation track prices precisely this incompleteness.

## Worked example

Peaker ($K = 60$) on the synthetic month with a level component (sd 8) and an added peak-only component (seed 29):

- Optimal single-product (base) hedge: residual std 1 772 € — vs 4 105 unhedged; PaR95 3 998 → **2 596** (−35%). The proxy works.
- Two-product (base + peak) hedge: residual std 1 648; PaR95 **1 629**. The missing peak product costs ≈ 967 €/year of PaR — the measurable price of incompleteness.
- Scaling the peak-only component 0 → 4 → 8 €/MWh: single-product residual 1 230 → 1 772 → 2 899 €. Price vol didn't change the story; *basis* vol did.

`labs/hedging_under_incomplete_markets_and_spike_dynamics/compute.py` reproduces every figure; its test asserts the orderings and the basis-vol scaling.

## Market variants

- **EU:** German peak products are liquid a year out, thin beyond; weekend and special-block products often don't exist at tenor — proxy with base+peak regression hedges and measure the residual monthly.
- **GB:** EFA-block products help (they add a third instrument); the 15-minute settlement still leaves intraday shape as residual basis.
- **US:** hub-to-node basis is the dominant incompleteness — hedging a plant at its node with hub futures leaves locational basis exactly as this concept describes.
- **CN:** most provinces have no peak/block forwards at all — the available "product" is the 中长期 monthly/quarterly contract, so nearly everything is a proxy hedge; the residual basis includes the 现货-中长期 deviation and 偏差考核, and the genuine completion (futures, options, capacity instruments) is this curriculum's derivatives argument.

## Common errors

1. **Refusing to hedge because the product is missing** — the proxy is measurably better than nothing; unhedged is the worst residual of all.
2. **Sizing the proxy 1:1 by volume** — the right size is the regression beta, which is not one when block compositions differ.
3. **Blaming price vol for the residual** — the residual tracks basis vol; a calm year with wild peak/base basis is worse than a volatile year with stable basis.
4. **Proxying spike risk with linear products** — gaps can't be proxied; buy options or own the flexibility.
5. **Never re-measuring the beta** — block compositions, outages, and seasons change the covariance; the proxy ratio is a live number.

## Assessable questions

1. Write the optimal single-product proxy ratio and the residual it leaves, and state what drives the residual's size.
2. Why does peak-only price volatility inflate the base-product residual but not the two-product one (as much)?
3. In the worked example, what is the annual PaR cost of the missing peak product?
4. Why can't a baseload forward hedge a spike in your running hours, regardless of rebalance frequency?
5. A CN province offers only monthly 中长期 contracts. Describe your proxy-hedge construction and name two components of its residual basis.
