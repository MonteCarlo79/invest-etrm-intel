---
id: gradual_linear_and_benchmark_hedging_strategies
track: hedging_trading
level: intermediate
prerequisites:
- delta_sensitivity_and_hedge_volume
- profit_at_risk_and_hedging_objective
markets:
- EU
- GB
status: drafted
sources:
- id: energy_risk_2012___kyos_power_plant_hedging_strategies
  use: derivation_reference
- id: plant_hedging_and_trading_strategies_kyos_20110530
  use: derivation_reference
- id: powerhedging
  use: background
originality: synthesized
translations:
  zh:
    status: drafted
    en_hash: aca1e514ad832150
---
## Learning objectives

- Define the gradual linear strategy: sell expected production in equal tranches spread over the time to delivery.
- Define the benchmark strategy: hold a target hedge ratio per period and rebalance to it.
- Compare the two on PaR reduction, margin cost, liquidity use, and behaviour in trending vs mean-reverting prices.
- Explain why the choice between them is a statement about which risk you fear most: price level at delivery vs price path along the way.

## Intuition

You have next year's expected production and a risk tolerance. Two honest ways to get there:

**Gradual linear** treats time as the diversifier. If you sell the whole year's volume today, you are fully exposed to today's price being a bad one; if you sell everything the day before delivery, you took the whole year's price risk. So you sell a small tranche every week — 1/52nd of the year's volume per week. The price you achieve is the *average* of the year's weekly prices, by construction. You are not forecasting; you are averaging, and averaging is what kills variance when you have no edge.

**Benchmark** treats the hedge ratio as the control variable. You set a target — say 70% hedged at all times — and rebalance to it as the curve and the delta ladder move. You keep a constant fraction of the risk off at every point, which bounds how bad any single price move can hurt, but you keep paying (or collecting) the forward-spot basis each time you rebalance.

The difference is a choice of what to smooth: linear smooths the *path* (entry prices), benchmark smooths the *exposure* (how much is open at any moment). In a trending market linear averages into the trend (selling rising prices too early); in a whipsaw market benchmark rebalances expensively. Neither dominates; the lab shows the comparison on a synthetic year and the framing for choosing.

## Formal treatment

**Linear.** Expected volume $Q$ per delivery month, $T$ sale points before delivery. Sell $Q/T$ at each, entry prices $F_1, \dots, F_T$. Delivery earnings per MWh: $\bar F - K$ with $\bar F = \frac{1}{T}\sum F_j$. Variance of $\bar F$ is roughly $\sigma^2/T$ for (near-)independent entry prices — the variance reduction is mechanical, ~$1/\sqrt{T}$.

**Benchmark.** Target ratio $h^*$; at each rebalance date sell or buy back to $h^* \times$ current delta volume. Delivery earnings: $h^*$ locked at each rebalance's forward + $(1 - h^*)$ floating at spot.

**Evaluation.** Same metric for both: distribution of annual earnings, PaR95 vs the unhedged benchmark, and expected margin. Report the exchange rate ΔPaR/Δmargin for each strategy.

**Behaviour by regime.** Trending prices: linear's average lags the trend (sells too early in a rally — opportunity cost, not loss); mean-reverting: linear is close to optimal per unit effort; high rebalance costs: benchmark degrades; thin liquidity far from delivery: linear naturally spreads its tickets, benchmark must too or it pays for immediacy.

## Worked example

Synthetic year, 12 delivery months (seed 17): expected volume 100 MWh/month at $K = 50$ €/MWh, monthly power price OU (θ = 55, κ = 0.3, σ = 6). 2 000 simulated years.

- Unhedged: **PaR95 ≈ 9 313 €**.
- Linear (12 tranches, one per sale month): **PaR95 ≈ 7 241 €** (−22%).
- Benchmark 70% (sold one month ahead): **PaR95 ≈ 9 007 €** (−3%).
- Under mean reversion with no trend, linear wins clearly: entry-price averaging attacks the variance directly, while the 70% benchmark both leaves 30% floating and — one month out — buys little variance reduction per unit of volume. In a trending market the ranking can flip (linear averages into the trend), and where far-dated liquidity is thin or expensive, a benchmark that waits beats a linear schedule that pays for immediacy. The comparison is a regime statement, not a league table.

`labs/gradual_linear_and_benchmark_hedging_strategies/compute.py` runs the comparison; its test asserts the orderings and reports the numbers used below.

## Market variants

- **EU/GB:** gradual linear is the documented KYOS benchmark implementation for asset-backed portfolios far from delivery; closer in, the desk switches to delta-driven (FD) hedging as the ladder becomes reliable.
- **US:** same logic with PJM/ERCOT monthly strips; liquidity further out is thinner, so tranche sizing also spreads market impact.
- **CN:** 中长期 placement has a calendar of its own (年度/月度/月内 windows) — the "tranche schedule" is partly set by the exchange calendar, and the benchmark ratio reads as the 签约率 (contract coverage ratio) that many provinces effectively mandate in bands.

## Common errors

1. **Judging linear by hindsight** — in a rally it always "loses" money vs unhedged; the strategy buys variance reduction, not outperformance.
2. **Benchmark without rebalance rules** — a target ratio with no trigger discipline drifts to whatever the market does.
3. **Comparing on margin only** — both strategies must be compared on PaR vs margin, or the comparison is rigged.
4. **Tranche too large late** — a "linear" schedule that back-loads the last 40% into the delivery month is not linear and re-inherits the path risk.
5. **Ignoring liquidity and impact** — the model price is not the fill price; far-dated tranches move the market least, which linear exploits and big benchmark rebalances don't.

## Assessable questions

1. Write the delivery earnings formula for the linear strategy and show where the $1/\sqrt{T}$ variance reduction comes from.
2. What exactly does the benchmark strategy hold constant, and what does it give up in exchange?
3. In a steadily rising market, which strategy "underperforms" and why is that the wrong word?
4. Why does a mean-reverting price process favour the linear strategy?
5. Your 签约率 mandate is 80% by year-start. Which elements of a benchmark strategy survive inside that constraint, and which must change?
