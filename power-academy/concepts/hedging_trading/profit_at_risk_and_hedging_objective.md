---
id: profit_at_risk_and_hedging_objective
track: hedging_trading
level: foundation
prerequisites:
- power_plant_economics_and_dispatch
markets:
- EU
- GB
- US
status: drafted
sources:
- id: 0109_electricity_price_modelling_for_profit_at_risk_manageme
  use: derivation_reference
- id: plant_hedging_and_trading_strategies_kyos_20110530
  use: background
- id: dipeng_credit_risk_model
  use: practice_example
originality: synthesized
translations:
  zh:
    status: none
    en_hash: null
---
## Learning objectives

- Define Profit-at-Risk (PaR): the shortfall of realised earnings below expectation at a chosen quantile, over a chosen horizon.
- Estimate PaR from a simulated earnings distribution and state the convention precisely (mean minus quantile).
- Explain why PaR — not variance, not price VaR — is the natural hedging objective for an asset-backed portfolio.
- Frame the hedging problem as trading expected margin for tail protection, and measure the trade per unit.

## Intuition

Every hedging discussion eventually hits the question: "how bad can it get?" Variance answers weakly (it counts good and bad surprises alike), and price VaR answers the wrong question (we don't hold prices, we hold a business). **Profit-at-Risk** answers the right one: over the coming year, earnings will fall below expectation by more than $X$ only in the worst $\alpha\%$ of scenarios. It is a number the board can hold you to.

PaR also makes the hedging trade-off honest. Hedging is not free: selling forward or buying caps gives up expected margin (or pays premium) in exchange for a thinner left tail. The objective is never "minimise PaR" — that over-hedges — it is "reduce PaR to tolerance at the lowest expected-margin cost", i.e. maximise PaR reduction per euro of margin given up. Every strategy in this track (linear, benchmark, options) is just a different exchange rate on that trade.

## Formal treatment

**Definition.** Let $X$ be earnings over the horizon (e.g. the year) and $q_\alpha(X)$ its $\alpha$-quantile. Then

$$\text{PaR}_\alpha = \mathbb{E}[X] - q_\alpha(X)$$

the distance from expectation to the bad tail, in euros. (Convention matters: some shops quote the quantile itself, others the shortfall; state yours. This curriculum uses the shortfall.)

**Estimation.** Simulate $N$ earnings paths from the price/dispatch model (exactly the machinery from the valuation track); the estimate is the empirical quantile: sort the $N$ outcomes, read off the $\alpha N$-th. No distribution assumption needed — which is the point, since power earnings are fat-tailed and skewed.

**Properties and caveats.** Quantiles are not subadditive (PaR of a portfolio can exceed the sum of the parts in odd cases); the coherent alternative is expected shortfall ($\mathbb{E}[X \mid X \le q_\alpha]$), same idea averaged over the tail. For reporting, always pair PaR with expected margin — a PaR number without the expectation it is measured from is meaningless.

**Hedge objective.** Choose hedge $h$ (volumes, products, timing) to minimise expected-margin cost subject to $\text{PaR}_\alpha(h) \le$ tolerance. Equivalently: rank candidate hedges by $\Delta\text{PaR} / \Delta\text{margin}$ and buy the cheapest tail reduction first.

## Worked example

Simulated annual earnings of a merchant plant portfolio, 10 000 paths (seed 13): monthly margins normal with mean 30 k€ and std 18 k€ plus a 5%-chance stress month at −40 k€.

- First note the expectation: the naive $12 \times 30 = 360$ k€ is wrong — stress months cost $12 \times 0.05 \times 70 = 42$ k€, so $\mathbb{E}[\text{margin}] \approx 318$ k€ (true monthly mean 26.5, not 30).
- Unhedged: $\mathbb{E} \approx 318$ k€; $q_{5\%} \approx 180$; **PaR₉₅ ≈ 138 k€**.
- Hedge: sell 60% of expected volume forward at 28 k€/month-equivalent. Hedged: $q_{5\%} \approx 274$, **PaR₉₅ ≈ 55 k€** — and expected earnings *rise* slightly (≈ 329 k€), because the forward at 28 is priced above the true expected margin of 26.5. Exchange rate: 83 k€ of PaR removed for *negative* margin cost.
- The lesson is not "hedging is free" — it is that hedges transact at the forward price, not at your expected spot; the exchange rate must be computed with the true expectation, stress months included.

`labs/profit_at_risk_and_hedging_objective/compute.py` runs the simulation; its test asserts each figure.

## Market variants

- **EU/GB:** PaR (or EaR, Earnings-at-Risk) is the standard utility risk metric; typical reporting: monthly PaR95 per portfolio with a hedge-ratio corridor derived from it.
- **US:** more often framed as Gross-Margin-at-Risk; same construction.
- **CN:** for a 售电公司 the earnings distribution has a distinctive extra term — 偏差考核 (deviation penalties) — which is discrete and rule-based; PaR over 售电毛利 must include the penalty distribution, and the hedge is 中长期 volume placement rather than exchange forwards.

## Common errors

1. **Quoting VaR on prices instead of PaR on earnings** — the portfolio is not the price; volume-price covariance belongs inside the earnings simulation.
2. **Gaussian PaR** — computing the quantile from mean and std; the whole point is that earnings tails are fat and asymmetric.
3. **PaR without the expectation** — the shortfall convention needs the mean; report "E[X] = 380, PaR95 = 85", not just "85".
4. **Minimising PaR** — full hedging minimises PaR and margin together; the objective is the exchange rate, not the extreme.
5. **Annual PaR for monthly decisions** — horizon mismatch hides intra-year tail events; compute both.

## Assessable questions

1. Define PaR₉₅ in one equation and state the convention.
2. Why is variance a poor risk objective for a hedging decision?
3. A portfolio has E[X] = 500 k€ and q₅% = 420 k€. What is PaR₉₅? What changes if the shop quotes the quantile convention instead?
4. Why is expected shortfall called coherent while the quantile shortfall is not?
5. Your hedge reduces PaR₉₅ by 40 k€ and costs 10 k€ of expected margin. A second hedge reduces PaR by 50 k€ at a cost of 18 k€. Which do you take under the exchange-rate rule?
