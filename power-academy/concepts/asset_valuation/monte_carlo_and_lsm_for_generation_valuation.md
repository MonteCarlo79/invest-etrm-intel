---
id: monte_carlo_and_lsm_for_generation_valuation
track: asset_valuation
level: advanced
prerequisites:
- real_options_framework_for_generation_assets
- stochastic_price_processes_for_power_and_fuel
markets:
- EU
- GB
- US
status: drafted
sources:
- id: plant_hedging_and_trading_strategies_kyos_20110530
  use: derivation_reference
- id: clewlow
  use: derivation_reference
- id: framework_tseng
  use: background
originality: synthesized
translations:
  zh:
    status: drafted
    en_hash: dd58234f4513e550
---
## Learning objectives

- Explain why path dependence (start costs, min up/down, ramps) breaks closed-form pricing and forces numerical methods.
- Value a plant by Monte Carlo: simulate price paths, dispatch along each, average the cashflows.
- Apply Longstaff–Schwartz (LSM) regression to estimate continuation values when the dispatch decision is American-style (start/stop under uncertainty).
- State the direction of the biases in perfect-foresight, greedy, and LSM valuations, and bracket the true value with them.

## Intuition

Margrabe prices one hour's option in isolation. A real plant's options are chained: starting today commits you to hours of running (min up time), and whether that start was wise depends on what prices do *after* you are locked in. Once the decision at hour $t$ depends on the state created by decisions at $t-1$, there is no closed form — you must simulate.

The naive simulation answer is wrong in a subtle way. Dispatching each simulated path *with full knowledge of that path* (perfect foresight) overstates value: the operator must decide under uncertainty, not with tomorrow's prices in hand. The correct question at every point is "what is the expected value of continuing, given what I know now?" — a conditional expectation. Longstaff–Schwartz estimates it by regressing next-step values across many paths on the current state (spread level, hours online), turning an impossible conditional expectation into a small least-squares problem per time step. The result is a dispatch *policy* that uses only current information and captures most of the flexibility value.

## Formal treatment

**Setup.** Simulate $N$ spread paths $S^{(i)}_t$, $t = 1..H$, from the price process (OU+jumps from the earlier concept). Plant state $u_t \in \{0, 1, \dots, U\}$ = consecutive hours online (capped at min-up $U$); actions: if offline, start (pay $c_{start}$) or stay off; if online and $u_t \ge U$, may stop.

**Perfect foresight (upper bound).** Along each realised path, solve the small backward DP exactly — this is the value of dispatching with tomorrow known. No real operator achieves it.

**Greedy (lower bound).** Start whenever the spread is positive, stop when negative and allowed; simple, constraint-feasible, and systematically below optimal because it ignores the value of waiting and of staying on through small losses to avoid a restart.

**LSM (the practical estimate).** Backward induction over $t = H-1 \dots 1$. At each step, for every path, record the state $(S_t, u_t)$ and the realised next-step optimal value $Y_{t+1}$. Regress $Y_{t+1}$ on a polynomial basis of the state (e.g. $1, S, S^2, u, u^2$) *across paths* — the fitted value is the estimated continuation value $\hat C(S_t, u_t)$. The decision rule: take the action whose immediate payoff plus $\hat C$ is larger. Valuation then replays the policy **forward on fresh paths** (out-of-sample — reusing training paths inflates the estimate).

**Bias bracket:** greedy ≤ LSM policy ≤ true value ≤ perfect foresight. LSM itself is biased *downward* (any suboptimal rule earns less than optimal), which is why the forward replay number is the honest one.

## Worked example

Per-unit plant: min up $U = 3$h, start cost 40 €, horizon 12h, 2 000 OU spread paths (mean 30 €/MWh, seed 5).

- Perfect-foresight DP mean value: the upper bound.
- Greedy mean: the lower bound.
- LSM with basis $[1, S, S^2]$ trained on 1 000 paths, replayed on 1 000 fresh paths: 62.3 € vs greedy 50.7 € and foresight 73.1 € — LSM recovers 85% of the upper bound, strictly above greedy, and strictly above the static intrinsic (run-when-forward-positive, no reoptimisation).

`labs/monte_carlo_and_lsm_for_generation_valuation/compute.py` runs the full bracket; its test asserts the ordering and that LSM recovers at least 85% of the perfect-foresight value on this toy.

## Market variants

- **EU/GB:** intraday shape and negative prices make the spread process two-regime; LSM basis should include hour-of-day dummies or run separate regressions per block.
- **US:** gas-basis risk means the *spread* (not power) should be simulated as the state variable; ERCOT scarcity adders break lognormal MC and need regime-switching paths.
- **CN:** price caps bind the tail — simulate then clamp at 限价; with 15-minute granularity and mandatory 中长期 volumes, the "dispatch" decision is partly pre-sold, so model only the residual merchant share as flexible.

## Common errors

1. **Perfect foresight reported as value** — the most common and most flattering mistake; it is an upper bound, not an estimate.
2. **In-sample LSM** — evaluating the policy on the same paths used to fit the regressions; always replay out-of-sample.
3. **Basis too rich** — high-order polynomials overfit the noise of $N$ paths and destroy the policy; start with 4–6 basis functions.
4. **Ignoring the state** — regressing continuation value on price alone when hours-online (or SoC for storage) drives the feasible set.
5. **MC error vs model error** — quoting 0.5% standard errors while the price process itself is wrong; the second error dwarfs the first.

## Assessable questions

1. Why does path dependence rule out Margrabe-style closed forms even for a simple min-up constraint?
2. Order greedy, LSM policy, true value, and perfect foresight, and justify each inequality.
3. In LSM, what quantity does the cross-path regression estimate, and why across paths rather than along one path?
4. Your LSM value exceeds your perfect-foresight value. What must be wrong?
5. Name two state variables you would add to the basis for (a) a coal plant, (b) a battery.
