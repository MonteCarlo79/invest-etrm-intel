# Nodal Price Forecast & Per-Asset Trading Agents — Design Spec

**Date:** 2026-09-22
**Status:** Pending user review
**Pillars:** P3 Trader (mengxi-dashboard, home) + P2 Quant (bess-map, grid-wide forecast, parallel session)
**Decisions locked (user, 2026-09-22):** DB contract between sessions (no code coupling); asset registry auto-extracted from md_* then user-reviewed; coal fleet = aggregate supply-stack model (CCGT_Valuation.py is a 2012 stub, nothing reusable).

---

## 1. Goal

For each of ~40 Mengxi BESS assets: a trading agent that produces a **next-day nominated dispatch strategy** from (a) a nodal price forecast for its node, (b) forecast of other assets' behavior (BESS + coal fleet + renewables at the same substation), under (c) substation output caps and traffic-light restriction windows. Strategies feed Envision's second-round optimisation (more realistic dispatch) and are continuously improved via the existing strategy promote loop. Optimal strategies are compared against traders' asset-risk strategies (which carry green/red/orange zone constraints).

## 2. Architecture — three layers, DB as the only contract

```
L1  GRID-WIDE (parallel session, bess-map, feat/price-forecasting)
    merit-order stack (province_fuel_fleet + interconnector import blocks)
    + PCA shapes + hybrid forecast + backtest
    → marketdata.nodal_fc_grid_daily  (NEW table, written by L1's runner)
        (province, fc_date, target_date, price_hat, model_version)

L2  NODAL (this project: services/nodal_forecast/)
    per node/cluster: nodal_price_hat = f(grid_price_hat, local bidding space,
        substation renewable forecast, substation capacity headroom, congestion regime)

L3  ASSET AGENTS (this project: services/nodal_agents/ + mengxi-dashboard "Nodal Trading" tab)
    per asset: L2 curve + asset constraints (capacity, SOC model, substation cap,
        traffic-light zones) + same-zone BESS behavior forecast + coal stack response
    → nominated charge/discharge schedule (96×15min)
    → strategy registered into services/bess_map/strategy_experiments (promote loop)
    → comparison vs asset-risk trader strategies (side-by-side P&L attribution)
```

**BESS feedback loop (recursive):** L3's aggregate BESS dispatch prediction (same-zone + grid-wide totals) is written back to L1's input layer as a bidding-space adjustment. The adjusted price forecast re-forms the nodal curve and the strategy re-optimizes — iterated until the dispatch change between iterations falls below tolerance (default: <2% of asset MWh) or max 3 iterations. **The converged strategy is the "recursive optimal dispatch".** All downstream evaluation uses the converged strategy, never the first-pass one.

**Comparison (after convergence):** the recursive-optimal strategy is benchmarked against the traders' **realized** asset-risk strategies (actual dispatch with their green/red/orange zone constraints) — side-by-side P&L attribution on actual RT prices via the existing strategy_comparison workflow: what would the recursive equilibrium have earned vs what traders actually did, and how much of the gap is zone-restriction cost vs forecast error vs strategy shape.

## 3. Data

| Source | Content | Status |
|---|---|---|
| `marketdata.province_fuel_fleet` | coal/gas prices + fleet segments per province (L1's stack input) | exists (parallel session) |
| `staging.interconnector_trades/channels` | import blocks with land prices + GW | exists (this session) |
| `marketdata.spot_fundamentals_hourly` | load/renewable/bidding space/wind/solar/net_export D-1 + actual | exists (LingFeng) |
| `marketdata.md_mengxi_nodal_price_96` | 15-min nodal RT prices per node | exists (Fengxing) |
| `md_id_cleared_energy` + ops tables | per-asset 15-min cleared energy (dispatch patterns for behavior forecast) | exists |
| `data/nodal/网架图/内蒙/2025年12月22日内蒙古电网主接线图(1).pdf` | substation capacities & connectivity | **extraction task** (vision) |
| asset-risk `restriction` patterns | traffic-light restricted windows per asset | exists in asset_risk app |
| `marketdata.nodal_fc_grid_daily` (new) | L1 grid forecast predictions | **new — L1 writes** |
| `knowledge/mengxi/substation_capacity.md` (new) | **ALL nodes/substations** in the 网架图: name, voltage, transformers, rated MVA, N-1 firm, connected plants | **new — full node map (user-reviewed); future-proof as BESS fleet grows** |
| `marketdata.nodal_node_registry` (new) | every node from the node map + price-cluster/zone association + Fengxing node_name match | **new — seeded from reviewed map** |
| `marketdata.nodal_asset_registry` (new) | dynamic BESS→node mapping: plant, node, substation, capacity_mw, zone, settle_node (~40 assets, grows) | **new — extracted from md_* + user-reviewed** |
| `marketdata.nodal_strategy_daily` (new) | per-asset daily nominated strategies (JSON 96-pt curve + metadata + model_version) | **new — L3 writes** |

## 4. L2 — Nodal price formation

Statistical per node (fallback to its price cluster when node history is thin — my earlier finding: clusters identify zones, not nodes):

- **Base signal:** L1 grid price_hat (fallback: LingFeng provincial RT if L1 table empty for a date — the tab must work before L1 lands)
- **Local features:** substation bidding-space deviation (local residual vs grid), substation renewable forecast = Σ(installed wind/solar capacity at the substation) × grid wind/solar forecast ratio, substation capacity headroom = cap − (local gen + scheduled import), congestion regime dummy (headroom < threshold)
- **Model:** per-cluster ridge regression on 90-day history; regime-specific coefficients. No PTDF/physical flow model (no network parameters available; flagged as future work if a network model arrives)
- **Backtest:** daily nodal MAE/sMAPE per cluster vs actuals (same metric style as L1's backtest)

## 5. L3 — Asset agent

Per asset, daily (target: run after L1 publishes, before 17:00 for D+1 nominations):

1. **Forecast curve:** L2 nodal 96-pt price curve for its node
2. **Constraints:** capacity_mw / duration_h / RTE / SOC window; **substation_cap_mw** (from registry) applied as a shared constraint across same-substation assets (coordinated allocation, proportional by capacity); traffic-light restricted intervals (from asset-risk's restriction history per asset, learned as recurring windows)
3. **Behavior forecast:** same-zone BESS aggregate dispatch (from md_id_cleared_energy patterns, day-type matched) + coal fleet response (aggregate stack: merit-order coal segments from fuel_fleet at forecast coal price, response ≈ historical stack utilization by residual-load level)
4. **Strategy:** deterministic LP dispatch maximizing spread capture against the forecast curve (existing `libs/decision_models/bess_dispatch_optimization.py` engine reused with nodal curve + substation cap + zone restrictions)
5. **Persist:** `nodal_strategy_daily` (curve + assumptions + model_version + **iteration count + convergence delta**)
6. **Recursive loop:** aggregate all agents' strategies + other-asset predicted dispatch → bidding-space adjustment → L1/L2 price re-form → re-optimize until convergence (<2% MWh change or 3 iterations)
7. **Evaluate:** promote loop — windowed capture-rate vs actual RT per asset (on the **converged** strategy); champion/candidate status per asset; agents self-retire losing variants
8. **Comparison (post-convergence):** recursive-optimal vs traders' **realized** asset-risk strategies → side-by-side realized P&L attribution (strategy_comparison workflow): total gap split into zone-restriction cost / forecast error / strategy shape

## 6. Node map & asset registry (largest data task)

**Principle (user, 2026-09-22): the BESS fleet is growing — record ALL nodes, not just current BESS nodes.** Two artifacts, deliberately separate:

1. **Full node map** (static-ish, updated when a new 网架图 edition arrives): vision-read `2025年12月22日内蒙古电网主接线图(1).pdf` → **every substation/node**: name, voltage level, transformer count, rated MVA, N-1 firm estimate, connected plants (wind/solar/BESS/thermal where legible). Output → `knowledge/mengxi/substation_capacity.md` → **user review gate** (like channel registry) → seed `nodal_node_registry` (with Fengxing `node_name` matching + price-cluster/zone association).
2. **Dynamic asset registry** (changes as assets are added): the ~40 BESS assets from md_* plant list → proposed node/substation mapping (接入系统报告 naming conventions + 网架图） → `nodal_asset_registry`, user-corrected. New BESS connections look up the existing node map — no re-extraction needed.

## 7. Tab — "Nodal Trading" (mengxi-dashboard)

- **S1 Forecast & stack:** L2 nodal curves per cluster vs actuals; L1 grid forecast + stack display (reuse L1's tables)
- **S2 Asset agents:** per-asset cards — nodal forecast, constraints, nominated strategy curve, zone restrictions, promote status
- **S3 Comparison:** optimal vs trader strategies P&L attribution; zone-restriction impact
- **S4 Registry & data:** asset registry editor (user corrections), substation capacity table, backtest metrics

## 8. Out of scope (v1)

- Physical network flow model (PTDF, contingencies)
- Unit commitment for coal plants
- CECI coal-price scraping (parked: public endpoint not found — needs Playwright collector; fuel prices currently from province_fuel_fleet which is manually confirmed)
- Real-time intraday re-dispatch (v1 targets D+1 nomination only)
- Auto-submission of nominations to the exchange (agents produce strategy documents; humans submit)

## 9. Testing

- services/nodal_forecast + nodal_agents: pure-function tests (price formation, regime logic, behavior forecast, LP with substation cap, zone restrictions)
- Registry extraction: sanity tests (capacity positive, node mapping complete for all md_* plants)
- Backtest harness: nodal MAE vs grid-only baseline (must not be worse — gate)
- Tab: render smoke with mocked loaders (RDS unreachable from Mac)

## 10. Sequencing (implementation plan preview)

1. `nodal_fc_grid_daily` contract table + L1 writer shim (if L1 not ready, L2 falls back to LingFeng RT)
2. Asset registry extraction (~40 assets) → user review
3. 网架图 vision extraction → substation_capacity.md → user review
4. L2 nodal price formation + backtest
5. L3 agent engine (LP with caps/zones) + daily writer
6. Promote loop integration
7. Tab sections S1–S4
8. Comparison vs trader strategies
