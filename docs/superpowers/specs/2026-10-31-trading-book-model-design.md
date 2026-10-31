# Trading Book Object Model — Design Spec

**Date:** 2026-10-31
**Status:** Design approved in dialogue (5 decisions locked) + user review 2026-10-31: **scope = 蒙西 asset books only**
**Apps touched:** mengxi-dashboard (Trader, Pillar 3) primary; deal-structurer (DP engine reuse)

---

## Goal

Model each trading identity as an object — a **TradingBook** — that owns the full chain from forecast to settled cash, so that every P&L number decomposes causally: *what we forecast → what we planned → what cleared → what physically happened → what we were paid*, with each inter-layer delta named and measured.

**Scope (user review 2026-10-31): AssetBook for the 蒙西 (IM) BESS stations only.** No other asset types, no RetailBook, for now. The interface is still designed family-neutral so a RetailBook can attach later without rework — but nothing retail is built.

## Locked decisions

| # | Decision | Choice |
|---|----------|--------|
| 1 | Book scope | Per-station asset book only （蒙西）; retail books and other asset types out of scope (user review 2026-10-31) |
| 2 | Strategy anchor | **Both** — `nodal_agent` (nominated) and `dp_optimal` (option-engine DP) as competing Strategy objects; one marked primary; DP-vs-nominated spread = explicit "strategy selection cost" attribution line |
| 3 | Settlement source | **Trust the bill's daily breakdown** where the settlement document carries one; monthly-only lumps accrue pro-rata, flagged accrual-grade |
| 4 | Fleet response model | **Aggregate stack** — provincial price-response curves estimated from history, bounded by per-class optimization floors (coal ≥ variable cost, BESS ≥ negative spread) |
| 5 | Forecast horizon | **D-1 16:00 CST artifact** first (price + fleet response + net load for tomorrow's 96 intervals); intraday re-forecast deferred |

## Object model

```
TradingBook (interface — family-neutral so RetailBook can attach later)
│   book_id, market, timezone='Asia/Shanghai'
│   .forecast(d) → ForecastSet         # D-1 16:00 artifact
│   .strategies(d) → list[Strategy]    # tagged by scope; one .primary
│   .cleared(d) → ClearedDispatch
│   .actual(d) → ActualDispatch
│   .settlement(period) → Settlement
│   .attribution(period) → Waterfall
│
└── AssetBook(station: StationRef)     # only family built in v1
      ForecastSet:  NodalPriceForecast (fundamentals + history)
                    FleetResponseForecast (aggregate stack, opt-bounded)
                    NetLoadForecast
      Strategy:     scope='nodal_agent' | 'dp_optimal' | …
      Chain:        cleared (md_id_cleared_energy)
                    → actual (ops_bess_dispatch_15min)
                    → settled (rm_settlements daily breakdown)
```

## The attribution waterfall (AssetBook v1)

```
strategy P&L (primary strategy against actual prices)
  − forecast error delta        (forecast price vs actual price, strategy held fixed)
  − strategy-selection delta    (primary vs dp_optimal spread)
  − clearing delta              (nominated vs cleared volumes)
  − curtailment / SO delta      (cleared vs actual, system-operator adjustments)
  − equipment / site delta      (actual vs nameplate capability: derates, outages)
  − settlement residual         (computed vs invoiced; bill daily breakdown trusted)
= settled P&L (rm_settlements)
```

## Data sources (all existing)

| Layer | Table(s) |
|---|---|
| Nodal prices (CST-naive frame) | `marketdata.md_mengxi_nodal_price_96_filled` (v5, correct frame since 2026-10-30) |
| Fundamentals | `marketdata.spot_fundamentals_hourly` |
| Cleared dispatch | `marketdata.md_id_cleared_energy` |
| Actual dispatch | `ops_bess_dispatch_15min` (ops ingestion, 4 IM assets) |
| Strategies | `nodal_strategy_daily`, `strategy_experiments` |
| Settlement | `marketdata.rm_settlements`, `rm_settlement_items` (bill parsers per book) |
| PF/economics | `reports.nodal_pf_node_daily` |

## New components to build

1. **`libs/trading_book/`** — the object model (TradingBook interface, AssetBook, Waterfall). Thin: orchestration over existing tables, no new pipelines. RetailBook out of scope.
2. **Fleet-response estimator** — `services/mengxi_nodal/fleet_response.py`: estimate provincial price→response curves from history (observed cleared dispatch vs price), clipped by optimization floors computed per asset class (coal variable-cost floor, BESS spread floor via the existing DP engine).
3. **D-1 forecast job** — daily 16:00 CST: fundamentals forecast → nodal price forecast (per station node) → FleetResponseForecast → persisted as the day's ForecastSet artifact.
4. **Settlement bridge** — bill daily-breakdown extraction into per-book daily settled rows; monthly lumps accrue pro-rata with `accrual_grade` flag.
5. **Trader UI surface** — mengxi-dashboard: per-station book view (forecast vs strategy vs cleared vs actual vs settled, waterfall chart).

## Hard invariants (lessons burned in 2026-10-30)

- **All wall-clock rendering uses named zone `Asia/Shanghai`** — never the interval literal `'+08:00'` (POSIX-inverted: renders UTC−8). Applies to every SQL rendering expression, view, and fill.
- The filled table's `metric_time`/`time_order_96` are **CST-naive walls** (v5 frame). Readers window with naive CST literals (no `+08` suffix); only genuinely-timestamptz columns keep `+08` literals.
- Strategy objects carry `scope`; attribution always names which scope is primary.
- Bills with daily breakdown are authoritative for the settled layer; recomputation from intervals is a cross-check, never the source.

## Phasing

- **Phase 1 (D-1 artifact):** object model + AssetBook for the 4 IM stations + settlement bridge + Trader book view. Forecast v1 = existing capture-pipeline models extended to nodal; fleet response v1 = historical stack.
- **Phase 2:** DP-optimal strategy wired as daily comparison scope; intraday re-forecast loop.
- **Future (not scoped):** RetailBook family; other asset types (wind/solar stations); coal unit registry if per-unit fleet granularity is ever justified.

## Open follow-ups (flagged, not in scope)

- hermes `mengxi_ranking_report` reads the same price view with its own windowing — needs the frame review (warned "no price data for 2026-10-28").
- `nodal_pf_daily` 蒙西 window fix is committed (`5f3c1f59`) but the daily rule runs the old image until the next mengxi-dashboard deploy — deploy before 23:45 UTC or the rule rewrites recomputed days with the old frame.
- ix_snp96_mengxi_cst_ts (inverted-expression index) is now dead weight; drop later.
