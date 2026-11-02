# Task 13 report — market-impact coefficient (λ) estimation + LP integration

Status: DONE. 18/18 tests pass (`tests/mengxi_nodal/test_market_impact.py`).

## What was built

1. **`services/mengxi_nodal/market_impact.py`** (new)
   - `estimate_lambda(engine, asset_code, lookback_days=90, as_of=None) -> dict` —
     resolves the station's price node (first registry candidate with filled-table
     rows in the window, own node preferred — same rule as book_forecast), pulls
     node price + cross-sectional fleet mean in one query
     (`md_mengxi_nodal_price_96_filled`, AVG over ALL nodes per metric_time — own
     node included; 1/N attenuation negligible at the 蒙西 node count, documented),
     pulls own dispatch (`ops_bess_dispatch_15min.actual_dispatch_mw`,
     positive = discharge), computes `price_dev = node − fleet`, and fits per-regime
     OLS `price_dev ~ hour dummies (saturated) + net_mw` on discharge (net>0) and
     charge (net<0) slots separately. Reported λ = −(raw coefficient), so positive
     means "discharge depresses the node price". Returns the brief's shape
     (`lambda_cny_per_mwh_per_mw` headline = discharge regime, `n_obs`, `r2`, `se`,
     `per_regime: {charge, discharge}`) plus per-regime se/r2/n detail for
     persistence. Loud skip (`lambda None` + `reason` + stdout WARNING) on: no
     registry node with data, no prices, no dispatch rows, or both regimes under
     the identification floor (`_MIN_REGIME_OBS = 30`). Module docstring carries
     the required identification caveat (reduced-form magnitude, NOT the causal
     supply-shift coefficient — reverse causality).
   - `_DDL` — `reports.station_market_impact(asset_code, estimated_at,
     lambda_discharge, lambda_charge, n_obs, r2, se_discharge, se_charge,
     lookback_days, PK(asset_code, estimated_at))`, exactly as briefed (no extra
     columns; λ/se/r2 nullable for one-regime-identified estimates).
     `ensure_table` runs `CREATE SCHEMA IF NOT EXISTS reports` on PG only.
   - `persist_lambda(engine, asset_code, lookback_days, as_of, estimated_at)` —
     upsert on PK; writes NOTHING when no regime identified (loud-skip reason in
     the returned dict; latest-read then keeps any previous estimate).
   - `get_latest_lambdas(engine) -> {asset_code: {lambda_discharge, lambda_charge,
     estimated_at}}` — PG/SQLite-portable latest-per-asset (MAX self-join, no
     DISTINCT ON); floats, None for unidentified regimes.

2. **Engine integration** (`services/bess_map/optimisation_engine.py`)
   - `compute_dispatch_from_15min_prices` / `compute_dispatch_from_hourly_prices`
     gain `impact_lambda: float | None`; `optimise_window` gains `impact_lambda`
     + `impact_blocks=8`.
   - Implementation: the clearing rule (discharge at p − λ·Q, charge at p + λ·Q)
     makes revenue quadratic — PuLP/CBC is LP-only (non-constant multiplication
     raises), so it is linearised as **K power blocks per slot** (Δ = power/K)
     whose objective coefficients are the **chord slopes p ∓ 2λ·(k+0.5)·Δ** — the
     marginal prices (factor 2 vs the average clearing price), making the LP
     objective the exact piecewise-linear interpolation of (p ∓ λ·Q)·Q at block
     edges. Slopes are monotone in k so blocks fill in order: a price adjustment
     inside the objective, no new constraints, per the brief. Zone masks bind
     every block's upBound in impact mode. Reported `profit` is the market cash
     flow at impact-adjusted clearing prices.
   - `None`/`0.0` → plain price-taker path, bit-identical; negative → ValueError.

3. **nodal_pf_daily** — 蒙西 only: `_mengxi_node_impacts(engine)` maps each BESS
   station's latest discharge-regime λ onto its registry price nodes (own +
   expanded parents, first-station-wins on shared nodes, same rule as zone
   masks); absent or **non-positive** λ (the reverse-causality sign) → node stays
   price-taker. `run_day` passes `impact_lambda=` per node and appends
   `(N impact-adjusted nodes)` to the day message. Single symmetric λ per node
   (engine signature) — discharge regime passed as the dominant revenue channel,
   documented.

## Test evidence (18 tests, no RDS)

- OLS recovery on synthetic frames with known λ: λ_d=0.8, λ_c=0.3 recovered to
  ±0.01 with R²>0.99 (12 days × 96 slots; hour dummies absorb a node-specific
  diurnal effect; fleet noise cancels in the deviation).
- Sign split: distinct per-regime λs, both positive; min-obs gate: 5 discharge
  obs → λ_d None while λ_c still identified.
- SQLite-ATTACH harness end-to-end: `estimate_lambda("suyou")` resolves the own
  node (`内蒙.景蓝乌尔图储能电站/220kV.1M`) over parents, recovers both λs
  (fleet nodes constructed so cross-sectional AVG equals the intended fleet
  series exactly); missing dispatch → loud skip with WARNING, and
  `persist_lambda` writes nothing; no-node station (wuhai) skips before any
  engine use; persist upsert idempotent on same `estimated_at`, latest-read
  ignores an older 9.9-λ row.
- `_mengxi_node_impacts`: suyou λ=0.55 maps to own + expanded parent nodes
  (incl. `内蒙.苏尼特站/220kV.2M` fragment expansion); wulate λ=−0.4 and
  never-estimated gushanliang both absent.
- Engine (HiGHS monkeypatch, T12 pattern): 8-slot [10×4, 100×4] case — baseline
  profit 180; λ=50 → 0.5 MW/slot, profit 80 exactly (hand-computed PL optimum);
  λ=200 → 0.125 MW/slot (one block), profit 20; reported profit ==
  Σ[(p−λ·dis)·dis − (p+λ·cha)·cha]·dt recomputed from the dispatch; λ=0.0 ≡
  baseline dispatch bit-for-bit; negative λ raises; zone masks still bind all
  blocks; 15-min wrapper with λ=1e4 suppresses dispatch entirely.

Regression scan: full `tests/mengxi_nodal/` — 74 passed, 4 failed in
`test_pf_results.py` (pandas-3.0 groupby-apply `KeyError: discharge_mwh` in
`services/mengxi_nodal/pf_results.py` — a file this task does not touch;
pre-existing). `tests/services/test_optimisation_engine.py` — 5 failures are the
pre-existing bundled-CBC-x86 issue (`OSError: [Errno 86] Bad CPU type`), per T12.

## Deviations / decisions

- **Plain `:s`/`:e` bound params instead of `CAST(:s AS timestamp)`** in the
  module SQL: identical on PG (unknown-typed param coerces to TIMESTAMP — the
  proven nodal_pf_daily pattern), and executable on the SQLite test harness,
  where CAST-to-unknown-type takes NUMERIC affinity and silently compares wrong.
  Frame constraint kept: naive CST literals, no '+08', no AT TIME ZONE (asserted
  in tests).
- **Block linearisation with chord (marginal) slopes p ∓ 2λ·(k+0.5)·Δ** — the
  LP-exact representation of the brief's clearing rule; the naive per-block
  average price p − λ·(k+0.5)Δ would understate impact cost 2×. Side effect:
  slot optima snap to block boundaries (12.5 MW at 8 blocks/100 MW) —
  documented in the engine docstring.
- **Non-positive λ never reaches the LP**: stored as estimated (it's data), but
  `_mengxi_node_impacts` skips λ ≤ 0 (price-taker fallback) so a
  reverse-causality estimate can't make the LP pathological.
- `_candidate_nodes` / `_history_window` reused from `book_forecast` (same
  package, same plan family) rather than duplicated.

## Follow-ups (not done, out of scope)

- Nothing calls `persist_lambda` on a schedule yet — needs a runner (daily job
  or manual backfill over the 6 full-path BESS stations) before the PF picks up
  any λ; until then every node is price-taker, exactly as today.
- Backfill + first production estimates should be eyeballed per station before
  trusting them in the PF (expect small |λ| and possibly negative values where
  the dispatch-follows-price channel dominates).
- `tests/mengxi_nodal/test_pf_results.py` pandas-3.0 failure is worth its own
  fix (pre-existing, unrelated).
