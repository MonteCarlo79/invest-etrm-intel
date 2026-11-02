# Task 12 report — 蒙西 dispatch zone pipeline (red/yellow/green)

Status: DONE. 16/16 tests pass (`tests/mengxi_nodal/test_dispatch_zones.py`).

## What was built

1. **`services/mengxi_nodal/dispatch_zones.py`** (new)
   - `parse_nomination_zones(path, asset_code)` — walks day sheets (`^\d{1,2}\.\d{1,2}$`,
     tolerates '09.01' and '9.13'), reads col A rows 2..97 fill colors
     (FFFF0000→red / FFFFFF00→yellow / FF00B050→green; unfilled or unrecognised → green).
     `data_date` comes from each sheet's A2 timestamp (00:00 slot — the only year
     source; the filename carries only the month), cross-checked against the sheet
     tab (loud print + A2 wins on mismatch), with a workbook-level year fallback
     for sheets whose A2 is unparseable; unparseable sheets are skipped loudly.
   - `_DDL` + `ensure_table()` + `load_zones()` — upsert on
     PK(asset_code, data_date, slot), batches of 500.
   - `get_zones(engine, asset_code, day) -> (list[str], bool)` — 96 labels;
     missing day falls back to the station's latest PRIOR day with `stale=True`;
     `([], False)` when the station has no zones at all (unconstrained signal).
   - `zones_to_masks()` — red→no_charge, yellow→no_discharge, 1-based slots.
2. **`services/bess_map/optimisation_engine.py`** — optional
   `no_charge_slots` / `no_discharge_slots` on `compute_dispatch_from_15min_prices`
   and `compute_dispatch_from_hourly_prices` (1-based per-day, applied to every day
   of every window) and on `optimise_window` (0-based window-absolute). Implemented
   as variable `upBound = 0` — no extra LP rows. Defaults keep every existing caller
   bit-identical.
3. **`services/bess_map/nodal_pf_daily.py`** — 蒙西 only: `_mengxi_zone_masks()`
   maps each BESS station's zone masks onto its registry price nodes via the
   existing `_expand_price_nodes` (own + expanded parents); `run_day` passes them
   into the LP and appends `(N zone-masked nodes, stale zones: …)` to the day
   message. Nodes shared by two stations (谷山梁 parents: gushanliang/hangjinqi)
   keep the first station's mask in registry order — deterministic, noted in the
   docstring.

## Test evidence

- Real 谷山梁 September file: 30 days × 96 slots parsed; 2026-09-02 decode asserted
  EXACTLY (green slots 1–38/67–75/83–96, yellow 39–66, red 76–82; counts 61/28/7);
  '9.13' tab → 2026-09-13.
- Engine: 8-slot LP — full masks zero charge/discharge and profit; partial masks
  shift dispatch with profit unchanged; 15-min wrapper masks verified 1-based
  (slot {1} hits index 0 only, {96} index 95 only — off-by-one would leak).
- SQLite ATTACH harness: load idempotent upsert (2×2880 rows → 2880), exact-day
  read, stale fallback returns 2026-09-30's zones for 2026-10-05 with stale=True,
  empty for pre-first-day and unknown station.

## Deviations / decisions

- **`created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP`** instead of `default now()` —
  PG-equivalent, and keeps `_DDL` executable on the SQLite test harness (unknown
  function `now()` would fire at insert). Everything else in the DDL is as briefed.
- **`get_zones` returns `(list[str], bool)`** — the brief's `-> list[str]` plus its
  own "with a stale flag" requirement; a tuple is the minimal honest signature.
- **LP tests monkeypatch the solver to HiGHS**: pulp's bundled CBC is an x86_64
  binary and cannot execute on this arm64 Mac (no Rosetta) — the pre-existing
  `tests/services/test_optimisation_engine.py` fails locally for the same reason.
  The fixture swaps `pulp.PULP_CBC_CMD` → `pulp.HiGHS` (highspy is installed);
  constraints under test are solver-agnostic and production Fargate (amd64) keeps
  CBC. Engine code untouched by this.
- No timezone handling anywhere (CST-naive frame, per constraint).

## Follow-ups (not done, out of scope)

- Backfill: `load_zones` the other monthly nomination files (谷山梁 5–8月, other
  stations) and re-run nodal_pf_daily for affected days — needs a decision on
  which station folders map to which asset_codes and an RDS run-task (bulk DB
  writes go via ECS, not the Mac).
- zones table is not yet read by anything besides nodal_pf_daily; mengxi-dashboard
  display is a later task.
