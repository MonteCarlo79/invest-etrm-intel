# Mengxi Settlement Price Basis — Investigation Handoff

**Date:** 2026-08-30 (investigation run 2026-08-29)
**Branch:** `feat/deal-structurer-bedrock-migration`
**Trigger:** Stage-2 arbitrage matching (commit `e3f2f58`) reconciled volumes within 3% for four assets, but invoice prices did not match RT nodal pricing. Task: confirm the Mengxi settlement price basis for charge and discharge, and explain the 巴盟 volume discrepancy.

---

## Verdict

### Settlement price basis (confirmed)

Sources: `202412 内蒙古电力多边交易市场规则体系` (knowledge pool doc id 6704, ch.3 §14–15), bill structures in `rm_settlement_items`, **user confirmation 2026-08-29**, and end-to-end numerical verification (below).

| Leg | Basis |
|---|---|
| **Discharge (发电侧)** | RT nodal, **15-min interval** price × interval discharge energy. |
| **Charge (用户侧)** | RT nodal, **hourly average** of the 15-min prices × interval charge energy (basis: trading practice, per user — overrides the rules-doc per-interval reading). T&D, line-loss, system-operation, government surcharges are itemized as separate bill lines. |
| **DA component** | **None.** DA nodal prices are not used for settlement (per user 2026-08-29). No 中长期差价合约 lines on any of the six assets' bills → pure spot. |

**This is exactly the rule Stage-2 implements. The price mismatch was NOT a rule problem.**

### Root cause of the price mismatch: 8-hour timezone skew

`services/arb_match/compute.py::_load_dispatch` used `interval_start::timestamp`. Under the RDS session's UTC timezone this reads the UTC wall clock, but `md_rt_nodal_price` / `md_id_cleared_energy` are Beijing-wall naive timestamps — every interval was priced **8 hours late** (discharge at post-midnight prices instead of evening peak; charge at morning instead of valley).

**Verification** (alignment fixed to `interval_start AT TIME ZONE 'Asia/Shanghai'`, charge at hourly-avg, discharge at 15-min, vs invoice effective prices, 2026-06):

| Asset | Discharge modeled vs bill | Charge modeled vs bill |
|---|---|---|
| 悦杭独贵 | 257.8 vs 246.5 (+4.6%) | 165.1 vs 167.3 (−1.3%) |
| 裕昭沙子坝 | 258.5 vs 244.3 (+5.8%) | 159.1 vs 165.9 (−4.1%) |
| 远景乌拉特 | 244.4 vs 246.5 (−0.9%) | 177.3 vs 180.0 (−1.5%) |
| 四子王旗 | 266.0 vs 260.1 (+2.3%) | 230.6 vs 225.0 (+2.5%) |

All within ±6% — same order as the volume residuals. Basis and rule both confirmed.

Bill evidence (structure): discharge bill (上网电费结算单) prints one monthly effective 现货 price (e.g. 景蓝乌尔图 2026-06: 0.27467 ¥/kWh stated = ¥3,491,459.98 / 12,711.64 MWh); charge bill (下网电费结算单) shows 电能电费(市场化购电) with network/system charges itemized separately.

### Falsified candidate explanations (evidence)

| Candidate | Test | Result |
|---|---|---|
| Wrong rule (per-interval charge) | user confirmation | charge is hourly-avg per trading practice |
| DA/RT two-part settlement | user confirmation | DA not used; DA data also absent for all 6 BESS plants in `md_da_cleared_energy` (the "乌尔图" rows are 国电乌尔图光伏电站, a PV station) |
| Province-uniform RT as basis | window-weighted vs invoice | ~2× off on charge, ±15–40% on discharge with sign flips |
| Price-series disagreement (enos nodal vs Fengxing cleared) | daily abs diff, 景蓝乌尔图 May | 0–3% — series agree |

### Falsified candidate bases (evidence)

| Candidate | Test | Result |
|---|---|---|
| RT nodal, 15-min, window-weighted (current Stage-2) | modeled vs invoice, all asset-months | June discharge: modeled 150–156 vs invoice 244–247; charge: modeled 222–239 vs invoice 165–180 |
| RT nodal, hourly-avg for charge | same | same table |
| Province-uniform RT (`hist_mengxi_provincerealtimeclearprice_15min`), window-weighted | same join | June charge: 321–335 vs invoice 165–180 (~2×); discharge: 283–291 vs 244–247; May signs flip |
| Price-series disagreement (enos nodal vs Fengxing cleared) | daily abs diff, 景蓝乌尔图 May | 0–3% — series agree; not the error source |

Cross-asset clustering: three 杭锦旗-area assets' June invoice discharge prices = 244.3 / 246.5 / 246.5 ¥/MWh despite different nodes — uniform-ish component ~¥43 below window-weighted province RT. 景蓝乌尔图 (274.7) ≈ its own nodal monthly simple average (275.2) — possibly significant, unverified.

### Volume reconciliation — confirmed fine

- 3% claim holds wherever dispatch-chain coverage is complete: May — 悦杭独贵 (-1.8%), 景蓝乌尔图 (-1.4%), 裕昭沙子坝 (-1.1%); June adds 四子王旗 (-2.2%).
- Big gaps are **coverage holes, not method errors**: April ≈ -55% everywhere (chain ingestion started mid-April); 景蓝乌尔图 June -100%; 景怡查干哈达 June -91% / July -99%; 裕昭沙子坝 Feb–Apr ≈ -99%.

### 巴盟 = 景怡查干哈达 (invoice folder "B-9 内蒙巴盟")

- Registry capacity: **1000 MW / 4 h = 4 GWh** — a genuinely giant station (10× the 杭锦旗 assets). Invoice volumes (~140–160 GWh/mo ≈ 1.3 cycles/day) are physically normal.
- **Root cause (confirmed 2026-08-30):** the monthly dispatch workbooks were **exported mid-month** — actuals exist only through each export date (Apr 19→, May →15th, Jun 18–30 only, Jul 3–4 only). Interval-level data is full-plant (max ±1,004 MW) and correct when present; the "multi-section plant" hypothesis was tested and rejected.
- **Fix applied:** backfilled `rt_cleared_mw` only (10,648 intervals, Apr 1 → Aug 26) from `md_id_cleared_energy` — semantically exact (RT cleared = RT cleared). `actual_mw` deliberately **not** filled: metered execution ≠ cleared, and filling it would fabricate zero-deviation days (user correction). Provenance: `source_file='backfill:md_id_cleared_energy'`, `upload_batch_id='backfill-md_id-20260830'`; `ON CONFLICT DO NOTHING` — Excel rows untouched, later re-uploads upsert over backfill.
- **Result:** complete monthly RT-cleared coverage; RT-vs-invoice volume gaps now read ±4–9% (Jun discharge +15.4%) = the plant's **true deviation**, visible for analysis.
- **Still open (ops, not code):** trader must re-export the monthly workbooks *after* month-end so `actual_mw` can be ingested for the missing days; `md_id` itself misses ~2 days each in Jun/Jul.
- 远景乌拉特 (B-7, also Bayannur geography) shows a different, smaller pattern — **opposite-sign residuals** (Jun: charge -7.5% / discharge +5.6%; May: -3.4% / +1.6%) vs all other assets' same-signed gaps. Likely station-use/netting structure vs settlement meter. Secondary; check its ops Excel metered-vs-dispatch columns.

---

## Next moves (tracked as tasks)

| # | Task | Status |
|---|---|---|
| 7 | ~~Ingest DA nodal prices~~ — **cancelled: DA not used in settlement (user 2026-08-29)** | deleted |
| 8 | Fix 8h timestamp skew in `arb_match` (`AT TIME ZONE 'Asia/Shanghai'`), recompute `rm_arb_match_daily` full range, verify vs bills | in progress |
| 9 | ~~Fix 景怡查干哈达 dispatch-chain coverage~~ — root cause = mid-month Excel exports; `rt_cleared_mw` backfilled from md_id (10,648 rows, Apr 1→Aug 26). Open: trader re-exports full-month files for `actual_mw` | done (data) / open (ops) |
| 10 | 远景乌拉特 opposite-sign residual check (meter structure) | pending |

## Key file paths

| What | Path |
|---|---|
| Stage-2 model | `services/arb_match/compute.py` (PRICE_SOURCES map, `compute_day`) |
| Stage-2 output | `marketdata.rm_arb_match_daily` |
| Rules source | knowledge pool doc id 6704 (chunks ~p149–153: 第三/四章, §14–15) |
| Bill parsers | `services/settlement_ingest/parser_charge.py` (下网/购电), `parser_discharge.py` + `parser_vision.py` (上网/发电) |
| Invoice data | `marketdata.rm_settlement_items` (has `price_cny_kwh`, `peak_period`, `notes`) |
| Dispatch chain | `marketdata.rm_dispatch_chain` (nominated/da/rt/actual_mw, restriction) |
| Price series | `marketdata.md_rt_nodal_price` (enos, 5 nodes), `md_id_cleared_energy` (RT cleared, 6 plants), `md_da_cleared_energy` (DA cleared — **no BESS plants**), `public.hist_mengxi_province*15min` (uniform/hub) |
| Analysis scripts | session `/tmp`: `reconcile_prices.py`, `test_uniform_weighted.py`, `monthly_price_avgs.py`, `blend_test.py`, `price_series_check.py` (ephemeral — recreate from this doc's queries if needed) |
