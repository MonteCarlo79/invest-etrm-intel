# Mengxi Settlement Price Basis — Investigation Handoff

**Date:** 2026-08-30 (investigation run 2026-08-29)
**Branch:** `feat/deal-structurer-bedrock-migration`
**Trigger:** Stage-2 arbitrage matching (commit `e3f2f58`) reconciled volumes within 3% for four assets, but invoice prices did not match RT nodal pricing. Task: confirm the Mengxi settlement price basis for charge and discharge, and explain the 巴盟 volume discrepancy.

---

## Verdict

### Settlement price basis (confirmed)

Source: `202412 内蒙古电力多边交易市场规则体系` (knowledge pool doc id 6704, ch.3 §14–15), corroborated by the bill structures in `rm_settlement_items`.

| Leg | Basis |
|---|---|
| **Discharge (发电侧)** | 现货全电量: Σt Q放电(t) × **所在节点电价** per 15-min interval. Plus separately-itemized 中长期差价合约电费 (合约价 − 用户侧区域结算参考点电价) × 合约电量 if contracts held. |
| **Charge (用户侧)** | Storage is the **explicit exception** to the uniform-price rule: Σt Q充电(t) × **所在节点电价** per 15-min interval (ordinary market users pay 区域结算参考点电价). T&D, line-loss, system-operation, government surcharges are itemized as separate bill lines. |

蒙西 settlement is two-part: **DA-cleared × DA nodal + deviation × RT nodal**.

Bill evidence:
- Discharge bill (上网电费结算单) prints **one monthly effective 现货 price** = amount/volume (e.g. 景蓝乌尔图 2026-06: 0.27467 ¥/kWh stated, matching ¥3,491,459.98 / 12,711.64 MWh exactly). Note "放电结算: 现货".
- Charge bill (下网电费结算单): charge_energy = 电能电费(市场化购电) with no stated price; 输配电费 / 上网线损费 / 系统运行费 / 政府基金及附加 itemized separately.
- No 中长期差价合约 lines on any of the six assets' bills → assets are **pure spot**.

### Why Stage-2 (`services/arb_match/compute.py`) prices miss

1. **Charge modeled at hourly-average nodal** — the rule prices charge per 15-min interval, same as discharge. (Caveat: the 2026-08-28 note in `compute.py` docstring states "charging settles at the HOURLY AVERAGE of 15-min nodal prices" — source of that claim unverified; rules doc says per-interval. **Open question for user.**)
2. **Everything priced at RT** — settlement is DA-cleared × DA nodal + deviation × RT nodal. June magnitudes imply DA-at-discharge-windows ≈ 240–250 ¥/MWh vs RT-windowed ≈ 150 ¥/MWh — the dominant gap.
3. **DA price data does not exist for these plants**: `marketdata.md_da_cleared_energy` has **zero rows for all six BESS stations** (the table covers wind/PV/thermal; the 52k "乌尔图" rows belong to 国电乌尔图光伏电站, a PV station — substring false positive). The two-settlement blend is uncomputable until DA nodal prices are ingested.

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
- The gap is on our side: dispatch chain captures only **~45% of metered energy** (Apr–May: 42.8% / 44.7%), collapsing to **~9% / 1%** (Jun–Jul).
- Hypothesis: multi-section plant, only the `220kV.1M` bus section/node ingested (only one node exists in `md_rt_nodal_price`). **Ingestion coverage problem, not a settlement-rule issue.**
- 远景乌拉特 (B-7, also Bayannur geography) shows a different, smaller pattern — **opposite-sign residuals** (Jun: charge -7.5% / discharge +5.6%; May: -3.4% / +1.6%) vs all other assets' same-signed gaps. Likely station-use/netting structure vs settlement meter. Secondary; check its ops Excel metered-vs-dispatch columns.

---

## Next moves (tracked as tasks)

| # | Task | Blocked by |
|---|---|---|
| 7 | Ingest DA nodal prices for the 6 BESS nodes (Fengxing DA nodal endpoint or alternative) | — |
| 8 | Rewrite Stage-2: per-15-min both legs + DA/RT blend; re-verify the hourly-avg note | #7 |
| 9 | Fix 景怡查干哈达 dispatch-chain coverage (multi-section plant; Jun–Jul gap) | — |
| 10 | 远景乌拉特 opposite-sign residual check (meter structure) | — |

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
