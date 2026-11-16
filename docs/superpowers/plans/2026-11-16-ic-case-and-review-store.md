# IC Case Stage + 复盘 Review Store — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** (A) add an investment-committee "投决预期" stage to the asset-risk P&L waterfall, fed by an in-app IC-assumptions upload; (B) ingest monthly 复盘 review decks (蒙西 BESS first) into one shared store, with analytics/reconciliation in asset-risk, overlays on the waterfall and Dispatch Diagnostics, and read-only views + an agent tool in mengxi-dashboard.

**Architecture:** Pure domain code in `libs/asset_risk/` (no streamlit/apps imports): `ic_case.py` (parse + monthly value) and `review/` (deck parser + store). Two new tables in `marketdata`. Asset-risk owns upload; mengxi-dashboard only reads.

**Tech stack:** Python 3.11+, pandas, SQLAlchemy 2, python-pptx, openpyxl, Streamlit, pytest (sqlite fixtures with `ATTACH ... AS marketdata`, see tests/asset_risk/test_pnl_selection.py).

**Spec:** owner decisions in-session 2026-11-16 (this plan records them as Global Constraints).

## Global Constraints
- Owner decisions: IC stage label **投决预期**, placed before 投资标准; deviation 投决预期→投资标准 labelled **市场偏差**. Assets without an IC case show no IC bar (never 0).
- Operating year 1 starts at `rm_assets.commission_date`; if NULL or year < 2000 (景蓝乌尔图 is stored as 0024-12-31) fall back to the IC file's 项目运营日; if neither, no IC bar.
- IC capacity compensation uses the IC rate (0.35 元/kWh = 350 ¥/MWh) for `capcomp_years` (10) operating years — NOT the asset's actual capcomp rate.
- Reviewed (复盘) figures are an **overlay + reconciliation** only; they never replace computed legs.
- Upload lives in asset-risk; mengxi-dashboard is read-only on the review table.
- Station aliases → `rm_assets.asset_code`: 苏右/远景景蓝/景蓝乌尔图→suyou; 杭锦旗/悦杭独贵→hangjinqi; 四子王旗/四子王/景通四益堂→siziwangqi; 谷山梁/固山梁/裕昭新能源/裕昭沙子坝→gushanliang; 乌海/乌海康富→wuhai; 乌拉特/远景乌拉特→wulate; 巴盟/景怡查干哈达→bameng; 武川→wuchuan; 阿拉善→alashan. Unknown names are reported, never guessed.
- Units: deck revenue in 万元 → store ¥ (×10,000); energy MWh; prices ¥/MWh (IC 元/kWh ×1000).
- New tables are created by an idempotent DDL file + a dry-run-by-default migrate script (pattern: scripts/market_maker/migrate.py). Never run DDL from app code paths except `CREATE TABLE IF NOT EXISTS` already used by legs_store-style `ensure_table` (allowed for these two tables too).
- No changes to settlement parsers, LP legs, or the D+15 cleared frame. Commit with `git commit -- <paths>` only. Never stage infra/terraform/terraform.tfvars.
- Tests run with `~/.venvs/bess-platform/bin/python -m pytest`.

## Review Focus
1. A deck month label "（9月）" dated "2026.10" → 2026-09; "（12月）" dated "2027.1" → 2026-12.
2. Overview table with two-month columns (8月 | 9月): take only the deck month's column.
3. 武川/阿拉善 rows with blank cells (unit revenue, capcomp 0) → store what is present; blanks are absent, not 0.
4. An IC year table with "后15年" row → applies to all years ≥ 6.
5. Same deck uploaded twice → idempotent (file hash); a corrected deck for the same month replaces that asset-month's metrics.

---

### Task 1: IC assumptions — parser, monthly value, store

**Files:** Create `libs/asset_risk/ic_case.py`, `db/ddl/marketdata/ic_review_migration.sql` (both tables, Task 1 + Task 4), `scripts/asset_risk/migrate_ic_review.py`; Test `tests/asset_risk/test_ic_case.py`.

**Interfaces (produces):**
- `IC_COLUMNS = ["asset_code","op_year","soh","cycles_per_year","charge_price","discharge_price","dod","rte","capcomp_rate","capcomp_years","grid_fee","capacity_mw","energy_mwh","ic_cod","case_label","source_file"]` (prices/fees ¥/MWh).
- `parse_ic_workbook(path_or_buffer, file_name: str) -> tuple[pd.DataFrame, list[str]]` — rows per asset × op_year 1..20 + notes (skipped sheets with reason).
  - Format A (sheets "<站名>-IPMT 边界"/"巴盟-IPMT"): scalars 建设容量 "0.5 / 2" (GW/GWh → MW 500, MWh 2000), DOD, RTE, 容量补偿 元/kWh, 补偿年限; SOH Table I by 年份 1..20; year table 年度 1..5 + "后15年" (applies to years ≥6) with 全年充放次数, 充电价格, 放电价格 (元/MWh). grid_fee 0.
  - Format B (sheets "<站名>-测算边界汇总"): 额定功率 万kW ×10 → MW, 额定容量 万kWh ×10 → MWh, DOD, 充放电效率, 放电电价/充电电价 元/kWh ×1000, 日均充放电次数 ×365 = cycles_per_year (all years), 电网收费 元/kWh ×1000 → grid_fee, 项目运营日 → ic_cod, SOH: use Format A Table I values `[0.946,0.923,0.905,0.888,0.872,0.857,0.844,0.83,0.818,0.806,0.794,0.783,0.772,0.762,0.752,0.742,0.732,0.723,0.714,0.706]`, capcomp 350 for 10 years. Where a sheet has two value columns (杭锦旗: 历史上会版 | 更新边界) use the **last** non-empty value column and set case_label to its header.
  - Sheet→asset: by alias substring (Global Constraints) on the sheet name; 锡盟 and sheets with no rows → skipped with a note.
- `ic_month_value(ic: pd.DataFrame, asset_code: str, month: date, cod: date | None) -> dict | None` — keys `arbitrage_cny, capcomp_cny, grid_fee_cny, total_cny, discharge_mwh, charge_mwh, op_year`. Rule: effective cod = cod if cod and cod.year ≥ 2000 else ic_cod; None if no rows/cod or month ends before cod. days = days of month on/after cod. op_year = floor((month_start_or_cod − cod).days / 365.25) + 1, capped 20. E = energy_mwh; per cycle discharge = E·DOD·SOH·√RTE, charge = E·DOD·SOH/√RTE; cycles = cycles_per_year·days/365. arbitrage = cycles·(dis·P_dis − chg·P_ch); capcomp = cycles·dis·capcomp_rate if op_year ≤ capcomp_years else 0; grid_fee = cycles·chg·grid_fee; total = arbitrage + capcomp − grid_fee.
- `save_ic(engine, df)` (DELETE by asset_code then INSERT, one txn), `load_ic(engine) -> pd.DataFrame`.
- DDL `marketdata.rm_ic_assumptions` (IC_COLUMNS + uploaded_at timestamptz default now(); PK (asset_code, op_year)).

- [ ] Tests: seed workbook built with openpyxl in tmp_path replicating 武川 Format A (cycles 234; year1 177.73/275.77; 后15年 61.72/246.24; 0.5/2 GW/GWh) and 四子王旗 Format B (10万kW, 40万kWh, 0.42964/0.07467 元/kWh, 1次/日, 电网收费 0.0273, 项目运营日 2025-10-15). Assert: 武川 year 7 prices = 后15年 row; energy_mwh 2000; 四子王 cycles_per_year 365, grid_fee 27.3; `ic_month_value` for 武川 2026-09 with cod 2026-07-20: op_year 1, cycles 234·30/365, discharge per cycle 2000·0.9·0.946·√0.85; partial first month prorated; cod 0024-12-31 falls back to ic_cod; no rows → None.
- [ ] Implement; tests pass; commit.

### Task 2: Seed IC data + upload UI + waterfall stage

**Files:** Modify `libs/asset_risk/waterfall.py` (add stage builder), `apps/asset_risk/tab_waterfall.py` (render stage + upload expander); Test `tests/asset_risk/test_ic_waterfall.py`.
- Upload: an expander "投决边界上传 (IC assumptions)" in the waterfall tab: xlsx uploader → `parse_ic_workbook` → preview table + notes → "保存" → `save_ic`. Seed = upload the two files in reports/投决/ (`内蒙古储能项目经评边界值  20260729.xlsx`, `蒙西独立储能-投委会预审会-0509-sent.xlsx`) via a script `scripts/asset_risk/seed_ic.py <files...>` (dry-run default, `--apply`).
- Waterfall: for each selected asset-month with IC rows, prepend stage 投决预期 (arbitrage + capcomp − grid fee, same stacking as other stages) and deviation 市场偏差 = 投资标准 − 投决预期. Multi-asset "全部资产" sums only assets that have IC; caption lists assets without IC. Do not alter existing legs.
- [ ] Tests: waterfall stage list starts with 投决预期 when IC exists; absent asset → no stage and caption; sums. Commit.

### Task 3: 复盘 BESS deck parser

**Files:** Create `libs/asset_risk/review/__init__.py`, `libs/asset_risk/review/parse_bess.py`, `libs/asset_risk/review/aliases.py`; Test `tests/asset_risk/test_review_parse.py` (build pptx fixtures with python-pptx tables mirroring the 9月 deck rows quoted below).
- `STATION_ALIASES: dict[str, str]` (Global Constraints) + `asset_code_for(name) -> str | None` (longest alias substring match).
- `deck_month(texts: list[str]) -> date | None` from "（N月）" + "YYYY.M" date line (Review Focus 1); fallback filename "N月".
- `parse_bess_deck(path_or_buffer, file_name) -> tuple[pd.DataFrame, list[str]]` → long rows `asset_code, month, metric, value, unit` + notes. Metrics:
  - Overview table (header has 场站名称 and 月度总收益): `total_revenue_cny` (万元×1e4), `unit_capacity_revenue` (元/MWh), `daily_cycles`, `spot_spread` (元/MWh), `capcomp_unit_price` (元/MWh), `sysop_fee_unit` (元/MWh). Two-month variant: header row 2 holds months → use deck month's column (Review Focus 2). Blank cells skipped.
  - Breakdown tables (first cell "曲线"; row 1 station names spanning 3 cols; rows 策略申报收益/实时出清收益/实际执行收益): `{strategy,cleared,executed}_revenue_cny` (万元×1e4), `{..}_charge_mwh`, `{..}_discharge_mwh` (strip thousands commas).
  - Example rows (9月): overview 苏右 466.70 | 11668 | 1.24 | 73.40 | 352.07 | 35.5; breakdown 远景景蓝 策略 651.7 / 16,596.0 / 16,151.2; 实时出清 598.7 / 16,133.3 / 14,856.5; 实际执行 540.2 / 15,471.8 / 13,867.3.
- [ ] Tests incl. Review Focus 1–3; unknown station → note. Commit.

### Task 4: Review store + asset-risk 复盘 tab

**Files:** Create `libs/asset_risk/review/store.py`, `apps/asset_risk/tab_review.py`; Modify `apps/asset_risk/app.py` (add tab "复盘 Review"); Test `tests/asset_risk/test_review_store.py`, AppTest smoke.
- DDL `marketdata.rm_review_metrics (asset_code TEXT, month DATE, metric TEXT, value DOUBLE PRECISION, unit TEXT, source_file TEXT, file_hash TEXT, extracted_by TEXT, uploaded_at TIMESTAMPTZ DEFAULT now(), PRIMARY KEY (asset_code, month, metric))` (in the Task 1 DDL file).
- `save_review(engine, df, source_file, file_hash, extracted_by="table") -> int` — skip if file_hash already stored; else upsert rows (replace asset-month-metric). `load_review(engine, asset_codes=None, months=None) -> pd.DataFrame`.
- Tab: multi-file pptx upload → parse → preview + notes → save; local "Scan folder" for `assets/operating/复盘/储能/蒙西` when present (else caption that scan is local-only); per-asset metric × month table; trend chart of executed revenue; reconciliation table per asset-month: reviewed executed revenue vs settlement realised (reuse `libs.asset_risk.settlement.analytics` loaders) and vs waterfall 实际 leg, flag |diff| > 5%.
- [ ] Tests (Review Focus 5); commit.

### Task 5: Overlays — waterfall + Dispatch Diagnostics

**Files:** Modify `apps/asset_risk/tab_waterfall.py`, `apps/asset_risk/tab_diagnostics.py`; Test `tests/asset_risk/test_review_overlay.py`.
- Waterfall: when the selected asset-month(s) have review metrics, add marker traces "复盘" at the 申报/出清/实际 stage totals (strategy/cleared/executed_revenue_cny) and a reconciliation table (stage, computed ¥, reviewed ¥, diff, diff %).
- Diagnostics: per asset-month table beside the computed bid-fail / exec-gap: reviewed 策略→出清 = cleared − strategy, 出清→执行 = executed − cleared (¥), and "unexplained" = reviewed − computed.
- [ ] Tests on the pure helpers (put computation in `libs/asset_risk/review/overlay.py`). Commit.

### Task 6: mengxi-dashboard read views + Trader tool

**Files:** Modify the mengxi-dashboard P&L attribution/strategy page (implementer locates it: `apps/mengxi-dashboard/`), the Trader agent tool registry; Test under `tests/` matching existing mengxi tests.
- Read-only "复盘 reported" section: per asset (registry `libs/trading_book/registry.py` names) × month: strategy/cleared/executed revenue and gaps, from `load_review`.
- Trader tool `get_review_metrics(asset, month_from, month_to)` returning the long rows; add to the Trader system prompt's tool list per CLAUDE.md agent rules.
- [ ] Tests; commit.

### Task 7 (STOP — owner approval for each): migration, seed, deploys
- Dry-run then apply DDL via run-task; seed IC (two files) and review (5 BESS decks) via run-task scripts; build/deploy asset-risk and mengxi-dashboard (and hermes if the Trader bridge imports the new tool); log LATEST-IMAGES.md.

Out of scope here: 零碳46 wind deck extraction via Haiku (next plan).
