# Retail Risk — Trades, Settlement & MTM Ingestion Design

**Date:** 2026-09-30
**Status:** Approved design, pre-implementation
**Scope:** `apps/retail_risk` + new `services/retail_risk/` ingestion/analytics. **Asset-risk app and asset books are not touched** — shared `rm_*` tables only gain retail rows (currently all retail-relevant tables are empty).

---

## 1. Goals

Three goals, framed by the user's monthly 经营复盘 materials (`data/trading/*/复盘*`):

1. **Reconcile trades with settlement invoice numbers** — per book × month: traded 中长期 cost vs invoice 中长期电费, volume bridge to spot-settled exposure, total-bill residual explained.
2. **P&L breakdown by source** — 复盘-aligned analytics: 批零价差 headline, channel alpha vs spot (年度/月度/月内 each marked to 现货均价), bridge waterfall, 月度盈亏 YTD trend. **Plus trader/sales attribution**: trader alpha = (market-avg channel price − our trade price) × volume (bought below market = gain); sales alpha = (retail price − 渠道费用 − wholesale market-avg price) × volume (sold above market net of channel fee = gain). The wholesale market average is the pivot, so批零价差 (net of 渠道费） = trader alpha + sales alpha.
3. **MTM per book** — open positions × scenario forward curves (base / +10 / −10) plus retail-contract MtM, snapshotted daily.

The 复盘 PPTs are the analytics **specification**, not an ingestion source: every number in them (批零价差, 景融合同价 vs 现货, 月度盈亏, 月末持仓%) is recomputed in-app from hard data.

## 2. Current state

- `apps/retail_risk/`: 6-tab Streamlit shell (CRM, Settlement, Realised P&L, Positions & MtM, VaR & Greeks, Agent). All tabs render "no data".
- DB: `rm_books` holds 16 asset books only. `rm_positions`, `rm_position_volumes`, `rm_forward_curves`, `rm_customers`, `rm_customer_contracts`, `rm_settlements`, `rm_retail_settlements` = **0 rows each**.
- `marketdata.spot_prices_hourly(province, datetime, rt_price, da_price)` is populated by the LingFeng pipeline for all P1 provinces — spot data needs **no new ingestion**.
- `services/settlement_ingest/` holds proven asset-side patterns (per-format parsers, file-hash dedup via `raw_data->>'file_hash'`, `split_insertable`). Retail modules **import patterns, never modify asset-side code**.

## 3. Locked design decisions

| # | Decision | Choice |
|---|----------|--------|
| D1 | Books | 8 load books `景融售电-{province}` （山东/冀南/浙江/安徽/福建/广东/江苏/上海）, `book_type='load'`, `asset_id=NULL`, get-or-create |
| D2 | `rm_positions` grain | day × channel × direction (Σ volume, VWAP price); `status='closed'` if end_date < today else `'open'`; hourly fidelity lives in `rm_position_volumes` |
| D3 | Scope | trades + forward curves + retail contracts + wholesale invoices |
| D4 | Ingestion path | batch backfill script (local, no S3) + in-app upload tab reusing the same parsers |
| D5 | 山东 daily-grain | spread daily channel volumes to hours via MTM workbook 分月分时比例； `upload_batch_id` suffixed `_est` marks estimated rows |
| D6 | Settlement categories | widen `rm_settlement_items` CHECK with `spot_energy`, `midlong_energy`, `retail_revenue`, `green_premium` (backward-compatible migration, §10) |
| D7 | Retail revenue | invoice retail-side lines primary; fallback = contract 套餐价 × actual volume |
| D8 | Province alias for spot joins | `冀南 → 河北南网` (spot_prices_hourly naming); 上海 absent from spot tables → phase 2 (track-log Excel) |
| D9 | 绿电 | separate `rm_positions` row with `counterparty='绿电'`; folds into tenor channel in `rm_position_volumes`; invoice side uses `green_premium` |
| D10 | Trader/sales attribution | trader alpha per channel: `(mkt_avg_c − our_price_c) × vol_c`; sales alpha: `(retail_price − 渠道费用 − mkt_avg_blended) × retail_vol` where `mkt_avg_blended = Σ(mkt_avg_c × vol_c)/Σvol_c` — identity with 批零价差 (net of 渠道费） holds by construction. 渠道费用 per contract from the MTM workbook 渠道分成比例 fields; exact formula verified against workbook 测算表格 logic at implementation |
| D11 | Market-average benchmark | new additive table `marketdata.rm_market_benchmarks` (§10), sourced from `台账/mtm/*电力市场信息汇总by*.xlsx` 中长期价格 sheets (market-wide channel price + volume by month). Files are `by<date>` snapshots — keep latest row per (province, channel, month); months beyond the snapshot show alpha as N/A |
| D12 | Policy docs (`data/exchange-annual-reports/2026年政策/`, 31 provinces + 国家层面） | P1: authoritative reference for per-province recon category maps and settlement-rule interpretation during implementation. P2: ingest into knowledge pool (`services/knowledge_pool`, supports PDF/DOCX) with province tags so the retail Agent tab can cite rules |

### Channel mapping (province term → schema)

| Source term | `channel` | `instrument_type` |
|---|---|---|
| 年度双边 / 年度竞价 / 年度挂牌 | `annual` | bilateral / forward / forward |
| 月度双边 | `monthly_auction` | bilateral |
| 月度竞价 | `monthly_auction` | forward |
| 月度挂牌 | `monthly_listed` | forward |
| 滚撮 / 日滚动 / 月内 | `intramonth_match` | forward |
| 绿电 | per tenor | forward, `counterparty='绿电'` |

`rm_position_volumes`: 年度* folds into `annual_*`; 月度双边 VWAP-folds into `monthly_auction_*`; sub-channel fidelity preserved in `rm_positions`.

## 4. Data architecture

```
data/trading/                                       marketdata.
├── 冀南/中长期交易结果/2026年X月/*.xlsx          →  rm_positions + rm_position_volumes
├── 浙江/交易记录/景融浙江持仓_*.csv                →  rm_position_volumes (hourly × channel)
│   浙江/交易记录/滚动撮合/汇总YYYYMM.xlsx          →  rm_positions
├── 山东/2026XX/景融/持仓明细+持仓量.xlsx           →  rm_position_volumes (daily → hourly est.)
├── 安徽/交易记录/X月-中长期交易.xlsx               →  rm_positions + rm_position_volumes
├── {province}/月结算单*.pdf, 山东7021-*.xlsx      →  rm_settlements + rm_settlement_items
├── 台账/mtm/*.xlsx (8 provinces × 3 scenarios)    →  rm_forward_curves + rm_customers
│                                                        + rm_customer_contracts
└── 台账/mtm/*电力市场信息汇总by*.xlsx               →  rm_market_benchmarks (全网 channel avg price/vol)

reference (P1 implementation; P2 KB ingestion):
  data/exchange-annual-reports/2026年政策/{province}/*.pdf|docx  →  settlement/trading rules per province

computed (services/retail_risk/):
  reconcile.py   trades × invoices                →  recon report (Goal 1)
  pnl_bridge.py  trades × invoices × spot_hourly  →  rm_pnl_snapshots (Goal 2)
  mtm.py         open positions × curves, contracts×curves → rm_pnl_snapshots.unrealized (Goal 3)
```

## 5. Module layout

```
services/retail_risk/
  __init__.py
  schemas.py            # canonical frame dtypes, channel map, province alias map,
                        # settlement category map (Chinese label → category)
  loader.py             # get_or_create_load_books(); upserts: positions, position_volumes,
                        # forward_curves, customers, contracts, settlement(items);
                        # file-hash dedup (raw_data->>'file_hash'); upload_batch_id convention
  run_backfill.py       # CLI: --province X --root data/trading [--dry-run] [--invoices-only|--trades-only]
  reconcile.py          # Goal 1 engine (pure functions on frames; DB read/write thin shell)
  pnl_bridge.py         # Goal 2 engine → rm_pnl_snapshots
  mtm.py                # Goal 3 engine → rm_pnl_snapshots.unrealized_mtm_cny
  parsers/
    __init__.py         # registry: province → {trades_parser, invoice_parser}
    trades_jinan.py     # 冀南 我的交易结果 sheets (transaction × hour block)
    trades_zhejiang.py  # 浙江 持仓 CSV + 滚动撮合汇总 monthly workbooks
    trades_shandong.py  # 山东 持仓明细/持仓量 (daily × channel + price sheet)
    trades_anhui.py     # 安徽 X月-中长期交易.xlsx (滚搓市场成交结果, 中长期合计)
    mtm_workbook.py     # all 8 provinces: price-forecast sheets → curves; 合约总表 → contracts
    benchmark_infohub.py# 信息汇总 workbooks: 中长期价格 sheet → market benchmarks
    invoice_jinan.py    # 冀南 现货月结算 PDF
    invoice_zhejiang.py # 浙江 现货月依据 PDF
    invoice_anhui.py    # 安徽 统推售电公司结算单 PDF (reuse parser_stategrid_anhui patterns if importable)
    invoice_shandong.py # 山东 7021 Excel (+ PDF fallback later)

apps/retail_risk/
  app.py                # +2 tab registrations only
  tab_reconciliation.py # NEW — Goal 1
  tab_pnl.py            # REBUILT — Goal 2 (批零价差 cards / channel alpha / bridge + YTD)
  tab_positions.py      # MtM scenario selector + per-book cards (Goal 3); existing queries unchanged otherwise
  tab_data_upload.py    # NEW — upload → province + type → parse → preview → write

tests/services/retail_risk/
  test_parsers_trades.py    # tiny generated fixture files per province format
  test_parsers_mtm.py
  test_parsers_invoice.py
  test_loader.py            # DB-gated (RETAIL_TEST_DB_DSN env), default skip
  test_reconcile.py         # pure-function, synthetic frames
  test_pnl_bridge.py
  test_mtm.py
```

## 6. Canonical frames (parsers → loader)

All parsers emit plain DataFrames; the loader is the only DB writer.

- **trades**: `delivery_date, hour(0-23|None), channel, direction(buy|sell), volume_mwh, price_cny_mwh, counterparty, source_file` — the loader rolls trades up to day × channel × direction (Σ volume, VWAP price) for `rm_positions` and writes hour-level rows to `rm_position_volumes`
- **volumes**: `delivery_date, hour, channel, volume_mwh, vwap_cny_mwh, estimated(bool)` — hourly always (daily-grain sources like 山东 are spread to hours inside the parser via 分月分时比例, marked `estimated=True` → `_est` batch suffix)
- **curves**: `province, product(spot_base|spot_p10|spot_m10), delivery_month, hour, price_cny_kwh, curve_date`
- **contracts**: `customer_name, contract_ref, package_name, package_class, price_cny_mwh, share_ratio, start_date, end_date, annual_mwh, monthly_mwh{json}`
- **invoice**: `settlement_month, line items: (category, label_cn, volume_mwh, price_cny_mwh, amount_cny, delivery_date?)`

Loader conventions: `upload_batch_id = {province}_{yyyymm}_{kind}_{rundate}`; re-running a scope deletes its prior batch rows before insert (positions/volumes); curves upsert ON CONFLICT; customers get-or-create by `(name, province)`; contracts matched by `contract_ref`; invoices dedup by file SHA-256.

## 7. Engines

### 7.1 reconcile.py (Goal 1) — per book × month

1. **中长期 check**: `expected = Σ trades cost (channel VWAP × vol)` vs `invoice = Σ items[category='midlong_energy']` → Δ1.
2. **Volume bridge**: `cleared_midlong_vol` (rm_position_volumes annual+monthly+intramonth) vs `actual_load` (invoice settled volume) → `spot_exposure = actual − cleared`; `expected_spot = spot_exposure × month RT VWAP (spot_prices_hourly)` vs `Σ items['spot_energy']` → Δ2.
3. **Total check**: expected total vs invoice total → residual must equal Σ(imbalance+penalty+govt_surcharges+market_redistribution+other) → Δ3.

Status: `matched` if all |Δ| ≤ tolerance, `explained` if residual maps to itemised fees, else `flagged`. Tolerance default `max(0.5% of invoice, ¥5,000)`, configurable per run.

### 7.2 pnl_bridge.py (Goal 2) — per book × month

- `retail_revenue` (D7) − channel costs (annual / monthly_auction / monthly_listed / intramonth / green) − `spot_energy` − deviation (`imbalance`+`penalty`) − other (`govt_surcharges`+`market_redistribution`+`other`) = **net margin**.
- **批零价差** = retail_avg_price − wholesale_avg_cost, `wholesale_avg_cost = (midlong + spot cost) / settled volume`.
- **Channel alpha** = per channel: `(month_spot_VWAP − channel_VWAP) × channel_volume` (positive = 降本); hourly drill-down uses hour-level spot vs channel price (mirrors 冀南 复盘 slide 6).
- **Trader/sales attribution** (D10):
  - `trader_alpha_c = (mkt_avg_c − our_price_c) × vol_c` per channel (rm_market_benchmarks vs rm_positions VWAP)
  - `sales_alpha = (retail_price − 渠道费用 − mkt_avg_blended) × retail_vol`, `mkt_avg_blended = Σ(mkt_avg_c × vol_c) / Σvol_c`
  - Display per book × month: trader alpha by channel, sales alpha by contract type （联动/固定/分时）, and the identity check `trader + sales ≈ 批零价差(net of 渠道费)` (residual flows to spot/deviation lines). Months lacking benchmark data show N/A (D11).
- Persist monthly: `rm_pnl_snapshots(snapshot_date = month-01)`: `realized_cny=net`, `bilateral_pnl_cny=midlong_alpha_total`, `spot_pnl_cny=−spot_cost`, `deviation_pnl_cny=−deviation`, `other_pnl_cny=−other`, upsert on UNIQUE(book_id, snapshot_date). Detailed per-channel alpha is computed on the fly for UI, not snapshotted.

### 7.3 mtm.py (Goal 3) — per book, per scenario

- **Procurement MtM**: open buy positions vs scenario curve via `libs/risk/mtm.compute_mtm`.
- **Retail-contract MtM** (remaining months of active contracts):
  - `fixed` / `peak_offpeak` 套餐: `(套餐价 − scenario_forward) × remaining_volume`
  - `indexed` / `indexed_band` （联动类, ~92% of 山东 book): margin = uplift/spread × remaining_volume (price floats with market → MtM is the locked service margin, e.g. 山东 联动+上浮 6 元/MWh)
- `book_mtm = procurement + retail`; daily write → `rm_pnl_snapshots.unrealized_mtm_cny` (UNIQUE upsert).

## 8. Forward curves

Each MTM workbook × scenario sheet → month×hour price grid → expand to `(delivery_date = each day of month, delivery_hour)`, `curve_date = file mtime`, `source='manual'`, ON CONFLICT upsert. ~200k rows for 8 provinces × 3 scenarios × 12 months. Products: `spot_base`, `spot_p10`, `spot_m10` (from filename −10/base/+10).

## 9. App changes

- **Reconciliation tab (new)**: book × month status matrix (matched/explained/flagged) → per-month drill-down: expected-vs-invoice per line, volume bridge, residual attribution.
- **Realised P&L tab (rebuilt)**: §批零价差 cards (零售结算均价, 批发结算均价, spread, 月末持仓%); channel alpha table (vol, 成交均价, 现货均价, 降本/增支 CNY/MWh + ¥) with hourly drill; **trader/sales attribution section** (trader alpha per channel vs 全网均价, sales alpha per contract type net of 渠道费， identity check vs 批零价差）; bridge waterfall; 月度盈亏 YTD trend (from rm_pnl_snapshots).
- **Positions & MtM tab**: scenario selector (base/+10/−10), per-book cards (realised YTD + unrealised MtM + scenario band), open-position table, curve viewer. Existing queries otherwise unchanged.
- **Data Upload tab (new)**: file → province + type (trades / invoice / MTM workbook) → parse → preview counts → write via loader. No S3.

## 10. DDL migration (needs explicit confirmation at apply time)

`db/ddl/marketdata/rm_settlements.sql` updated + migration `db/migrations/2026-09-30_rm_settlement_items_retail_categories.sql`:

```sql
ALTER TABLE marketdata.rm_settlement_items DROP CONSTRAINT rm_settlement_items_category_check;
ALTER TABLE marketdata.rm_settlement_items ADD CONSTRAINT rm_settlement_items_category_check
  CHECK (category IN ('charge_energy','discharge_energy','generation_revenue',
    'capacity_compensation','bilateral_energy','transmission','govt_surcharges',
    'system_operation','coal_capacity_charge','basic_fee','curtailment','flex_fees',
    'imbalance','market_redistribution','rule_charges','frequency','penalty','rebate',
    'subsidy','other',
    'spot_energy','midlong_energy','retail_revenue','green_premium'));
```

Rollback: reverse ALTER after deleting any rows using the 4 new categories. Backward-compatible: asset-side categories unchanged, asset-risk code untouched.

Additive new table (same migration file; no existing-table impact):

```sql
CREATE TABLE IF NOT EXISTS marketdata.rm_market_benchmarks (
    id               SERIAL PRIMARY KEY,
    province         TEXT NOT NULL,
    channel          TEXT NOT NULL,          -- same channel vocabulary as rm_positions
    month            DATE NOT NULL,          -- 1st of delivery month
    avg_price_cny_mwh NUMERIC(10,4) NOT NULL, -- 全网 market-average contract price
    volume_mwh       NUMERIC(14,4),           -- 全网 cleared volume, when published
    source           TEXT NOT NULL DEFAULT 'infohub',  -- infohub / manual
    source_file      TEXT,
    uploaded_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (province, channel, month, source)
);
```

## 11. Phasing

- **P1 (this implementation)**: trades+invoices for 冀南/浙江/山东/安徽； all 8 MTM workbooks (curves+contracts); 信息汇总 benchmarks for the 8 provinces; engines (incl. trader/sales attribution); 4 tabs; tests. Goals 1–3 live on 4 books; MtM live on all 8. Policy docs used as implementation reference for per-province recon category maps.
- **P2**: 福建/广东/江苏/广西/上海 invoice + trade formats; daily-clearing (日清算/日清分) detail; 上海 spot from track-log Excel; policy docs → knowledge pool with province tags, retail Agent tab gains rule-citation search.
- **P3**: 信息披露月报 overlay (fresher 全网 benchmark + regulatory watch); per-customer retail settlement upload; 各省份台账 CRM enrichment via `rm_crm_import_configs`.

## 12. Testing

- Parser tests: tiny generated fixtures per format (no real data committed), assert canonical frames.
- Engine tests: pure functions on synthetic frames (recon tolerance logic, alpha signs, MtM formulas incl. indexed-contract service-margin case).
- Loader tests: gated by `RETAIL_TEST_DB_DSN` (skip by default); cover get-or-create books, batch delete-reinsert, file-hash dedup.
- Verification: local `streamlit run apps/retail_risk/app.py --server.port 8513` smoke test; backfill dry-run row counts reviewed before any live DB write (all DB writes require in-session confirmation).

## 13. Out of scope

Asset-risk app/code/data; 复盘 PPT ingestion; VaR & Greeks tab; retail per-customer settlement (P3); policy-doc KB ingestion + agent rule search (P2); 信息披露 overlay (P3); automated scheduling of backfill (manual CLI this round).
