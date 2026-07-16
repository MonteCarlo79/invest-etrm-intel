# App 1 — Asset Risk Management: Design Spec

**Date:** 2026-07-16  
**Status:** Approved  
**App path:** `apps/asset-risk/`  
**Route:** `/asset-risk/*`  
**Port:** 8512  
**ECR repo:** `bess-asset-risk`

---

## 1. Purpose

A risk management cockpit for asset-backed trading books covering power generation (wind, solar, thermal) and BESS assets. Provides realised P&L attribution, position mark-to-market, VaR/Greeks, settlement consolidation, and asset configuration — all organised by trading book.

**Users:** Asset managers, asset traders, risk officers managing generation and storage books.

---

## 2. Architecture

### 2.1 Platform fit

Follows the established bess-platform pattern: standalone Streamlit app, ECS Fargate service, Cognito auth, shared PostgreSQL `marketdata` RDS schema. Deployed alongside existing apps, no new infra primitives needed.

### 2.2 Shared libs (new)

```
libs/risk/
  mtm.py            ← forward curve lookup + open position valuation
  var.py            ← historical simulation VaR + parametric delta-normal VaR
  pnl.py            ← P&L waterfall decomposition (realised + MtM)
  greeks.py         ← delta/gamma/vega aggregation across a book

libs/settlement/
  parser.py         ← multi-format ingestion: PDF + Excel/CSV
                       format detection → province-keyed column mapping → canonical output
  categorizer.py    ← rule-based line item categorization by settlement category
```

`libs/options/` (existing Black-Scholes, Greeks, SVI) is reused for per-instrument Greek computation. `libs/risk/greeks.py` aggregates across instruments to book level.

### 2.3 Services

```
services/forward_curve/
  lingfeng_pull.py  ← pulls near-term price forecasts from existing LingFeng pipeline
                       writes to rm_forward_curves with source='lingfeng'
  manual_upload.py  ← processes manually uploaded curve CSV → rm_forward_curves
                       source='manual', validates date/price/province columns
```

---

## 3. Database Schema

All new tables in `marketdata` schema, prefixed `rm_`.

### 3.1 Asset & book registry

```sql
CREATE TABLE rm_assets (
  id               SERIAL PRIMARY KEY,
  name             TEXT NOT NULL,
  asset_type       TEXT NOT NULL CHECK (asset_type IN ('wind','solar','bess','thermal')),
  province         TEXT NOT NULL,
  capacity_mw      NUMERIC(10,2) NOT NULL,
  bess_duration_h  NUMERIC(5,2),          -- BESS only
  bess_dod_pct     NUMERIC(5,2),          -- BESS only (0–100)
  fuel_type        TEXT,                  -- thermal only
  commission_date  DATE,
  status           TEXT DEFAULT 'active' CHECK (status IN ('active','retired')),
  notes            TEXT,
  created_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE rm_books (
  id               SERIAL PRIMARY KEY,
  name             TEXT NOT NULL,
  book_type        TEXT NOT NULL CHECK (book_type IN ('asset','load')),
  asset_id         INTEGER REFERENCES rm_assets(id),   -- nullable for virtual books
  currency         TEXT DEFAULT 'CNY',
  description      TEXT,
  created_at       TIMESTAMPTZ DEFAULT NOW()
);
```

### 3.2 Positions & volume waterfall

```sql
CREATE TABLE rm_positions (
  id               SERIAL PRIMARY KEY,
  book_id          INTEGER NOT NULL REFERENCES rm_books(id),
  instrument_type  TEXT NOT NULL CHECK (instrument_type IN
                     ('bilateral','spot','futures','option','forward','profile')),
  province         TEXT NOT NULL,
  -- Trading channel: maps directly to rm_position_volumes columns
  -- DA=日前, RT=实时, monthly_auction=月度竞价, monthly_listed=月度挂牌,
  -- intramonth_match=月内撮合, annual=年度, ancillary/capacity=non-energy
  channel          TEXT NOT NULL CHECK (channel IN (
                     'DA','RT','monthly_auction','monthly_listed',
                     'intramonth_match','annual','ancillary','capacity')),
  direction        TEXT NOT NULL CHECK (direction IN ('buy','sell')),
  volume_mwh       NUMERIC(14,4) NOT NULL,
  price_cny_mwh    NUMERIC(10,4),            -- contract/entry price ¥/MWh
  start_date       DATE NOT NULL,
  end_date         DATE NOT NULL,
  counterparty     TEXT,
  status           TEXT DEFAULT 'open' CHECK (status IN ('open','closed','expired')),
  uploaded_at      TIMESTAMPTZ DEFAULT NOW(),
  upload_batch_id  TEXT
);

-- Daily operational dispatch: one row per asset per day (from 运营统计 daily report)
-- Source: 【日期】内蒙储能电站运营统计.xlsx — one sheet per station
CREATE TABLE rm_dispatch_daily (
  id                       SERIAL PRIMARY KEY,
  asset_id                 INTEGER NOT NULL REFERENCES rm_assets(id),
  dispatch_date            DATE NOT NULL,
  operator_name            TEXT,
  -- volumes (MWh)
  charge_mwh               NUMERIC(10,4),    -- 日充电量
  discharge_mwh            NUMERIC(10,4),    -- 日放电量
  auxiliary_consumption_mwh NUMERIC(10,4),   -- 综合站用电
  cumulative_charge_mwh    NUMERIC(12,4),    -- 累计充电量 (month-to-date)
  cumulative_discharge_mwh NUMERIC(12,4),    -- 累计放电量
  -- cycle metrics
  cycle_count_day          NUMERIC(6,2),     -- 日充放次数
  cycle_count_month        NUMERIC(8,2),     -- 月累计充放次数
  conversion_ratio         NUMERIC(6,4),     -- 日充放转化率 = discharge/charge
  -- charge/discharge time windows (stored as text arrays; expanded to hourly on query)
  charge_windows           TEXT[],           -- e.g. ['09:46-14:33']
  discharge_windows        TEXT[],           -- e.g. ['00:00-00:15','05:16-05:53','19:01-23:59']
  -- financial (from 电价日报)
  discharge_revenue_cny    NUMERIC(14,2),    -- 放电收入
  charge_cost_cny          NUMERIC(14,2),    -- 充电费用 (negative)
  system_op_fee_cny        NUMERIC(14,2),    -- 系统运营费
  net_margin_cny           NUMERIC(14,2),    -- 站点毛利
  -- anomalies
  anomaly_notes            TEXT,
  upload_batch_id          TEXT,
  UNIQUE (asset_id, dispatch_date)
);

-- 15-min dispatch plan: source = 电力交易调度计划表 Excel (one sheet per day)
-- Columns: 时间, SOC(%), 操作员申报计划(MW), 当前预测(MW), 实时调度出力(MW), 实际执行功率(MW)
-- Positive MW = discharge (放电), Negative MW = charge (充电)
CREATE TABLE rm_dispatch_plan (
  id                    SERIAL PRIMARY KEY,
  asset_id              INTEGER NOT NULL REFERENCES rm_assets(id),
  interval_start        TIMESTAMPTZ NOT NULL,        -- e.g. 2026-07-01 00:00, 00:15, 00:30 ...
  soc_pct               NUMERIC(6,2),               -- SOC (%)
  nominated_mw          NUMERIC(10,4),              -- 操作员申报计划
  forecast_mw           NUMERIC(10,4),              -- 当前预测
  dispatched_mw         NUMERIC(10,4),              -- 实时调度出力 (cleared by grid)
  actual_mw             NUMERIC(10,4),              -- 实际执行功率
  upload_batch_id       TEXT,
  UNIQUE (asset_id, interval_start)
);

-- Unified hourly position volumes — book-level, one row per book per delivery hour.
-- Stores price + traded volume for each of 6 Chinese electricity market channels:
--   日前(DA), 实时(RT), 月度竞价(monthly_auction), 月度挂牌(monthly_listed),
--   月内撮合(intramonth_match), 年度(annual)
-- Used by both asset books (generation side) and load books (retail side).
-- Populated by: exchange file upload → parser → aggregation; or direct CSV template upload.
CREATE TABLE rm_position_volumes (
  id                          SERIAL PRIMARY KEY,
  book_id                     INTEGER NOT NULL REFERENCES rm_books(id),
  delivery_date               DATE NOT NULL,
  hour                        SMALLINT NOT NULL CHECK (hour BETWEEN 0 AND 23),

  -- Trading channel prices (¥/MWh)
  da_price_cny_mwh            NUMERIC(10,4),   -- 日前价格
  rt_price_cny_mwh            NUMERIC(10,4),   -- 实时价格
  monthly_auction_price_cny_mwh   NUMERIC(10,4),   -- 月度竞价价格
  monthly_listed_price_cny_mwh    NUMERIC(10,4),   -- 月度挂牌价格
  intramonth_match_price_cny_mwh  NUMERIC(10,4),   -- 月内撮合价格
  annual_price_cny_mwh        NUMERIC(10,4),   -- 年度价格

  -- Trading channel volumes (MWh)
  da_volume_mwh               NUMERIC(10,4),   -- 日前交易电量
  rt_volume_mwh               NUMERIC(10,4),   -- 实时交易电量
  monthly_auction_volume_mwh  NUMERIC(10,4),   -- 月度竞价交易电量
  monthly_listed_volume_mwh   NUMERIC(10,4),   -- 月度挂牌交易电量
  intramonth_match_volume_mwh NUMERIC(10,4),   -- 月内撮合交易电量
  annual_volume_mwh           NUMERIC(10,4),   -- 年度交易电量

  -- Derived / computed
  market_price_cny_mwh        NUMERIC(10,4),   -- reference spot price (DA or RT blend)
  actual_price_cny_mwh        NUMERIC(10,4),   -- volume-weighted blended price across all channels
  pnl_cny                     NUMERIC(14,2),   -- realised P&L for this hour

  -- Volume waterfall (MWh): nomination → cleared → settled + deviation attribution
  -- For asset books: sourced from rm_dispatch_plan (15-min → hourly) or exchange upload
  -- For retail books: sourced from exchange nomination files or rm_customer_profiles aggregate
  nominated_mwh               NUMERIC(10,4),
  cleared_mwh                 NUMERIC(10,4),
  settled_mwh                 NUMERIC(10,4),
  deviation_bid_mwh           NUMERIC(10,4) DEFAULT 0,   -- unsuccessful bid
  deviation_equipment_mwh     NUMERIC(10,4) DEFAULT 0,   -- equipment/system failure
  deviation_sysop_mwh         NUMERIC(10,4) DEFAULT 0,   -- system operator modification
  deviation_grid_flow_mwh     NUMERIC(10,4) DEFAULT 0,   -- grid power flow adjustment

  upload_batch_id             TEXT,
  UNIQUE (book_id, delivery_date, hour)
);
```

**Ingestion pipeline:** `services/operating_assets/`

Two ingestion paths — both write to the same tables:

*Path A — WeCom auto-download (production):*
```
Operator posts file to 康富资产管理-储能场站日报群 (WeCom enterprise group, 38 members)
  → WeCom self-built app receives webhook (message_type=file)
  → wecom_receiver.py downloads file via GET /cgi-bin/media/get → S3
  → ingest.py triggered: filename_mapper.py resolves station → asset_id
  → 运营统计 → rm_dispatch_daily
  → 调度计划表 → rm_dispatch_plan (15-min intervals, all sheets in file)
```

*Path B — folder watcher (short-term, while WeCom app is pending approval):*
```
Windows Task Scheduler (08:00 daily) runs services/operating_assets/ingest.py
  → scans assets/operating/ for files modified since last run
  → same parsing + DB write logic as Path A
```

**Filename → asset_id mapping** (config file, not hardcoded):

| Filename pattern | Asset | Type |
|---|---|---|
| 零碳46 / 零碳46风电经营统计 | 零碳46风电 | wind (manual upload, migration) |
| 裕昭沙子坝 / 220kV裕昭 | 裕昭沙子坝 BESS | bess (WeCom) |
| 远景乌拉特 | 远景乌拉特 BESS | bess (WeCom) |
| 景怡查干哈达 | 景怡查干哈达 BESS | bess (WeCom) |
| 景通四益堂 | 景通四益堂 BESS | bess (WeCom) |
| 四子王旗 | 四子王旗 BESS | bess (WeCom) |
| 悦杭独贵 | 悦杭独贵 BESS | bess (WeCom) |
| 景蓝乌尔图 | 景蓝乌尔图 BESS | bess (WeCom) |

**Wind farm ingestion path (零碳46):**

The 零碳46风电经营统计_YYYYMMDD.xlsx is maintained manually and uploaded via the Tab 2 upload panel (no WeCom automation for wind farms at launch). The migration parser handles both historical backfill (full file) and incremental updates (new months appended to the Excel).

```
Upload 零碳46风电经营统计_YYYYMMDD.xlsx
  → filename matcher resolves asset_id (零碳46风电)
  → parser detects "wind_farm_ops" format by sheet name signature
  → 风场功率 sheet → rm_dispatch_plan (15-min, asset_id, interval_start, forecast_mw, actual_mw)
  → 结算明细 sheet → rm_position_volumes (15-min → hourly aggregation, all channel columns)
  → 市场价格 sheet → rm_forward_curves (TOU monthly reference by province/product)
  → 经营统计 sheet → rm_pnl_snapshots (monthly, including curtailment_mwh, curtailment_rate_pct,
                       curtailment_opportunity_cost_cny, equivalent_hours)
  → deduplication: skip rows where delivery_date+hour already in DB for this asset
  → report: N intervals loaded, M updated, K skipped (already current)
```

WeCom app credentials required for BESS: `corpid`, `corpsecret`, `token`, `encoding_aes_key` (from WeCom admin console → 自建应用).

### 3.3 Forward curves

```sql
CREATE TABLE rm_forward_curves (
  id               SERIAL PRIMARY KEY,
  province         TEXT NOT NULL,
  product          TEXT NOT NULL,
  curve_date       DATE NOT NULL,             -- date the curve was generated/uploaded
  delivery_date    DATE NOT NULL,             -- delivery date the price applies to
  delivery_hour    SMALLINT,                  -- NULL = daily average
  price_cny_kwh    NUMERIC(10,6) NOT NULL,
  source           TEXT NOT NULL CHECK (source IN ('lingfeng','manual','exchange')),
  uploaded_at      TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE (province, product, curve_date, delivery_date, delivery_hour, source)
);
```

### 3.4 Settlements

```sql
CREATE TABLE rm_settlements (
  id               SERIAL PRIMARY KEY,
  book_id          INTEGER NOT NULL REFERENCES rm_books(id),
  settlement_month DATE NOT NULL,             -- first day of settlement month
  file_name        TEXT NOT NULL,
  file_type        TEXT NOT NULL CHECK (file_type IN ('excel','csv','pdf')),
  upload_date      TIMESTAMPTZ DEFAULT NOW(),
  status           TEXT DEFAULT 'pending' CHECK (status IN ('pending','processed','flagged')),
  total_amount_cny NUMERIC(16,2),
  raw_data         JSONB
);

-- Settlement item categories derived from actual Trade Capture ledger (Trades sheet):
-- China BESS: charge_energy, discharge_energy, capacity_compensation, transmission,
--             govt_surcharges, system_operation, coal_capacity_charge, basic_fee, penalty, subsidy
-- GB BESS:    energy_trades, flex_fees, imbalance, market_redistribution, rule_charges,
--             transmission, capacity (payable/receivable), frequency (payable/receivable)
CREATE TABLE rm_settlement_items (
  id               SERIAL PRIMARY KEY,
  settlement_id    INTEGER NOT NULL REFERENCES rm_settlements(id),
  category         TEXT NOT NULL CHECK (category IN (
                     -- BESS: charge/discharge energy sides
                     'charge_energy','discharge_energy',
                     -- Wind/Solar: generation revenue (replaces discharge_energy for generation assets)
                     'generation_revenue',
                     -- All China assets
                     'capacity_compensation','bilateral_energy',
                     'transmission','govt_surcharges','system_operation',
                     'coal_capacity_charge','basic_fee',
                     -- Wind-specific: curtailment opportunity cost (弃风量 × RT node price)
                     'curtailment',
                     -- GB assets
                     'flex_fees','imbalance','market_redistribution',
                     'rule_charges','frequency',
                     -- Universal
                     'penalty','rebate','subsidy','other')),
  peak_period      TEXT CHECK (peak_period IN ('peak','valley','flat','super_peak')),  -- China TOU periods
  delivery_date    DATE,
  volume_mwh       NUMERIC(14,4),
  price_cny_kwh    NUMERIC(10,6),
  amount_cny       NUMERIC(16,2) NOT NULL,
  amount_receivable_cny NUMERIC(16,2),   -- 应收: entitlement per contract/formula
  amount_settled_cny    NUMERIC(16,2),   -- 实际结算: actual amount paid by grid/exchange
  amount_diff_cny       NUMERIC(16,2),   -- 差异: receivable − settled (reconciliation gap)
  counterparty     TEXT,
  notes            TEXT
);
```

### 3.5 P&L and risk snapshots

```sql
CREATE TABLE rm_pnl_snapshots (
  id               SERIAL PRIMARY KEY,
  book_id          INTEGER NOT NULL REFERENCES rm_books(id),
  snapshot_date    DATE NOT NULL,
  realized_cny     NUMERIC(16,2),
  unrealized_mtm_cny NUMERIC(16,2),
  spot_pnl_cny     NUMERIC(16,2),
  bilateral_pnl_cny NUMERIC(16,2),
  ancillary_pnl_cny NUMERIC(16,2),
  deviation_pnl_cny NUMERIC(16,2),
  -- Wind-specific KPIs (NULL for BESS/other asset types)
  curtailment_mwh               NUMERIC(14,4),  -- 弃风量 (MWh curtailed)
  curtailment_rate_pct          NUMERIC(6,4),   -- 弃风率 = curtailed / (curtailed + dispatched)
  curtailment_opportunity_cost_cny NUMERIC(16,2), -- 弃风量 × RT node price (opportunity loss)
  equivalent_hours              NUMERIC(8,2),   -- 等效满负荷小时数
  other_pnl_cny    NUMERIC(16,2),
  UNIQUE (book_id, snapshot_date)
);

CREATE TABLE rm_var_snapshots (
  id               SERIAL PRIMARY KEY,
  book_id          INTEGER NOT NULL REFERENCES rm_books(id),
  snapshot_date    DATE NOT NULL,
  var_1d_95_cny    NUMERIC(16,2),
  var_1d_99_cny    NUMERIC(16,2),
  var_10d_95_cny   NUMERIC(16,2),
  method           TEXT CHECK (method IN ('historical','parametric')),
  delta_mwh        NUMERIC(14,4),
  gamma            NUMERIC(14,6),
  vega             NUMERIC(14,6),
  UNIQUE (book_id, snapshot_date, method)
);
```

---

## 4. App Tabs

### Tab 1 — Asset Configuration

CRUD interface for `rm_assets` and `rm_books`.

**Asset form fields by type:**

| Field | Wind | Solar | BESS | Thermal |
|---|---|---|---|---|
| Name, province, status | ✓ | ✓ | ✓ | ✓ |
| Capacity (MW) | ✓ | ✓ | ✓ | ✓ |
| Commission date | ✓ | ✓ | ✓ | ✓ |
| Duration (h), DoD (%) | — | — | ✓ | — |
| Fuel type | — | — | — | ✓ |
| Notes | ✓ | ✓ | ✓ | ✓ |

Creating an asset auto-creates a linked `rm_books` record (book_type='asset'). Additional virtual/aggregated books can be created manually.

**Seed assets (known portfolio at launch):**

| Name | Type | Province | Capacity | Notes |
|---|---|---|---|---|
| 零碳46风电 | wind | Inner Mongolia (Mengxi) | 46 MW | Migration source: 零碳46风电经营统计_YYYYMMDD.xlsx |
| 裕昭沙子坝 | bess | Inner Mongolia (Mengxi) | TBD MW | WeCom daily report source |
| 远景乌拉特 | bess | Inner Mongolia (Mengxi) | TBD MW | WeCom daily report source |
| 景怡查干哈达 | bess | Inner Mongolia (Mengxi) | TBD MW | WeCom daily report source |
| 景通四益堂 | bess | Inner Mongolia (Mengxi) | TBD MW | WeCom daily report source |
| 四子王旗 | bess | Inner Mongolia (Mengxi) | TBD MW | WeCom daily report source |
| 悦杭独贵 | bess | Inner Mongolia (Mengxi) | TBD MW | WeCom daily report source |
| 景蓝乌尔图 | bess | Inner Mongolia (Mengxi) | TBD MW | WeCom daily report source |

**Asset list view:** Table of all assets with status, capacity, province, linked book. Edit/deactivate inline.

---

### Tab 2 — Settlement

**Reference data model:** The existing `Trade Capture.xlsx` (Trades sheet) is the current manual ETRM — it contains the canonical settlement ledger structure: Date, Market, Station Name, Capacity, Size, Buy/Sell, Transaction, Transactions Type, Volume (MWh), Price (¥/MWh), Total (¥). The app replicates and automates this.

**Source file formats:**

*BESS assets (Mengxi):*

| Format | Source | Content |
|---|---|---|
| PDF 上网电费结算单 | Grid company (one per station per month) | Charge/discharge energy by TOU period, T&D fees, surcharges |
| 容量补偿数据.xlsx | Provincial exchange | Capacity compensation: multi-station × multi-month, 应收/实际结算/差异 columns |
| 补贴.xlsx | Provincial government | Subsidy: station × month, same 应收/实际结算/差异 structure |
| Trade Capture.xlsx (Trades sheet) | Internal manual ETRM | Migration source: row-level settlement ledger |

*Wind assets (零碳46 and future wind farms):*

| Format | Source | Content | Sheet → DB table |
|---|---|---|---|
| 零碳46风电经营统计_YYYYMMDD.xlsx | Internal management file | Migration source containing 3 years of operation data | See column mapping below |

**零碳46风电经营统计 column mapping (migration parser):**

The file has 7 sheets. Only 3 are ingested:

| Sheet | Target table | Key columns |
|---|---|---|
| 风场功率 | `rm_dispatch_plan` | 日期+时间 → interval_start; D+1日前预测功率(MW) → forecast_mw; 实际出力(MW) → actual_mw |
| 预测&实际电量 | `rm_position_volumes` | 日期+时间 → delivery_date+hour; 预测交易电量 → nominated_mwh; 实际核算电量 → settled_mwh; 实际节点价 → rt_price_cny_mwh |
| 结算明细 | `rm_position_volumes` (enriched) | See full mapping below |
| 市场价格 | `rm_forward_curves` | 月份 + 谷/平/峰 → TOU reference prices by month |
| 经营统计 | `rm_pnl_snapshots` | Monthly KPI summary |

**结算明细 → rm_position_volumes full column mapping:**

| Excel column | rm_position_volumes field | Notes |
|---|---|---|
| 日期 + 时间 (15-min) | delivery_date, hour (aggregated × 4) | Sum volumes, average prices weighted by volume |
| 省调电量 (col 5) | settled_mwh | Total grid-dispatched energy |
| 省级实时价格 (col 7) | rt_price_cny_mwh | Provincial RT price |
| 省级实时节点价 (col 8) | market_price_cny_mwh | RT node price = settlement reference |
| 省级日前价格 (col 11) | da_price_cny_mwh | Provincial DA price |
| 省级日前电量 (col 12) | da_volume_mwh | DA cleared volume |
| 省级月内撮合价格 (col 13) | intramonth_match_price_cny_mwh | |
| 省级月内撮合电量 (col 14) | intramonth_match_volume_mwh | |
| 市场合约价格 (col 10) | annual_price_cny_mwh | Bilateral contract price |
| 收益 (col 18) | pnl_cny (hourly sum) | |
| 弃风量 (col 20) | deviation_grid_flow_mwh | Negative = curtailed MWh; maps to grid curtailment deviation |
| 弃风量×RT价 (col 22) | — | Stored as `curtailment_opportunity_cost_cny` in rm_pnl_snapshots |

**Mengxi wind settlement rule (province-specific):**
Inner Mongolia (Mengxi) market uses a min/max comparison rule across channels before applying settlement prices:
- If generation ≤ DA volume: settled at DA price
- Residual above DA: settled at RT node price
- Bilateral (annual) contract premium/discount applied on top
- Curtailment (弃风) = dispatched capacity − actual settled volume; valued at RT node price for opportunity cost

This rule is implemented in `libs/settlement/categorizer.py` as a province+asset_type dispatch: `province='inner_mongolia_mengxi', asset_type='wind'`.

**Upload panel:**
- Drag-and-drop file upload (PDF, Excel, CSV)
- Book selector + settlement month picker
- Parser auto-detects format by file type + header signature:
  - PDF → extract table via pdfplumber, map to canonical schema
  - 容量补偿 Excel → multi-station wide-format → melt to long format per station
  - 补贴 Excel → same wide→long transform
  - Trade Capture Trades sheet → direct row-level import (migration path, BESS)
  - 零碳46风电经营统计 Excel → 3-sheet migration parser (wind farm migration path)
- Fallback: manual column mapping UI
- Validation: required fields present, amounts balance, no duplicate station×month

**Analytics panel** (post-processing):
- Settlement summary: total amount, total volume, implied average price
- Line items table grouped by `category` and `peak_period`, sortable and filterable
- **Reconciliation view:** 应收 vs 实际结算 vs 差异 per category — highlights underpayment/overpayment by grid
- Month-over-month delta per category
- Anomaly flags: amount diff > 5% of receivable, volume deviation > 20% vs prior month

---

### Tab 3 — Realised P&L

**Controls:** Book selector (single book or aggregate), date range.

**P&L waterfall chart (Plotly bar waterfall):**

*BESS assets:*
```
Discharge energy revenue  (by TOU period: peak / valley / flat / super-peak)
− Charge energy cost      (by TOU period)
+ Capacity compensation   (容量补偿)
+ Bilateral contracts
+ Subsidy / rebate
− Transmission            (输电费)
− Govt surcharges         (政府性基金及附加)
− System operation fee    (系统运行费)
− Coal capacity charge    (煤电容量电价)
− Basic fee               (基本电费)
− Deviation penalties     (equipment, sysop, grid flow)
= Realised P&L
  [Unsuccessful bid opportunity cost shown as grey bar]
  [Settlement reconciliation gap: 应收 − 实际结算]
```

*Wind/Solar assets:*
```
Generation revenue        (DA volume × DA price)
+ Intramonth match        (月内撮合电量 × 撮合价)
+ Bilateral contract      (合约电量 × 合约价)
+ RT balancing            (residual settled at RT node price)
+ Capacity compensation   (容量补偿)
+ Subsidy / rebate
− Transmission / surcharges
− System operation fee
− Deviation penalties
= Realised P&L
  [Curtailment opportunity cost: 弃风量 × RT node price — shown as red bar]
  [Curtailment rate % shown as KPI alongside waterfall]
  [Settlement reconciliation gap: 应收 − 实际结算]
```

Waterfall variant is selected automatically based on `rm_assets.asset_type` for the selected book.

**Operational KPI panel** (from `rm_dispatch_daily` for BESS; `rm_dispatch_plan` + `rm_pnl_snapshots` for wind):

*BESS:*
- Daily/monthly: charge MWh, discharge MWh, conversion ratio, cycle count
- Average discharge per cycle, average daily cycles
- Auxiliary consumption (站用电)
- Dispatch time windows visualised as Gantt-style bars (charge/discharge windows per day)

*Wind/Solar:*
- Daily/monthly: actual generation MWh, D+1 forecast MWh, forecast accuracy %
- Equivalent full-load hours (等效满负荷小时数) vs annual plan
- **Curtailment dashboard:**
  - Curtailment rate % by day/month (line chart vs threshold)
  - Curtailment MWh by hour-of-day heatmap (which hours are most curtailed)
  - Curtailment opportunity cost ¥ (monthly bar; cumulative YTD)
  - Curtailment decomposition where data available: grid-commanded vs equipment vs other
  - Alert: curtailment rate > 10% in any month flagged in red
- D+1 forecast vs actual output scatter + time series overlay

**Volume deviation table:** Per-period (daily/monthly toggle) view of:
- Nominated → Cleared → Settled volumes
- Deviation split: bid / equipment / sysop / grid flow (MWh and ¥)

**Comparison views:**
- Asset vs asset (same province)
- Month vs month
- Actual vs prior year same period

---

### Tab 4 — Positions & MtM

**Daily ops upload (primary input for BESS asset books):**
- Upload 运营统计 Excel (【日期】内蒙储能电站运营统计.xlsx format)
- Parser detects station sheets, extracts: date, volumes, time windows, financial summary
- Writes to `rm_dispatch_daily`; time windows stored raw, expanded to hourly on demand
- Also accepts 电价日报 Excel for cross-referencing daily revenue vs cost

**Unified hourly position upload (exchange files → rm_position_volumes):**
- Upload accepts Excel or CSV from exchange (nomination, cleared, settlement volume files)
- `libs/settlement/parser.py` handles format detection and column mapping per province
- Output: one row per book per delivery hour with per-channel price + volume filled where available
- Columns mapped to the 6 trading channels: DA, RT, monthly_auction, monthly_listed, intramonth_match, annual
- Computed on write: `actual_price_cny_mwh` = Σ(channel_price × channel_volume) / Σ(channel_volume)
- Computed on write: `pnl_cny` = Σ(channel_volume × (channel_price − market_price)) per hour
- Validation: settled_mwh within tolerance of cleared_mwh; sysop deviations may exceed nomination
- Batch ID assigned per upload for traceability

**Unified hourly position view (table):**
The core view is the `rm_position_volumes` table as a date × hour grid:

| date | hour | 日前价格 | 实时价格 | 月度竞价 | 月度挂牌 | 月内撮合 | 年度价格 | 日前电量 | 实时电量 | 月度竞价电量 | 月度挂牌电量 | 月内撮合电量 | 年度电量 | market_price | actual_price | P&L |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|

Filterable by book, date range. Exportable as CSV for external analysis.

**Contract register (rm_positions):**
- List of individual position records by channel (annual bilateral, monthly auction lots, etc.)
- Inline add/edit for bilateral contracts; exchange-sourced positions auto-created on volume upload
- Shows remaining volume, entry price, open/closed status

**Forward curve panel:**
- Near-term curve: auto-pulled from LingFeng pipeline (`services/forward_curve/lingfeng_pull.py`)
- Long-term curve: manual CSV upload (`delivery_date, province, product, price_cny_mwh`)
- Curve viewer: term structure chart (price vs delivery date) by province + product
- Last-updated timestamp per source

**MtM dashboard:**

For each open position channel with remaining forward volume:
- Remaining volume (MWh)
- Entry/contract price vs current forward curve price (¥/MWh)
- Unrealised P&L = (forward_price − entry_price) × remaining_volume × direction_sign

Book-level MtM aggregate (sum across all channels) with 30-day time series.
Open exposure by channel: which channels are unhedged and at floating price risk.

---

### Tab 5 — VaR & Greeks

**Greeks panel** (per book, aggregated):
- Delta (net MWh exposure; ∂P&L / ∂price per ¥/MWh)
- Gamma (∂delta / ∂price; placeholder active when options are traded)
- Vega (∂P&L / ∂vol; placeholder active when options are traded)

Delta is computed directly from open position volumes. Gamma/Vega are computed from `libs/options/` when option positions exist; otherwise displayed as 0 with a note.

**VaR panel** (both methods shown side-by-side):

*Historical simulation:*
- Rolling 252-trading-day price history from `spot_prices_hourly`
- For each historical day: reprice open positions at that day's price → P&L scenario
- Sort scenarios → 5th percentile = 95% VaR
- Output: 1-day 95%, 1-day 99%, 10-day 95% (×√10 scaling)

*Parametric delta-normal:*
- σ_price from rolling 20-day price returns
- VaR = delta × σ_price × z × √t
- z: 1.645 (95%), 2.326 (99%)

**Backtesting chart:** Actual daily P&L (from snapshots) vs VaR band — exceedance markers highlighted.

**Stress scenarios:** User-defined shocks panel:
- Spot price ±X% 
- Bilateral benchmark ±Y%
- Applied across all open positions → scenario P&L table

---

### Tab 6 — Agent

Claude claude-sonnet-4-6 agent following the platform agent pattern (DB-backed memory, domain grounding, no external knowledge contamination).

**Tools:**
- `get_book_pnl(book_id, start_date, end_date)` → P&L breakdown by category
- `get_position_mtm(book_id)` → current MtM summary with unrealised P&L
- `get_var(book_id, method)` → current VaR figures
- `get_settlement_summary(book_id, month)` → settlement line item summary
- `get_deviation_analysis(book_id, start_date, end_date)` → volume waterfall + deviation attribution
- `get_asset_list()` → registered assets and books

**Memory app key:** `asset_risk`

---

## 5. Instrument Extensibility

The `rm_positions.instrument_type` enum includes `futures`, `option`, `forward`, `profile` even though only `bilateral` and `spot` are used at launch. When futures/options are added:

- Futures: `price_cny_mwh` = futures price; `start_date`/`end_date` = delivery period; `channel` = 'DA' or 'monthly_auction'
- Options: additional columns `strike_cny_mwh`, `option_type (call|put)`, `expiry_date` added via migration
- Greeks computation routes through `libs/options/black_scholes.py` for options, `libs/risk/greeks.py` delta-only for linear instruments
- VaR Monte Carlo (Approach C from methodology discussion) activated when options book is non-trivial

The `rm_position_volumes` unified hourly schema accommodates all instrument types — futures and forward volumes flow into the same channel columns as bilateral/spot, with prices representing the contract/settlement price for that channel.

---

## 6. Deployment

```
ECR repo:     bess-asset-risk
ECS service:  bess-platform-asset-risk-svc
ALB path:     /asset-risk/*
Port:         8512
Auth:         Cognito (existing pool)
```

Dockerfile mirrors existing app pattern (Python 3.11, streamlit entrypoint, PGURL env var).

---

## 7. Build sequence

1. DB migrations (create all `rm_` tables)
2. `libs/settlement/parser.py` — core PDF/Excel ingestion engine
3. `libs/settlement/categorizer.py` — category mapping rules
4. `libs/risk/mtm.py` + `libs/risk/pnl.py` — valuation and attribution
5. `libs/risk/var.py` + `libs/risk/greeks.py` — risk metrics
6. `services/forward_curve/` — LingFeng pull + manual upload
7. App tabs 1–5 (no agent dependency)
8. Tab 6 agent (depends on all tools being stable)
9. Docker + ECS deploy
