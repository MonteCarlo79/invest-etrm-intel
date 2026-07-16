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
  product          TEXT NOT NULL CHECK (product IN ('DA','RT','ancillary','capacity')),
  direction        TEXT NOT NULL CHECK (direction IN ('buy','sell')),
  volume_mwh       NUMERIC(14,4) NOT NULL,
  price_cny_kwh    NUMERIC(10,6),
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

-- Hourly position volumes for settlement reconciliation and MtM
-- Populated by: (a) expanding dispatch_daily time windows, or (b) direct hourly file upload
CREATE TABLE rm_position_volumes (
  id                    SERIAL PRIMARY KEY,
  position_id           INTEGER NOT NULL REFERENCES rm_positions(id),
  delivery_date         DATE NOT NULL,
  hour                  SMALLINT NOT NULL CHECK (hour BETWEEN 0 AND 23),
  -- three-layer volume waterfall
  nominated_mwh         NUMERIC(10,4),
  cleared_mwh           NUMERIC(10,4),
  settled_mwh           NUMERIC(10,4),
  -- deviation attribution (nomination → cleared)
  deviation_bid_mwh     NUMERIC(10,4) DEFAULT 0,  -- unsuccessful bidding
  -- deviation attribution (cleared → settled)
  deviation_equipment_mwh  NUMERIC(10,4) DEFAULT 0,  -- equipment/system failure
  deviation_sysop_mwh      NUMERIC(10,4) DEFAULT 0,  -- system operator modification
  deviation_grid_flow_mwh  NUMERIC(10,4) DEFAULT 0,  -- grid power flow adjustment
  -- prices
  da_price_cny_kwh      NUMERIC(10,6),
  rt_price_cny_kwh      NUMERIC(10,6),
  settlement_price_cny_kwh NUMERIC(10,6),
  upload_batch_id       TEXT,
  UNIQUE (position_id, delivery_date, hour)
);
```

**Volume ingestion path:**
- Daily 运营统计 Excel → `rm_dispatch_daily` (primary source; charge/discharge windows stored as text arrays)
- Time windows expanded to hourly slots on demand: e.g., charge "09:46–14:33" → hours 9, 10, 11, 12, 13, 14 (partial hours prorated by minutes)
- When hourly PDF/Excel is available (from exchange), it writes directly to `rm_position_volumes` with full nomination/cleared/settled waterfall
- Monthly P&L KPIs cross-referenced between `rm_dispatch_daily` (operational) and `rm_settlement_items` (financial settlement)

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
                     'charge_energy','discharge_energy',
                     'capacity_compensation','bilateral_energy',
                     'transmission','govt_surcharges','system_operation',
                     'coal_capacity_charge','basic_fee',
                     'flex_fees','imbalance','market_redistribution',
                     'rule_charges','frequency',
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

**Asset list view:** Table of all assets with status, capacity, province, linked book. Edit/deactivate inline.

---

### Tab 2 — Settlement

**Reference data model:** The existing `Trade Capture.xlsx` (Trades sheet) is the current manual ETRM — it contains the canonical settlement ledger structure: Date, Market, Station Name, Capacity, Size, Buy/Sell, Transaction, Transactions Type, Volume (MWh), Price (¥/MWh), Total (¥). The app replicates and automates this.

**Three source file formats for China BESS:**

| Format | Source | Content |
|---|---|---|
| PDF 上网电费结算单 | Grid company (one per station per month) | Charge/discharge energy by TOU period, T&D fees, surcharges |
| 容量补偿数据.xlsx | Provincial exchange | Capacity compensation: multi-station × multi-month, 应收/实际结算/差异 columns |
| 补贴.xlsx | Provincial government | Subsidy: station × month, same 应收/实际结算/差异 structure |

**Upload panel:**
- Drag-and-drop file upload (PDF, Excel, CSV)
- Book selector + settlement month picker
- Parser auto-detects format by file type + header signature:
  - PDF → extract table via pdfplumber, map to canonical schema
  - 容量补偿 Excel → multi-station wide-format → melt to long format per station
  - 补贴 Excel → same wide→long transform
  - Trade Capture Trades sheet → direct row-level import (migration path)
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
  [Unsuccessful bid opportunity cost shown as grey bar — opportunity loss]
  [Settlement reconciliation gap: 应收 − 实际结算]
```

**Operational KPI panel** (from `rm_dispatch_daily`):
- Daily/monthly: charge MWh, discharge MWh, conversion ratio, cycle count
- Average discharge per cycle, average daily cycles
- Auxiliary consumption (站用电)
- Dispatch time windows visualised as Gantt-style bars (charge/discharge windows per day)

**Volume deviation table:** Per-period (daily/monthly toggle) view of:
- Nominated → Cleared → Settled volumes
- Deviation split: bid / equipment / sysop / grid flow (MWh and ¥)

**Comparison views:**
- Asset vs asset (same province)
- Month vs month
- Actual vs prior year same period

---

### Tab 4 — Positions & MtM

**Daily ops upload (primary input):**
- Upload 运营统计 Excel (【日期】内蒙储能电站运营统计.xlsx format)
- Parser detects station sheets, extracts: date, volumes, time windows, financial summary
- Writes to `rm_dispatch_daily`; time windows stored raw, expanded to hourly on demand
- Also accepts 电价日报 Excel for cross-referencing daily revenue vs cost

**Hourly position upload (when available from exchange):**
- Upload accepts PDF or Excel (nomination, cleared, settlement volume files from exchange)
- `libs/settlement/parser.py` handles format detection and column mapping
- Writes to `rm_position_volumes` with full nomination/cleared/settled waterfall
- Validation: settlement within tolerance of cleared; sysop modifications may increase cleared above nominated
- Batch ID assigned per upload for traceability

**Forward curve panel:**
- Near-term curve: auto-pulled from LingFeng pipeline (`services/forward_curve/lingfeng_pull.py`)
- Long-term curve: manual CSV upload (`delivery_date, province, product, price_cny_kwh`)
- Curve viewer: term structure chart (price vs delivery date) by province + product
- Last-updated timestamp per source

**MtM dashboard:**

For each open position:
- Remaining volume (MWh)
- Entry price vs current forward price (¥/kWh)
- Unrealised P&L = (forward_price − entry_price) × remaining_volume × direction_sign

Book-level MtM aggregate with 30-day time series.

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

- Futures: `price_cny_kwh` = futures price; `start_date`/`end_date` = delivery period
- Options: additional columns `strike_cny_kwh`, `option_type (call|put)`, `expiry_date` added via migration
- Greeks computation routes through `libs/options/black_scholes.py` for options, `libs/risk/greeks.py` delta-only for linear instruments
- VaR Monte Carlo (Approach C from methodology discussion) activated when options book is non-trivial

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
