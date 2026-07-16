# App 2 — Retail Risk Management: Design Spec

**Date:** 2026-07-16  
**Status:** Approved  
**App path:** `apps/retail-risk/`  
**Route:** `/retail-risk/*`  
**Port:** 8513  
**ECR repo:** `bess-retail-risk`

---

## 1. Purpose

A risk management cockpit for power retail trading books (load books). Covers customer relationship management, retail P&L attribution, procurement position MtM, VaR/Greeks, and settlement consolidation — all oriented to the retail/load side of the business.

**Users:** Retail trading team, account managers, risk officers managing customer portfolios and procurement books.

---

## 2. Architecture

### 2.1 Platform fit

Same pattern as App 1 (Asset Risk): standalone Streamlit app, ECS Fargate, Cognito auth, shared `marketdata` PostgreSQL schema. Shares all `libs/risk/` and `libs/settlement/` modules with App 1.

### 2.2 Shared libs (from App 1)

```
libs/risk/mtm.py          ← reused: forward curve lookup + position valuation
libs/risk/var.py          ← reused: historical + parametric VaR
libs/risk/pnl.py          ← reused: P&L waterfall decomposition
libs/risk/greeks.py       ← reused: delta/gamma/vega aggregation
libs/settlement/parser.py ← reused: PDF/Excel ingestion
libs/settlement/categorizer.py ← retail-specific category rules added
```

No new shared libs needed — all computation reuses App 1's `libs/risk/` layer.

---

## 3. Database Schema

Retail-specific tables extend the `rm_` schema established by App 1. Asset/book/position/settlement/VaR tables are shared (load books use `book_type='load'` in `rm_books`).

The `rm_position_volumes` table (defined in App 1 schema) is shared and stores procurement position data for retail books using the same date × hour × 6-channel structure.

### 3.1 Customer master

```sql
CREATE TABLE rm_customers (
  id                    SERIAL PRIMARY KEY,
  name                  TEXT NOT NULL,
  province              TEXT NOT NULL,
  district              TEXT,
  customer_type         TEXT CHECK (customer_type IN ('industrial','commercial','residential')),
  voltage_level         TEXT,                    -- e.g. '10kV', '35kV', '110kV'
  contracted_capacity_kva NUMERIC(12,2),
  bd_name               TEXT,                    -- account manager
  customer_source       TEXT,                    -- acquisition channel
  -- Revenue share channel (from 用户渠道信息)
  channel_name          TEXT,                    -- channel/渠道 (e.g. BD company name, platform)
  fixed_spread_cny_mwh  NUMERIC(10,4),           -- 固定差价 ¥/MWh paid to channel
  revenue_share_ratio   NUMERIC(6,4),            -- 分成 ratio (e.g. 0.9 = channel keeps 90%)
  status                TEXT DEFAULT 'active' CHECK (status IN ('active','prospect','churned')),
  notes                 TEXT,
  created_at            TIMESTAMPTZ DEFAULT NOW()
);
```

### 3.2 Customer contracts

```sql
CREATE TABLE rm_customer_contracts (
  id                    SERIAL PRIMARY KEY,
  customer_id           INTEGER NOT NULL REFERENCES rm_customers(id),
  contract_ref          TEXT,                    -- BD contract code (e.g. XSXGK-HYL1886)
  energy_source         TEXT CHECK (energy_source IN ('wind','solar','bess','mixed')),
  contract_type         TEXT NOT NULL CHECK (contract_type IN
                          ('fixed','indexed','peak_offpeak','indexed_band')),
  -- price fields (populated based on contract_type)
  price_cny_mwh         NUMERIC(10,4),           -- fixed price contracts
  price_formula         JSONB,                   -- indexed: {type, spread_cny_mwh, reference}
                                                 -- e.g. {type: "indexed", spread: 2.0}
                                                 --      {type: "indexed_band", floor: 360, cap: 420}
  peak_price_cny_mwh    NUMERIC(10,4),           -- peak-offpeak contracts
  offpeak_price_cny_mwh NUMERIC(10,4),
  -- adjustment factors (province-specific, e.g. Hunan K1/K2/K3)
  k1                    NUMERIC(8,4),
  k2                    NUMERIC(8,4),
  k3                    NUMERIC(8,4),
  -- generation asset binding
  bound_asset_id        INTEGER REFERENCES rm_assets(id),
  -- contract terms
  start_date            DATE NOT NULL,
  end_date              DATE NOT NULL,
  signing_date          DATE,
  annual_forecast_mwh   NUMERIC(14,4),
  monthly_forecast      JSONB,                   -- {1: 127.9, 2: 95.0, ..., 12: 110.2}
  contract_status       TEXT DEFAULT 'active' CHECK (contract_status IN
                          ('active','expired','pending','terminated')),
  notes                 TEXT,
  created_at            TIMESTAMPTZ DEFAULT NOW()
);
```

### 3.3 Customer load profiles (daily × 15-min or hourly)

```sql
-- Stores customer-level load at hourly granularity (aggregated from 15-min where needed).
-- Sources:
--   山东-style: daily .xls per day, rows=customers, columns=hours (0–23)
--   江苏-style: daily CSV from 江苏电力交易中心, format: 日期,户号,用户名称,售电公司名称,
--               then 96 × 15-min columns (00:15…24:00). Aggregated to hourly on ingest.
CREATE TABLE rm_customer_profiles (
  id               SERIAL PRIMARY KEY,
  customer_id      INTEGER NOT NULL REFERENCES rm_customers(id),
  profile_date     DATE NOT NULL,
  hour             SMALLINT NOT NULL CHECK (hour BETWEEN 0 AND 23),
  load_mwh         NUMERIC(10,4),               -- actual metered load (hourly)
  nominated_mwh    NUMERIC(10,4),               -- load nomination submitted
  settled_mwh      NUMERIC(10,4),               -- settled load
  upload_batch_id  TEXT,
  UNIQUE (customer_id, profile_date, hour)
);
```

### 3.4 CRM import config (province-keyed column mapping)

```sql
CREATE TABLE rm_crm_import_configs (
  id               SERIAL PRIMARY KEY,
  province         TEXT NOT NULL UNIQUE,
  column_map       JSONB NOT NULL,               -- maps source column names → canonical fields
  notes            TEXT,
  updated_at       TIMESTAMPTZ DEFAULT NOW()
);
```

Seed configs for known provinces (Hunan, Hubei, Zhejiang, Shandong) based on 各省份台账.xlsx column layouts. New provinces added via UI without code changes.

---

## 4. Procurement Positions (Shared Schema)

Retail load books use the same `rm_positions` and `rm_position_volumes` tables as App 1 asset books, with `book_type='load'` in `rm_books`. The 6 trading channels map to the Chinese retail electricity procurement channels:

| Channel | Chinese name | Typical use for load book |
|---|---|---|
| `annual` | 年度双边 | Annual bilateral procurement contracts |
| `monthly_auction` | 月度竞价 | Monthly exchange auction (竞价) |
| `monthly_listed` | 月度挂牌 | Monthly OTC listed trades (挂牌) |
| `intramonth_match` | 月内撮合 | Intramonth bilateral matching (滚动撮合) |
| `DA` | 日前 | Day-ahead spot market |
| `RT` | 实时 | Real-time balancing market |

Green power procurement (绿电) is recorded as a separate position with `energy_source='wind'` or `energy_source='solar'` in the contract, using the appropriate channel.

The unified `rm_position_volumes` table captures the full date × hour grid with per-channel price and volume for each load book, identical to the asset book view:

| date | hour | 日前价格 | 实时价格 | 月度竞价 | 月度挂牌 | 月内撮合 | 年度价格 | 日前电量 | 实时电量 | 月度竞价电量 | 月度挂牌电量 | 月内撮合电量 | 年度电量 | market_price | actual_price | P&L |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|

---

## 5. CRM Import Flow

Source: `各省份台账.xlsx` (one sheet per province).

```
Upload Excel → detect sheets (each sheet = one province)
  → for each sheet:
      look up rm_crm_import_configs[province]
      if config exists: auto-map columns
      if no config: prompt user for manual column mapping → save as new config
  → validate: required fields present, date formats parseable, numeric prices
  → upsert rm_customers (match on name + province; populate channel_name/fixed_spread/revenue_share)
  → upsert rm_customer_contracts (match on customer_id + contract_ref + start_date)
  → report: N customers updated, M new, K errors
```

**用户渠道信息 import:** From 用户渠道信息.xlsx (per province, per month):
- Columns: customer name/ID, 渠道 (channel), 固定差价 (fixed spread ¥/MWh), 分成 (revenue share ratio)
- Maps to `rm_customers.channel_name`, `fixed_spread_cny_mwh`, `revenue_share_ratio`
- Can be uploaded separately or as part of the province data bundle

---

## 6. Customer Load Profile Ingestion

Two source formats supported:

**Format A — 山东-style daily .xls (hourly):**
```
File: 日用电曲线/YYYY-MM-DD.xls (one file per day)
Structure: rows = customers, columns = hour 1..24
Ingest: melt to long format → rm_customer_profiles (hour 0..23 = column 1..24)
Match customer: by name or meter ID lookup against rm_customers
```

**Format B — 江苏-style daily CSV (15-min from exchange):**
```
File: 营销历史数据_YYYY-MM-DD (N).csv
Columns: 日期, 户号, 用户名称, 售电公司名称, 00:15, 00:30, ..., 24:00 (96 intervals)
Ingest: group 4 × 15-min intervals per hour → sum → rm_customer_profiles.load_mwh
Match customer: by 户号 (meter ID) → rm_customers lookup; create if missing
```

Both formats: upload via drag-and-drop in Tab 1 (load profile sub-tab) or Tab 2 (bulk upload). Duplicate date×customer detection → confirm-overwrite prompt.

---

## 7. App Tabs

### Tab 1 — CRM

**Customer registry table:** All customers across provinces. Filterable by province, BD, status, contract type. Click-through to customer detail.

**Customer detail view:**
- Summary card: name, province, BD, channel/渠道, contracted capacity, status
- **Contracts sub-tab:** all contracts for this customer; inline edit; add new contract form
- **Load profile sub-tab:** upload daily×hourly profiles (Format A or B); heatmap of load (hour × date); daily consumption trend; peak demand; profile vs contracted volume comparison
- **P&L contribution sub-tab:** this customer's realised margin (retail revenue − procurement cost allocation) by month

**Portfolio summary panel** (top of tab, always visible):
- Total active customers, total contracted MWh
- Distribution by: province, contract type (fixed/indexed/peak-offpeak), BD, channel
- Customers at risk: contracts expiring within 90 days, customers with negative margin in last 3 months

**CRM import:** Drag-and-drop 各省份台账.xlsx → auto-parse all province sheets → diff view (what changes) → confirm import.
**渠道信息 import:** Separate upload for 用户渠道信息.xlsx to update channel/spread/share fields.

---

### Tab 2 — Settlement

Same upload/analytics structure as App 1 Tab 2, with retail-specific categories:

**Line item categories for load books:**
- `retail_revenue` — billed revenue from customers (metered × retail tariff)
- `energy_procurement` — wholesale energy procurement cost (charge: peak/valley/flat/super-peak)
- `capacity_compensation` — capacity compensation pass-through to customers
- `transmission_uos` — transmission use-of-system charge
- `govt_surcharges` — government funds and surcharges pass-through
- `system_operation` — system operation fee allocation
- `coal_capacity_charge` — coal capacity charge pass-through
- `basic_fee` — basic power fee
- `penalty` — deviation penalties from load forecast error
- `rebate` — government subsidies, policy rebates
- `other`

**Settlement hierarchy (山东 sample structure):**
```
月度结算 (monthly)
  → 零售侧结算单.xlsx — per-customer monthly ledger
      Columns: 月份, 售电公司, 最终用户名称, customer_id, meter_id, 底层合同,
               电量(MWh), 金额(¥), deviation charges
  → 日清算单/ — daily per-company settlement files (date-named)
      One file per day: intraday P&L components
```

Both levels imported into `rm_settlement_items` with appropriate `settlement_month` and `delivery_date` fields.

**Settlement reconciliation:** Same 应收/实际结算/差异 three-column structure:
`amount_receivable_cny`, `amount_settled_cny`, `amount_diff_cny` from `rm_settlement_items`.

**Retail-specific reconciliation panel:**
Three-way match per customer per settlement period:
- Contracted volume (from `rm_customer_contracts.monthly_forecast`)
- Metered/settled volume (from `rm_customer_profiles.settled_mwh`)
- Settlement invoice volume (from `rm_settlement_items`)

Discrepancies flagged with traffic-light status (green < 2%, amber 2–5%, red > 5%).

**Customer P&L contribution table:** Per-customer margin = retail revenue − procurement cost allocation, sorted by margin descending. Identifies loss-making customers.

---

### Tab 3 — Realised P&L

**Load book P&L waterfall:**
```
Retail Revenue              (settled_mwh × retail tariff per customer contract)
  ├─ 售电分成               revenue share from power sales (split with channel)
  ├─ 服务费                 service fee income
  ├─ 滚搓 (rolling match)   P&L from intramonth match trading (buy low, sell high)
  └─ 代理用电量              agency electricity volume margin
− Energy Procurement        (settled_mwh × volume-weighted procurement price across channels)
− Transmission/Distrib      (transmission UoS + distribution charges)
− Ancillary Allocation      (system ancillary cost pass-through)
− Tax / Surcharges
− Deviation Penalties       (load forecast error penalties)
+ Rebates / Subsidies
= Net Retail Margin
```

**Margin analysis:**
- Per customer (who is profitable/underwater)
- Per contract type (fixed vs indexed: shows which type is winning in current price environment)
- Per province
- Per BD / channel (account manager + channel performance)

**Volume deviation waterfall** (load side):
- Load forecast → nominated load → metered/settled load
- Deviation drivers: forecast error, customer consumption change, system operator adjustment
- Penalty/rebate from each deviation type

**Comparison:** Month vs month, province vs province, BD vs BD.

---

### Tab 4 — Positions & MtM

**Unified hourly position view** (same as App 1 Tab 4):
The `rm_position_volumes` table shown as a date × hour grid for the selected load book, with all 6 procurement channels side-by-side.

**Procurement coverage ratio:**
- Forward-bought volume ÷ total contracted customer load by month
- Chart: coverage % over the next 12 months
- Colour coding: green ≥ 90%, amber 70–90%, red < 70%

**Open exposure (unhedged load):**
- Unhedged load volume = contracted customer load − sum(all channel volumes in rm_position_volumes)
- Mark at current spot forward price
- Unrealised cost-at-risk from spot price moves

**Position upload:** Exchange file upload → parser → writes to rm_position_volumes per channel column. Handles:
- 山东/浙江/安徽 annual bilateral (年度双边) confirmation files
- Monthly auction (月度竞价) cleared results
- Intramonth match (月内撮合) confirmation files
- DA/RT settlement (日前/实时) daily files

**MtM:** Same as App 1 — entry price vs forward curve → unrealised P&L per channel and book total.

---

### Tab 5 — VaR & Greeks

**Same methodology as App 1** (historical simulation + parametric delta-normal, 1d/10d, 95%/99%).

**Retail-specific VaR components:**

*Price VaR:* Risk from open (unhedged) procurement positions moving against the book. Identical computation to App 1.

*Load uncertainty VaR:* Additional VaR component from load forecast error:
- σ_load = rolling standard deviation of (forecast_load − actual_load) per customer/book
- Load VaR = σ_load × current_spot_forward × z
- Shown separately from price VaR; total VaR = √(price_VaR² + load_VaR²) assuming independence

*Basis risk:* Retail tariff (often fixed ¥/MWh) vs floating procurement cost → net delta of the retail book is the unhedged position. Shown as a bar by contract type and channel.

**Greeks:** Delta = net unhedged procurement position in MWh (contracted customer load − total procured volume). Gamma/Vega = 0 until options are traded in the procurement book.

---

### Tab 6 — Agent

Claude claude-sonnet-4-6 agent, same pattern as App 1.

**Tools:**
- `get_retail_margin(customer_id, start_date, end_date)` → margin breakdown
- `get_procurement_coverage(book_id, month)` → coverage ratio and open exposure by channel
- `get_customer_pnl_ranking(province, month)` → customers sorted by margin
- `get_load_deviation_analysis(book_id, start_date, end_date)` → forecast vs actual
- `get_contract_expiry_pipeline(days_ahead)` → contracts expiring in N days
- `get_settlement_reconciliation(customer_id, month)` → three-way volume match
- `get_position_volumes(book_id, start_date, end_date)` → unified hourly channel view

**Memory app key:** `retail_risk`

---

## 8. Province-Specific Price Structures

Different provinces use different retail pricing conventions. The `price_formula` JSONB field handles all variants:

| Province | Observed structure | Formula JSON |
|---|---|---|
| Hunan | Fixed ¥/MWh + K1/K2/K3 adjustments | `{type: "fixed_with_k", base: 370, k1: 0.5, k2: 0.5}` |
| Hubei | Fixed ¥/MWh, no adjustments | `{type: "fixed", price: 399}` |
| Zhejiang | Indexed (benchmark + spread), separate day/night | `{type: "indexed", spread: 2.0}` or `{type: "peak_offpeak", day: 370, night: null}` |
| Shandong | Indexed band or fixed, with monthly volume breakdown | `{type: "indexed_band", floor: 360, cap: 420}` |
| Jiangsu | Fixed or indexed, 15-min meter data from 江苏电力交易中心 | `{type: "fixed", price: 380}` |
| Others | Configurable via `rm_crm_import_configs` | flexible |

Retail P&L computation reads `price_formula` and dispatches to the appropriate pricing function.

### Province load curve ingestion formats

| Province | Load file format | Granularity | Ingest path |
|---|---|---|---|
| Shandong | 日用电曲线/YYYY-MM-DD.xls — rows=customers, cols=H1..H24 | Hourly | Format A |
| Jiangsu | 营销历史数据_YYYY-MM-DD (N).csv — 96×15-min, 户号+用户名称 | 15-min → hourly | Format B |
| Others | Configurable; fallback to manual column mapping | As available | Format A or B |

---

## 9. Deployment

```
ECR repo:     bess-retail-risk
ECS service:  bess-platform-retail-risk-svc
ALB path:     /retail-risk/*
Port:         8513
Auth:         Cognito (existing pool)
```

---

## 10. Build sequence

DB migrations (retail-specific tables) depend on App 1 migrations running first (shared `rm_assets`, `rm_books`, `rm_position_volumes` etc.).

1. `rm_customers`, `rm_customer_contracts`, `rm_customer_profiles`, `rm_crm_import_configs` migrations
2. CRM import parser (`各省份台账.xlsx` → DB, province column mapping)
3. 渠道信息 import (用户渠道信息.xlsx → channel/spread/share fields)
4. Load profile ingest (Format A: Shandong .xls hourly; Format B: Jiangsu 15-min CSV → hourly)
5. Tab 1 — CRM (registry, detail view, import)
6. Tab 2 — Settlement (reuses `libs/settlement/`, adds retail categories + settlement hierarchy)
7. Tab 3 — Realised P&L (reuses `libs/risk/pnl.py`, adds retail waterfall + customer breakdown)
8. Tab 4 — Positions & MtM (reuses App 1 unified rm_position_volumes, adds coverage ratio + open exposure)
9. Tab 5 — VaR & Greeks (reuses `libs/risk/var.py`, adds load uncertainty VaR)
10. Tab 6 — Agent
11. Docker + ECS deploy

App 2 build starts after App 1 `libs/risk/` and `libs/settlement/` are stable (not necessarily after App 1 is fully deployed).
