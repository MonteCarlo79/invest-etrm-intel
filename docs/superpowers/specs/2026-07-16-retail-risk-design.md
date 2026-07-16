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

### 3.3 Customer load profiles (daily × hourly)

```sql
CREATE TABLE rm_customer_profiles (
  id               SERIAL PRIMARY KEY,
  customer_id      INTEGER NOT NULL REFERENCES rm_customers(id),
  profile_date     DATE NOT NULL,
  hour             SMALLINT NOT NULL CHECK (hour BETWEEN 0 AND 23),
  load_mwh         NUMERIC(10,4),               -- actual metered load
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

## 4. CRM Import Flow

Source: `各省份台账.xlsx` (one sheet per province).

```
Upload Excel → detect sheets (each sheet = one province)
  → for each sheet:
      look up rm_crm_import_configs[province]
      if config exists: auto-map columns
      if no config: prompt user for manual column mapping → save as new config
  → validate: required fields present, date formats parseable, numeric prices
  → upsert rm_customers (match on name + province)
  → upsert rm_customer_contracts (match on customer_id + contract_ref + start_date)
  → report: N customers updated, M new, K errors
```

---

## 5. App Tabs

### Tab 1 — CRM

**Customer registry table:** All customers across provinces. Filterable by province, BD, status, contract type. Click-through to customer detail.

**Customer detail view:**
- Summary card: name, province, BD, contracted capacity, status
- **Contracts sub-tab:** all contracts for this customer; inline edit; add new contract form
- **Load profile sub-tab:** upload daily×hourly profiles (CSV or Excel); heatmap of load (hour × date); daily consumption trend; peak demand; profile vs contracted volume comparison
- **P&L contribution sub-tab:** this customer's realised margin (retail revenue − procurement cost allocation) by month

**Portfolio summary panel** (top of tab, always visible):
- Total active customers, total contracted MWh
- Distribution by: province, contract type (fixed/indexed/peak-offpeak), BD
- Customers at risk: contracts expiring within 90 days, customers with negative margin in last 3 months

**CRM import:** Drag-and-drop 各省份台账.xlsx → auto-parse all province sheets → diff view (what changes) → confirm import.

---

### Tab 2 — Settlement

Same upload/analytics structure as App 1 Tab 2, with retail-specific categories:

**Line item categories for load books:**
- `energy_procurement` — wholesale energy cost
- `retail_revenue` — billed revenue from customers
- `transmission_uos` — transmission use-of-system charge
- `distribution_charge` — distribution network charge
- `ancillary_allocation` — system ancillary cost pass-through
- `tax` — value-added tax, surcharges
- `penalty` — deviation penalties
- `rebate` — government subsidies, policy rebates
- `other`

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
Retail Revenue           (settled_mwh × retail tariff per customer contract)
− Energy Procurement     (settled_mwh × wholesale settlement price)
− Transmission/Distrib   (transmission UoS + distribution charges)
− Ancillary Allocation   (system ancillary cost pass-through)
− Tax / Surcharges
− Deviation Penalties    (load forecast error penalties)
+ Rebates / Subsidies
= Net Retail Margin
```

**Margin analysis:**
- Per customer (who is profitable/underwater)
- Per contract type (fixed vs indexed: shows which type is winning in current price environment)
- Per province
- Per BD (account manager performance)

**Volume deviation waterfall** (load side):
- Load forecast → nominated load → metered/settled load
- Deviation drivers: forecast error, customer consumption change, system operator adjustment
- Penalty/rebate from each deviation type

**Comparison:** Month vs month, province vs province, BD vs BD.

---

### Tab 4 — Positions & MtM

**Procurement positions:** What the retail book has bought forward to cover customer load obligations.

Same upload flow as App 1 Tab 4 (PDF/Excel from exchange, nomination→cleared→settled waterfall).

**Retail-specific views:**

*Procurement coverage ratio:*
- Forward-bought volume ÷ total contracted customer load by month
- Chart: coverage % over the next 12 months
- Colour coding: green ≥ 90%, amber 70–90%, red < 70%

*Open exposure (unhedged load):*
- Unhedged load volume = contracted load − forward-bought volume
- Mark at current spot forward price
- Unrealised cost-at-risk from spot price moves

**MtM:** Same as App 1 — entry price vs forward curve → unrealised P&L per position and book total.

---

### Tab 5 — VaR & Greeks

**Same methodology as App 1** (historical simulation + parametric delta-normal, 1d/10d, 95%/99%).

**Retail-specific VaR components:**

*Price VaR:* Risk from open (unhedged) procurement positions moving against the book. Identical computation to App 1.

*Load uncertainty VaR:* Additional VaR component from load forecast error:
- σ_load = rolling standard deviation of (forecast_load − actual_load) per customer/book
- Load VaR = σ_load × current_spot_forward × z
- Shown separately from price VaR; total VaR = √(price_VaR² + load_VaR²) assuming independence

*Basis risk:* Retail tariff (often fixed ¥/MWh) vs floating procurement cost → net delta of the retail book is the unhedged position. Shown as a bar by contract type.

**Greeks:** Delta = net unhedged procurement position in MWh. Gamma/Vega = 0 until options are traded in the procurement book.

---

### Tab 6 — Agent

Claude claude-sonnet-4-6 agent, same pattern as App 1.

**Tools:**
- `get_retail_margin(customer_id, start_date, end_date)` → margin breakdown
- `get_procurement_coverage(book_id, month)` → coverage ratio and open exposure
- `get_customer_pnl_ranking(province, month)` → customers sorted by margin
- `get_load_deviation_analysis(book_id, start_date, end_date)` → forecast vs actual
- `get_contract_expiry_pipeline(days_ahead)` → contracts expiring in N days
- `get_settlement_reconciliation(customer_id, month)` → three-way volume match

**Memory app key:** `retail_risk`

---

## 6. Province-Specific Price Structures

Different provinces use different retail pricing conventions. The `price_formula` JSONB field handles all variants:

| Province | Observed structure | Formula JSON |
|---|---|---|
| Hunan | Fixed ¥/MWh + K1/K2/K3 adjustments | `{type: "fixed_with_k", base: 370, k1: 0.5, k2: 0.5}` |
| Hubei | Fixed ¥/MWh, no adjustments | `{type: "fixed", price: 399}` |
| Zhejiang | Indexed (benchmark + spread), separate day/night | `{type: "indexed", spread: 2.0}` or `{type: "peak_offpeak", day: 370, night: null}` |
| Shandong | Indexed band or fixed, with monthly volume breakdown | `{type: "indexed_band"}` |
| Others | Configurable via `rm_crm_import_configs` | flexible |

Retail P&L computation reads `price_formula` and dispatches to the appropriate pricing function. New province pricing structures are added by extending the formula dispatch without schema changes.

---

## 7. Deployment

```
ECR repo:     bess-retail-risk
ECS service:  bess-platform-retail-risk-svc
ALB path:     /retail-risk/*
Port:         8513
Auth:         Cognito (existing pool)
```

---

## 8. Build sequence

DB migrations (retail-specific tables) depend on App 1 migrations running first (shared `rm_assets`, `rm_books` etc.).

1. `rm_customers`, `rm_customer_contracts`, `rm_customer_profiles`, `rm_crm_import_configs` migrations
2. CRM import parser (`各省份台账.xlsx` → DB, province column mapping)
3. Tab 1 — CRM (registry, detail view, import)
4. Tab 2 — Settlement (reuses `libs/settlement/`, adds retail categories)
5. Tab 3 — Realised P&L (reuses `libs/risk/pnl.py`, adds retail waterfall + customer breakdown)
6. Tab 4 — Positions & MtM (reuses App 1 code, adds coverage ratio + open exposure views)
7. Tab 5 — VaR & Greeks (reuses `libs/risk/var.py`, adds load uncertainty VaR)
8. Tab 6 — Agent
9. Docker + ECS deploy

App 2 build starts after App 1 `libs/risk/` and `libs/settlement/` are stable (not necessarily after App 1 is fully deployed).
