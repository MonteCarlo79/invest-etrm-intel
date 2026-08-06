# Asset Risk Management (App 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Streamlit risk management app for asset-backed trading books (wind/solar/BESS/thermal) with settlement ingestion, P&L attribution, MtM, VaR/Greeks, and an AI agent tab.

**Architecture:** Standalone Streamlit app at `apps/asset-risk/` (port 8512) following existing bess-platform patterns. Shared libs (`libs/risk/`, `libs/settlement/`) provide reusable valuation and ingestion logic. PostgreSQL `marketdata` schema with `rm_` prefix tables. ECS Fargate deployment behind ALB at `/asset-risk/*`.

**Tech Stack:** Python 3.11, Streamlit, psycopg2, SQLAlchemy, pandas, plotly, pdfplumber, openpyxl, anthropic SDK, pytest

---

## File Structure

### Database DDL
- `db/ddl/marketdata/rm_assets_books.sql` — rm_assets, rm_books tables
- `db/ddl/marketdata/rm_positions.sql` — rm_positions, rm_position_volumes tables
- `db/ddl/marketdata/rm_dispatch.sql` — rm_dispatch_plan, rm_dispatch_daily tables
- `db/ddl/marketdata/rm_forward_curves.sql` — rm_forward_curves table
- `db/ddl/marketdata/rm_settlements.sql` — rm_settlements, rm_settlement_items tables
- `db/ddl/marketdata/rm_snapshots.sql` — rm_pnl_snapshots, rm_var_snapshots tables

### Shared Libraries
- `libs/settlement/__init__.py`
- `libs/settlement/parser.py` — multi-format file ingestion (PDF, Excel, CSV)
- `libs/settlement/categorizer.py` — rule-based settlement categorization + Mengxi wind rule
- `libs/risk/__init__.py`
- `libs/risk/mtm.py` — forward curve lookup + open position MtM valuation
- `libs/risk/pnl.py` — P&L waterfall decomposition
- `libs/risk/var.py` — historical simulation + parametric VaR
- `libs/risk/greeks.py` — delta/gamma/vega aggregation

### Services
- `services/forward_curve/__init__.py`
- `services/forward_curve/lingfeng_pull.py` — pulls near-term curves from LingFeng
- `services/forward_curve/manual_upload.py` — validates + writes manual CSV curves
- `services/operating_assets/__init__.py`
- `services/operating_assets/ingest.py` — main ingestion orchestrator
- `services/operating_assets/filename_mapper.py` — filename → asset_id config
- `services/operating_assets/parsers/bess_daily.py` — 运营统计 Excel parser
- `services/operating_assets/parsers/bess_dispatch.py` — 调度计划表 Excel parser
- `services/operating_assets/parsers/wind_farm.py` — 零碳46风电经营统计 parser

### App
- `apps/asset-risk/app.py` — Streamlit entry point with 6 tabs
- `apps/asset-risk/tab_asset_config.py` — Tab 1: Asset & Book CRUD
- `apps/asset-risk/tab_settlement.py` — Tab 2: Settlement upload + analytics
- `apps/asset-risk/tab_pnl.py` — Tab 3: Realised P&L waterfall + KPIs
- `apps/asset-risk/tab_positions.py` — Tab 4: Positions, MtM, forward curves
- `apps/asset-risk/tab_var.py` — Tab 5: VaR & Greeks
- `apps/asset-risk/tab_agent.py` — Tab 6: Claude agent
- `apps/asset-risk/requirements.txt`
- `apps/asset-risk/Dockerfile`

### Tests
- `tests/risk/test_mtm.py`
- `tests/risk/test_pnl.py`
- `tests/risk/test_var.py`
- `tests/risk/test_greeks.py`
- `tests/settlement/test_parser.py`
- `tests/settlement/test_categorizer.py`
- `tests/operating_assets/test_ingest.py`
- `tests/operating_assets/test_wind_farm_parser.py`

---

## Task 1: Database DDL — Asset & Book Registry

**Files:**
- Create: `db/ddl/marketdata/rm_assets_books.sql`

- [ ] **Step 1: Write rm_assets_books.sql**

```sql
-- db/ddl/marketdata/rm_assets_books.sql
--
-- Asset registry and trading book tables for risk management.
-- All tables in marketdata schema with rm_ prefix.

CREATE TABLE IF NOT EXISTS marketdata.rm_assets (
    id               SERIAL PRIMARY KEY,
    name             TEXT NOT NULL,
    asset_type       TEXT NOT NULL CHECK (asset_type IN ('wind','solar','bess','thermal')),
    province         TEXT NOT NULL,
    capacity_mw      NUMERIC(10,2) NOT NULL,
    bess_duration_h  NUMERIC(5,2),
    bess_dod_pct     NUMERIC(5,2),
    fuel_type        TEXT,
    commission_date  DATE,
    status           TEXT DEFAULT 'active' CHECK (status IN ('active','retired')),
    notes            TEXT,
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE marketdata.rm_assets IS
    'Registry of power generation and storage assets (wind, solar, BESS, thermal).';
COMMENT ON COLUMN marketdata.rm_assets.bess_duration_h IS
    'Battery duration in hours. Only populated for BESS assets.';
COMMENT ON COLUMN marketdata.rm_assets.bess_dod_pct IS
    'Depth of discharge percentage (0-100). Only populated for BESS assets.';

CREATE INDEX IF NOT EXISTS idx_rm_assets_type ON marketdata.rm_assets(asset_type);
CREATE INDEX IF NOT EXISTS idx_rm_assets_province ON marketdata.rm_assets(province);

CREATE TABLE IF NOT EXISTS marketdata.rm_books (
    id               SERIAL PRIMARY KEY,
    name             TEXT NOT NULL,
    book_type        TEXT NOT NULL CHECK (book_type IN ('asset','load')),
    asset_id         INTEGER REFERENCES marketdata.rm_assets(id),
    currency         TEXT DEFAULT 'CNY',
    description      TEXT,
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE marketdata.rm_books IS
    'Trading books. Each asset gets an auto-created book (book_type=asset). '
    'Virtual/aggregated books may exist without a linked asset.';

CREATE INDEX IF NOT EXISTS idx_rm_books_asset ON marketdata.rm_books(asset_id);
CREATE INDEX IF NOT EXISTS idx_rm_books_type ON marketdata.rm_books(book_type);
```

- [ ] **Step 2: Apply DDL to database**

Run: `psql "$PGURL" -f db/ddl/marketdata/rm_assets_books.sql`
Expected: CREATE TABLE, CREATE INDEX messages with no errors.

- [ ] **Step 3: Commit**

```bash
git add db/ddl/marketdata/rm_assets_books.sql
git commit -m "feat(rm): add rm_assets and rm_books DDL"
```

---

## Task 2: Database DDL — Positions & Position Volumes

**Files:**
- Create: `db/ddl/marketdata/rm_positions.sql`

- [ ] **Step 1: Write rm_positions.sql**

```sql
-- db/ddl/marketdata/rm_positions.sql
--
-- Individual trade positions and unified hourly position volume table.

CREATE TABLE IF NOT EXISTS marketdata.rm_positions (
    id               SERIAL PRIMARY KEY,
    book_id          INTEGER NOT NULL REFERENCES marketdata.rm_books(id),
    instrument_type  TEXT NOT NULL CHECK (instrument_type IN
                       ('bilateral','spot','futures','option','forward','profile')),
    province         TEXT NOT NULL,
    channel          TEXT NOT NULL CHECK (channel IN (
                       'DA','RT','monthly_auction','monthly_listed',
                       'intramonth_match','annual','ancillary','capacity')),
    direction        TEXT NOT NULL CHECK (direction IN ('buy','sell')),
    volume_mwh       NUMERIC(14,4) NOT NULL,
    price_cny_mwh    NUMERIC(10,4),
    start_date       DATE NOT NULL,
    end_date         DATE NOT NULL,
    counterparty     TEXT,
    status           TEXT DEFAULT 'open' CHECK (status IN ('open','closed','expired')),
    uploaded_at      TIMESTAMPTZ DEFAULT NOW(),
    upload_batch_id  TEXT
);

COMMENT ON TABLE marketdata.rm_positions IS
    'Individual trade/position records by channel. '
    'Channels: DA=日前, RT=实时, monthly_auction=月度竞价, monthly_listed=月度挂牌, '
    'intramonth_match=月内撮合, annual=年度, ancillary/capacity=non-energy.';

CREATE INDEX IF NOT EXISTS idx_rm_positions_book ON marketdata.rm_positions(book_id);
CREATE INDEX IF NOT EXISTS idx_rm_positions_dates ON marketdata.rm_positions(start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_rm_positions_status ON marketdata.rm_positions(status);

CREATE TABLE IF NOT EXISTS marketdata.rm_position_volumes (
    id                              SERIAL PRIMARY KEY,
    book_id                         INTEGER NOT NULL REFERENCES marketdata.rm_books(id),
    delivery_date                   DATE NOT NULL,
    hour                            SMALLINT NOT NULL CHECK (hour BETWEEN 0 AND 23),

    -- Trading channel prices (CNY/MWh)
    da_price_cny_mwh                NUMERIC(10,4),
    rt_price_cny_mwh                NUMERIC(10,4),
    monthly_auction_price_cny_mwh   NUMERIC(10,4),
    monthly_listed_price_cny_mwh    NUMERIC(10,4),
    intramonth_match_price_cny_mwh  NUMERIC(10,4),
    annual_price_cny_mwh            NUMERIC(10,4),

    -- Trading channel volumes (MWh)
    da_volume_mwh                   NUMERIC(10,4),
    rt_volume_mwh                   NUMERIC(10,4),
    monthly_auction_volume_mwh      NUMERIC(10,4),
    monthly_listed_volume_mwh       NUMERIC(10,4),
    intramonth_match_volume_mwh     NUMERIC(10,4),
    annual_volume_mwh               NUMERIC(10,4),

    -- Derived / computed
    market_price_cny_mwh            NUMERIC(10,4),
    actual_price_cny_mwh            NUMERIC(10,4),
    pnl_cny                         NUMERIC(14,2),

    -- Volume waterfall
    nominated_mwh                   NUMERIC(10,4),
    cleared_mwh                     NUMERIC(10,4),
    settled_mwh                     NUMERIC(10,4),
    deviation_bid_mwh               NUMERIC(10,4) DEFAULT 0,
    deviation_equipment_mwh         NUMERIC(10,4) DEFAULT 0,
    deviation_sysop_mwh             NUMERIC(10,4) DEFAULT 0,
    deviation_grid_flow_mwh         NUMERIC(10,4) DEFAULT 0,

    upload_batch_id                 TEXT,
    UNIQUE (book_id, delivery_date, hour)
);

COMMENT ON TABLE marketdata.rm_position_volumes IS
    'Unified hourly position volumes per book. One row per book per delivery hour. '
    'Stores price + volume for 6 Chinese electricity market channels. '
    'Used by both asset books (generation) and load books (retail).';

CREATE INDEX IF NOT EXISTS idx_rm_pv_book_date ON marketdata.rm_position_volumes(book_id, delivery_date);
CREATE INDEX IF NOT EXISTS idx_rm_pv_date ON marketdata.rm_position_volumes(delivery_date);
```

- [ ] **Step 2: Apply DDL**

Run: `psql "$PGURL" -f db/ddl/marketdata/rm_positions.sql`

- [ ] **Step 3: Commit**

```bash
git add db/ddl/marketdata/rm_positions.sql
git commit -m "feat(rm): add rm_positions and rm_position_volumes DDL"
```

---

## Task 3: Database DDL — Dispatch Tables

**Files:**
- Create: `db/ddl/marketdata/rm_dispatch.sql`

- [ ] **Step 1: Write rm_dispatch.sql**

```sql
-- db/ddl/marketdata/rm_dispatch.sql
--
-- BESS daily operations summary and 15-min dispatch plan tables.

CREATE TABLE IF NOT EXISTS marketdata.rm_dispatch_daily (
    id                        SERIAL PRIMARY KEY,
    asset_id                  INTEGER NOT NULL REFERENCES marketdata.rm_assets(id),
    dispatch_date             DATE NOT NULL,
    operator_name             TEXT,
    charge_mwh                NUMERIC(10,4),
    discharge_mwh             NUMERIC(10,4),
    auxiliary_consumption_mwh NUMERIC(10,4),
    cumulative_charge_mwh     NUMERIC(12,4),
    cumulative_discharge_mwh  NUMERIC(12,4),
    cycle_count_day           NUMERIC(6,2),
    cycle_count_month         NUMERIC(8,2),
    conversion_ratio          NUMERIC(6,4),
    charge_windows            TEXT[],
    discharge_windows         TEXT[],
    discharge_revenue_cny     NUMERIC(14,2),
    charge_cost_cny           NUMERIC(14,2),
    system_op_fee_cny         NUMERIC(14,2),
    net_margin_cny            NUMERIC(14,2),
    anomaly_notes             TEXT,
    upload_batch_id           TEXT,
    UNIQUE (asset_id, dispatch_date)
);

COMMENT ON TABLE marketdata.rm_dispatch_daily IS
    'Daily BESS operations summary from 运营统计 Excel. One row per asset per day.';

CREATE INDEX IF NOT EXISTS idx_rm_dd_asset_date ON marketdata.rm_dispatch_daily(asset_id, dispatch_date);

CREATE TABLE IF NOT EXISTS marketdata.rm_dispatch_plan (
    id                    SERIAL PRIMARY KEY,
    asset_id              INTEGER NOT NULL REFERENCES marketdata.rm_assets(id),
    interval_start        TIMESTAMPTZ NOT NULL,
    soc_pct               NUMERIC(6,2),
    nominated_mw          NUMERIC(10,4),
    forecast_mw           NUMERIC(10,4),
    dispatched_mw         NUMERIC(10,4),
    actual_mw             NUMERIC(10,4),
    upload_batch_id       TEXT,
    UNIQUE (asset_id, interval_start)
);

COMMENT ON TABLE marketdata.rm_dispatch_plan IS
    '15-min dispatch plan. BESS: nominated/forecast/dispatched/actual MW. '
    'Wind: D+1 forecast + actual output. Positive=discharge/generation, Negative=charge.';

CREATE INDEX IF NOT EXISTS idx_rm_dp_asset_interval ON marketdata.rm_dispatch_plan(asset_id, interval_start);
```

- [ ] **Step 2: Apply DDL**

Run: `psql "$PGURL" -f db/ddl/marketdata/rm_dispatch.sql`

- [ ] **Step 3: Commit**

```bash
git add db/ddl/marketdata/rm_dispatch.sql
git commit -m "feat(rm): add rm_dispatch_daily and rm_dispatch_plan DDL"
```

---

## Task 4: Database DDL — Forward Curves

**Files:**
- Create: `db/ddl/marketdata/rm_forward_curves.sql`

- [ ] **Step 1: Write rm_forward_curves.sql**

```sql
-- db/ddl/marketdata/rm_forward_curves.sql

CREATE TABLE IF NOT EXISTS marketdata.rm_forward_curves (
    id               SERIAL PRIMARY KEY,
    province         TEXT NOT NULL,
    product          TEXT NOT NULL,
    curve_date       DATE NOT NULL,
    delivery_date    DATE NOT NULL,
    delivery_hour    SMALLINT,
    price_cny_kwh    NUMERIC(10,6) NOT NULL,
    source           TEXT NOT NULL CHECK (source IN ('lingfeng','manual','exchange')),
    uploaded_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (province, product, curve_date, delivery_date, delivery_hour, source)
);

COMMENT ON TABLE marketdata.rm_forward_curves IS
    'Forward price curves by province and product. Sources: LingFeng API, manual CSV upload, exchange data.';

CREATE INDEX IF NOT EXISTS idx_rm_fc_province_date ON marketdata.rm_forward_curves(province, delivery_date);
CREATE INDEX IF NOT EXISTS idx_rm_fc_source ON marketdata.rm_forward_curves(source);
```

- [ ] **Step 2: Apply DDL**

Run: `psql "$PGURL" -f db/ddl/marketdata/rm_forward_curves.sql`

- [ ] **Step 3: Commit**

```bash
git add db/ddl/marketdata/rm_forward_curves.sql
git commit -m "feat(rm): add rm_forward_curves DDL"
```

---

## Task 5: Database DDL — Settlements

**Files:**
- Create: `db/ddl/marketdata/rm_settlements.sql`

- [ ] **Step 1: Write rm_settlements.sql**

```sql
-- db/ddl/marketdata/rm_settlements.sql

CREATE TABLE IF NOT EXISTS marketdata.rm_settlements (
    id               SERIAL PRIMARY KEY,
    book_id          INTEGER NOT NULL REFERENCES marketdata.rm_books(id),
    settlement_month DATE NOT NULL,
    file_name        TEXT NOT NULL,
    file_type        TEXT NOT NULL CHECK (file_type IN ('excel','csv','pdf')),
    upload_date      TIMESTAMPTZ DEFAULT NOW(),
    status           TEXT DEFAULT 'pending' CHECK (status IN ('pending','processed','flagged')),
    total_amount_cny NUMERIC(16,2),
    raw_data         JSONB
);

COMMENT ON TABLE marketdata.rm_settlements IS
    'Settlement file upload records. One row per uploaded file per book per month.';

CREATE INDEX IF NOT EXISTS idx_rm_settlements_book ON marketdata.rm_settlements(book_id);
CREATE INDEX IF NOT EXISTS idx_rm_settlements_month ON marketdata.rm_settlements(settlement_month);

CREATE TABLE IF NOT EXISTS marketdata.rm_settlement_items (
    id                    SERIAL PRIMARY KEY,
    settlement_id         INTEGER NOT NULL REFERENCES marketdata.rm_settlements(id),
    category              TEXT NOT NULL CHECK (category IN (
                            'charge_energy','discharge_energy','generation_revenue',
                            'capacity_compensation','bilateral_energy',
                            'transmission','govt_surcharges','system_operation',
                            'coal_capacity_charge','basic_fee','curtailment',
                            'flex_fees','imbalance','market_redistribution',
                            'rule_charges','frequency',
                            'penalty','rebate','subsidy','other')),
    peak_period           TEXT CHECK (peak_period IN ('peak','valley','flat','super_peak')),
    delivery_date         DATE,
    volume_mwh            NUMERIC(14,4),
    price_cny_kwh         NUMERIC(10,6),
    amount_cny            NUMERIC(16,2) NOT NULL,
    amount_receivable_cny NUMERIC(16,2),
    amount_settled_cny    NUMERIC(16,2),
    amount_diff_cny       NUMERIC(16,2),
    counterparty          TEXT,
    notes                 TEXT
);

COMMENT ON TABLE marketdata.rm_settlement_items IS
    'Line items within a settlement. Categories cover BESS charge/discharge, wind generation, '
    'capacity compensation, T&D fees, surcharges, penalties, subsidies.';

CREATE INDEX IF NOT EXISTS idx_rm_si_settlement ON marketdata.rm_settlement_items(settlement_id);
CREATE INDEX IF NOT EXISTS idx_rm_si_category ON marketdata.rm_settlement_items(category);
```

- [ ] **Step 2: Apply DDL**

Run: `psql "$PGURL" -f db/ddl/marketdata/rm_settlements.sql`

- [ ] **Step 3: Commit**

```bash
git add db/ddl/marketdata/rm_settlements.sql
git commit -m "feat(rm): add rm_settlements and rm_settlement_items DDL"
```

---

## Task 6: Database DDL — P&L and VaR Snapshots

**Files:**
- Create: `db/ddl/marketdata/rm_snapshots.sql`

- [ ] **Step 1: Write rm_snapshots.sql**

```sql
-- db/ddl/marketdata/rm_snapshots.sql

CREATE TABLE IF NOT EXISTS marketdata.rm_pnl_snapshots (
    id                              SERIAL PRIMARY KEY,
    book_id                         INTEGER NOT NULL REFERENCES marketdata.rm_books(id),
    snapshot_date                   DATE NOT NULL,
    realized_cny                    NUMERIC(16,2),
    unrealized_mtm_cny              NUMERIC(16,2),
    spot_pnl_cny                    NUMERIC(16,2),
    bilateral_pnl_cny               NUMERIC(16,2),
    ancillary_pnl_cny               NUMERIC(16,2),
    deviation_pnl_cny               NUMERIC(16,2),
    curtailment_mwh                 NUMERIC(14,4),
    curtailment_rate_pct            NUMERIC(6,4),
    curtailment_opportunity_cost_cny NUMERIC(16,2),
    equivalent_hours                NUMERIC(8,2),
    other_pnl_cny                   NUMERIC(16,2),
    UNIQUE (book_id, snapshot_date)
);

COMMENT ON TABLE marketdata.rm_pnl_snapshots IS
    'Monthly P&L snapshots per book. Includes wind-specific KPIs (curtailment, equivalent hours).';

CREATE INDEX IF NOT EXISTS idx_rm_pnl_book_date ON marketdata.rm_pnl_snapshots(book_id, snapshot_date);

CREATE TABLE IF NOT EXISTS marketdata.rm_var_snapshots (
    id               SERIAL PRIMARY KEY,
    book_id          INTEGER NOT NULL REFERENCES marketdata.rm_books(id),
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

COMMENT ON TABLE marketdata.rm_var_snapshots IS
    'VaR and Greeks snapshots. Two methods: historical simulation, parametric delta-normal.';

CREATE INDEX IF NOT EXISTS idx_rm_var_book_date ON marketdata.rm_var_snapshots(book_id, snapshot_date);
```

- [ ] **Step 2: Apply DDL**

Run: `psql "$PGURL" -f db/ddl/marketdata/rm_snapshots.sql`

- [ ] **Step 3: Commit**

```bash
git add db/ddl/marketdata/rm_snapshots.sql
git commit -m "feat(rm): add rm_pnl_snapshots and rm_var_snapshots DDL"
```

---

## Task 7: Settlement Parser — Core Framework

**Files:**
- Create: `libs/settlement/__init__.py`
- Create: `libs/settlement/parser.py`
- Create: `tests/settlement/test_parser.py`

- [ ] **Step 1: Create package init**

```python
# libs/settlement/__init__.py
"""Settlement ingestion and categorization library."""
```

- [ ] **Step 2: Write failing test for parser**

```python
# tests/settlement/test_parser.py
"""Tests for libs/settlement/parser.py"""
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from libs.settlement.parser import detect_format, parse_excel_settlement, parse_trade_capture


def test_detect_format_trade_capture():
    """Trade Capture.xlsx detected by 'Trades' sheet with expected columns."""
    mock_xl = MagicMock()
    mock_xl.sheet_names = ["Trades", "Summary"]
    mock_xl.parse.return_value = pd.DataFrame(columns=[
        "Date", "Market", "Station Name", "Volume (MWh)", "Price (¥/MWh)", "Total (¥)"
    ])
    result = detect_format(mock_xl)
    assert result == "trade_capture"


def test_detect_format_capacity_compensation():
    """容量补偿数据.xlsx detected by column pattern."""
    mock_xl = MagicMock()
    mock_xl.sheet_names = ["Sheet1"]
    mock_xl.parse.return_value = pd.DataFrame(columns=[
        "电站", "月份", "应收", "实际结算", "差异"
    ])
    result = detect_format(mock_xl)
    assert result == "capacity_compensation"


def test_detect_format_wind_farm_ops():
    """零碳46风电经营统计 detected by sheet name signature."""
    mock_xl = MagicMock()
    mock_xl.sheet_names = ["风场功率", "预测&实际电量", "结算明细", "市场价格", "经营统计", "Other1", "Other2"]
    result = detect_format(mock_xl)
    assert result == "wind_farm_ops"


def test_parse_trade_capture():
    """Trade Capture Trades sheet parsed to canonical settlement items."""
    df = pd.DataFrame({
        "Date": ["2026-01-15", "2026-01-15"],
        "Market": ["DA", "DA"],
        "Station Name": ["裕昭沙子坝", "裕昭沙子坝"],
        "Buy/Sell": ["Sell", "Buy"],
        "Transactions Type": ["discharge", "charge"],
        "Volume (MWh)": [10.5, 8.2],
        "Price (¥/MWh)": [450.0, 280.0],
        "Total (¥)": [4725.0, -2296.0],
    })
    items = parse_trade_capture(df)
    assert len(items) == 2
    assert items[0]["category"] == "discharge_energy"
    assert items[0]["volume_mwh"] == 10.5
    assert items[0]["amount_cny"] == 4725.0
    assert items[1]["category"] == "charge_energy"
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pytest tests/settlement/test_parser.py -v`
Expected: FAIL (ModuleNotFoundError: libs.settlement.parser)

- [ ] **Step 4: Implement parser.py**

```python
# libs/settlement/parser.py
"""Multi-format settlement file parser.

Detects file format from sheet names and column signatures,
then parses to a canonical list of settlement item dicts.
"""
from __future__ import annotations

import pandas as pd
from typing import Any


def detect_format(xl: pd.ExcelFile) -> str:
    """Detect settlement file format from sheet names and column headers.

    Returns one of: 'trade_capture', 'capacity_compensation', 'subsidy',
                    'wind_farm_ops', 'unknown'
    """
    sheets = xl.sheet_names

    # Wind farm ops: has characteristic sheet names
    wind_sheets = {"风场功率", "结算明细", "市场价格", "经营统计"}
    if wind_sheets.issubset(set(sheets)):
        return "wind_farm_ops"

    # Trade Capture: has "Trades" sheet with expected columns
    if "Trades" in sheets:
        df = xl.parse("Trades", nrows=0)
        if "Volume (MWh)" in df.columns and "Price (¥/MWh)" in df.columns:
            return "trade_capture"

    # Capacity compensation or subsidy: wide-format with 应收/实际结算/差异
    first_sheet = sheets[0]
    df = xl.parse(first_sheet, nrows=5)
    cols = set(df.columns)
    if {"应收", "实际结算", "差异"}.issubset(cols) or {"应收金额", "实际结算金额"}.issubset(cols):
        if "容量" in first_sheet or "capacity" in str(xl).lower():
            return "capacity_compensation"
        return "subsidy"

    return "unknown"


def parse_trade_capture(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Parse Trade Capture 'Trades' sheet to canonical settlement items.

    Each row becomes one settlement item dict with keys:
      category, volume_mwh, price_cny_kwh, amount_cny, delivery_date,
      counterparty, peak_period, notes
    """
    items = []
    for _, row in df.iterrows():
        tx_type = str(row.get("Transactions Type", "")).lower().strip()
        if "discharge" in tx_type or "sell" in str(row.get("Buy/Sell", "")).lower():
            category = "discharge_energy"
        elif "charge" in tx_type or "buy" in str(row.get("Buy/Sell", "")).lower():
            category = "charge_energy"
        else:
            category = "other"

        items.append({
            "category": category,
            "delivery_date": pd.to_datetime(row.get("Date")).date() if pd.notna(row.get("Date")) else None,
            "volume_mwh": float(row.get("Volume (MWh)", 0)),
            "price_cny_kwh": float(row.get("Price (¥/MWh)", 0)) / 1000.0,
            "amount_cny": float(row.get("Total (¥)", 0)),
            "counterparty": row.get("Station Name"),
            "peak_period": None,
            "notes": row.get("Market"),
        })
    return items


def parse_capacity_compensation(xl: pd.ExcelFile) -> list[dict[str, Any]]:
    """Parse 容量补偿数据.xlsx — wide-format multi-station × multi-month.

    Melts to long format: one item per station per month.
    """
    df = xl.parse(xl.sheet_names[0])
    items = []
    for _, row in df.iterrows():
        items.append({
            "category": "capacity_compensation",
            "delivery_date": None,
            "volume_mwh": None,
            "price_cny_kwh": None,
            "amount_cny": float(row.get("实际结算", 0) or row.get("实际结算金额", 0)),
            "amount_receivable_cny": float(row.get("应收", 0) or row.get("应收金额", 0)),
            "amount_settled_cny": float(row.get("实际结算", 0) or row.get("实际结算金额", 0)),
            "amount_diff_cny": float(row.get("差异", 0) or 0),
            "counterparty": row.get("电站"),
            "peak_period": None,
            "notes": str(row.get("月份", "")),
        })
    return items
```

- [ ] **Step 5: Run tests**

Run: `pytest tests/settlement/test_parser.py -v`
Expected: All 4 tests PASS

- [ ] **Step 6: Commit**

```bash
git add libs/settlement/__init__.py libs/settlement/parser.py tests/settlement/test_parser.py
git commit -m "feat(settlement): add parser with format detection and Trade Capture parsing"
```

---

## Task 8: Settlement Categorizer

**Files:**
- Create: `libs/settlement/categorizer.py`
- Create: `tests/settlement/test_categorizer.py`

- [ ] **Step 1: Write failing test**

```python
# tests/settlement/test_categorizer.py
"""Tests for libs/settlement/categorizer.py"""
import pytest
from libs.settlement.categorizer import categorize_items, mengxi_wind_settlement


def test_categorize_items_bess():
    """BESS items categorized by transaction type."""
    items = [
        {"category": "discharge_energy", "amount_cny": 5000, "volume_mwh": 10},
        {"category": "charge_energy", "amount_cny": -3000, "volume_mwh": 12},
    ]
    result = categorize_items(items, asset_type="bess", province="inner_mongolia_mengxi")
    assert result[0]["category"] == "discharge_energy"
    assert result[1]["category"] == "charge_energy"


def test_mengxi_wind_settlement_da_only():
    """When generation <= DA volume, all settled at DA price."""
    hourly = {
        "settled_mwh": 8.0,
        "da_volume_mwh": 10.0,
        "da_price_cny_mwh": 400.0,
        "rt_price_cny_mwh": 350.0,
        "annual_price_cny_mwh": 380.0,
    }
    result = mengxi_wind_settlement(hourly)
    # All volume at DA price
    assert result["da_settled_mwh"] == 8.0
    assert result["rt_settled_mwh"] == 0.0
    assert result["pnl_cny"] == pytest.approx(8.0 * 400.0)


def test_mengxi_wind_settlement_residual_at_rt():
    """When generation > DA volume, residual settled at RT node price."""
    hourly = {
        "settled_mwh": 12.0,
        "da_volume_mwh": 8.0,
        "da_price_cny_mwh": 400.0,
        "rt_price_cny_mwh": 350.0,
        "annual_price_cny_mwh": 380.0,
    }
    result = mengxi_wind_settlement(hourly)
    assert result["da_settled_mwh"] == 8.0
    assert result["rt_settled_mwh"] == 4.0
    expected_pnl = 8.0 * 400.0 + 4.0 * 350.0
    assert result["pnl_cny"] == pytest.approx(expected_pnl)


def test_mengxi_wind_settlement_bilateral_premium():
    """Bilateral contract premium applied on top."""
    hourly = {
        "settled_mwh": 10.0,
        "da_volume_mwh": 10.0,
        "da_price_cny_mwh": 400.0,
        "rt_price_cny_mwh": 350.0,
        "annual_price_cny_mwh": 420.0,
        "annual_volume_mwh": 5.0,
    }
    result = mengxi_wind_settlement(hourly)
    # 5 MWh at bilateral premium (420-400=20 per MWh extra)
    bilateral_premium = 5.0 * (420.0 - 400.0)
    base_pnl = 10.0 * 400.0
    assert result["bilateral_premium_cny"] == pytest.approx(bilateral_premium)
```

- [ ] **Step 2: Run test to verify failure**

Run: `pytest tests/settlement/test_categorizer.py -v`
Expected: FAIL (ModuleNotFoundError)

- [ ] **Step 3: Implement categorizer.py**

```python
# libs/settlement/categorizer.py
"""Rule-based settlement item categorization.

Includes province-specific settlement rules (e.g., Mengxi wind).
"""
from __future__ import annotations

from typing import Any


def categorize_items(
    items: list[dict[str, Any]],
    asset_type: str,
    province: str,
) -> list[dict[str, Any]]:
    """Apply province+asset_type specific categorization rules to settlement items.

    For most cases, items arrive pre-categorized from the parser.
    This function validates and may reclassify based on rules.
    """
    result = []
    for item in items:
        categorized = dict(item)
        # Wind generation: reclassify discharge_energy → generation_revenue
        if asset_type in ("wind", "solar") and item["category"] == "discharge_energy":
            categorized["category"] = "generation_revenue"
        result.append(categorized)
    return result


def mengxi_wind_settlement(hourly: dict[str, Any]) -> dict[str, Any]:
    """Apply Inner Mongolia (Mengxi) wind settlement rule.

    Rule:
    - If generation <= DA volume: all settled at DA price
    - Residual above DA: settled at RT node price
    - Bilateral (annual) contract premium/discount applied on top

    Args:
        hourly: dict with keys: settled_mwh, da_volume_mwh, da_price_cny_mwh,
                rt_price_cny_mwh, annual_price_cny_mwh, annual_volume_mwh (optional)

    Returns:
        dict with: da_settled_mwh, rt_settled_mwh, pnl_cny, bilateral_premium_cny
    """
    settled = float(hourly.get("settled_mwh", 0) or 0)
    da_vol = float(hourly.get("da_volume_mwh", 0) or 0)
    da_price = float(hourly.get("da_price_cny_mwh", 0) or 0)
    rt_price = float(hourly.get("rt_price_cny_mwh", 0) or 0)
    annual_price = float(hourly.get("annual_price_cny_mwh", 0) or 0)
    annual_vol = float(hourly.get("annual_volume_mwh", 0) or 0)

    # Step 1: DA allocation (min of settled vs DA volume)
    da_settled = min(settled, da_vol)
    # Step 2: Residual at RT
    rt_settled = max(0.0, settled - da_vol)

    # Base P&L
    pnl = da_settled * da_price + rt_settled * rt_price

    # Step 3: Bilateral premium (annual contract volume at premium over DA)
    bilateral_premium = 0.0
    if annual_vol > 0 and annual_price > 0:
        # Premium is on the lesser of annual_vol and da_settled (bilateral replaces DA)
        bilateral_mwh = min(annual_vol, da_settled)
        bilateral_premium = bilateral_mwh * (annual_price - da_price)

    return {
        "da_settled_mwh": da_settled,
        "rt_settled_mwh": rt_settled,
        "pnl_cny": pnl,
        "bilateral_premium_cny": bilateral_premium,
    }
```

- [ ] **Step 4: Run tests**

Run: `pytest tests/settlement/test_categorizer.py -v`
Expected: All 4 tests PASS

- [ ] **Step 5: Commit**

```bash
git add libs/settlement/categorizer.py tests/settlement/test_categorizer.py
git commit -m "feat(settlement): add categorizer with Mengxi wind settlement rule"
```

---

## Task 9: Risk Library — MtM Valuation

**Files:**
- Create: `libs/risk/__init__.py`
- Create: `libs/risk/mtm.py`
- Create: `tests/risk/test_mtm.py`

- [ ] **Step 1: Create package init**

```python
# libs/risk/__init__.py
"""Risk analytics library: MtM, P&L, VaR, Greeks."""
```

- [ ] **Step 2: Write failing test**

```python
# tests/risk/test_mtm.py
"""Tests for libs/risk/mtm.py"""
import pytest
import pandas as pd
from libs.risk.mtm import compute_mtm, get_forward_price


def test_get_forward_price():
    """Forward price lookup returns latest curve price for province+date."""
    curves = pd.DataFrame({
        "province": ["inner_mongolia_mengxi", "inner_mongolia_mengxi"],
        "delivery_date": [pd.Timestamp("2026-08-01"), pd.Timestamp("2026-08-02")],
        "delivery_hour": [10, 10],
        "price_cny_kwh": [0.45, 0.46],
        "curve_date": [pd.Timestamp("2026-07-20"), pd.Timestamp("2026-07-20")],
    })
    price = get_forward_price(curves, "inner_mongolia_mengxi", pd.Timestamp("2026-08-01"), 10)
    assert price == pytest.approx(450.0)  # converted to CNY/MWh


def test_compute_mtm_buy_position():
    """MtM for a buy position: (forward - entry) * volume."""
    positions = [
        {
            "direction": "buy",
            "volume_mwh": 100.0,
            "price_cny_mwh": 400.0,
            "province": "inner_mongolia_mengxi",
            "start_date": pd.Timestamp("2026-08-01"),
            "end_date": pd.Timestamp("2026-08-31"),
        }
    ]
    # Forward price is 450 CNY/MWh
    forward_prices = {"inner_mongolia_mengxi": 450.0}
    result = compute_mtm(positions, forward_prices)
    # Buy at 400, market at 450 → gain of 50 * 100 = 5000
    assert result[0]["unrealized_pnl_cny"] == pytest.approx(5000.0)


def test_compute_mtm_sell_position():
    """MtM for a sell position: (entry - forward) * volume."""
    positions = [
        {
            "direction": "sell",
            "volume_mwh": 50.0,
            "price_cny_mwh": 420.0,
            "province": "inner_mongolia_mengxi",
            "start_date": pd.Timestamp("2026-08-01"),
            "end_date": pd.Timestamp("2026-08-31"),
        }
    ]
    forward_prices = {"inner_mongolia_mengxi": 450.0}
    result = compute_mtm(positions, forward_prices)
    # Sell at 420, market at 450 → loss of 30 * 50 = -1500
    assert result[0]["unrealized_pnl_cny"] == pytest.approx(-1500.0)
```

- [ ] **Step 3: Run test to verify failure**

Run: `pytest tests/risk/test_mtm.py -v`
Expected: FAIL (ModuleNotFoundError)

- [ ] **Step 4: Implement mtm.py**

```python
# libs/risk/mtm.py
"""Mark-to-Market valuation for open positions.

Uses forward curves to value remaining open position volume
against entry/contract prices.
"""
from __future__ import annotations

import pandas as pd
from typing import Any


def get_forward_price(
    curves: pd.DataFrame,
    province: str,
    delivery_date: pd.Timestamp,
    delivery_hour: int | None = None,
) -> float | None:
    """Look up forward price from curves DataFrame.

    Args:
        curves: DataFrame with columns: province, delivery_date, delivery_hour,
                price_cny_kwh, curve_date
        province: Province to filter on
        delivery_date: Target delivery date
        delivery_hour: Target hour (None for daily average)

    Returns:
        Price in CNY/MWh (converted from CNY/kWh), or None if not found.
    """
    mask = (curves["province"] == province) & (curves["delivery_date"] == delivery_date)
    if delivery_hour is not None:
        mask = mask & (curves["delivery_hour"] == delivery_hour)

    subset = curves[mask]
    if subset.empty:
        return None

    # Use latest curve_date
    latest = subset.sort_values("curve_date", ascending=False).iloc[0]
    return float(latest["price_cny_kwh"]) * 1000.0  # kWh → MWh


def compute_mtm(
    positions: list[dict[str, Any]],
    forward_prices: dict[str, float],
) -> list[dict[str, Any]]:
    """Compute unrealised MtM P&L for open positions.

    Args:
        positions: list of position dicts with keys:
            direction, volume_mwh, price_cny_mwh, province, start_date, end_date
        forward_prices: dict mapping province → current forward price (CNY/MWh)

    Returns:
        List of position dicts enriched with 'unrealized_pnl_cny' and 'forward_price_cny_mwh'.
    """
    results = []
    for pos in positions:
        province = pos["province"]
        entry_price = float(pos.get("price_cny_mwh", 0) or 0)
        volume = float(pos.get("volume_mwh", 0) or 0)
        direction = pos.get("direction", "buy")
        fwd_price = forward_prices.get(province, entry_price)

        if direction == "buy":
            unrealized = (fwd_price - entry_price) * volume
        else:
            unrealized = (entry_price - fwd_price) * volume

        result = dict(pos)
        result["forward_price_cny_mwh"] = fwd_price
        result["unrealized_pnl_cny"] = unrealized
        results.append(result)

    return results
```

- [ ] **Step 5: Run tests**

Run: `pytest tests/risk/test_mtm.py -v`
Expected: All 3 tests PASS

- [ ] **Step 6: Commit**

```bash
git add libs/risk/__init__.py libs/risk/mtm.py tests/risk/test_mtm.py
git commit -m "feat(risk): add MtM valuation library"
```

---

## Task 10: Risk Library — P&L Waterfall

**Files:**
- Create: `libs/risk/pnl.py`
- Create: `tests/risk/test_pnl.py`

- [ ] **Step 1: Write failing test**

```python
# tests/risk/test_pnl.py
"""Tests for libs/risk/pnl.py"""
import pytest
import pandas as pd
from libs.risk.pnl import compute_pnl_waterfall


def test_bess_pnl_waterfall():
    """BESS P&L waterfall decomposes into discharge, charge, fees."""
    settlement_items = pd.DataFrame({
        "category": ["discharge_energy", "charge_energy", "capacity_compensation",
                     "transmission", "system_operation"],
        "amount_cny": [50000.0, -30000.0, 8000.0, -2000.0, -1500.0],
    })
    result = compute_pnl_waterfall(settlement_items, asset_type="bess")
    assert result["discharge_energy"] == pytest.approx(50000.0)
    assert result["charge_energy"] == pytest.approx(-30000.0)
    assert result["capacity_compensation"] == pytest.approx(8000.0)
    assert result["net_pnl"] == pytest.approx(24500.0)


def test_wind_pnl_waterfall():
    """Wind P&L includes generation revenue and curtailment."""
    settlement_items = pd.DataFrame({
        "category": ["generation_revenue", "curtailment", "transmission", "subsidy"],
        "amount_cny": [80000.0, -15000.0, -3000.0, 5000.0],
    })
    result = compute_pnl_waterfall(settlement_items, asset_type="wind")
    assert result["generation_revenue"] == pytest.approx(80000.0)
    assert result["curtailment"] == pytest.approx(-15000.0)
    assert result["net_pnl"] == pytest.approx(67000.0)
```

- [ ] **Step 2: Run test to verify failure**

Run: `pytest tests/risk/test_pnl.py -v`
Expected: FAIL

- [ ] **Step 3: Implement pnl.py**

```python
# libs/risk/pnl.py
"""P&L waterfall decomposition by settlement category.

Aggregates settlement items into a structured waterfall suitable
for Plotly waterfall chart rendering.
"""
from __future__ import annotations

import pandas as pd
from typing import Any

# Categories treated as revenue (positive contributes to P&L)
REVENUE_CATEGORIES = {
    "discharge_energy", "generation_revenue", "capacity_compensation",
    "bilateral_energy", "subsidy", "rebate",
}

# Categories treated as cost (negative contributes to P&L)
COST_CATEGORIES = {
    "charge_energy", "transmission", "govt_surcharges", "system_operation",
    "coal_capacity_charge", "basic_fee", "penalty", "curtailment",
    "flex_fees", "imbalance", "market_redistribution", "rule_charges", "frequency",
}


def compute_pnl_waterfall(
    settlement_items: pd.DataFrame,
    asset_type: str,
) -> dict[str, float]:
    """Compute P&L waterfall from settlement items.

    Args:
        settlement_items: DataFrame with columns: category, amount_cny
        asset_type: 'bess', 'wind', 'solar', 'thermal'

    Returns:
        Dict mapping category → total amount, plus 'net_pnl' key.
    """
    result: dict[str, float] = {}
    net = 0.0

    for category, group in settlement_items.groupby("category"):
        total = float(group["amount_cny"].sum())
        result[str(category)] = total
        net += total

    result["net_pnl"] = net
    return result
```

- [ ] **Step 4: Run tests**

Run: `pytest tests/risk/test_pnl.py -v`
Expected: All 2 tests PASS

- [ ] **Step 5: Commit**

```bash
git add libs/risk/pnl.py tests/risk/test_pnl.py
git commit -m "feat(risk): add P&L waterfall decomposition"
```

---

## Task 11: Risk Library — VaR

**Files:**
- Create: `libs/risk/var.py`
- Create: `tests/risk/test_var.py`

- [ ] **Step 1: Write failing test**

```python
# tests/risk/test_var.py
"""Tests for libs/risk/var.py"""
import pytest
import numpy as np
import pandas as pd
from libs.risk.var import historical_var, parametric_var


def test_historical_var_95():
    """Historical VaR at 95% = 5th percentile of P&L scenarios."""
    # 100 historical price returns; position = 100 MWh buy
    np.random.seed(42)
    price_history = pd.Series(np.random.normal(0, 10, 252))  # daily returns CNY/MWh
    delta_mwh = 100.0
    result = historical_var(price_history, delta_mwh, confidence=0.95)
    # VaR should be positive (loss amount)
    assert result > 0
    # 5th percentile of 100*returns should be close to 100 * np.percentile(returns, 5) magnitude
    expected = -delta_mwh * np.percentile(price_history, 5)
    assert result == pytest.approx(expected, rel=0.01)


def test_parametric_var_95():
    """Parametric VaR = delta * sigma * z * sqrt(t)."""
    delta_mwh = 100.0
    sigma = 15.0  # CNY/MWh daily vol
    result = parametric_var(delta_mwh, sigma, confidence=0.95, horizon_days=1)
    expected = 100.0 * 15.0 * 1.645
    assert result == pytest.approx(expected, rel=0.01)


def test_parametric_var_10day():
    """10-day VaR uses sqrt(10) scaling."""
    delta_mwh = 100.0
    sigma = 15.0
    result = parametric_var(delta_mwh, sigma, confidence=0.95, horizon_days=10)
    expected = 100.0 * 15.0 * 1.645 * np.sqrt(10)
    assert result == pytest.approx(expected, rel=0.01)
```

- [ ] **Step 2: Run test to verify failure**

Run: `pytest tests/risk/test_var.py -v`
Expected: FAIL

- [ ] **Step 3: Implement var.py**

```python
# libs/risk/var.py
"""Value at Risk computation.

Two methods:
- Historical simulation: reprice positions using historical price scenarios
- Parametric delta-normal: VaR = delta * sigma * z * sqrt(t)
"""
from __future__ import annotations

import numpy as np
import pandas as pd

# Z-scores for confidence levels
Z_SCORES = {0.95: 1.6449, 0.99: 2.3263}


def historical_var(
    price_returns: pd.Series,
    delta_mwh: float,
    confidence: float = 0.95,
) -> float:
    """Compute VaR using historical simulation.

    Args:
        price_returns: Series of historical daily price changes (CNY/MWh)
        delta_mwh: Net MWh exposure (positive = long)
        confidence: Confidence level (0.95 or 0.99)

    Returns:
        VaR as a positive number representing potential loss (CNY).
    """
    # P&L scenarios = delta * each historical price change
    scenarios = delta_mwh * price_returns.values
    # VaR = negative of the (1-confidence) percentile
    percentile = (1 - confidence) * 100
    var_value = -np.percentile(scenarios, percentile)
    return float(max(var_value, 0.0))


def parametric_var(
    delta_mwh: float,
    sigma_price: float,
    confidence: float = 0.95,
    horizon_days: int = 1,
) -> float:
    """Compute VaR using parametric delta-normal method.

    Args:
        delta_mwh: Net MWh exposure (positive = long)
        sigma_price: Daily price volatility (CNY/MWh)
        confidence: Confidence level (0.95 or 0.99)
        horizon_days: VaR horizon in days

    Returns:
        VaR as a positive number representing potential loss (CNY).
    """
    z = Z_SCORES.get(confidence, 1.6449)
    var_value = abs(delta_mwh) * sigma_price * z * np.sqrt(horizon_days)
    return float(var_value)
```

- [ ] **Step 4: Run tests**

Run: `pytest tests/risk/test_var.py -v`
Expected: All 3 tests PASS

- [ ] **Step 5: Commit**

```bash
git add libs/risk/var.py tests/risk/test_var.py
git commit -m "feat(risk): add VaR (historical + parametric)"
```

---

## Task 12: Risk Library — Greeks

**Files:**
- Create: `libs/risk/greeks.py`
- Create: `tests/risk/test_greeks.py`

- [ ] **Step 1: Write failing test**

```python
# tests/risk/test_greeks.py
"""Tests for libs/risk/greeks.py"""
import pytest
from libs.risk.greeks import compute_book_greeks


def test_book_delta_long():
    """Book delta = sum of position deltas (buy = +, sell = -)."""
    positions = [
        {"direction": "buy", "volume_mwh": 100.0, "status": "open"},
        {"direction": "sell", "volume_mwh": 30.0, "status": "open"},
        {"direction": "buy", "volume_mwh": 50.0, "status": "closed"},  # ignored
    ]
    result = compute_book_greeks(positions)
    # Only open positions: +100 - 30 = 70
    assert result["delta_mwh"] == pytest.approx(70.0)
    # No options → gamma and vega are 0
    assert result["gamma"] == 0.0
    assert result["vega"] == 0.0


def test_book_delta_net_short():
    """Net short book has negative delta."""
    positions = [
        {"direction": "sell", "volume_mwh": 200.0, "status": "open"},
        {"direction": "buy", "volume_mwh": 50.0, "status": "open"},
    ]
    result = compute_book_greeks(positions)
    assert result["delta_mwh"] == pytest.approx(-150.0)
```

- [ ] **Step 2: Run test to verify failure**

Run: `pytest tests/risk/test_greeks.py -v`
Expected: FAIL

- [ ] **Step 3: Implement greeks.py**

```python
# libs/risk/greeks.py
"""Book-level Greeks aggregation.

Delta: net MWh exposure from open linear positions.
Gamma/Vega: aggregated from libs/options/ when option positions exist;
            otherwise 0.
"""
from __future__ import annotations

from typing import Any


def compute_book_greeks(positions: list[dict[str, Any]]) -> dict[str, float]:
    """Compute aggregated Greeks for a trading book.

    Args:
        positions: list of position dicts with keys:
            direction ('buy'/'sell'), volume_mwh, status ('open'/'closed'/'expired')

    Returns:
        dict with keys: delta_mwh, gamma, vega
    """
    delta = 0.0
    gamma = 0.0
    vega = 0.0

    for pos in positions:
        if pos.get("status") != "open":
            continue

        volume = float(pos.get("volume_mwh", 0) or 0)
        direction = pos.get("direction", "buy")

        if direction == "buy":
            delta += volume
        else:
            delta -= volume

        # Gamma/Vega from option positions (future extension)
        # When options exist, route through libs/options/black_scholes.py
        gamma += float(pos.get("gamma", 0) or 0)
        vega += float(pos.get("vega", 0) or 0)

    return {
        "delta_mwh": delta,
        "gamma": gamma,
        "vega": vega,
    }
```

- [ ] **Step 4: Run tests**

Run: `pytest tests/risk/test_greeks.py -v`
Expected: All 2 tests PASS

- [ ] **Step 5: Commit**

```bash
git add libs/risk/greeks.py tests/risk/test_greeks.py
git commit -m "feat(risk): add book-level Greeks aggregation"
```

---

## Task 13: Forward Curve Service

**Files:**
- Create: `services/forward_curve/__init__.py`
- Create: `services/forward_curve/lingfeng_pull.py`
- Create: `services/forward_curve/manual_upload.py`

- [ ] **Step 1: Create package init**

```python
# services/forward_curve/__init__.py
"""Forward curve services: LingFeng pull and manual CSV upload."""
```

- [ ] **Step 2: Implement lingfeng_pull.py**

```python
# services/forward_curve/lingfeng_pull.py
"""Pull near-term forward price forecasts from LingFeng pipeline.

Uses the existing services/lingfeng/collector.py to download data,
then writes parsed prices to rm_forward_curves.
"""
from __future__ import annotations

import os
import pandas as pd
from shared.agents.db import get_conn


def pull_lingfeng_curves(
    province: str,
    product: str = "spot",
    start_date: str | None = None,
    end_date: str | None = None,
) -> int:
    """Pull curves from LingFeng and write to rm_forward_curves.

    Args:
        province: Province code (e.g. 'inner_mongolia_mengxi')
        product: Product type (e.g. 'spot', 'da', 'rt')
        start_date: Start date (YYYY-MM-DD), defaults to today
        end_date: End date (YYYY-MM-DD), defaults to +30 days

    Returns:
        Number of rows inserted/updated.
    """
    from services.lingfeng.collector import collect

    if start_date is None:
        start_date = pd.Timestamp.now().strftime("%Y-%m-%d")
    if end_date is None:
        end_date = (pd.Timestamp.now() + pd.Timedelta(days=30)).strftime("%Y-%m-%d")

    username = os.environ.get("LINGFENG_USERNAME", "")
    password = os.environ.get("LINGFENG_PASSWORD", "")
    download_dir = os.environ.get("LINGFENG_DOWNLOAD_DIR", "/tmp/lingfeng")

    path = collect(username, password, "mengxi", "price_forecast", start_date, end_date, download_dir)

    # Parse downloaded Excel
    df = pd.read_excel(path)
    curve_date = pd.Timestamp.now().date()
    rows_written = 0

    with get_conn() as conn:
        with conn.cursor() as cur:
            for _, row in df.iterrows():
                cur.execute("""
                    INSERT INTO marketdata.rm_forward_curves
                        (province, product, curve_date, delivery_date, delivery_hour, price_cny_kwh, source)
                    VALUES (%s, %s, %s, %s, %s, %s, 'lingfeng')
                    ON CONFLICT (province, product, curve_date, delivery_date, delivery_hour, source)
                    DO UPDATE SET price_cny_kwh = EXCLUDED.price_cny_kwh, uploaded_at = NOW()
                """, (
                    province, product, curve_date,
                    row.get("delivery_date"), row.get("hour"),
                    float(row.get("price", 0)) / 1000.0,  # MWh → kWh
                ))
                rows_written += 1
        conn.commit()

    return rows_written
```

- [ ] **Step 3: Implement manual_upload.py**

```python
# services/forward_curve/manual_upload.py
"""Process manually uploaded forward curve CSV files.

Expected CSV columns: delivery_date, province, product, price_cny_mwh
Optional: delivery_hour (if hourly granularity)
"""
from __future__ import annotations

import pandas as pd
from shared.agents.db import get_conn


def validate_curve_csv(df: pd.DataFrame) -> list[str]:
    """Validate uploaded curve CSV. Returns list of error messages (empty = valid)."""
    errors = []
    required = {"delivery_date", "province", "product", "price_cny_mwh"}
    missing = required - set(df.columns)
    if missing:
        errors.append(f"Missing columns: {missing}")
    if df.empty:
        errors.append("File is empty")
    if "price_cny_mwh" in df.columns and (df["price_cny_mwh"] <= 0).any():
        errors.append("price_cny_mwh must be positive")
    return errors


def upload_manual_curve(df: pd.DataFrame, curve_date: str | None = None) -> int:
    """Write validated curve DataFrame to rm_forward_curves.

    Args:
        df: DataFrame with columns: delivery_date, province, product, price_cny_mwh,
            optional: delivery_hour
        curve_date: Date the curve was generated (defaults to today)

    Returns:
        Number of rows written.
    """
    if curve_date is None:
        curve_date = pd.Timestamp.now().strftime("%Y-%m-%d")

    rows_written = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for _, row in df.iterrows():
                hour = row.get("delivery_hour") if "delivery_hour" in df.columns else None
                cur.execute("""
                    INSERT INTO marketdata.rm_forward_curves
                        (province, product, curve_date, delivery_date, delivery_hour, price_cny_kwh, source)
                    VALUES (%s, %s, %s, %s, %s, %s, 'manual')
                    ON CONFLICT (province, product, curve_date, delivery_date, delivery_hour, source)
                    DO UPDATE SET price_cny_kwh = EXCLUDED.price_cny_kwh, uploaded_at = NOW()
                """, (
                    row["province"], row["product"], curve_date,
                    row["delivery_date"], hour,
                    float(row["price_cny_mwh"]) / 1000.0,  # MWh → kWh
                ))
                rows_written += 1
        conn.commit()

    return rows_written
```

- [ ] **Step 4: Commit**

```bash
git add services/forward_curve/__init__.py services/forward_curve/lingfeng_pull.py services/forward_curve/manual_upload.py
git commit -m "feat(services): add forward curve service (LingFeng + manual upload)"
```

---

## Task 14: Operating Assets Service — Filename Mapper + Ingest Orchestrator

**Files:**
- Create: `services/operating_assets/__init__.py`
- Create: `services/operating_assets/filename_mapper.py`
- Create: `services/operating_assets/ingest.py`
- Create: `services/operating_assets/parsers/__init__.py`

- [ ] **Step 1: Create package structure**

```python
# services/operating_assets/__init__.py
"""Operating assets ingestion service: WeCom/folder watcher → DB."""
```

```python
# services/operating_assets/parsers/__init__.py
"""File parsers for BESS daily reports and wind farm operations."""
```

- [ ] **Step 2: Implement filename_mapper.py**

```python
# services/operating_assets/filename_mapper.py
"""Map uploaded filenames to asset IDs.

Configuration-driven (not hardcoded). Patterns checked in order;
first match wins.
"""
from __future__ import annotations

import re
from typing import Any

# Filename patterns → asset name mapping
# This is the canonical config; asset_id is resolved from rm_assets at runtime.
FILENAME_PATTERNS: list[dict[str, Any]] = [
    {"pattern": r"零碳46|零碳46风电经营统计", "asset_name": "零碳46风电", "asset_type": "wind"},
    {"pattern": r"裕昭沙子坝|220kV裕昭", "asset_name": "裕昭沙子坝", "asset_type": "bess"},
    {"pattern": r"远景乌拉特", "asset_name": "远景乌拉特", "asset_type": "bess"},
    {"pattern": r"景怡查干哈达", "asset_name": "景怡查干哈达", "asset_type": "bess"},
    {"pattern": r"景通四益堂", "asset_name": "景通四益堂", "asset_type": "bess"},
    {"pattern": r"四子王旗", "asset_name": "四子王旗", "asset_type": "bess"},
    {"pattern": r"悦杭独贵", "asset_name": "悦杭独贵", "asset_type": "bess"},
    {"pattern": r"景蓝乌尔图", "asset_name": "景蓝乌尔图", "asset_type": "bess"},
]


def resolve_asset(filename: str) -> dict[str, str] | None:
    """Match filename to an asset entry.

    Args:
        filename: Original filename (e.g. '裕昭沙子坝_20260715.xlsx')

    Returns:
        Dict with asset_name and asset_type, or None if no match.
    """
    for entry in FILENAME_PATTERNS:
        if re.search(entry["pattern"], filename):
            return {"asset_name": entry["asset_name"], "asset_type": entry["asset_type"]}
    return None
```

- [ ] **Step 3: Implement ingest.py**

```python
# services/operating_assets/ingest.py
"""Main ingestion orchestrator for operating asset files.

Scans a directory (or processes a single file) and routes to
the appropriate parser based on filename match and file structure.
"""
from __future__ import annotations

import os
import uuid
from pathlib import Path

import pandas as pd
from shared.agents.db import get_conn, execute_sql
from services.operating_assets.filename_mapper import resolve_asset


def ingest_file(file_path: str) -> dict:
    """Ingest a single file into the appropriate rm_ tables.

    Args:
        file_path: Absolute path to Excel file

    Returns:
        Dict with keys: asset_name, asset_type, parser, rows_written, errors
    """
    filename = os.path.basename(file_path)
    asset_info = resolve_asset(filename)

    if asset_info is None:
        return {"asset_name": None, "parser": None, "rows_written": 0,
                "errors": [f"No asset match for filename: {filename}"]}

    batch_id = str(uuid.uuid4())[:8]

    # Route to parser by asset type
    if asset_info["asset_type"] == "wind":
        from services.operating_assets.parsers.wind_farm import parse_wind_farm
        return parse_wind_farm(file_path, asset_info["asset_name"], batch_id)
    else:
        # BESS: detect if 运营统计 or 调度计划表
        xl = pd.ExcelFile(file_path)
        sheets = xl.sheet_names

        results = {"asset_name": asset_info["asset_name"], "asset_type": "bess",
                   "parser": "bess", "rows_written": 0, "errors": []}

        # Check for dispatch plan sheets (time-based 15-min data)
        if any("调度" in s or "计划" in s for s in sheets):
            from services.operating_assets.parsers.bess_dispatch import parse_bess_dispatch
            r = parse_bess_dispatch(xl, asset_info["asset_name"], batch_id)
            results["rows_written"] += r.get("rows_written", 0)
            results["errors"].extend(r.get("errors", []))

        # Check for daily ops summary
        if any("运营" in s or "统计" in s for s in sheets):
            from services.operating_assets.parsers.bess_daily import parse_bess_daily
            r = parse_bess_daily(xl, asset_info["asset_name"], batch_id)
            results["rows_written"] += r.get("rows_written", 0)
            results["errors"].extend(r.get("errors", []))

        return results


def scan_and_ingest(directory: str) -> list[dict]:
    """Scan directory for new/modified Excel files and ingest each.

    Args:
        directory: Path to scan (e.g. 'assets/operating/')

    Returns:
        List of per-file result dicts.
    """
    results = []
    path = Path(directory)
    for f in path.glob("**/*.xlsx"):
        if f.name.startswith("~$"):  # skip temp files
            continue
        result = ingest_file(str(f))
        results.append(result)
    return results
```

- [ ] **Step 4: Commit**

```bash
git add services/operating_assets/__init__.py services/operating_assets/filename_mapper.py \
    services/operating_assets/ingest.py services/operating_assets/parsers/__init__.py
git commit -m "feat(services): add operating assets ingest orchestrator + filename mapper"
```

---

## Task 15: Operating Assets — BESS Parsers

**Files:**
- Create: `services/operating_assets/parsers/bess_daily.py`
- Create: `services/operating_assets/parsers/bess_dispatch.py`

- [ ] **Step 1: Implement bess_daily.py**

```python
# services/operating_assets/parsers/bess_daily.py
"""Parser for BESS 运营统计 Excel (daily operations summary).

Source: 【日期】内蒙储能电站运营统计.xlsx — one sheet per station.
Target: rm_dispatch_daily (one row per asset per day).
"""
from __future__ import annotations

import pandas as pd
from shared.agents.db import get_conn


def parse_bess_daily(xl: pd.ExcelFile, asset_name: str, batch_id: str) -> dict:
    """Parse BESS daily operations sheets and write to rm_dispatch_daily.

    Args:
        xl: Open ExcelFile object
        asset_name: Resolved asset name from filename_mapper
        batch_id: Upload batch ID for traceability

    Returns:
        Dict with rows_written, errors
    """
    rows_written = 0
    errors = []

    with get_conn() as conn:
        with conn.cursor() as cur:
            # Get asset_id
            cur.execute(
                "SELECT id FROM marketdata.rm_assets WHERE name = %s", (asset_name,)
            )
            row = cur.fetchone()
            if not row:
                return {"rows_written": 0, "errors": [f"Asset not found: {asset_name}"]}
            asset_id = row[0]

            for sheet_name in xl.sheet_names:
                if asset_name not in sheet_name and "运营" not in sheet_name:
                    continue
                try:
                    df = xl.parse(sheet_name)
                    for _, r in df.iterrows():
                        dispatch_date = pd.to_datetime(r.get("日期", r.get("Date"))).date()
                        cur.execute("""
                            INSERT INTO marketdata.rm_dispatch_daily
                                (asset_id, dispatch_date, charge_mwh, discharge_mwh,
                                 auxiliary_consumption_mwh, cycle_count_day, conversion_ratio,
                                 discharge_revenue_cny, charge_cost_cny, net_margin_cny,
                                 upload_batch_id)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT (asset_id, dispatch_date) DO UPDATE SET
                                charge_mwh = EXCLUDED.charge_mwh,
                                discharge_mwh = EXCLUDED.discharge_mwh,
                                auxiliary_consumption_mwh = EXCLUDED.auxiliary_consumption_mwh,
                                cycle_count_day = EXCLUDED.cycle_count_day,
                                conversion_ratio = EXCLUDED.conversion_ratio,
                                discharge_revenue_cny = EXCLUDED.discharge_revenue_cny,
                                charge_cost_cny = EXCLUDED.charge_cost_cny,
                                net_margin_cny = EXCLUDED.net_margin_cny,
                                upload_batch_id = EXCLUDED.upload_batch_id
                        """, (
                            asset_id, dispatch_date,
                            r.get("日充电量", r.get("charge_mwh")),
                            r.get("日放电量", r.get("discharge_mwh")),
                            r.get("综合站用电", r.get("auxiliary_consumption_mwh")),
                            r.get("日充放次数", r.get("cycle_count_day")),
                            r.get("日充放转化率", r.get("conversion_ratio")),
                            r.get("放电收入", r.get("discharge_revenue_cny")),
                            r.get("充电费用", r.get("charge_cost_cny")),
                            r.get("站点毛利", r.get("net_margin_cny")),
                            batch_id,
                        ))
                        rows_written += 1
                except Exception as e:
                    errors.append(f"Sheet '{sheet_name}': {str(e)}")

        conn.commit()

    return {"rows_written": rows_written, "errors": errors}
```

- [ ] **Step 2: Implement bess_dispatch.py**

```python
# services/operating_assets/parsers/bess_dispatch.py
"""Parser for BESS 调度计划表 Excel (15-min dispatch plan).

Source: 电力交易调度计划表 — one sheet per day.
Columns: 时间, SOC(%), 操作员申报计划(MW), 当前预测(MW), 实时调度出力(MW), 实际执行功率(MW)
Target: rm_dispatch_plan (one row per asset per 15-min interval).
"""
from __future__ import annotations

import pandas as pd
from shared.agents.db import get_conn


def parse_bess_dispatch(xl: pd.ExcelFile, asset_name: str, batch_id: str) -> dict:
    """Parse BESS 15-min dispatch plan sheets.

    Args:
        xl: Open ExcelFile object
        asset_name: Resolved asset name
        batch_id: Upload batch ID

    Returns:
        Dict with rows_written, errors
    """
    rows_written = 0
    errors = []

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM marketdata.rm_assets WHERE name = %s", (asset_name,)
            )
            row = cur.fetchone()
            if not row:
                return {"rows_written": 0, "errors": [f"Asset not found: {asset_name}"]}
            asset_id = row[0]

            for sheet_name in xl.sheet_names:
                try:
                    df = xl.parse(sheet_name)
                    # Detect date from sheet name or first row
                    date_str = None
                    for col in df.columns:
                        if "日期" in str(col) or "date" in str(col).lower():
                            date_str = str(df[col].iloc[0])
                            break

                    if date_str is None:
                        # Try parsing sheet name as date
                        try:
                            date_str = pd.to_datetime(sheet_name).strftime("%Y-%m-%d")
                        except Exception:
                            continue

                    base_date = pd.to_datetime(date_str).date()

                    # Find time column and MW columns
                    time_col = next((c for c in df.columns if "时间" in str(c) or "time" in str(c).lower()), None)
                    if time_col is None:
                        continue

                    for _, r in df.iterrows():
                        time_val = r[time_col]
                        if pd.isna(time_val):
                            continue

                        # Construct interval_start as TIMESTAMPTZ
                        interval_start = pd.Timestamp(f"{base_date} {time_val}", tz="Asia/Shanghai")

                        soc = r.get("SOC(%)", r.get("SOC", None))
                        nominated = r.get("操作员申报计划(MW)", r.get("nominated_mw", None))
                        forecast = r.get("当前预测(MW)", r.get("forecast_mw", None))
                        dispatched = r.get("实时调度出力(MW)", r.get("dispatched_mw", None))
                        actual = r.get("实际执行功率(MW)", r.get("actual_mw", None))

                        cur.execute("""
                            INSERT INTO marketdata.rm_dispatch_plan
                                (asset_id, interval_start, soc_pct, nominated_mw,
                                 forecast_mw, dispatched_mw, actual_mw, upload_batch_id)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT (asset_id, interval_start) DO UPDATE SET
                                soc_pct = EXCLUDED.soc_pct,
                                nominated_mw = EXCLUDED.nominated_mw,
                                forecast_mw = EXCLUDED.forecast_mw,
                                dispatched_mw = EXCLUDED.dispatched_mw,
                                actual_mw = EXCLUDED.actual_mw,
                                upload_batch_id = EXCLUDED.upload_batch_id
                        """, (
                            asset_id, interval_start, soc, nominated,
                            forecast, dispatched, actual, batch_id,
                        ))
                        rows_written += 1
                except Exception as e:
                    errors.append(f"Sheet '{sheet_name}': {str(e)}")

        conn.commit()

    return {"rows_written": rows_written, "errors": errors}
```

- [ ] **Step 3: Commit**

```bash
git add services/operating_assets/parsers/bess_daily.py services/operating_assets/parsers/bess_dispatch.py
git commit -m "feat(services): add BESS daily and dispatch parsers"
```

---

## Task 16: Operating Assets — Wind Farm Parser

**Files:**
- Create: `services/operating_assets/parsers/wind_farm.py`
- Create: `tests/operating_assets/test_wind_farm_parser.py`

- [ ] **Step 1: Write failing test**

```python
# tests/operating_assets/test_wind_farm_parser.py
"""Tests for wind farm parser (零碳46风电经营统计)."""
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock
from services.operating_assets.parsers.wind_farm import (
    aggregate_15min_to_hourly,
    parse_settlement_detail_row,
)


def test_aggregate_15min_to_hourly():
    """4 x 15-min intervals aggregated to 1 hourly row: sum volumes, weighted-avg prices."""
    rows = [
        {"time": "00:00", "da_volume": 2.0, "da_price": 400.0, "rt_volume": 1.0, "rt_price": 350.0},
        {"time": "00:15", "da_volume": 2.5, "da_price": 410.0, "rt_volume": 0.5, "rt_price": 360.0},
        {"time": "00:30", "da_volume": 3.0, "da_price": 420.0, "rt_volume": 0.0, "rt_price": 0.0},
        {"time": "00:45", "da_volume": 2.5, "da_price": 390.0, "rt_volume": 1.5, "rt_price": 340.0},
    ]
    result = aggregate_15min_to_hourly(rows)
    assert result["da_volume_mwh"] == pytest.approx(10.0)
    assert result["rt_volume_mwh"] == pytest.approx(3.0)
    # Weighted avg DA price = (2*400 + 2.5*410 + 3*420 + 2.5*390) / 10
    expected_da_price = (800 + 1025 + 1260 + 975) / 10.0
    assert result["da_price_cny_mwh"] == pytest.approx(expected_da_price)


def test_parse_settlement_detail_row():
    """Single 结算明细 row parsed to canonical dict."""
    row = pd.Series({
        "日期": "2025-04-01",
        "时间": "00:00",
        "省调电量": 8.5,
        "省级实时价格": 0.35,
        "省级实时节点价": 0.36,
        "省级日前价格": 0.40,
        "省级日前电量": 6.0,
        "省级月内撮合价格": 0.38,
        "省级月内撮合电量": 1.0,
        "市场合约价格": 0.42,
        "收益": 3200.0,
        "弃风量": -2.5,
    })
    result = parse_settlement_detail_row(row)
    assert result["settled_mwh"] == 8.5
    assert result["da_volume_mwh"] == 6.0
    assert result["da_price_cny_mwh"] == pytest.approx(400.0)  # 0.40 * 1000
    assert result["deviation_grid_flow_mwh"] == -2.5
```

- [ ] **Step 2: Run test to verify failure**

Run: `pytest tests/operating_assets/test_wind_farm_parser.py -v`
Expected: FAIL

- [ ] **Step 3: Implement wind_farm.py**

```python
# services/operating_assets/parsers/wind_farm.py
"""Parser for 零碳46风电经营统计_YYYYMMDD.xlsx (wind farm operations file).

Ingests 3 sheets:
- 风场功率 → rm_dispatch_plan (15-min, forecast_mw + actual_mw)
- 结算明细 → rm_position_volumes (15-min aggregated to hourly)
- 经营统计 → rm_pnl_snapshots (monthly KPIs)

Also reads 市场价格 → rm_forward_curves (TOU monthly reference prices).
"""
from __future__ import annotations

import pandas as pd
import numpy as np
from typing import Any
from shared.agents.db import get_conn


def parse_settlement_detail_row(row: pd.Series) -> dict[str, Any]:
    """Parse a single 结算明细 row to canonical position volume dict.

    Prices in source are CNY/kWh; converted to CNY/MWh (* 1000).
    """
    return {
        "date": str(row.get("日期", "")),
        "time": str(row.get("时间", "")),
        "settled_mwh": float(row.get("省调电量", 0) or 0),
        "rt_price_cny_mwh": float(row.get("省级实时价格", 0) or 0) * 1000,
        "market_price_cny_mwh": float(row.get("省级实时节点价", 0) or 0) * 1000,
        "da_price_cny_mwh": float(row.get("省级日前价格", 0) or 0) * 1000,
        "da_volume_mwh": float(row.get("省级日前电量", 0) or 0),
        "intramonth_match_price_cny_mwh": float(row.get("省级月内撮合价格", 0) or 0) * 1000,
        "intramonth_match_volume_mwh": float(row.get("省级月内撮合电量", 0) or 0),
        "annual_price_cny_mwh": float(row.get("市场合约价格", 0) or 0) * 1000,
        "pnl_cny": float(row.get("收益", 0) or 0),
        "deviation_grid_flow_mwh": float(row.get("弃风量", 0) or 0),
    }


def aggregate_15min_to_hourly(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Aggregate 4 x 15-min rows into one hourly row.

    Volumes are summed. Prices are volume-weighted averages.
    """
    da_vol = sum(r.get("da_volume", 0) or 0 for r in rows)
    rt_vol = sum(r.get("rt_volume", 0) or 0 for r in rows)

    da_price_wt = sum((r.get("da_volume", 0) or 0) * (r.get("da_price", 0) or 0) for r in rows)
    rt_price_wt = sum((r.get("rt_volume", 0) or 0) * (r.get("rt_price", 0) or 0) for r in rows)

    return {
        "da_volume_mwh": da_vol,
        "rt_volume_mwh": rt_vol,
        "da_price_cny_mwh": da_price_wt / da_vol if da_vol > 0 else 0.0,
        "rt_price_cny_mwh": rt_price_wt / rt_vol if rt_vol > 0 else 0.0,
    }


def parse_wind_farm(file_path: str, asset_name: str, batch_id: str) -> dict:
    """Parse full wind farm operations Excel and write to DB.

    Args:
        file_path: Path to 零碳46风电经营统计_YYYYMMDD.xlsx
        asset_name: Resolved asset name (e.g. '零碳46风电')
        batch_id: Upload batch ID

    Returns:
        Dict with asset_name, asset_type, parser, rows_written, errors
    """
    xl = pd.ExcelFile(file_path)
    rows_written = 0
    errors = []

    with get_conn() as conn:
        with conn.cursor() as cur:
            # Resolve asset_id
            cur.execute("SELECT id FROM marketdata.rm_assets WHERE name = %s", (asset_name,))
            row = cur.fetchone()
            if not row:
                return {"asset_name": asset_name, "asset_type": "wind", "parser": "wind_farm",
                        "rows_written": 0, "errors": [f"Asset not found: {asset_name}"]}
            asset_id = row[0]

            # Resolve book_id
            cur.execute("SELECT id FROM marketdata.rm_books WHERE asset_id = %s", (asset_id,))
            book_row = cur.fetchone()
            book_id = book_row[0] if book_row else None

            # --- Sheet 1: 风场功率 → rm_dispatch_plan ---
            if "风场功率" in xl.sheet_names:
                try:
                    df = xl.parse("风场功率")
                    for _, r in df.iterrows():
                        date_val = r.get("日期")
                        time_val = r.get("时间")
                        if pd.isna(date_val) or pd.isna(time_val):
                            continue
                        interval_start = pd.Timestamp(
                            f"{pd.to_datetime(date_val).date()} {time_val}",
                            tz="Asia/Shanghai"
                        )
                        forecast = r.get("D+1日前预测功率(MW)", r.get("D+1预测功率(MW)"))
                        actual = r.get("实际出力(MW)", r.get("实际功率(MW)"))
                        cur.execute("""
                            INSERT INTO marketdata.rm_dispatch_plan
                                (asset_id, interval_start, forecast_mw, actual_mw, upload_batch_id)
                            VALUES (%s,%s,%s,%s,%s)
                            ON CONFLICT (asset_id, interval_start) DO UPDATE SET
                                forecast_mw = EXCLUDED.forecast_mw,
                                actual_mw = EXCLUDED.actual_mw,
                                upload_batch_id = EXCLUDED.upload_batch_id
                        """, (asset_id, interval_start, forecast, actual, batch_id))
                        rows_written += 1
                except Exception as e:
                    errors.append(f"风场功率: {str(e)}")

            # --- Sheet 2: 结算明细 → rm_position_volumes (hourly) ---
            if "结算明细" in xl.sheet_names and book_id:
                try:
                    df = xl.parse("结算明细")
                    # Group by date+hour (4 x 15-min → 1 hour)
                    for _, r in df.iterrows():
                        parsed = parse_settlement_detail_row(r)
                        delivery_date = pd.to_datetime(parsed["date"]).date()
                        time_parts = str(parsed["time"]).split(":")
                        hour = int(time_parts[0]) if time_parts else 0

                        cur.execute("""
                            INSERT INTO marketdata.rm_position_volumes
                                (book_id, delivery_date, hour, da_price_cny_mwh, rt_price_cny_mwh,
                                 da_volume_mwh, intramonth_match_price_cny_mwh,
                                 intramonth_match_volume_mwh, annual_price_cny_mwh,
                                 market_price_cny_mwh, settled_mwh,
                                 deviation_grid_flow_mwh, pnl_cny, upload_batch_id)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT (book_id, delivery_date, hour) DO UPDATE SET
                                da_price_cny_mwh = COALESCE(EXCLUDED.da_price_cny_mwh, marketdata.rm_position_volumes.da_price_cny_mwh),
                                rt_price_cny_mwh = COALESCE(EXCLUDED.rt_price_cny_mwh, marketdata.rm_position_volumes.rt_price_cny_mwh),
                                da_volume_mwh = COALESCE(EXCLUDED.da_volume_mwh, marketdata.rm_position_volumes.da_volume_mwh),
                                settled_mwh = COALESCE(EXCLUDED.settled_mwh, marketdata.rm_position_volumes.settled_mwh),
                                deviation_grid_flow_mwh = COALESCE(EXCLUDED.deviation_grid_flow_mwh, marketdata.rm_position_volumes.deviation_grid_flow_mwh),
                                pnl_cny = COALESCE(EXCLUDED.pnl_cny, marketdata.rm_position_volumes.pnl_cny),
                                upload_batch_id = EXCLUDED.upload_batch_id
                        """, (
                            book_id, delivery_date, hour,
                            parsed["da_price_cny_mwh"], parsed["rt_price_cny_mwh"],
                            parsed["da_volume_mwh"], parsed["intramonth_match_price_cny_mwh"],
                            parsed["intramonth_match_volume_mwh"], parsed["annual_price_cny_mwh"],
                            parsed["market_price_cny_mwh"], parsed["settled_mwh"],
                            parsed["deviation_grid_flow_mwh"], parsed["pnl_cny"], batch_id,
                        ))
                        rows_written += 1
                except Exception as e:
                    errors.append(f"结算明细: {str(e)}")

            # --- Sheet 3: 经营统计 → rm_pnl_snapshots ---
            if "经营统计" in xl.sheet_names and book_id:
                try:
                    df = xl.parse("经营统计")
                    for _, r in df.iterrows():
                        snapshot_date = pd.to_datetime(r.get("月份", r.get("日期"))).date()
                        cur.execute("""
                            INSERT INTO marketdata.rm_pnl_snapshots
                                (book_id, snapshot_date, realized_cny,
                                 curtailment_mwh, curtailment_rate_pct,
                                 curtailment_opportunity_cost_cny, equivalent_hours)
                            VALUES (%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT (book_id, snapshot_date) DO UPDATE SET
                                realized_cny = EXCLUDED.realized_cny,
                                curtailment_mwh = EXCLUDED.curtailment_mwh,
                                curtailment_rate_pct = EXCLUDED.curtailment_rate_pct,
                                curtailment_opportunity_cost_cny = EXCLUDED.curtailment_opportunity_cost_cny,
                                equivalent_hours = EXCLUDED.equivalent_hours
                        """, (
                            book_id, snapshot_date,
                            r.get("收益", r.get("realized_cny")),
                            r.get("弃风量"),
                            r.get("弃风率"),
                            r.get("弃风损失", r.get("curtailment_opportunity_cost_cny")),
                            r.get("等效满负荷小时数", r.get("equivalent_hours")),
                        ))
                        rows_written += 1
                except Exception as e:
                    errors.append(f"经营统计: {str(e)}")

        conn.commit()

    return {"asset_name": asset_name, "asset_type": "wind", "parser": "wind_farm",
            "rows_written": rows_written, "errors": errors}
```

- [ ] **Step 4: Run tests**

Run: `pytest tests/operating_assets/test_wind_farm_parser.py -v`
Expected: All 2 tests PASS

- [ ] **Step 5: Commit**

```bash
git add services/operating_assets/parsers/wind_farm.py tests/operating_assets/test_wind_farm_parser.py
git commit -m "feat(services): add wind farm operations parser"
```

---

## Task 17: Streamlit App — Entry Point + Tab 1 (Asset Config)

**Files:**
- Create: `apps/asset-risk/app.py`
- Create: `apps/asset-risk/tab_asset_config.py`

- [ ] **Step 1: Create app.py**

```python
# apps/asset-risk/app.py
"""
Asset Risk Management — Streamlit Application

Risk cockpit for asset-backed trading books: wind, solar, BESS, thermal.

Run locally:
    streamlit run apps/asset-risk/app.py --server.port=8512
"""
from __future__ import annotations

import os
import sys

_repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, _repo_root)

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(_repo_root, "config", ".env"), override=False)
except ImportError:
    pass

import streamlit as st

st.set_page_config(
    page_title="Asset Risk Management",
    layout="wide",
    initial_sidebar_state="expanded",
)

from sqlalchemy import create_engine


@st.cache_resource
def _get_engine():
    url = os.environ.get("PGURL") or os.environ.get("DB_DSN")
    if not url:
        st.error("Database URL not configured (PGURL or DB_DSN)")
        st.stop()
    return create_engine(url, pool_pre_ping=True)


engine = _get_engine()

# Auth
try:
    from auth.rbac import require_role
    require_role(["Admin", "Trader", "Quant", "RiskOfficer"])
except Exception:
    pass  # Auth optional in dev

st.title("Asset Risk Management")

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "Asset Config", "Settlement", "Realised P&L",
    "Positions & MtM", "VaR & Greeks", "Agent"
])

with tab1:
    from apps.asset_risk.tab_asset_config import render_asset_config
    render_asset_config(engine)

with tab2:
    from apps.asset_risk.tab_settlement import render_settlement
    render_settlement(engine)

with tab3:
    from apps.asset_risk.tab_pnl import render_pnl
    render_pnl(engine)

with tab4:
    from apps.asset_risk.tab_positions import render_positions
    render_positions(engine)

with tab5:
    from apps.asset_risk.tab_var import render_var
    render_var(engine)

with tab6:
    from apps.asset_risk.tab_agent import render_agent
    render_agent(engine)
```

- [ ] **Step 2: Create tab_asset_config.py**

```python
# apps/asset-risk/tab_asset_config.py
"""Tab 1 — Asset Configuration: CRUD for rm_assets and rm_books."""
from __future__ import annotations

import pandas as pd
import streamlit as st
from sqlalchemy import text


def render_asset_config(engine):
    """Render asset configuration tab."""
    st.subheader("Asset Registry")

    # Load existing assets
    with engine.connect() as conn:
        assets_df = pd.read_sql(text("""
            SELECT a.id, a.name, a.asset_type, a.province, a.capacity_mw,
                   a.bess_duration_h, a.bess_dod_pct, a.status, a.commission_date,
                   b.id as book_id, b.name as book_name
            FROM marketdata.rm_assets a
            LEFT JOIN marketdata.rm_books b ON b.asset_id = a.id
            ORDER BY a.name
        """), conn)

    if not assets_df.empty:
        st.dataframe(assets_df, use_container_width=True, hide_index=True)
    else:
        st.info("No assets registered yet. Add one below.")

    # Add new asset form
    st.subheader("Add Asset")
    with st.form("add_asset"):
        col1, col2, col3 = st.columns(3)
        with col1:
            name = st.text_input("Asset Name")
            asset_type = st.selectbox("Type", ["wind", "solar", "bess", "thermal"])
        with col2:
            province = st.text_input("Province", value="inner_mongolia_mengxi")
            capacity = st.number_input("Capacity (MW)", min_value=0.0, step=0.5)
        with col3:
            commission_date = st.date_input("Commission Date")
            bess_duration = st.number_input("BESS Duration (h)", min_value=0.0, step=0.5,
                                            disabled=(asset_type != "bess"))
            bess_dod = st.number_input("BESS DoD (%)", min_value=0.0, max_value=100.0,
                                       step=1.0, disabled=(asset_type != "bess"))

        notes = st.text_area("Notes", height=68)
        submitted = st.form_submit_button("Create Asset + Book")

        if submitted and name:
            with engine.begin() as conn:
                result = conn.execute(text("""
                    INSERT INTO marketdata.rm_assets
                        (name, asset_type, province, capacity_mw, bess_duration_h,
                         bess_dod_pct, commission_date, notes)
                    VALUES (:name, :type, :prov, :cap, :dur, :dod, :cd, :notes)
                    RETURNING id
                """), {
                    "name": name, "type": asset_type, "prov": province,
                    "cap": capacity, "dur": bess_duration if asset_type == "bess" else None,
                    "dod": bess_dod if asset_type == "bess" else None,
                    "cd": commission_date, "notes": notes,
                })
                asset_id = result.scalar()
                # Auto-create linked book
                conn.execute(text("""
                    INSERT INTO marketdata.rm_books (name, book_type, asset_id)
                    VALUES (:name, 'asset', :aid)
                """), {"name": f"{name} Book", "aid": asset_id})
            st.success(f"Created asset '{name}' (ID: {asset_id}) with linked book.")
            st.rerun()
```

- [ ] **Step 3: Commit**

```bash
git add apps/asset-risk/app.py apps/asset-risk/tab_asset_config.py
git commit -m "feat(app): add asset-risk Streamlit app entry point + Tab 1 (Asset Config)"
```

---

## Task 18: App Tab 2 — Settlement Upload + Analytics

**Files:**
- Create: `apps/asset-risk/tab_settlement.py`

- [ ] **Step 1: Create tab_settlement.py**

```python
# apps/asset-risk/tab_settlement.py
"""Tab 2 — Settlement: file upload, parsing, analytics."""
from __future__ import annotations

import pandas as pd
import streamlit as st
from sqlalchemy import text


def render_settlement(engine):
    """Render settlement tab with upload panel and analytics."""
    st.subheader("Settlement Upload")

    # Book selector
    with engine.connect() as conn:
        books = pd.read_sql(text(
            "SELECT id, name FROM marketdata.rm_books ORDER BY name"
        ), conn)

    if books.empty:
        st.warning("No books found. Create an asset in Tab 1 first.")
        return

    book_id = st.selectbox("Book", books["id"].tolist(),
                           format_func=lambda x: books[books["id"] == x]["name"].iloc[0])
    settlement_month = st.date_input("Settlement Month (1st of month)")

    # File upload
    uploaded = st.file_uploader("Upload settlement file", type=["xlsx", "xls", "csv", "pdf"])

    if uploaded and st.button("Process File"):
        file_type = uploaded.name.split(".")[-1].lower()
        if file_type == "pdf":
            _process_pdf(uploaded, book_id, settlement_month, engine)
        else:
            _process_excel(uploaded, book_id, settlement_month, engine)

    # Analytics panel
    st.divider()
    st.subheader("Settlement Analytics")
    _render_analytics(book_id, engine)


def _process_excel(uploaded, book_id: int, settlement_month, engine):
    """Process uploaded Excel settlement file."""
    import io
    from libs.settlement.parser import detect_format, parse_trade_capture, parse_capacity_compensation

    xl = pd.ExcelFile(io.BytesIO(uploaded.read()))
    fmt = detect_format(xl)
    st.info(f"Detected format: **{fmt}**")

    if fmt == "trade_capture":
        df = xl.parse("Trades")
        items = parse_trade_capture(df)
    elif fmt == "capacity_compensation":
        items = parse_capacity_compensation(xl)
    elif fmt == "wind_farm_ops":
        # Route to operating assets parser
        import tempfile, os
        from services.operating_assets.ingest import ingest_file
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            uploaded.seek(0)
            tmp.write(uploaded.read())
            tmp_path = tmp.name
        result = ingest_file(tmp_path)
        os.unlink(tmp_path)
        st.success(f"Wind farm data ingested: {result['rows_written']} rows. Errors: {result['errors']}")
        return
    else:
        st.error(f"Unknown format: {fmt}. Please use manual column mapping.")
        return

    # Write settlement + items to DB
    with engine.begin() as conn:
        result = conn.execute(text("""
            INSERT INTO marketdata.rm_settlements (book_id, settlement_month, file_name, file_type, status)
            VALUES (:bid, :month, :fname, :ftype, 'processed')
            RETURNING id
        """), {"bid": book_id, "month": settlement_month, "fname": uploaded.name, "ftype": "excel"})
        settlement_id = result.scalar()

        for item in items:
            conn.execute(text("""
                INSERT INTO marketdata.rm_settlement_items
                    (settlement_id, category, delivery_date, volume_mwh,
                     price_cny_kwh, amount_cny, amount_receivable_cny,
                     amount_settled_cny, amount_diff_cny, counterparty, notes)
                VALUES (:sid, :cat, :dd, :vol, :price, :amt, :recv, :settled, :diff, :cp, :notes)
            """), {
                "sid": settlement_id, "cat": item["category"],
                "dd": item.get("delivery_date"), "vol": item.get("volume_mwh"),
                "price": item.get("price_cny_kwh"), "amt": item["amount_cny"],
                "recv": item.get("amount_receivable_cny"),
                "settled": item.get("amount_settled_cny"),
                "diff": item.get("amount_diff_cny"),
                "cp": item.get("counterparty"), "notes": item.get("notes"),
            }))

    st.success(f"Processed {len(items)} settlement items.")


def _process_pdf(uploaded, book_id: int, settlement_month, engine):
    """Process uploaded PDF settlement (上网电费结算单)."""
    st.warning("PDF parsing not yet implemented — requires pdfplumber integration.")


def _render_analytics(book_id: int, engine):
    """Render settlement analytics for selected book."""
    with engine.connect() as conn:
        items_df = pd.read_sql(text("""
            SELECT si.category, si.peak_period, si.volume_mwh, si.amount_cny,
                   si.amount_receivable_cny, si.amount_settled_cny, si.amount_diff_cny,
                   s.settlement_month
            FROM marketdata.rm_settlement_items si
            JOIN marketdata.rm_settlements s ON s.id = si.settlement_id
            WHERE s.book_id = :bid
            ORDER BY s.settlement_month DESC, si.category
        """), conn, params={"bid": book_id})

    if items_df.empty:
        st.info("No settlement data yet for this book.")
        return

    # Summary metrics
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Amount", f"¥{items_df['amount_cny'].sum():,.0f}")
    col2.metric("Total Volume", f"{items_df['volume_mwh'].sum():,.1f} MWh")
    if items_df["volume_mwh"].sum() > 0:
        avg_price = items_df["amount_cny"].sum() / items_df["volume_mwh"].sum()
        col3.metric("Avg Price", f"¥{avg_price:,.1f}/MWh")

    # Category breakdown
    st.dataframe(
        items_df.groupby("category").agg(
            total_amount=("amount_cny", "sum"),
            total_volume=("volume_mwh", "sum"),
        ).sort_values("total_amount", ascending=False),
        use_container_width=True,
    )

    # Reconciliation view
    recon = items_df[items_df["amount_diff_cny"].notna() & (items_df["amount_diff_cny"] != 0)]
    if not recon.empty:
        st.subheader("Reconciliation (应收 vs 实际结算)")
        st.dataframe(recon[["category", "amount_receivable_cny", "amount_settled_cny", "amount_diff_cny"]],
                     use_container_width=True, hide_index=True)
```

- [ ] **Step 2: Commit**

```bash
git add apps/asset-risk/tab_settlement.py
git commit -m "feat(app): add Tab 2 — Settlement upload + analytics"
```

---

## Task 19: App Tab 3 — Realised P&L

**Files:**
- Create: `apps/asset-risk/tab_pnl.py`

- [ ] **Step 1: Create tab_pnl.py**

```python
# apps/asset-risk/tab_pnl.py
"""Tab 3 — Realised P&L: waterfall chart + operational KPIs."""
from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import text


def render_pnl(engine):
    """Render Realised P&L tab."""
    st.subheader("Realised P&L")

    with engine.connect() as conn:
        books = pd.read_sql(text(
            "SELECT b.id, b.name, a.asset_type FROM marketdata.rm_books b "
            "LEFT JOIN marketdata.rm_assets a ON a.id = b.asset_id ORDER BY b.name"
        ), conn)

    if books.empty:
        st.warning("No books found.")
        return

    col1, col2 = st.columns([2, 1])
    with col1:
        book_id = st.selectbox("Book", books["id"].tolist(),
                               format_func=lambda x: books[books["id"] == x]["name"].iloc[0],
                               key="pnl_book")
    with col2:
        date_range = st.date_input("Date Range", value=[], key="pnl_dates")

    asset_type = books[books["id"] == book_id]["asset_type"].iloc[0] or "bess"

    # Load settlement items for waterfall
    with engine.connect() as conn:
        items_df = pd.read_sql(text("""
            SELECT si.category, SUM(si.amount_cny) as total
            FROM marketdata.rm_settlement_items si
            JOIN marketdata.rm_settlements s ON s.id = si.settlement_id
            WHERE s.book_id = :bid
            GROUP BY si.category
            ORDER BY total DESC
        """), conn, params={"bid": book_id})

    if items_df.empty:
        st.info("No P&L data yet. Upload settlements in Tab 2.")
        return

    # Build waterfall chart
    fig = _build_waterfall(items_df, asset_type)
    st.plotly_chart(fig, use_container_width=True)

    # KPI panel
    _render_kpis(book_id, asset_type, engine)


def _build_waterfall(items_df: pd.DataFrame, asset_type: str) -> go.Figure:
    """Build Plotly waterfall chart from settlement items."""
    categories = items_df["category"].tolist()
    values = items_df["total"].tolist()

    # Add net total
    categories.append("Net P&L")
    values.append(sum(values))

    measures = ["relative"] * (len(categories) - 1) + ["total"]

    fig = go.Figure(go.Waterfall(
        orientation="v",
        measure=measures,
        x=categories,
        y=values,
        connector={"line": {"color": "rgb(63, 63, 63)"}},
        increasing={"marker": {"color": "#2ecc71"}},
        decreasing={"marker": {"color": "#e74c3c"}},
        totals={"marker": {"color": "#3498db"}},
    ))
    fig.update_layout(
        title=f"P&L Waterfall ({asset_type.upper()})",
        yaxis_title="CNY",
        showlegend=False,
        height=450,
    )
    return fig


def _render_kpis(book_id: int, asset_type: str, engine):
    """Render operational KPIs based on asset type."""
    st.subheader("Operational KPIs")

    if asset_type == "bess":
        with engine.connect() as conn:
            ops = pd.read_sql(text("""
                SELECT dispatch_date, charge_mwh, discharge_mwh, cycle_count_day,
                       conversion_ratio, discharge_revenue_cny, charge_cost_cny, net_margin_cny
                FROM marketdata.rm_dispatch_daily dd
                JOIN marketdata.rm_books b ON b.asset_id = dd.asset_id
                WHERE b.id = :bid
                ORDER BY dispatch_date DESC LIMIT 30
            """), conn, params={"bid": book_id})

        if not ops.empty:
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Discharge", f"{ops['discharge_mwh'].sum():,.1f} MWh")
            col2.metric("Total Charge", f"{ops['charge_mwh'].sum():,.1f} MWh")
            col3.metric("Avg Conversion", f"{ops['conversion_ratio'].mean():.2%}")
            col4.metric("Net Margin", f"¥{ops['net_margin_cny'].sum():,.0f}")
            st.dataframe(ops, use_container_width=True, hide_index=True)

    elif asset_type == "wind":
        with engine.connect() as conn:
            snapshots = pd.read_sql(text("""
                SELECT snapshot_date, realized_cny, curtailment_mwh,
                       curtailment_rate_pct, curtailment_opportunity_cost_cny, equivalent_hours
                FROM marketdata.rm_pnl_snapshots
                WHERE book_id = :bid
                ORDER BY snapshot_date DESC LIMIT 12
            """), conn, params={"bid": book_id})

        if not snapshots.empty:
            col1, col2, col3 = st.columns(3)
            col1.metric("Curtailment Rate (latest)",
                        f"{snapshots['curtailment_rate_pct'].iloc[0]:.1%}" if snapshots['curtailment_rate_pct'].iloc[0] else "N/A")
            col2.metric("Curtailment Cost (YTD)",
                        f"¥{snapshots['curtailment_opportunity_cost_cny'].sum():,.0f}")
            col3.metric("Equiv. Hours (YTD)",
                        f"{snapshots['equivalent_hours'].sum():,.0f} h")

            # Curtailment rate time series
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=snapshots["snapshot_date"], y=snapshots["curtailment_rate_pct"],
                mode="lines+markers", name="Curtailment Rate"
            ))
            fig.add_hline(y=0.10, line_dash="dash", line_color="red",
                          annotation_text="10% threshold")
            fig.update_layout(title="Monthly Curtailment Rate", yaxis_title="%", height=300)
            st.plotly_chart(fig, use_container_width=True)
```

- [ ] **Step 2: Commit**

```bash
git add apps/asset-risk/tab_pnl.py
git commit -m "feat(app): add Tab 3 — Realised P&L waterfall + KPIs"
```

---

## Task 20: App Tab 4 — Positions & MtM

**Files:**
- Create: `apps/asset-risk/tab_positions.py`

- [ ] **Step 1: Create tab_positions.py**

```python
# apps/asset-risk/tab_positions.py
"""Tab 4 — Positions & MtM: position volumes, contract register, forward curves, MtM."""
from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import text
from libs.risk.mtm import compute_mtm


def render_positions(engine):
    """Render Positions & MtM tab."""
    with engine.connect() as conn:
        books = pd.read_sql(text(
            "SELECT id, name FROM marketdata.rm_books ORDER BY name"
        ), conn)

    if books.empty:
        st.warning("No books found.")
        return

    book_id = st.selectbox("Book", books["id"].tolist(),
                           format_func=lambda x: books[books["id"] == x]["name"].iloc[0],
                           key="pos_book")

    subtab1, subtab2, subtab3, subtab4 = st.tabs([
        "Hourly Volumes", "Contract Register", "Forward Curves", "MtM"
    ])

    with subtab1:
        _render_hourly_volumes(book_id, engine)
    with subtab2:
        _render_contract_register(book_id, engine)
    with subtab3:
        _render_forward_curves(engine)
    with subtab4:
        _render_mtm(book_id, engine)


def _render_hourly_volumes(book_id: int, engine):
    """Render rm_position_volumes as date×hour grid."""
    st.subheader("Hourly Position Volumes")
    date_range = st.date_input("Filter dates", value=[], key="vol_dates")

    query = """
        SELECT delivery_date, hour, da_price_cny_mwh, rt_price_cny_mwh,
               da_volume_mwh, rt_volume_mwh, monthly_auction_volume_mwh,
               intramonth_match_volume_mwh, annual_volume_mwh,
               market_price_cny_mwh, actual_price_cny_mwh, pnl_cny,
               nominated_mwh, cleared_mwh, settled_mwh
        FROM marketdata.rm_position_volumes
        WHERE book_id = :bid
        ORDER BY delivery_date DESC, hour
        LIMIT 500
    """
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn, params={"bid": book_id})

    if df.empty:
        st.info("No position volume data yet.")
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)
        csv = df.to_csv(index=False)
        st.download_button("Export CSV", csv, "position_volumes.csv", "text/csv")


def _render_contract_register(book_id: int, engine):
    """Render rm_positions for this book."""
    st.subheader("Contract Register")
    with engine.connect() as conn:
        positions = pd.read_sql(text("""
            SELECT id, channel, instrument_type, direction, volume_mwh,
                   price_cny_mwh, start_date, end_date, counterparty, status
            FROM marketdata.rm_positions
            WHERE book_id = :bid
            ORDER BY start_date DESC
        """), conn, params={"bid": book_id})

    if positions.empty:
        st.info("No positions recorded.")
    else:
        st.dataframe(positions, use_container_width=True, hide_index=True)


def _render_forward_curves(engine):
    """Render forward curve viewer."""
    st.subheader("Forward Curves")
    with engine.connect() as conn:
        curves = pd.read_sql(text("""
            SELECT province, product, delivery_date, delivery_hour,
                   price_cny_kwh * 1000 as price_cny_mwh, source, curve_date
            FROM marketdata.rm_forward_curves
            ORDER BY delivery_date
            LIMIT 1000
        """), conn)

    if curves.empty:
        st.info("No forward curves loaded.")
        return

    # Manual upload
    st.subheader("Upload Curve CSV")
    uploaded = st.file_uploader("CSV (delivery_date, province, product, price_cny_mwh)",
                                type=["csv"], key="curve_upload")
    if uploaded and st.button("Upload Curve"):
        from services.forward_curve.manual_upload import validate_curve_csv, upload_manual_curve
        df = pd.read_csv(uploaded)
        errors = validate_curve_csv(df)
        if errors:
            st.error(f"Validation errors: {errors}")
        else:
            n = upload_manual_curve(df)
            st.success(f"Uploaded {n} curve points.")
            st.rerun()

    # Chart
    fig = go.Figure()
    for source in curves["source"].unique():
        subset = curves[curves["source"] == source]
        fig.add_trace(go.Scatter(x=subset["delivery_date"], y=subset["price_cny_mwh"],
                                 mode="lines", name=source))
    fig.update_layout(title="Forward Curve", xaxis_title="Delivery Date",
                      yaxis_title="CNY/MWh", height=350)
    st.plotly_chart(fig, use_container_width=True)


def _render_mtm(book_id: int, engine):
    """Render MtM dashboard."""
    st.subheader("Mark-to-Market")
    with engine.connect() as conn:
        positions = pd.read_sql(text("""
            SELECT direction, volume_mwh, price_cny_mwh, province, start_date, end_date, channel
            FROM marketdata.rm_positions
            WHERE book_id = :bid AND status = 'open'
        """), conn, params={"bid": book_id})

        # Get latest forward prices by province
        fwd = pd.read_sql(text("""
            SELECT DISTINCT ON (province) province, price_cny_kwh * 1000 as price
            FROM marketdata.rm_forward_curves
            ORDER BY province, curve_date DESC, delivery_date DESC
        """), conn)

    if positions.empty:
        st.info("No open positions for MtM.")
        return

    forward_prices = dict(zip(fwd["province"], fwd["price"])) if not fwd.empty else {}
    mtm_results = compute_mtm(positions.to_dict("records"), forward_prices)
    mtm_df = pd.DataFrame(mtm_results)

    total_unrealized = mtm_df["unrealized_pnl_cny"].sum()
    st.metric("Total Unrealised P&L", f"¥{total_unrealized:,.0f}")
    st.dataframe(mtm_df[["channel", "direction", "volume_mwh", "price_cny_mwh",
                          "forward_price_cny_mwh", "unrealized_pnl_cny"]],
                 use_container_width=True, hide_index=True)
```

- [ ] **Step 2: Commit**

```bash
git add apps/asset-risk/tab_positions.py
git commit -m "feat(app): add Tab 4 — Positions & MtM"
```

---

## Task 21: App Tab 5 — VaR & Greeks

**Files:**
- Create: `apps/asset-risk/tab_var.py`

- [ ] **Step 1: Create tab_var.py**

```python
# apps/asset-risk/tab_var.py
"""Tab 5 — VaR & Greeks: risk metrics dashboard."""
from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import text
from libs.risk.var import historical_var, parametric_var
from libs.risk.greeks import compute_book_greeks


def render_var(engine):
    """Render VaR & Greeks tab."""
    with engine.connect() as conn:
        books = pd.read_sql(text(
            "SELECT id, name FROM marketdata.rm_books ORDER BY name"
        ), conn)

    if books.empty:
        st.warning("No books found.")
        return

    book_id = st.selectbox("Book", books["id"].tolist(),
                           format_func=lambda x: books[books["id"] == x]["name"].iloc[0],
                           key="var_book")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Greeks")
        _render_greeks(book_id, engine)

    with col2:
        st.subheader("Value at Risk")
        _render_var_panel(book_id, engine)

    # Backtesting
    st.divider()
    _render_backtest(book_id, engine)

    # Stress scenarios
    st.divider()
    _render_stress(book_id, engine)


def _render_greeks(book_id: int, engine):
    """Compute and display book-level Greeks."""
    with engine.connect() as conn:
        positions = pd.read_sql(text("""
            SELECT direction, volume_mwh, status
            FROM marketdata.rm_positions WHERE book_id = :bid
        """), conn, params={"bid": book_id})

    if positions.empty:
        st.info("No positions.")
        return

    greeks = compute_book_greeks(positions.to_dict("records"))
    st.metric("Delta (net MWh)", f"{greeks['delta_mwh']:,.1f}")
    st.metric("Gamma", f"{greeks['gamma']:.4f}")
    st.metric("Vega", f"{greeks['vega']:.4f}")


def _render_var_panel(book_id: int, engine):
    """Compute and display VaR."""
    with engine.connect() as conn:
        # Get delta
        positions = pd.read_sql(text("""
            SELECT direction, volume_mwh, status
            FROM marketdata.rm_positions WHERE book_id = :bid AND status = 'open'
        """), conn, params={"bid": book_id})

        # Get price history for historical VaR
        prices = pd.read_sql(text("""
            SELECT delivery_date, AVG(market_price_cny_mwh) as price
            FROM marketdata.rm_position_volumes
            WHERE book_id = :bid AND market_price_cny_mwh IS NOT NULL
            GROUP BY delivery_date ORDER BY delivery_date
        """), conn, params={"bid": book_id})

    if positions.empty:
        st.info("No open positions for VaR.")
        return

    greeks = compute_book_greeks(positions.to_dict("records"))
    delta = greeks["delta_mwh"]

    if len(prices) >= 20:
        returns = prices["price"].diff().dropna()
        sigma = float(returns.tail(20).std())

        # Historical VaR
        hist_95 = historical_var(returns, delta, 0.95)
        hist_99 = historical_var(returns, delta, 0.99)

        # Parametric VaR
        param_95 = parametric_var(delta, sigma, 0.95, 1)
        param_99 = parametric_var(delta, sigma, 0.99, 1)
        param_10d = parametric_var(delta, sigma, 0.95, 10)

        var_df = pd.DataFrame({
            "Metric": ["1D 95% VaR", "1D 99% VaR", "10D 95% VaR"],
            "Historical": [f"¥{hist_95:,.0f}", f"¥{hist_99:,.0f}", f"¥{hist_95 * np.sqrt(10):,.0f}"],
            "Parametric": [f"¥{param_95:,.0f}", f"¥{param_99:,.0f}", f"¥{param_10d:,.0f}"],
        })
        st.dataframe(var_df, use_container_width=True, hide_index=True)
    else:
        st.warning(f"Need at least 20 price observations for VaR (have {len(prices)}).")


def _render_backtest(book_id: int, engine):
    """VaR backtesting chart."""
    st.subheader("VaR Backtesting")
    with engine.connect() as conn:
        snapshots = pd.read_sql(text("""
            SELECT snapshot_date, var_1d_95_cny, method
            FROM marketdata.rm_var_snapshots
            WHERE book_id = :bid ORDER BY snapshot_date
        """), conn, params={"bid": book_id})

        pnl = pd.read_sql(text("""
            SELECT snapshot_date, realized_cny
            FROM marketdata.rm_pnl_snapshots
            WHERE book_id = :bid ORDER BY snapshot_date
        """), conn, params={"bid": book_id})

    if snapshots.empty or pnl.empty:
        st.info("Insufficient data for backtesting.")
        return

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=pnl["snapshot_date"], y=pnl["realized_cny"],
                             mode="lines", name="Actual P&L"))
    fig.add_trace(go.Scatter(x=snapshots["snapshot_date"], y=-snapshots["var_1d_95_cny"],
                             mode="lines", name="VaR Band (95%)", line=dict(dash="dash", color="red")))
    fig.update_layout(height=300, yaxis_title="CNY")
    st.plotly_chart(fig, use_container_width=True)


def _render_stress(book_id: int, engine):
    """User-defined stress scenarios."""
    st.subheader("Stress Scenarios")
    col1, col2 = st.columns(2)
    with col1:
        spot_shock = st.slider("Spot price shock (%)", -50, 50, 0, key="spot_shock")
    with col2:
        bilateral_shock = st.slider("Bilateral benchmark shock (%)", -50, 50, 0, key="bi_shock")

    if spot_shock == 0 and bilateral_shock == 0:
        st.info("Adjust sliders to see stress P&L impact.")
        return

    with engine.connect() as conn:
        positions = pd.read_sql(text("""
            SELECT direction, volume_mwh, price_cny_mwh, channel
            FROM marketdata.rm_positions WHERE book_id = :bid AND status = 'open'
        """), conn, params={"bid": book_id})

    if positions.empty:
        return

    # Compute scenario P&L
    scenario_pnl = 0.0
    for _, pos in positions.iterrows():
        vol = float(pos["volume_mwh"])
        price = float(pos["price_cny_mwh"] or 0)
        shock = spot_shock / 100.0 if pos["channel"] in ("DA", "RT") else bilateral_shock / 100.0
        price_change = price * shock
        if pos["direction"] == "buy":
            scenario_pnl += vol * price_change
        else:
            scenario_pnl -= vol * price_change

    st.metric("Scenario P&L Impact", f"¥{scenario_pnl:,.0f}",
              delta_color="inverse" if scenario_pnl < 0 else "normal")
```

- [ ] **Step 2: Commit**

```bash
git add apps/asset-risk/tab_var.py
git commit -m "feat(app): add Tab 5 — VaR & Greeks"
```

---

## Task 22: App Tab 6 — Agent

**Files:**
- Create: `apps/asset-risk/tab_agent.py`

- [ ] **Step 1: Create tab_agent.py**

```python
# apps/asset-risk/tab_agent.py
"""Tab 6 — Agent: Claude-powered risk assistant."""
from __future__ import annotations

import os
import json
import pandas as pd
import streamlit as st
from sqlalchemy import text


def render_agent(engine):
    """Render agent chat tab."""
    st.subheader("Risk Agent")

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        st.warning("ANTHROPIC_API_KEY not set. Agent unavailable.")
        return

    # Initialize chat history
    if "agent_messages" not in st.session_state:
        st.session_state.agent_messages = []

    # Display chat history
    for msg in st.session_state.agent_messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    # Chat input
    if prompt := st.chat_input("Ask about your asset risk positions..."):
        st.session_state.agent_messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                response = _call_agent(prompt, engine, api_key)
                st.markdown(response)
        st.session_state.agent_messages.append({"role": "assistant", "content": response})


def _call_agent(user_message: str, engine, api_key: str) -> str:
    """Call Claude with risk management tools."""
    import anthropic

    client = anthropic.Anthropic(api_key=api_key)

    tools = [
        {
            "name": "get_book_pnl",
            "description": "Get P&L breakdown by category for a book and date range.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "book_id": {"type": "integer"},
                    "start_date": {"type": "string", "description": "YYYY-MM-DD"},
                    "end_date": {"type": "string", "description": "YYYY-MM-DD"},
                },
                "required": ["book_id"],
            },
        },
        {
            "name": "get_position_mtm",
            "description": "Get current MtM summary with unrealised P&L for a book.",
            "input_schema": {
                "type": "object",
                "properties": {"book_id": {"type": "integer"}},
                "required": ["book_id"],
            },
        },
        {
            "name": "get_var",
            "description": "Get current VaR figures for a book.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "book_id": {"type": "integer"},
                    "method": {"type": "string", "enum": ["historical", "parametric"]},
                },
                "required": ["book_id"],
            },
        },
        {
            "name": "get_asset_list",
            "description": "Get list of registered assets and their books.",
            "input_schema": {"type": "object", "properties": {}},
        },
    ]

    system_prompt = (
        "You are an asset risk management assistant for a Chinese electricity trading company. "
        "You have access to tools that query the risk management database. "
        "Answer questions about P&L, positions, VaR, and assets. "
        "Use CNY for all monetary values. Respond concisely."
    )

    messages = [{"role": "user", "content": user_message}]
    response = client.messages.create(
        model="claude-sonnet-4-6-20250514",
        max_tokens=2048,
        system=system_prompt,
        tools=tools,
        messages=messages,
    )

    # Handle tool use
    if response.stop_reason == "tool_use":
        tool_results = []
        for block in response.content:
            if block.type == "tool_use":
                result = _execute_tool(block.name, block.input, engine)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": json.dumps(result, default=str),
                })

        messages.append({"role": "assistant", "content": response.content})
        messages.append({"role": "user", "content": tool_results})

        final = client.messages.create(
            model="claude-sonnet-4-6-20250514",
            max_tokens=2048,
            system=system_prompt,
            tools=tools,
            messages=messages,
        )
        return _extract_text(final)

    return _extract_text(response)


def _extract_text(response) -> str:
    """Extract text content from Claude response."""
    for block in response.content:
        if hasattr(block, "text"):
            return block.text
    return "No response generated."


def _execute_tool(name: str, inputs: dict, engine) -> dict:
    """Execute an agent tool against the database."""
    with engine.connect() as conn:
        if name == "get_asset_list":
            df = pd.read_sql(text("""
                SELECT a.id, a.name, a.asset_type, a.province, a.capacity_mw,
                       b.id as book_id, b.name as book_name
                FROM marketdata.rm_assets a
                LEFT JOIN marketdata.rm_books b ON b.asset_id = a.id
                ORDER BY a.name
            """), conn)
            return {"assets": df.to_dict("records")}

        elif name == "get_book_pnl":
            query = """
                SELECT si.category, SUM(si.amount_cny) as total_cny, SUM(si.volume_mwh) as total_mwh
                FROM marketdata.rm_settlement_items si
                JOIN marketdata.rm_settlements s ON s.id = si.settlement_id
                WHERE s.book_id = :bid
                GROUP BY si.category ORDER BY total_cny DESC
            """
            df = pd.read_sql(text(query), conn, params={"bid": inputs["book_id"]})
            return {"pnl_by_category": df.to_dict("records"), "net_pnl": float(df["total_cny"].sum())}

        elif name == "get_position_mtm":
            df = pd.read_sql(text("""
                SELECT direction, volume_mwh, price_cny_mwh, channel, province
                FROM marketdata.rm_positions WHERE book_id = :bid AND status = 'open'
            """), conn, params={"bid": inputs["book_id"]})
            if df.empty:
                return {"positions": [], "total_unrealized": 0}
            from libs.risk.mtm import compute_mtm
            fwd = pd.read_sql(text("""
                SELECT DISTINCT ON (province) province, price_cny_kwh * 1000 as price
                FROM marketdata.rm_forward_curves ORDER BY province, curve_date DESC
            """), conn)
            fwd_prices = dict(zip(fwd["province"], fwd["price"])) if not fwd.empty else {}
            results = compute_mtm(df.to_dict("records"), fwd_prices)
            total = sum(r["unrealized_pnl_cny"] for r in results)
            return {"positions": results, "total_unrealized": total}

        elif name == "get_var":
            df = pd.read_sql(text("""
                SELECT * FROM marketdata.rm_var_snapshots
                WHERE book_id = :bid ORDER BY snapshot_date DESC LIMIT 1
            """), conn, params={"bid": inputs["book_id"]})
            return df.to_dict("records")[0] if not df.empty else {"message": "No VaR data"}

    return {"error": f"Unknown tool: {name}"}
```

- [ ] **Step 2: Commit**

```bash
git add apps/asset-risk/tab_agent.py
git commit -m "feat(app): add Tab 6 — Agent (Claude risk assistant)"
```

---

## Task 23: Dockerfile + Requirements + Deploy Config

**Files:**
- Create: `apps/asset-risk/requirements.txt`
- Create: `apps/asset-risk/Dockerfile`

- [ ] **Step 1: Create requirements.txt**

```
streamlit>=1.30
plotly>=5.18
pandas>=2.0
psycopg2-binary
sqlalchemy>=2.0
openpyxl>=3.0
pdfplumber>=0.10
scipy>=1.10
numpy>=1.26
python-dotenv
anthropic>=0.40
boto3>=1.34
```

- [ ] **Step 2: Create Dockerfile**

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY apps/asset-risk/requirements.txt ./apps/asset-risk/requirements.txt
RUN pip install --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple \
    --trusted-host pypi.tuna.tsinghua.edu.cn --timeout 300 --retries 5 \
    -r apps/asset-risk/requirements.txt

COPY libs/ ./libs/
COPY services/ ./services/
COPY shared/ ./shared/
COPY auth/ ./auth/
COPY apps/asset-risk/ ./apps/asset-risk/

ENV PYTHONPATH=/app

EXPOSE 8512

CMD ["streamlit", "run", "apps/asset-risk/app.py", \
     "--server.port=8512", "--server.address=0.0.0.0", \
     "--server.baseUrlPath=asset-risk", \
     "--server.fileWatcherType=none", \
     "--server.enableCORS=false", "--server.enableXsrfProtection=false"]
```

- [ ] **Step 3: Commit**

```bash
git add apps/asset-risk/requirements.txt apps/asset-risk/Dockerfile
git commit -m "feat(app): add Dockerfile and requirements for asset-risk"
```

---

## Notes for execution

- **Import path fix:** The `app.py` uses `from apps.asset_risk.tab_*` — since the folder is `asset-risk` (with hyphen), either rename to `asset_risk` or use direct relative imports. Recommend renaming folder to `apps/asset_risk/` for Python import compatibility, keeping the URL path as `/asset-risk` via `baseUrlPath`.
- **Test `__init__.py` files:** Create empty `tests/settlement/__init__.py`, `tests/risk/__init__.py`, `tests/operating_assets/__init__.py` for pytest discovery.
- **Tasks 1-6 (DDL)** can run in parallel as they have no cross-file dependencies.
- **Tasks 7-12 (libs)** depend on DDL being applied (for integration tests) but unit tests are DB-free.
- **Tasks 13-16 (services)** depend on libs.
- **Tasks 17-22 (app tabs)** depend on libs + services.
- **Task 23 (deploy)** is last.
