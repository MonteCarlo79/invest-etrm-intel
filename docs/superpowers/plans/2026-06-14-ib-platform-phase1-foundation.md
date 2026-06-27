# IB Trading Platform — Phase 1: Foundation

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Working `ib-platform` repo with DB schema, multi-broker abstraction (IB + Paper + Alpaca), and a FastAPI broker_service that syncs positions/fills to shared RDS and enforces pre-trade risk controls.

**Architecture:** Separate repo (`ib-platform`) sharing the existing bess-platform AWS RDS. `services/broker/` provides a `BaseBroker` ABC with three implementations. `services/broker_service/` is a FastAPI app that runs on the personal laptop alongside TWS, wrapping the broker behind a REST API so analytics apps on any machine can read positions and submit orders without a direct IB connection.

**Tech Stack:** Python 3.11+, FastAPI + uvicorn, ib_insync (IB), alpaca-py (Alpaca), psycopg2, APScheduler 3.x, pytest, httpx (test client)

---

## Phase Scope

This plan covers:
- Repo scaffold, requirements, docker-compose
- `trading` DB schema (all tables)
- `services/broker/` — BaseBroker ABC + Position/Order dataclasses + 3 implementations + factory
- `services/broker_service/` — FastAPI app, order_router, data_writer, algo_scheduler skeleton

Subsequent phases (analytics libs, apps, knowledge base, ML, execution app) are in separate plans.

---

## File Map

```
ib-platform/
├── config/
│   └── .env.example
├── db/
│   └── schema.sql                          # CREATE TABLE statements for trading.*
├── services/
│   ├── broker/
│   │   ├── __init__.py
│   │   ├── base.py                         # BaseBroker ABC + all dataclasses
│   │   ├── paper_broker.py                 # PaperBroker — simulated fills from DB bars
│   │   ├── ib_broker.py                    # IBBroker — ib_insync wrapper
│   │   ├── alpaca_broker.py                # AlpacaBroker — REST API
│   │   └── broker_factory.py              # get_broker(type) → BaseBroker
│   └── broker_service/
│       ├── __init__.py
│       ├── main.py                         # FastAPI app, mounts routers
│       ├── order_router.py                 # pre-trade risk checks
│       ├── data_writer.py                  # position/fill/bar sync → trading.* tables
│       └── algo_scheduler.py               # APScheduler skeleton
├── tests/
│   ├── conftest.py
│   ├── broker/
│   │   ├── test_base.py
│   │   ├── test_paper_broker.py
│   │   └── test_broker_factory.py
│   └── broker_service/
│       ├── test_order_router.py
│       └── test_main.py
├── requirements.txt
└── docker-compose.yml
```

---

## Task 1: Repo Scaffold

**Files:**
- Create: `requirements.txt`
- Create: `config/.env.example`
- Create: `docker-compose.yml`

- [ ] **Step 1: Create `requirements.txt`**

```
# Broker
ib_insync==0.9.86
alpaca-py==0.29.0

# Web service
fastapi==0.115.0
uvicorn[standard]==0.30.0
httpx==0.27.0

# DB
psycopg2-binary==2.9.9
sqlalchemy==2.0.35

# Scheduling
apscheduler==3.10.4

# Data
pandas==2.2.3
numpy==1.26.4

# Config
python-dotenv==1.0.1

# Test
pytest==8.3.3
pytest-asyncio==0.24.0

# Market data (later phases)
yfinance==0.2.44
requests==2.32.3
```

- [ ] **Step 2: Create `config/.env.example`**

```bash
# Shared RDS (same as bess-platform)
PGURL=postgresql://postgres:postgres@localhost:5432/marketdata

# Anthropic (advisor agent)
ANTHROPIC_API_KEY=sk-ant-...

# Broker: ib | alpaca | paper
BROKER_TYPE=paper

# IB TWS/Gateway (only needed when BROKER_TYPE=ib)
IB_HOST=127.0.0.1
IB_PORT=7497
IB_CLIENT_ID=1

# Alpaca (only needed when BROKER_TYPE=alpaca)
ALPACA_API_KEY=
ALPACA_SECRET_KEY=
ALPACA_BASE_URL=https://paper-api.alpaca.markets

# Paper broker initial cash
PAPER_INITIAL_CASH=100000.0

# Broker service
BROKER_SERVICE_HOST=0.0.0.0
BROKER_SERVICE_PORT=8600
```

- [ ] **Step 3: Create `docker-compose.yml`**

```yaml
services:
  broker_service:
    build: .
    container_name: ib-broker-service
    ports:
      - "8600:8600"
    environment:
      PGURL: ${PGURL}
      BROKER_TYPE: paper
      PAPER_INITIAL_CASH: "100000.0"
      ANTHROPIC_API_KEY: ${ANTHROPIC_API_KEY}
    command: uvicorn services.broker_service.main:app --host 0.0.0.0 --port 8600 --reload
    volumes:
      - ./config/.env:/app/.env

networks:
  default:
    driver: bridge
```

- [ ] **Step 4: Create `config/.env` from example**

```bash
cp config/.env.example config/.env
# Edit config/.env and set PGURL to point at the shared bess-platform RDS
```

- [ ] **Step 5: Commit**

```bash
git init
git add requirements.txt config/.env.example docker-compose.yml
git commit -m "feat: repo scaffold — requirements, config, docker-compose"
```

---

## Task 2: DB Schema

**Files:**
- Create: `db/schema.sql`

- [ ] **Step 1: Create `db/schema.sql`**

```sql
-- Run once against the shared bess-platform RDS:
-- psql $PGURL -f db/schema.sql

CREATE SCHEMA IF NOT EXISTS trading;

-- ─── Accounts ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS trading.accounts (
    account_id      TEXT PRIMARY KEY,
    broker          TEXT NOT NULL,          -- ib | alpaca | paper
    currency        TEXT NOT NULL DEFAULT 'USD',
    account_type    TEXT NOT NULL DEFAULT 'individual',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ─── Positions ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS trading.positions (
    id              BIGSERIAL PRIMARY KEY,
    account_id      TEXT NOT NULL REFERENCES trading.accounts(account_id),
    symbol          TEXT NOT NULL,
    asset_class     TEXT NOT NULL,  -- equity|option|future|bond|swap|fx_spot|fx_option
    expiry          TEXT,
    strike          NUMERIC(18,4),
    right           TEXT,           -- C|P for options
    quantity        NUMERIC(18,6) NOT NULL,
    avg_cost        NUMERIC(18,6) NOT NULL,
    unrealised_pnl  NUMERIC(18,4),
    currency        TEXT NOT NULL DEFAULT 'USD',
    ts_snapshot     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_positions_account ON trading.positions(account_id, ts_snapshot DESC);

-- ─── Orders ──────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS trading.orders (
    order_id        TEXT PRIMARY KEY,
    account_id      TEXT NOT NULL,
    symbol          TEXT NOT NULL,
    order_type      TEXT NOT NULL,   -- MKT|LMT|STP
    side            TEXT NOT NULL,   -- BUY|SELL
    quantity        NUMERIC(18,6) NOT NULL,
    limit_price     NUMERIC(18,6),
    status          TEXT NOT NULL,   -- OPEN|FILLED|CANCELLED|REJECTED
    strategy_id     TEXT,
    ts_submitted    TIMESTAMPTZ,
    ts_last_update  TIMESTAMPTZ
);

-- ─── Trades (fills) ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS trading.trades (
    trade_id        TEXT PRIMARY KEY,
    account_id      TEXT NOT NULL,
    symbol          TEXT NOT NULL,
    asset_class     TEXT NOT NULL,
    side            TEXT NOT NULL,
    quantity        NUMERIC(18,6) NOT NULL,
    fill_price      NUMERIC(18,6) NOT NULL,
    commission      NUMERIC(18,4) NOT NULL DEFAULT 0,
    strategy_id     TEXT,
    order_id        TEXT REFERENCES trading.orders(order_id),
    ts_fill         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_trades_account ON trading.trades(account_id, ts_fill DESC);
CREATE INDEX IF NOT EXISTS idx_trades_strategy ON trading.trades(strategy_id, ts_fill DESC);

-- ─── Market Data ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS trading.bars_1d (
    symbol          TEXT NOT NULL,
    ts_date         DATE NOT NULL,
    open            NUMERIC(18,6),
    high            NUMERIC(18,6),
    low             NUMERIC(18,6),
    close           NUMERIC(18,6),
    volume          BIGINT,
    source          TEXT,
    PRIMARY KEY (symbol, ts_date)
);

CREATE TABLE IF NOT EXISTS trading.bars_1h (
    symbol          TEXT NOT NULL,
    ts              TIMESTAMPTZ NOT NULL,
    open            NUMERIC(18,6),
    high            NUMERIC(18,6),
    low             NUMERIC(18,6),
    close           NUMERIC(18,6),
    volume          BIGINT,
    source          TEXT,
    PRIMARY KEY (symbol, ts)
);

CREATE TABLE IF NOT EXISTS trading.options_chain (
    id              BIGSERIAL PRIMARY KEY,
    symbol          TEXT NOT NULL,
    expiry          TEXT NOT NULL,
    strike          NUMERIC(18,4) NOT NULL,
    right           TEXT NOT NULL,   -- C|P
    bid             NUMERIC(18,6),
    ask             NUMERIC(18,6),
    iv              NUMERIC(10,6),
    delta           NUMERIC(10,6),
    gamma           NUMERIC(10,6),
    theta           NUMERIC(10,6),
    vega            NUMERIC(10,6),
    ts_snapshot     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_options_symbol ON trading.options_chain(symbol, expiry, ts_snapshot DESC);

CREATE TABLE IF NOT EXISTS trading.fx_rates (
    pair            TEXT NOT NULL,
    ts              TIMESTAMPTZ NOT NULL,
    spot            NUMERIC(18,8),
    bid             NUMERIC(18,8),
    ask             NUMERIC(18,8),
    source          TEXT,
    PRIMARY KEY (pair, ts)
);

CREATE TABLE IF NOT EXISTS trading.bond_quotes (
    isin            TEXT NOT NULL,
    ts              TIMESTAMPTZ NOT NULL,
    clean_price     NUMERIC(18,6),
    dirty_price     NUMERIC(18,6),
    ytm             NUMERIC(10,8),
    source          TEXT,
    PRIMARY KEY (isin, ts)
);

-- ─── Quant / Risk ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS trading.vol_surface (
    underlying      TEXT NOT NULL,
    ts_date         DATE NOT NULL,
    expiry          TEXT NOT NULL,
    strike          NUMERIC(18,4) NOT NULL,
    iv              NUMERIC(10,6),
    PRIMARY KEY (underlying, ts_date, expiry, strike)
);

CREATE TABLE IF NOT EXISTS trading.yield_curves (
    curve_id        TEXT NOT NULL,
    ts_date         DATE NOT NULL,
    tenor_label     TEXT NOT NULL,
    tenor_years     NUMERIC(10,4),
    rate            NUMERIC(10,8),
    source          TEXT,
    PRIMARY KEY (curve_id, ts_date, tenor_label)
);

CREATE TABLE IF NOT EXISTS trading.portfolio_risk (
    ts              TIMESTAMPTZ NOT NULL,
    account_id      TEXT NOT NULL,
    total_delta     NUMERIC(18,4),
    total_gamma     NUMERIC(18,6),
    total_theta     NUMERIC(18,4),
    total_vega      NUMERIC(18,4),
    dv01            NUMERIC(18,4),
    fx_delta_usd    NUMERIC(18,4),
    var_1d_95       NUMERIC(18,4),
    nav             NUMERIC(18,4),
    PRIMARY KEY (ts, account_id)
);

CREATE TABLE IF NOT EXISTS trading.strategy_pnl (
    strategy_id         TEXT NOT NULL,
    ts_date             DATE NOT NULL,
    realized_pnl        NUMERIC(18,4),
    unrealized_pnl      NUMERIC(18,4),
    trades_n            INTEGER,
    capital_employed    NUMERIC(18,4),
    roace_daily         NUMERIC(10,8),
    sharpe_rolling_30d  NUMERIC(10,6),
    PRIMARY KEY (strategy_id, ts_date)
);

CREATE TABLE IF NOT EXISTS trading.cashflows (
    cf_id           BIGSERIAL PRIMARY KEY,
    ts              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    cf_type         TEXT NOT NULL,  -- trade|margin|dividend|coupon|roll|funding
    symbol          TEXT,
    amount          NUMERIC(18,4) NOT NULL,
    currency        TEXT NOT NULL DEFAULT 'USD',
    strategy_id     TEXT,
    description     TEXT
);

CREATE TABLE IF NOT EXISTS trading.capital_summary (
    ts_date                 DATE PRIMARY KEY,
    nav                     NUMERIC(18,4),
    cash_free               NUMERIC(18,4),
    margin_posted           NUMERIC(18,4),
    margin_available        NUMERIC(18,4),
    capital_employed        NUMERIC(18,4),
    capital_utilisation_pct NUMERIC(10,4),
    gross_notional          NUMERIC(18,4),
    leverage_ratio          NUMERIC(10,4)
);

-- ─── Algo / Signals ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS trading.signals (
    signal_id       TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    strategy_id     TEXT NOT NULL,
    symbol          TEXT NOT NULL,
    direction       TEXT NOT NULL,   -- LONG|SHORT|CLOSE
    strength        NUMERIC(6,4),
    source          TEXT NOT NULL,   -- rule|ml|agent
    status          TEXT NOT NULL DEFAULT 'open',  -- open|acknowledged|resolved
    ts_generated    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ts_acted        TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS trading.algo_runs (
    run_id          TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    strategy_id     TEXT NOT NULL,
    status          TEXT NOT NULL,   -- active|paused|stopped
    params_json     JSONB,
    auto_approve    BOOLEAN NOT NULL DEFAULT FALSE,
    ts_start        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ts_end          TIMESTAMPTZ
);

-- ─── VIX ─────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS trading.vix_term_structure (
    ts_date                 DATE PRIMARY KEY,
    vix_index               NUMERIC(10,4),
    m1                      NUMERIC(10,4),
    m2                      NUMERIC(10,4),
    m3                      NUMERIC(10,4),
    m4                      NUMERIC(10,4),
    m5                      NUMERIC(10,4),
    m6                      NUMERIC(10,4),
    m7                      NUMERIC(10,4),
    m8                      NUMERIC(10,4),
    contango_pct            NUMERIC(10,4),
    roll_yield_annualised   NUMERIC(10,4),
    vvix                    NUMERIC(10,4),
    regime                  TEXT,  -- contango|backwardation|spike
    source                  TEXT
);

-- ─── Knowledge Base ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS trading.kb_docs (
    id              BIGSERIAL PRIMARY KEY,
    source          TEXT NOT NULL,
    doc_type        TEXT NOT NULL,
    title           TEXT,
    url             TEXT UNIQUE,
    published_date  DATE,
    content         TEXT NOT NULL,
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    search_vector   TSVECTOR GENERATED ALWAYS AS (
                        to_tsvector('english',
                            coalesce(title, '') || ' ' || left(content, 100000))
                    ) STORED
);
CREATE INDEX IF NOT EXISTS idx_kb_docs_fts ON trading.kb_docs USING GIN(search_vector);

CREATE TABLE IF NOT EXISTS trading.kb_insights (
    id                  BIGSERIAL PRIMARY KEY,
    insight_text        TEXT NOT NULL,
    insight_type        TEXT NOT NULL,
    confidence          TEXT NOT NULL DEFAULT 'medium',  -- high|medium|low
    source_session      TEXT,
    source_doc_url      TEXT,
    source_model_run    TEXT,
    source_backtest_id  TEXT,
    source_trade_id     TEXT,
    validated_at        TIMESTAMPTZ,
    active              BOOLEAN NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_kb_insights_active ON trading.kb_insights(active, created_at DESC);

CREATE TABLE IF NOT EXISTS trading.kb_briefings (
    id              BIGSERIAL PRIMARY KEY,
    briefing_date   DATE NOT NULL,
    market_section  TEXT NOT NULL,   -- macro|rates|vol|equity|fx
    content         TEXT NOT NULL,
    model_outputs_json JSONB,
    generated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (briefing_date, market_section)
);

-- ─── Agent Memory ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS trading.agent_memory (
    id          BIGSERIAL PRIMARY KEY,
    app_key     TEXT NOT NULL,
    category    TEXT NOT NULL,
    subject     TEXT NOT NULL,
    content     TEXT NOT NULL,
    ts_updated  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (app_key, category, subject)
);
```

- [ ] **Step 2: Apply schema to RDS**

```bash
# From bess-platform project root (where PGURL is set):
psql $PGURL -f db/schema.sql
# Expected: series of CREATE TABLE / CREATE INDEX lines with no errors
```

- [ ] **Step 3: Verify tables created**

```bash
psql $PGURL -c "\dt trading.*"
# Expected: 20+ tables listed under trading schema
```

- [ ] **Step 4: Commit**

```bash
git add db/schema.sql
git commit -m "feat: trading schema — all tables for Phase 1"
```

---

## Task 3: Broker Dataclasses + BaseBroker ABC

**Files:**
- Create: `services/broker/__init__.py`
- Create: `services/broker/base.py`
- Create: `tests/broker/test_base.py`

- [ ] **Step 1: Write failing test**

Create `tests/broker/test_base.py`:

```python
"""Tests for BaseBroker dataclasses and ABC contract."""
import pytest
from services.broker.base import (
    Position, Order, OrderRequest, OrderResult, AccountSummary, BaseBroker
)


def test_position_dataclass():
    p = Position(symbol="AAPL", asset_class="equity", quantity=100.0,
                 avg_cost=150.0, unrealised_pnl=500.0)
    assert p.symbol == "AAPL"
    assert p.currency == "USD"
    assert p.expiry is None


def test_order_request_defaults():
    req = OrderRequest(symbol="SPY", side="BUY", quantity=10.0)
    assert req.order_type == "MKT"
    assert req.limit_price is None
    assert req.strategy_id is None


def test_base_broker_is_abstract():
    """BaseBroker cannot be instantiated directly."""
    with pytest.raises(TypeError):
        BaseBroker()


def test_base_broker_requires_all_methods():
    """Concrete subclass missing any abstract method raises TypeError."""
    class IncompleteBroker(BaseBroker):
        def connect(self): pass
        # Missing all other methods

    with pytest.raises(TypeError):
        IncompleteBroker()
```

- [ ] **Step 2: Run test — confirm failure**

```bash
cd ib-platform
pytest tests/broker/test_base.py -v
# Expected: ModuleNotFoundError: No module named 'services.broker.base'
```

- [ ] **Step 3: Create `services/broker/__init__.py`**

```python
```
(empty file)

- [ ] **Step 4: Create `services/broker/base.py`**

```python
"""
services/broker/base.py

BaseBroker ABC and all shared dataclasses.
All broker implementations must inherit BaseBroker and implement every abstract method.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import pandas as pd


@dataclass
class Position:
    symbol: str
    asset_class: str        # equity|option|future|bond|swap|fx_spot|fx_option
    quantity: float
    avg_cost: float
    unrealised_pnl: float
    currency: str = "USD"
    expiry: Optional[str] = None
    strike: Optional[float] = None
    right: Optional[str] = None     # C|P for options
    account_id: str = "default"


@dataclass
class Order:
    order_id: str
    symbol: str
    side: str               # BUY|SELL
    quantity: float
    order_type: str         # MKT|LMT|STP
    status: str             # OPEN|FILLED|CANCELLED|REJECTED
    limit_price: Optional[float] = None
    fill_price: Optional[float] = None
    strategy_id: Optional[str] = None
    ts_submitted: Optional[datetime] = None
    ts_last_update: Optional[datetime] = None


@dataclass
class OrderRequest:
    symbol: str
    side: str               # BUY|SELL
    quantity: float
    order_type: str = "MKT"
    limit_price: Optional[float] = None
    strategy_id: Optional[str] = None


@dataclass
class OrderResult:
    order_id: str
    status: str             # FILLED|REJECTED|PENDING
    message: str = ""


@dataclass
class AccountSummary:
    account_id: str
    nav: float
    cash: float
    margin_used: float
    margin_available: float
    currency: str = "USD"


class BaseBroker(ABC):
    """Abstract base for all broker implementations."""

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to broker."""
        ...

    @abstractmethod
    def disconnect(self) -> None:
        """Close connection."""
        ...

    @abstractmethod
    def health(self) -> dict:
        """Return connection status: {"connected": bool, "latency_ms": int, "mode": str}."""
        ...

    @abstractmethod
    def get_positions(self) -> list[Position]:
        """Return current open positions."""
        ...

    @abstractmethod
    def get_orders(self) -> list[Order]:
        """Return open orders."""
        ...

    @abstractmethod
    def submit_order(self, req: OrderRequest) -> OrderResult:
        """Submit an order. Returns OrderResult with status."""
        ...

    @abstractmethod
    def cancel_order(self, order_id: str) -> bool:
        """Cancel an open order. Returns True if cancelled."""
        ...

    @abstractmethod
    def get_bars(self, symbol: str, resolution: str, n: int) -> pd.DataFrame:
        """Return last n OHLCV bars. resolution: '1d'|'1h'|'5m'."""
        ...

    @abstractmethod
    def get_options_chain(self, symbol: str, expiry: str) -> pd.DataFrame:
        """Return options chain snapshot for symbol/expiry."""
        ...

    @abstractmethod
    def get_account_summary(self) -> AccountSummary:
        """Return account NAV, cash, margin."""
        ...
```

- [ ] **Step 5: Also create `tests/__init__.py` and `tests/broker/__init__.py`**

```bash
touch tests/__init__.py tests/broker/__init__.py
```

- [ ] **Step 6: Run tests — confirm pass**

```bash
pytest tests/broker/test_base.py -v
# Expected: 4 passed
```

- [ ] **Step 7: Commit**

```bash
git add services/broker/__init__.py services/broker/base.py \
        tests/__init__.py tests/broker/__init__.py tests/broker/test_base.py
git commit -m "feat: BaseBroker ABC and dataclasses"
```

---

## Task 4: PaperBroker

**Files:**
- Create: `services/broker/paper_broker.py`
- Create: `tests/broker/test_paper_broker.py`

- [ ] **Step 1: Write failing tests**

Create `tests/broker/test_paper_broker.py`:

```python
"""Tests for PaperBroker — simulated fills, no live connection needed."""
import pytest
from services.broker.paper_broker import PaperBroker
from services.broker.base import OrderRequest


@pytest.fixture
def broker():
    b = PaperBroker(initial_cash=50_000.0)
    b.connect()
    return b


def test_health_connected(broker):
    h = broker.health()
    assert h["connected"] is True
    assert h["mode"] == "paper"


def test_initial_positions_empty(broker):
    assert broker.get_positions() == []


def test_buy_creates_position(broker):
    req = OrderRequest(symbol="AAPL", side="BUY", quantity=10.0)
    result = broker.submit_order(req)
    assert result.status == "FILLED"
    positions = broker.get_positions()
    assert len(positions) == 1
    assert positions[0].symbol == "AAPL"
    assert positions[0].quantity == 10.0


def test_sell_reduces_position(broker):
    broker.submit_order(OrderRequest(symbol="SPY", side="BUY", quantity=5.0))
    broker.submit_order(OrderRequest(symbol="SPY", side="SELL", quantity=3.0))
    positions = broker.get_positions()
    assert len(positions) == 1
    assert positions[0].quantity == pytest.approx(2.0)


def test_full_sell_removes_position(broker):
    broker.submit_order(OrderRequest(symbol="TSLA", side="BUY", quantity=2.0))
    broker.submit_order(OrderRequest(symbol="TSLA", side="SELL", quantity=2.0))
    assert broker.get_positions() == []


def test_account_summary_nav(broker):
    summary = broker.get_account_summary()
    assert summary.nav == pytest.approx(50_000.0)
    assert summary.cash == pytest.approx(50_000.0)
    assert summary.account_id == "paper"


def test_nav_decreases_after_buy(broker):
    # Price will be 100.0 (fallback when no DB)
    broker.submit_order(OrderRequest(symbol="XYZ", side="BUY", quantity=10.0))
    summary = broker.get_account_summary()
    # Cash reduced by 10 × 100 = 1000, position worth 1000, NAV unchanged
    assert summary.nav == pytest.approx(50_000.0)
    assert summary.cash == pytest.approx(49_000.0)


def test_cancel_open_order_not_possible_after_fill(broker):
    req = OrderRequest(symbol="MSFT", side="BUY", quantity=1.0)
    result = broker.submit_order(req)
    # Paper broker fills immediately — cancel should return False
    cancelled = broker.cancel_order(result.order_id)
    assert cancelled is False


def test_get_bars_returns_empty_df(broker):
    df = broker.get_bars("AAPL", "1d", 10)
    assert list(df.columns) == ["ts", "open", "high", "low", "close", "volume"]
    assert len(df) == 0


def test_disconnect(broker):
    broker.disconnect()
    assert broker.health()["connected"] is False
```

- [ ] **Step 2: Run — confirm failure**

```bash
pytest tests/broker/test_paper_broker.py -v
# Expected: ModuleNotFoundError: No module named 'services.broker.paper_broker'
```

- [ ] **Step 3: Create `services/broker/paper_broker.py`**

```python
"""
services/broker/paper_broker.py

PaperBroker — simulates order fills from market data bars.
No live broker connection required. Fills execute immediately at last known price.
Falls back to $100.0 if no bars exist in DB.
"""
from __future__ import annotations

import os
import uuid
from datetime import datetime
from typing import Optional

import pandas as pd

from .base import (
    BaseBroker, Position, Order, OrderRequest, OrderResult, AccountSummary
)


class PaperBroker(BaseBroker):
    """
    Simulated broker for development and testing.
    Fills are immediate at last close price from trading.bars_1d (or $100 fallback).
    Maintains in-memory position book.
    """

    def __init__(self, initial_cash: float = 100_000.0):
        self._cash: float = float(initial_cash)
        self._positions: dict[str, Position] = {}
        self._orders: dict[str, Order] = {}
        self._connected: bool = False

    # ── Connection ────────────────────────────────────────────────────────────

    def connect(self) -> None:
        self._connected = True

    def disconnect(self) -> None:
        self._connected = False

    def health(self) -> dict:
        return {"connected": self._connected, "latency_ms": 0, "mode": "paper"}

    # ── Positions & Orders ────────────────────────────────────────────────────

    def get_positions(self) -> list[Position]:
        return list(self._positions.values())

    def get_orders(self) -> list[Order]:
        return list(self._orders.values())

    def get_account_summary(self) -> AccountSummary:
        position_value = sum(p.quantity * p.avg_cost for p in self._positions.values())
        nav = self._cash + position_value
        return AccountSummary(
            account_id="paper",
            nav=nav,
            cash=self._cash,
            margin_used=0.0,
            margin_available=nav,
        )

    # ── Order Execution ───────────────────────────────────────────────────────

    def submit_order(self, req: OrderRequest) -> OrderResult:
        order_id = str(uuid.uuid4())[:12]
        fill_price = self._get_last_price(req.symbol)
        sign = 1.0 if req.side == "BUY" else -1.0
        qty_delta = sign * req.quantity

        self._update_position(req.symbol, qty_delta, fill_price)
        self._cash -= sign * req.quantity * fill_price

        order = Order(
            order_id=order_id,
            symbol=req.symbol,
            side=req.side,
            quantity=req.quantity,
            order_type=req.order_type,
            status="FILLED",
            fill_price=fill_price,
            strategy_id=req.strategy_id,
            ts_submitted=datetime.utcnow(),
            ts_last_update=datetime.utcnow(),
        )
        self._orders[order_id] = order
        return OrderResult(order_id=order_id, status="FILLED")

    def cancel_order(self, order_id: str) -> bool:
        order = self._orders.get(order_id)
        if order and order.status == "OPEN":
            order.status = "CANCELLED"
            return True
        return False

    # ── Market Data ───────────────────────────────────────────────────────────

    def get_bars(self, symbol: str, resolution: str, n: int) -> pd.DataFrame:
        """Returns empty DataFrame — paper broker doesn't fetch live market data."""
        return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])

    def get_options_chain(self, symbol: str, expiry: str) -> pd.DataFrame:
        return pd.DataFrame()

    # ── Internals ─────────────────────────────────────────────────────────────

    def _update_position(self, symbol: str, qty_delta: float, price: float) -> None:
        if symbol in self._positions:
            pos = self._positions[symbol]
            new_qty = pos.quantity + qty_delta
            if abs(new_qty) < 1e-9:
                del self._positions[symbol]
                return
            # Update avg cost only when adding to the position
            if (pos.quantity > 0 and qty_delta > 0) or (pos.quantity < 0 and qty_delta < 0):
                total_cost = pos.quantity * pos.avg_cost + qty_delta * price
                pos.avg_cost = total_cost / new_qty
            pos.quantity = new_qty
        else:
            self._positions[symbol] = Position(
                symbol=symbol,
                asset_class="equity",
                quantity=qty_delta,
                avg_cost=price,
                unrealised_pnl=0.0,
            )

    def _get_last_price(self, symbol: str) -> float:
        """Try trading.bars_1d for last close; fallback to $100."""
        pgurl = os.getenv("PGURL")
        if not pgurl:
            return 100.0
        try:
            import psycopg2
            conn = psycopg2.connect(pgurl)
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT close FROM trading.bars_1d "
                    "WHERE symbol = %s ORDER BY ts_date DESC LIMIT 1",
                    (symbol,),
                )
                row = cur.fetchone()
            conn.close()
            return float(row[0]) if row else 100.0
        except Exception:
            return 100.0
```

- [ ] **Step 4: Run tests — confirm all pass**

```bash
pytest tests/broker/test_paper_broker.py -v
# Expected: 10 passed
```

- [ ] **Step 5: Commit**

```bash
git add services/broker/paper_broker.py tests/broker/test_paper_broker.py
git commit -m "feat: PaperBroker — simulated fills, in-memory position book"
```

---

## Task 5: IBBroker

**Files:**
- Create: `services/broker/ib_broker.py`

> IBBroker requires a running TWS/Gateway — no unit tests. Integration tested manually against paper TWS account.

- [ ] **Step 1: Create `services/broker/ib_broker.py`**

```python
"""
services/broker/ib_broker.py

IBBroker — wraps ib_insync for Interactive Brokers TWS/Gateway.
Requires TWS or IB Gateway running on IB_HOST:IB_PORT (default 127.0.0.1:7497).
"""
from __future__ import annotations

import os
import time
import uuid
from datetime import datetime
from typing import Optional

import pandas as pd
from ib_insync import IB, Stock, Option, Future, Forex, Contract
from ib_insync import MarketOrder, LimitOrder, StopOrder

from .base import (
    BaseBroker, Position, Order, OrderRequest, OrderResult, AccountSummary
)

_ASSET_CLASS_MAP = {
    "STK": "equity",
    "OPT": "option",
    "FUT": "future",
    "BOND": "bond",
    "CASH": "fx_spot",
    "FOP": "fx_option",
}


class IBBroker(BaseBroker):
    """
    Interactive Brokers broker via ib_insync.
    Uses TWS paper or live account.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 7497,
        client_id: int = 1,
    ):
        self._host = host
        self._port = port
        self._client_id = client_id
        self._ib = IB()
        self._connect_ts: Optional[float] = None

    # ── Connection ────────────────────────────────────────────────────────────

    def connect(self) -> None:
        self._ib.connect(self._host, self._port, clientId=self._client_id)
        self._connect_ts = time.time()

    def disconnect(self) -> None:
        self._ib.disconnect()
        self._connect_ts = None

    def health(self) -> dict:
        connected = self._ib.isConnected()
        latency = 0
        if connected and self._connect_ts:
            try:
                t0 = time.time()
                self._ib.reqCurrentTime()
                latency = int((time.time() - t0) * 1000)
            except Exception:
                pass
        return {"connected": connected, "latency_ms": latency, "mode": "ib_live"}

    # ── Positions ─────────────────────────────────────────────────────────────

    def get_positions(self) -> list[Position]:
        positions = []
        for p in self._ib.positions():
            contract = p.contract
            asset_class = _ASSET_CLASS_MAP.get(contract.secType, contract.secType.lower())
            positions.append(Position(
                symbol=contract.symbol,
                asset_class=asset_class,
                quantity=float(p.position),
                avg_cost=float(p.avgCost),
                unrealised_pnl=0.0,  # populated by data_writer from portfolio values
                currency=contract.currency,
                expiry=contract.lastTradeDateOrContractMonth or None,
                strike=float(contract.strike) if contract.strike else None,
                right=contract.right or None,
                account_id=p.account,
            ))
        return positions

    # ── Orders ────────────────────────────────────────────────────────────────

    def get_orders(self) -> list[Order]:
        orders = []
        for trade in self._ib.trades():
            o = trade.order
            s = trade.orderStatus
            orders.append(Order(
                order_id=str(o.orderId),
                symbol=trade.contract.symbol,
                side="BUY" if o.action == "BUY" else "SELL",
                quantity=float(o.totalQuantity),
                order_type=o.orderType,
                status=s.status.upper(),
                limit_price=float(o.lmtPrice) if o.lmtPrice else None,
                fill_price=float(s.avgFillPrice) if s.avgFillPrice else None,
                ts_submitted=datetime.utcnow(),
                ts_last_update=datetime.utcnow(),
            ))
        return orders

    def submit_order(self, req: OrderRequest) -> OrderResult:
        contract = Stock(req.symbol, "SMART", "USD")
        if req.order_type == "LMT" and req.limit_price:
            ib_order = LimitOrder(req.side, req.quantity, req.limit_price)
        elif req.order_type == "STP" and req.limit_price:
            ib_order = StopOrder(req.side, req.quantity, req.limit_price)
        else:
            ib_order = MarketOrder(req.side, req.quantity)

        trade = self._ib.placeOrder(contract, ib_order)
        self._ib.sleep(1)  # allow order to register
        return OrderResult(
            order_id=str(trade.order.orderId),
            status=trade.orderStatus.status.upper(),
        )

    def cancel_order(self, order_id: str) -> bool:
        for trade in self._ib.trades():
            if str(trade.order.orderId) == order_id:
                self._ib.cancelOrder(trade.order)
                return True
        return False

    # ── Market Data ───────────────────────────────────────────────────────────

    def get_bars(self, symbol: str, resolution: str, n: int) -> pd.DataFrame:
        _dur_map = {"1d": f"{n} D", "1h": f"{n} H", "5m": f"{n} min"}
        _bar_map = {"1d": "1 day", "1h": "1 hour", "5m": "5 mins"}
        contract = Stock(symbol, "SMART", "USD")
        bars = self._ib.reqHistoricalData(
            contract,
            endDateTime="",
            durationStr=_dur_map.get(resolution, f"{n} D"),
            barSizeSetting=_bar_map.get(resolution, "1 day"),
            whatToShow="TRADES",
            useRTH=True,
        )
        if not bars:
            return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])
        df = pd.DataFrame([
            {"ts": b.date, "open": b.open, "high": b.high,
             "low": b.low, "close": b.close, "volume": b.volume}
            for b in bars
        ])
        return df

    def get_options_chain(self, symbol: str, expiry: str) -> pd.DataFrame:
        contract = Stock(symbol, "SMART", "USD")
        chains = self._ib.reqSecDefOptParams(symbol, "", "STK", contract.conId)
        if not chains:
            return pd.DataFrame()
        chain = chains[0]
        rows = []
        for strike in chain.strikes:
            for right in ["C", "P"]:
                rows.append({"expiry": expiry, "strike": strike, "right": right})
        return pd.DataFrame(rows)

    def get_account_summary(self) -> AccountSummary:
        vals = {v.tag: v.value for v in self._ib.accountValues()}
        nav = float(vals.get("NetLiquidation", 0))
        cash = float(vals.get("TotalCashValue", 0))
        margin_used = float(vals.get("MaintMarginReq", 0))
        margin_avail = float(vals.get("AvailableFunds", 0))
        account_id = self._ib.managedAccounts()[0] if self._ib.managedAccounts() else "ib"
        return AccountSummary(
            account_id=account_id,
            nav=nav,
            cash=cash,
            margin_used=margin_used,
            margin_available=margin_avail,
        )
```

- [ ] **Step 2: Commit**

```bash
git add services/broker/ib_broker.py
git commit -m "feat: IBBroker — ib_insync wrapper for TWS/Gateway"
```

---

## Task 6: AlpacaBroker + BrokerFactory

**Files:**
- Create: `services/broker/alpaca_broker.py`
- Create: `services/broker/broker_factory.py`
- Create: `tests/broker/test_broker_factory.py`

- [ ] **Step 1: Write failing factory test**

Create `tests/broker/test_broker_factory.py`:

```python
import os
import pytest
from services.broker.broker_factory import get_broker
from services.broker.paper_broker import PaperBroker


def test_get_broker_paper(monkeypatch):
    monkeypatch.setenv("BROKER_TYPE", "paper")
    monkeypatch.setenv("PAPER_INITIAL_CASH", "25000.0")
    broker = get_broker()
    assert isinstance(broker, PaperBroker)


def test_get_broker_unknown_raises(monkeypatch):
    monkeypatch.setenv("BROKER_TYPE", "unknown_broker")
    with pytest.raises(ValueError, match="Unknown BROKER_TYPE"):
        get_broker()


def test_get_broker_default_is_paper(monkeypatch):
    monkeypatch.delenv("BROKER_TYPE", raising=False)
    broker = get_broker()
    assert isinstance(broker, PaperBroker)
```

- [ ] **Step 2: Run — confirm failure**

```bash
pytest tests/broker/test_broker_factory.py -v
# Expected: ModuleNotFoundError
```

- [ ] **Step 3: Create `services/broker/alpaca_broker.py`**

```python
"""
services/broker/alpaca_broker.py

AlpacaBroker — REST API wrapper for Alpaca Markets.
Supports US equities and crypto. Paper or live account via ALPACA_BASE_URL.
"""
from __future__ import annotations

import os
from datetime import datetime
from typing import Optional

import pandas as pd

from .base import (
    BaseBroker, Position, Order, OrderRequest, OrderResult, AccountSummary
)


class AlpacaBroker(BaseBroker):
    def __init__(self):
        from alpaca.trading.client import TradingClient
        from alpaca.data.historical import StockHistoricalDataClient
        api_key = os.environ["ALPACA_API_KEY"]
        secret_key = os.environ["ALPACA_SECRET_KEY"]
        paper = "paper" in os.getenv("ALPACA_BASE_URL", "paper")
        self._trading = TradingClient(api_key, secret_key, paper=paper)
        self._data = StockHistoricalDataClient(api_key, secret_key)
        self._connected = False

    def connect(self) -> None:
        self._trading.get_account()  # raises if credentials invalid
        self._connected = True

    def disconnect(self) -> None:
        self._connected = False

    def health(self) -> dict:
        return {"connected": self._connected, "latency_ms": 0, "mode": "alpaca"}

    def get_positions(self) -> list[Position]:
        positions = []
        for p in self._trading.get_all_positions():
            positions.append(Position(
                symbol=p.symbol,
                asset_class="equity",
                quantity=float(p.qty),
                avg_cost=float(p.avg_entry_price),
                unrealised_pnl=float(p.unrealized_pl),
                currency="USD",
            ))
        return positions

    def get_orders(self) -> list[Order]:
        orders = []
        from alpaca.trading.requests import GetOrdersRequest
        for o in self._trading.get_orders(GetOrdersRequest(status="open")):
            orders.append(Order(
                order_id=str(o.id),
                symbol=o.symbol,
                side=o.side.value.upper(),
                quantity=float(o.qty or 0),
                order_type=o.order_type.value.upper(),
                status=o.status.value.upper(),
                limit_price=float(o.limit_price) if o.limit_price else None,
                fill_price=float(o.filled_avg_price) if o.filled_avg_price else None,
                ts_submitted=o.submitted_at,
                ts_last_update=o.updated_at,
            ))
        return orders

    def submit_order(self, req: OrderRequest) -> OrderResult:
        from alpaca.trading.requests import MarketOrderRequest, LimitOrderRequest
        from alpaca.trading.enums import OrderSide, TimeInForce
        side = OrderSide.BUY if req.side == "BUY" else OrderSide.SELL
        if req.order_type == "LMT" and req.limit_price:
            request = LimitOrderRequest(
                symbol=req.symbol, qty=req.quantity,
                side=side, time_in_force=TimeInForce.DAY,
                limit_price=req.limit_price,
            )
        else:
            request = MarketOrderRequest(
                symbol=req.symbol, qty=req.quantity,
                side=side, time_in_force=TimeInForce.DAY,
            )
        order = self._trading.submit_order(request)
        return OrderResult(order_id=str(order.id), status=order.status.value.upper())

    def cancel_order(self, order_id: str) -> bool:
        try:
            self._trading.cancel_order_by_id(order_id)
            return True
        except Exception:
            return False

    def get_bars(self, symbol: str, resolution: str, n: int) -> pd.DataFrame:
        from alpaca.data.requests import StockBarsRequest
        from alpaca.data.timeframe import TimeFrame
        tf_map = {"1d": TimeFrame.Day, "1h": TimeFrame.Hour}
        tf = tf_map.get(resolution, TimeFrame.Day)
        request = StockBarsRequest(symbol_or_symbols=symbol, timeframe=tf, limit=n)
        bars = self._data.get_stock_bars(request)
        df = bars.df.reset_index()
        df = df.rename(columns={"timestamp": "ts"})
        return df[["ts", "open", "high", "low", "close", "volume"]]

    def get_options_chain(self, symbol: str, expiry: str) -> pd.DataFrame:
        return pd.DataFrame()  # Alpaca options API not yet supported

    def get_account_summary(self) -> AccountSummary:
        acct = self._trading.get_account()
        return AccountSummary(
            account_id=str(acct.id),
            nav=float(acct.portfolio_value),
            cash=float(acct.cash),
            margin_used=float(acct.initial_margin or 0),
            margin_available=float(acct.buying_power),
        )
```

- [ ] **Step 4: Create `services/broker/broker_factory.py`**

```python
"""
services/broker/broker_factory.py

Reads BROKER_TYPE env var and returns the appropriate BaseBroker implementation.
"""
from __future__ import annotations

import os
from .base import BaseBroker


def get_broker() -> BaseBroker:
    """
    Factory function. Reads BROKER_TYPE from environment.
    Supported values: paper (default), ib, alpaca
    """
    broker_type = os.getenv("BROKER_TYPE", "paper").lower()

    if broker_type == "paper":
        from .paper_broker import PaperBroker
        initial_cash = float(os.getenv("PAPER_INITIAL_CASH", "100000.0"))
        return PaperBroker(initial_cash=initial_cash)

    elif broker_type == "ib":
        from .ib_broker import IBBroker
        return IBBroker(
            host=os.getenv("IB_HOST", "127.0.0.1"),
            port=int(os.getenv("IB_PORT", "7497")),
            client_id=int(os.getenv("IB_CLIENT_ID", "1")),
        )

    elif broker_type == "alpaca":
        from .alpaca_broker import AlpacaBroker
        return AlpacaBroker()

    else:
        raise ValueError(
            f"Unknown BROKER_TYPE={broker_type!r}. "
            f"Supported: paper, ib, alpaca"
        )
```

- [ ] **Step 5: Run tests**

```bash
pytest tests/broker/test_broker_factory.py -v
# Expected: 3 passed
```

- [ ] **Step 6: Run all broker tests**

```bash
pytest tests/broker/ -v
# Expected: 17 passed
```

- [ ] **Step 7: Commit**

```bash
git add services/broker/alpaca_broker.py services/broker/broker_factory.py \
        tests/broker/test_broker_factory.py
git commit -m "feat: AlpacaBroker + BrokerFactory — multi-broker abstraction complete"
```

---

## Task 7: Broker Service — FastAPI App

**Files:**
- Create: `services/broker_service/__init__.py`
- Create: `services/broker_service/main.py`
- Create: `tests/broker_service/__init__.py`
- Create: `tests/broker_service/test_main.py`

- [ ] **Step 1: Write failing tests**

Create `tests/broker_service/test_main.py`:

```python
"""Integration tests for broker_service FastAPI endpoints."""
import os
import pytest
from fastapi.testclient import TestClient


@pytest.fixture(autouse=True)
def set_paper_env(monkeypatch):
    monkeypatch.setenv("BROKER_TYPE", "paper")
    monkeypatch.setenv("PAPER_INITIAL_CASH", "100000.0")


@pytest.fixture
def client():
    from services.broker_service.main import app
    return TestClient(app)


def test_status_endpoint(client):
    resp = client.get("/status")
    assert resp.status_code == 200
    data = resp.json()
    assert "connected" in data
    assert data["mode"] == "paper"


def test_positions_empty(client):
    resp = client.get("/positions")
    assert resp.status_code == 200
    assert resp.json() == []


def test_submit_order(client):
    resp = client.post("/orders", json={
        "symbol": "AAPL",
        "side": "BUY",
        "quantity": 5.0,
        "order_type": "MKT",
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "FILLED"
    assert "order_id" in data


def test_get_orders(client):
    client.post("/orders", json={"symbol": "SPY", "side": "BUY", "quantity": 1.0})
    resp = client.get("/orders")
    assert resp.status_code == 200
    orders = resp.json()
    assert len(orders) >= 1


def test_positions_after_buy(client):
    client.post("/orders", json={"symbol": "TSLA", "side": "BUY", "quantity": 3.0})
    resp = client.get("/positions")
    assert resp.status_code == 200
    symbols = [p["symbol"] for p in resp.json()]
    assert "TSLA" in symbols


def test_cancel_order_not_found(client):
    resp = client.delete("/orders/nonexistent-order-id")
    assert resp.status_code == 404


def test_algo_status(client):
    resp = client.get("/algo/status")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)
```

- [ ] **Step 2: Run — confirm failure**

```bash
pytest tests/broker_service/test_main.py -v
# Expected: ImportError or ModuleNotFoundError
```

- [ ] **Step 3: Create `services/broker_service/__init__.py`** (empty)

```bash
touch services/broker_service/__init__.py tests/broker_service/__init__.py
```

- [ ] **Step 4: Create `services/broker_service/main.py`**

```python
"""
services/broker_service/main.py

FastAPI broker service. Runs on personal laptop alongside TWS.
Exposes broker operations as REST endpoints so analytics apps on any
machine can read positions and submit orders without a direct IB connection.
"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Optional

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# Load .env from repo root
load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", "config", ".env"))

from services.broker.broker_factory import get_broker
from services.broker.base import BaseBroker, OrderRequest

# ── Globals ───────────────────────────────────────────────────────────────────

_broker: Optional[BaseBroker] = None


def _get_broker() -> BaseBroker:
    global _broker
    if _broker is None:
        _broker = get_broker()
        _broker.connect()
    return _broker


# ── Pydantic models ───────────────────────────────────────────────────────────

class OrderRequestBody(BaseModel):
    symbol: str
    side: str               # BUY|SELL
    quantity: float
    order_type: str = "MKT"
    limit_price: Optional[float] = None
    strategy_id: Optional[str] = None


# ── App ───────────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Connect broker on startup
    _get_broker()
    yield
    # Disconnect on shutdown
    if _broker:
        _broker.disconnect()


app = FastAPI(title="IB Platform — Broker Service", lifespan=lifespan)


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/status")
def get_status():
    """Broker connection health + account summary."""
    broker = _get_broker()
    health = broker.health()
    try:
        summary = broker.get_account_summary()
        health["nav"] = summary.nav
        health["cash"] = summary.cash
        health["margin_available"] = summary.margin_available
    except Exception:
        pass
    return health


@app.get("/positions")
def get_positions():
    """Live position snapshot."""
    broker = _get_broker()
    positions = broker.get_positions()
    return [
        {
            "symbol": p.symbol,
            "asset_class": p.asset_class,
            "quantity": p.quantity,
            "avg_cost": p.avg_cost,
            "unrealised_pnl": p.unrealised_pnl,
            "currency": p.currency,
            "expiry": p.expiry,
            "strike": p.strike,
            "right": p.right,
            "account_id": p.account_id,
        }
        for p in positions
    ]


@app.post("/orders")
def submit_order(body: OrderRequestBody):
    """Submit an order. Returns order_id and status."""
    broker = _get_broker()
    req = OrderRequest(
        symbol=body.symbol,
        side=body.side,
        quantity=body.quantity,
        order_type=body.order_type,
        limit_price=body.limit_price,
        strategy_id=body.strategy_id,
    )
    result = broker.submit_order(req)
    return {"order_id": result.order_id, "status": result.status, "message": result.message}


@app.delete("/orders/{order_id}")
def cancel_order(order_id: str):
    """Cancel an open order."""
    broker = _get_broker()
    cancelled = broker.cancel_order(order_id)
    if not cancelled:
        raise HTTPException(status_code=404, detail=f"Order {order_id!r} not found or not cancellable")
    return {"order_id": order_id, "status": "CANCELLED"}


@app.get("/orders")
def get_orders():
    """Return open orders."""
    broker = _get_broker()
    orders = broker.get_orders()
    return [
        {
            "order_id": o.order_id,
            "symbol": o.symbol,
            "side": o.side,
            "quantity": o.quantity,
            "order_type": o.order_type,
            "status": o.status,
            "fill_price": o.fill_price,
            "strategy_id": o.strategy_id,
        }
        for o in orders
    ]


@app.post("/algo/start/{strategy_id}")
def start_algo(strategy_id: str):
    """Activate a strategy in algo_scheduler."""
    from services.broker_service.algo_scheduler import start_strategy
    start_strategy(strategy_id)
    return {"strategy_id": strategy_id, "status": "started"}


@app.post("/algo/stop/{strategy_id}")
def stop_algo(strategy_id: str):
    """Deactivate a strategy."""
    from services.broker_service.algo_scheduler import stop_strategy
    stop_strategy(strategy_id)
    return {"strategy_id": strategy_id, "status": "stopped"}


@app.get("/algo/status")
def algo_status():
    """Running strategies and last signal per strategy."""
    from services.broker_service.algo_scheduler import get_status
    return get_status()
```

- [ ] **Step 5: Run tests**

```bash
pytest tests/broker_service/test_main.py -v
# Expected: 7 passed (algo tests may fail until algo_scheduler exists — add it in Task 9)
```

- [ ] **Step 6: Commit**

```bash
git add services/broker_service/__init__.py services/broker_service/main.py \
        tests/broker_service/__init__.py tests/broker_service/test_main.py
git commit -m "feat: broker_service FastAPI — status/positions/orders endpoints"
```

---

## Task 8: Order Router (Pre-Trade Risk Controls)

**Files:**
- Create: `services/broker_service/order_router.py`
- Create: `tests/broker_service/test_order_router.py`

- [ ] **Step 1: Write failing tests**

Create `tests/broker_service/test_order_router.py`:

```python
"""Tests for order_router pre-trade risk checks."""
import pytest
from services.broker_service.order_router import OrderRouter, RiskConfig, RejectionReason
from services.broker.base import OrderRequest, Position, AccountSummary


def _make_summary(nav=100_000.0, cash=80_000.0):
    return AccountSummary(
        account_id="test", nav=nav, cash=cash,
        margin_used=10_000.0, margin_available=90_000.0,
    )


def _make_positions(symbol="AAPL", qty=100.0, avg_cost=150.0):
    return [Position(symbol=symbol, asset_class="equity",
                     quantity=qty, avg_cost=avg_cost, unrealised_pnl=0.0)]


@pytest.fixture
def router():
    config = RiskConfig(
        max_position_pct_nav=0.10,   # max 10% of NAV per symbol
        max_daily_loss=5_000.0,
        duplicate_guard_seconds=30,
    )
    return OrderRouter(config)


def test_passes_within_limits(router):
    req = OrderRequest(symbol="MSFT", side="BUY", quantity=10.0)
    summary = _make_summary()
    result = router.check(req, positions=[], summary=summary, daily_pnl=0.0)
    assert result.approved is True


def test_rejects_position_size_over_limit(router):
    # 100 shares × $150 = $15,000 = 15% of $100k NAV → over 10% limit
    req = OrderRequest(symbol="AAPL", side="BUY", quantity=100.0)
    summary = _make_summary()
    result = router.check(req, positions=[], summary=summary, daily_pnl=0.0,
                          last_price=150.0)
    assert result.approved is False
    assert result.reason == RejectionReason.POSITION_SIZE


def test_rejects_daily_loss_exceeded(router):
    req = OrderRequest(symbol="SPY", side="BUY", quantity=1.0)
    summary = _make_summary()
    # daily_pnl of -6000 exceeds max_daily_loss of 5000
    result = router.check(req, positions=[], summary=summary, daily_pnl=-6_000.0)
    assert result.approved is False
    assert result.reason == RejectionReason.DAILY_LOSS_LIMIT


def test_rejects_duplicate_order(router):
    req = OrderRequest(symbol="TSLA", side="BUY", quantity=5.0)
    summary = _make_summary()
    router.check(req, positions=[], summary=summary, daily_pnl=0.0)
    # Same symbol+side within 30s → reject
    result = router.check(req, positions=[], summary=summary, daily_pnl=0.0)
    assert result.approved is False
    assert result.reason == RejectionReason.DUPLICATE_ORDER


def test_allows_sell_after_buy(router):
    buy = OrderRequest(symbol="NVDA", side="BUY", quantity=1.0)
    sell = OrderRequest(symbol="NVDA", side="SELL", quantity=1.0)
    summary = _make_summary()
    router.check(buy, positions=[], summary=summary, daily_pnl=0.0)
    result = router.check(sell, positions=[], summary=summary, daily_pnl=0.0)
    assert result.approved is True
```

- [ ] **Step 2: Run — confirm failure**

```bash
pytest tests/broker_service/test_order_router.py -v
# Expected: ModuleNotFoundError
```

- [ ] **Step 3: Create `services/broker_service/order_router.py`**

```python
"""
services/broker_service/order_router.py

Pre-trade risk controls. Called before every order submission.
Hard blocks return approved=False with a RejectionReason.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from services.broker.base import OrderRequest, Position, AccountSummary


class RejectionReason(str, Enum):
    POSITION_SIZE = "position_size_exceeds_nav_limit"
    DAILY_LOSS_LIMIT = "daily_loss_limit_breached"
    DUPLICATE_ORDER = "duplicate_order_within_guard_window"
    DELTA_NOTIONAL = "options_delta_notional_cap"
    FX_NOTIONAL = "fx_notional_cap"
    BOND_DV01 = "bond_dv01_limit"


@dataclass
class RiskConfig:
    max_position_pct_nav: float = 0.10      # max notional per symbol as % of NAV
    max_daily_loss: float = 5_000.0         # halt new orders if daily loss exceeds this
    duplicate_guard_seconds: int = 30       # reject same symbol+side within N seconds
    delta_notional_cap: float = 50_000.0    # max options delta-notional per position
    fx_notional_cap: float = 100_000.0      # max FX notional per currency pair
    bond_dv01_limit: float = 500.0          # max DV01 per bond position


@dataclass
class CheckResult:
    approved: bool
    reason: Optional[RejectionReason] = None
    message: str = ""


@dataclass
class OrderRouter:
    config: RiskConfig
    _recent_orders: dict = field(default_factory=dict)  # (symbol, side) → timestamp

    def check(
        self,
        req: OrderRequest,
        positions: list[Position],
        summary: AccountSummary,
        daily_pnl: float,
        last_price: Optional[float] = None,
    ) -> CheckResult:
        """
        Run all pre-trade checks. Returns CheckResult with approved=True/False.
        Checks run in order: daily loss → position size → duplicate.
        """
        # 1. Daily loss limit
        if daily_pnl < -abs(self.config.max_daily_loss):
            return CheckResult(
                approved=False,
                reason=RejectionReason.DAILY_LOSS_LIMIT,
                message=f"Daily loss ${abs(daily_pnl):.0f} exceeds limit ${self.config.max_daily_loss:.0f}",
            )

        # 2. Position size limit
        price = last_price or 100.0  # fallback if price not provided
        notional = req.quantity * price
        max_notional = summary.nav * self.config.max_position_pct_nav
        if notional > max_notional:
            return CheckResult(
                approved=False,
                reason=RejectionReason.POSITION_SIZE,
                message=(
                    f"Order notional ${notional:,.0f} exceeds "
                    f"{self.config.max_position_pct_nav*100:.0f}% NAV limit "
                    f"(${max_notional:,.0f})"
                ),
            )

        # 3. Duplicate order guard
        key = (req.symbol, req.side)
        now = time.time()
        if key in self._recent_orders:
            elapsed = now - self._recent_orders[key]
            if elapsed < self.config.duplicate_guard_seconds:
                return CheckResult(
                    approved=False,
                    reason=RejectionReason.DUPLICATE_ORDER,
                    message=(
                        f"Duplicate {req.side} {req.symbol} order within "
                        f"{self.config.duplicate_guard_seconds}s guard window"
                    ),
                )
        self._recent_orders[key] = now

        return CheckResult(approved=True)
```

- [ ] **Step 4: Run tests**

```bash
pytest tests/broker_service/test_order_router.py -v
# Expected: 5 passed
```

- [ ] **Step 5: Commit**

```bash
git add services/broker_service/order_router.py \
        tests/broker_service/test_order_router.py
git commit -m "feat: order_router — pre-trade risk controls (size, loss limit, duplicate guard)"
```

---

## Task 9: Data Writer + Algo Scheduler Skeleton

**Files:**
- Create: `services/broker_service/data_writer.py`
- Create: `services/broker_service/algo_scheduler.py`

- [ ] **Step 1: Create `services/broker_service/data_writer.py`**

```python
"""
services/broker_service/data_writer.py

Syncs live broker data to the trading.* RDS tables.
Called by APScheduler on a fixed interval and on fill events.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, date

import psycopg2

from services.broker.base import BaseBroker, Position

logger = logging.getLogger(__name__)


def _get_conn():
    return psycopg2.connect(os.environ["PGURL"])


def upsert_account(broker: BaseBroker) -> None:
    """Write account summary to trading.capital_summary (daily snapshot)."""
    try:
        summary = broker.get_account_summary()
        conn = _get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO trading.capital_summary
                    (ts_date, nav, cash_free, margin_posted, margin_available,
                     capital_employed, capital_utilisation_pct, gross_notional, leverage_ratio)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ts_date) DO UPDATE SET
                    nav = EXCLUDED.nav,
                    cash_free = EXCLUDED.cash_free,
                    margin_posted = EXCLUDED.margin_posted,
                    margin_available = EXCLUDED.margin_available,
                    capital_employed = EXCLUDED.capital_employed,
                    capital_utilisation_pct = EXCLUDED.capital_utilisation_pct
            """, (
                date.today(),
                summary.nav,
                summary.cash,
                summary.margin_used,
                summary.margin_available,
                summary.margin_used,  # capital_employed ≈ margin_used for now
                (summary.margin_used / summary.nav * 100) if summary.nav else 0,
                summary.nav,          # gross_notional approximation
                (summary.nav / summary.cash) if summary.cash > 0 else 1.0,
            ))
        conn.commit()
        conn.close()
        logger.info("[data_writer] account snapshot written: NAV=%.2f", summary.nav)
    except Exception as exc:
        logger.error("[data_writer] upsert_account failed: %s", exc)


def upsert_positions(broker: BaseBroker, account_id: str) -> None:
    """Write current positions snapshot to trading.positions."""
    try:
        positions = broker.get_positions()
        if not positions:
            return
        conn = _get_conn()
        ts = datetime.utcnow()
        with conn.cursor() as cur:
            for p in positions:
                cur.execute("""
                    INSERT INTO trading.positions
                        (account_id, symbol, asset_class, expiry, strike, right,
                         quantity, avg_cost, unrealised_pnl, currency, ts_snapshot)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    account_id, p.symbol, p.asset_class,
                    p.expiry, p.strike, p.right,
                    p.quantity, p.avg_cost, p.unrealised_pnl,
                    p.currency, ts,
                ))
        conn.commit()
        conn.close()
        logger.info("[data_writer] %d positions written", len(positions))
    except Exception as exc:
        logger.error("[data_writer] upsert_positions failed: %s", exc)


def write_fill(
    account_id: str,
    order_id: str,
    symbol: str,
    asset_class: str,
    side: str,
    quantity: float,
    fill_price: float,
    commission: float = 0.0,
    strategy_id: str | None = None,
) -> None:
    """Write a confirmed fill to trading.trades and trading.cashflows."""
    import uuid
    trade_id = str(uuid.uuid4())
    try:
        conn = _get_conn()
        ts = datetime.utcnow()
        with conn.cursor() as cur:
            # Write trade
            cur.execute("""
                INSERT INTO trading.trades
                    (trade_id, account_id, symbol, asset_class, side,
                     quantity, fill_price, commission, strategy_id, order_id, ts_fill)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (
                trade_id, account_id, symbol, asset_class, side,
                quantity, fill_price, commission, strategy_id, order_id, ts,
            ))
            # Write cashflow: negative for buys, positive for sells
            sign = -1.0 if side == "BUY" else 1.0
            net_cf = sign * quantity * fill_price - commission
            cur.execute("""
                INSERT INTO trading.cashflows
                    (ts, cf_type, symbol, amount, currency, strategy_id, description)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                ts, "trade", symbol, net_cf, "USD", strategy_id,
                f"{side} {quantity} {symbol} @ {fill_price:.4f}",
            ))
        conn.commit()
        conn.close()
        logger.info("[data_writer] fill written: %s %s %s @ %.4f",
                    side, quantity, symbol, fill_price)
    except Exception as exc:
        logger.error("[data_writer] write_fill failed: %s", exc)
```

- [ ] **Step 2: Create `services/broker_service/algo_scheduler.py`**

```python
"""
services/broker_service/algo_scheduler.py

APScheduler-based strategy execution loop.
Strategies are registered by strategy_id and run on their configured interval.
Phase 1: skeleton only — strategy execution added in Phase 7 (libs/strategies).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

# In-memory registry of active strategies
@dataclass
class StrategyRun:
    strategy_id: str
    status: str          # active|paused|stopped
    started_at: datetime = field(default_factory=datetime.utcnow)
    last_signal: Optional[str] = None


_registry: dict[str, StrategyRun] = {}


def start_strategy(strategy_id: str) -> None:
    """Register a strategy as active. Actual execution wired in Phase 7."""
    _registry[strategy_id] = StrategyRun(strategy_id=strategy_id, status="active")
    logger.info("[algo_scheduler] started strategy: %s", strategy_id)


def stop_strategy(strategy_id: str) -> None:
    """Mark a strategy as stopped."""
    if strategy_id in _registry:
        _registry[strategy_id].status = "stopped"
    logger.info("[algo_scheduler] stopped strategy: %s", strategy_id)


def get_status() -> list[dict]:
    """Return status of all registered strategies."""
    return [
        {
            "strategy_id": run.strategy_id,
            "status": run.status,
            "started_at": run.started_at.isoformat(),
            "last_signal": run.last_signal,
        }
        for run in _registry.values()
    ]
```

- [ ] **Step 3: Run full test suite**

```bash
pytest tests/ -v
# Expected: all tests pass (17+ tests)
```

- [ ] **Step 4: Commit**

```bash
git add services/broker_service/data_writer.py \
        services/broker_service/algo_scheduler.py
git commit -m "feat: data_writer + algo_scheduler skeleton — position sync and strategy registry"
```

---

## Task 10: Smoke Test — Run Broker Service Locally

- [ ] **Step 1: Install dependencies**

```bash
pip install -r requirements.txt
```

- [ ] **Step 2: Start broker service (paper mode)**

```bash
BROKER_TYPE=paper PAPER_INITIAL_CASH=100000 PGURL=$PGURL \
  uvicorn services.broker_service.main:app --host 0.0.0.0 --port 8600 --reload
# Expected: "Application startup complete" in logs
```

- [ ] **Step 3: Test endpoints manually**

```bash
# Health check
curl http://localhost:8600/status
# Expected: {"connected": true, "mode": "paper", "nav": 100000.0, ...}

# Positions (empty)
curl http://localhost:8600/positions
# Expected: []

# Submit order
curl -X POST http://localhost:8600/orders \
  -H "Content-Type: application/json" \
  -d '{"symbol":"AAPL","side":"BUY","quantity":10,"order_type":"MKT"}'
# Expected: {"order_id":"...","status":"FILLED","message":""}

# Positions after order
curl http://localhost:8600/positions
# Expected: [{"symbol":"AAPL","quantity":10.0,"avg_cost":100.0,...}]

# Algo status
curl http://localhost:8600/algo/status
# Expected: []
```

- [ ] **Step 4: Apply DB schema**

```bash
psql $PGURL -f db/schema.sql
# Expected: all CREATE TABLE succeed with no errors
```

- [ ] **Step 5: Final commit**

```bash
git add .
git commit -m "feat: Phase 1 complete — broker abstraction, FastAPI service, DB schema"
```

---

## Self-Review Checklist

**Spec coverage:**
- [x] Repo scaffold (Task 1)
- [x] DB schema — all 20+ trading.* tables (Task 2)
- [x] BaseBroker ABC + dataclasses (Task 3)
- [x] PaperBroker — simulated fills, paper trading (Task 4)
- [x] IBBroker — ib_insync TWS wrapper (Task 5)
- [x] AlpacaBroker — multi-broker compatibility (Task 6)
- [x] BrokerFactory — BROKER_TYPE env selection (Task 6)
- [x] FastAPI broker_service — all REST endpoints (Task 7)
- [x] OrderRouter — pre-trade risk controls (Task 8)
- [x] DataWriter — position/fill/cashflow sync to RDS (Task 9)
- [x] AlgoScheduler — strategy registry skeleton (Task 9)
- [x] Smoke test (Task 10)

**Not in this phase (planned later):**
- analytics libs (Phase 2), Streamlit apps (Phase 3), market data ingestion (Phase 4),
  knowledge base (Phase 5), ML/backtest (Phase 7), execution app (Phase 9)

**Type consistency:**
- `OrderRequest`, `OrderResult`, `Position`, `Order`, `AccountSummary` defined once in `base.py`, used consistently across all tasks
- `BaseBroker` method signatures in `base.py` match all implementations

---

*Plan written by Claude Sonnet 4.6, 2026-06-14. Phase 1 of 10.*
