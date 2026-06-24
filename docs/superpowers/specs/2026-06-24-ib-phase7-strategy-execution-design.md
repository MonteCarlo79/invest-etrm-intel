# IB Platform Phase 7 — Strategy Execution Design

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Close the loop from market signals to live IB orders by building a shared Strategy ABC, a backtest engine, live execution wiring, and a Strategies dashboard tab.

**Architecture:** A `BaseStrategy` ABC with a single `generate_signal(bars, context) -> Signal | None` method works identically in backtest and live contexts. The backtest engine feeds it historical bars day-by-day; the live APScheduler feeds it the latest bars from DB (yfinance fallback). Strategy configuration is stored in `trading.strategy_config` (DB), editable from the dashboard.

**Tech Stack:** Python dataclasses, APScheduler, psycopg2, pandas, yfinance, Plotly, Streamlit, existing `libs/signals/vix.py` and `libs/risk/performance.py`.

---

## 1. Architecture Overview

### New files

```
libs/strategies/
  __init__.py
  base.py          — BaseStrategy ABC, Signal dataclass
  vix_regime.py    — VixRegimeStrategy (reference implementation)

libs/backtest/
  __init__.py
  engine.py        — run_backtest(strategy, bars, vix_bars, capital) → BacktestResult
  metrics.py       — compute_metrics(trades, equity_curve) → dict

services/broker_service/
  signal_writer.py — write_signal / mark_executed

db/migrations/004_signals.sql
  trading.strategy_config
  trading.signals

apps/portfolio/tabs/strategies.py   — new 10th tab
```

### Modified files

```
services/broker_service/algo_scheduler.py  — replace skeleton with real execution loop
apps/portfolio/app.py                      — add Strategies tab (9 → 10 tabs)
apps/shared/db.py                          — add get_signals, get_strategy_configs, upsert_strategy_config
db/schema.sql                              — add strategy_config + signals tables
```

### Data flow

**Live:**
APScheduler fires (Mon–Fri 09:35 ET) → load strategy config from DB → fetch bars (DB → yfinance fallback) + VIX term structure → `generate_signal()` → `signal_writer.write_signal()` → if `auto_execute=TRUE` and side ≠ "flat" → `order_router.check()` → if approved → `broker.place_order()` → `signal_writer.mark_executed()`

**Backtest:**
Dashboard user selects strategy + date range → `run_backtest()` iterates bars day-by-day → collects signals/trades → `compute_metrics()` → displayed as metrics table + equity curve chart + trades table

---

## 2. Strategy Layer

### `libs/strategies/base.py`

```python
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
import pandas as pd

@dataclass
class Signal:
    strategy_id: str
    symbol: str
    side: str           # "buy" | "sell" | "flat"
    quantity: float
    confidence: float   # 0.0–1.0
    reason: str
    generated_at: datetime = field(default_factory=datetime.utcnow)

class BaseStrategy(ABC):
    def __init__(self, config: dict):
        self.config = config

    @property
    @abstractmethod
    def strategy_id(self) -> str: ...

    @abstractmethod
    def generate_signal(self, bars: pd.DataFrame, context: dict) -> Signal | None:
        """
        bars: DataFrame with columns [date, open, high, low, close, volume],
              containing only rows up to and including the current bar (no look-ahead).
        context: dict — at minimum {"vix_term": VixTermStructure | None}
        Returns Signal or None if insufficient data. Never raises.
        """
        ...
```

### `libs/strategies/vix_regime.py`

`VixRegimeStrategy(BaseStrategy)`:

- `strategy_id = "vix_regime"`
- Config keys (from DB `params` JSONB):
  - `symbol` (default `"SPY"`)
  - `base_qty` (default `10`)
  - `threshold_contango` (default `5.0`) — contango_pct above this → buy
  - `threshold_backwardation` (default `-2.0`) — contango_pct below this → sell/flat
- Logic:
  1. Extract `vix_term` from context; return `None` if missing
  2. Compute `cpct = contango_pct(vix_term)` from `libs/signals/vix.py`
  3. If `cpct > threshold_contango`: side=`"buy"`, qty=`base_qty * cpct / 10` (capped at `base_qty * 3`)
  4. If `cpct < threshold_backwardation`: side=`"sell"`, qty=`base_qty`
  5. Else: side=`"flat"`, qty=`0`
  6. Returns `Signal(strategy_id, symbol, side, qty, confidence, reason)` — confidence = `min(abs(cpct) / 20, 1.0)`
- Wraps everything in `try/except`; returns `None` on any error

---

## 3. Backtest Engine

### `libs/backtest/engine.py`

```python
@dataclass
class BacktestResult:
    signals: list[Signal]
    trades: list[dict]   # {date, symbol, side, qty, entry_price, exit_price, pnl}
    equity_curve: list[float]
    metrics: dict        # from compute_metrics()

def run_backtest(
    strategy: BaseStrategy,
    bars: pd.DataFrame,      # columns: date, open, high, low, close, volume; sorted asc
    vix_bars: pd.DataFrame,  # columns: date, m1..m8; sorted asc; may be empty
    initial_capital: float = 100_000.0,
) -> BacktestResult
```

Execution rules:
- Iterate bars index `i` from 0 to `len(bars)-2` (need `i+1` for execution price)
- On bar `i`: build `VixTermStructure` from `vix_bars` row for that date (or `None` if missing); call `strategy.generate_signal(bars.iloc[:i+1], context)`
- Signal fires → execute at `bars.iloc[i+1]["open"]` (next bar open — no look-ahead)
- Track: `cash`, `position_qty`, `position_cost`; record trade when position changes
- `equity_curve[i]` = `cash + position_qty * bars.iloc[i]["close"]`
- Returns `BacktestResult` with all signals, trades, equity curve, and metrics

### `libs/backtest/metrics.py`

`compute_metrics(trades: list[dict], equity_curve: list[float]) -> dict`:
- `total_return_pct`: `(equity_curve[-1] - equity_curve[0]) / equity_curve[0] * 100`
- `sharpe`: delegates to `libs/risk/performance.sharpe(returns)`
- `max_drawdown_pct`: delegates to `libs/risk/performance.max_drawdown(equity_curve)`
- `win_rate`: `profitable_trades / total_trades` (0.0 if no trades)
- `num_trades`: `len(trades)`

---

## 4. DB Schema

### `db/migrations/004_signals.sql`

```sql
CREATE TABLE IF NOT EXISTS trading.strategy_config (
    strategy_id   TEXT PRIMARY KEY,
    symbol        TEXT NOT NULL DEFAULT 'SPY',
    auto_execute  BOOLEAN NOT NULL DEFAULT FALSE,
    params        JSONB NOT NULL DEFAULT '{}',
    enabled       BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS trading.signals (
    id            SERIAL PRIMARY KEY,
    strategy_id   TEXT NOT NULL,
    symbol        TEXT NOT NULL,
    side          TEXT NOT NULL,
    quantity      FLOAT NOT NULL,
    confidence    FLOAT NOT NULL,
    reason        TEXT NOT NULL,
    executed      BOOLEAN NOT NULL DEFAULT FALSE,
    order_id      TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS signals_strategy_created
    ON trading.signals (strategy_id, created_at DESC);

INSERT INTO trading.strategy_config (strategy_id, symbol, auto_execute, params)
VALUES ('vix_regime', 'SPY', FALSE,
    '{"base_qty": 10, "threshold_contango": 5.0, "threshold_backwardation": -2.0}')
ON CONFLICT DO NOTHING;
```

`auto_execute` defaults to `FALSE` — no live orders until explicitly enabled in the dashboard.

---

## 5. Live Execution Wiring

### `services/broker_service/signal_writer.py`

```python
def write_signal(conn, signal: Signal) -> int:
    """Insert signal into trading.signals. Returns new row id."""

def mark_executed(conn, signal_id: int, order_id: str) -> None:
    """Set executed=TRUE and order_id on the signal row."""
```

### `services/broker_service/algo_scheduler.py` (replacement)

Replaces the Phase 1 skeleton. Key changes:
- `STRATEGY_REGISTRY: dict[str, type[BaseStrategy]] = {"vix_regime": VixRegimeStrategy}`
- `build_scheduler(broker, conn) -> BlockingScheduler`:
  - Loads all `enabled=TRUE` rows from `trading.strategy_config`
  - Instantiates each strategy: `STRATEGY_REGISTRY[row.strategy_id](row.params)`
  - Adds one `CronTrigger(day_of_week="mon-fri", hour=9, minute=35, timezone="America/New_York")` job per strategy
- Per-strategy job function:
  1. Fetch bars: `get_bars_1d(conn, symbol)` → if < 30 rows, `yfinance.download(symbol, period="1y")`
  2. Fetch VIX term structure: `get_vix_term_structure(conn)` → build `VixTermStructure`
  3. `signal = strategy.generate_signal(bars, {"vix_term": vix_term})`
  4. If `signal` is `None` or `side == "flat"`: write flat signal (for audit trail), return
  5. `signal_id = write_signal(conn, signal)`
  6. If `auto_execute=TRUE`: `result = order_router.check(req, positions, summary, daily_pnl)` → if approved → `order = broker.place_order(req)` → `mark_executed(conn, signal_id, order.order_id)`
- Each strategy job wrapped in `try/except`; errors logged, never propagate
- `services/broker_service/main.py` must be updated to call `build_scheduler(broker, conn).start()` on startup instead of the old `start_strategy()`/`stop_strategy()` skeleton calls

---

## 6. Strategies Dashboard Tab

`apps/portfolio/tabs/strategies.py` — single `render(conn)` function, `import streamlit as st` inside render only.

Three vertical panels:

**Strategy Config:**
- Load `get_strategy_configs(conn)` → one expander per strategy
- Shows `symbol`, `enabled` checkbox, `auto_execute` checkbox, `params` JSON text area
- "Save" button → `upsert_strategy_config(conn, strategy_id, updated_params)` → `st.rerun()`

**Recent Signals:**
- Strategy selectbox → `get_signals(conn, strategy_id, limit=50)`
- DataFrame with `side` column styled green (buy) / red (sell) / grey (flat)
- Shows `created_at`, `symbol`, `side`, `quantity`, `confidence`, `reason`, `executed`

**Backtest:**
- Inputs: strategy selectbox, start date (`date_input`), end date, initial capital (`number_input`)
- "Run Backtest" button → fetches bars (DB → yfinance fallback) + VIX bars → `run_backtest()`
- Results:
  - Metrics row: `total_return`, `sharpe`, `max_drawdown`, `win_rate`, `num_trades`
  - Equity curve: Plotly line chart
  - Trades table: `date`, `side`, `qty`, `entry_price`, `exit_price`, `pnl`

`apps/portfolio/app.py`: add `from apps.portfolio.tabs import strategies` and wire as tab 10 ("Strategies").

### `apps/shared/db.py` additions

```python
def get_signals(conn, strategy_id: str, limit: int = 50) -> pd.DataFrame:
    # SELECT * FROM trading.signals WHERE strategy_id=%s ORDER BY created_at DESC LIMIT %s

def get_strategy_configs(conn) -> pd.DataFrame:
    # SELECT * FROM trading.strategy_config ORDER BY strategy_id

def upsert_strategy_config(conn, strategy_id: str, symbol: str, auto_execute: bool, enabled: bool, params: dict) -> None:
    # INSERT INTO trading.strategy_config ... ON CONFLICT (strategy_id) DO UPDATE SET ...
    # then conn.commit()
```

---

## 7. Testing

All tests use mock DB cursors and mock broker — no live IB calls, no live DB.

| Test file | What it covers |
|---|---|
| `tests/libs/strategies/test_base.py` | `Signal` construction, field defaults |
| `tests/libs/strategies/test_vix_regime.py` | contango → buy; backwardation → sell; missing VIX → None; config overrides |
| `tests/libs/backtest/test_engine.py` | 30-bar synthetic data; no look-ahead (signal at t, executes at t+1 open); correct trade count |
| `tests/libs/backtest/test_metrics.py` | known equity curve → sharpe/max_dd/win_rate within tolerance |
| `tests/broker_service/test_signal_writer.py` | write_signal inserts correct row; mark_executed sets executed=TRUE |
| `tests/broker_service/test_algo_scheduler.py` | auto_execute=TRUE → place_order called; auto_execute=FALSE → not called |
| `tests/apps/portfolio/test_strategies_tab.py` | render() with mocked DB functions runs without error |

Target: ~30 new tests, total ~424 passing.

---

## 8. Run Commands

```bash
# Apply migration (run once)
psql $PGURL -f db/migrations/004_signals.sql

# Run tests
cd C:/Users/dipeng.chen/OneDrive/ETRM/ib-platform
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/ -q

# Start broker service (with strategy execution)
& "C:\Users\dipeng.chen\AppData\Local\Python\bin\python.exe" -m services.broker_service.main
```
