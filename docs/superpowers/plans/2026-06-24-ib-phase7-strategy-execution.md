# IB Platform Phase 7 — Strategy Execution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the loop from market signals to live IB orders by building a shared Strategy ABC, a VIX-regime strategy, a backtest engine, live execution wiring inside the broker service, and a Strategies tab in the portfolio app.

**Architecture:** `BaseStrategy` ABC with `generate_signal(bars, context) -> Signal | None` works identically in backtest and live contexts. `BackgroundScheduler` inside FastAPI fires Mon–Fri 09:35 ET, loads enabled strategies from `trading.strategy_config`, writes signals to `trading.signals`, and submits orders via `order_router` + `broker` when `auto_execute=TRUE`.

**Tech Stack:** Python dataclasses, APScheduler `BackgroundScheduler`, psycopg2, pandas, yfinance, Plotly, Streamlit, existing `libs/signals/vix.VixTermStructure`, `libs/risk/performance.sharpe` / `max_drawdown`.

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `db/migrations/004_signals.sql` | Create | `trading.strategy_config` + `trading.signals` tables + seed row |
| `db/schema.sql` | Modify | Add same two tables to canonical schema |
| `libs/strategies/__init__.py` | Create | Empty |
| `libs/strategies/base.py` | Create | `Signal` dataclass + `BaseStrategy` ABC |
| `libs/strategies/vix_regime.py` | Create | `VixRegimeStrategy(BaseStrategy)` |
| `libs/backtest/__init__.py` | Create | Empty |
| `libs/backtest/metrics.py` | Create | `compute_metrics(trades, equity_curve) -> dict` |
| `libs/backtest/engine.py` | Create | `BacktestResult` + `run_backtest()` |
| `services/broker_service/signal_writer.py` | Create | `write_signal`, `mark_executed` |
| `services/broker_service/algo_scheduler.py` | Replace | `build_scheduler(broker)` replacing Phase 1 skeleton |
| `apps/shared/db.py` | Modify | Add `get_signals`, `get_strategy_configs`, `upsert_strategy_config` |
| `services/broker_service/main.py` | Modify | Start scheduler in lifespan; update `/algo/*` endpoints |
| `apps/portfolio/tabs/strategies.py` | Create | Strategies tab (Config / Recent Signals / Backtest panels) |
| `apps/portfolio/app.py` | Modify | Add Strategies as tab 10 |
| `tests/libs/strategies/__init__.py` | Create | Empty |
| `tests/libs/strategies/test_base.py` | Create | Signal construction tests |
| `tests/libs/strategies/test_vix_regime.py` | Create | VixRegimeStrategy logic tests |
| `tests/libs/backtest/__init__.py` | Create | Empty |
| `tests/libs/backtest/test_metrics.py` | Create | compute_metrics tests |
| `tests/libs/backtest/test_engine.py` | Create | run_backtest no-look-ahead tests |
| `tests/broker_service/test_signal_writer.py` | Create | write_signal / mark_executed tests |
| `tests/broker_service/test_algo_scheduler.py` | Modify | Replace skeleton tests; add auto_execute tests |
| `tests/apps/portfolio/test_strategies_tab.py` | Create | render() smoke test |

---

## Task 1: DB Migration

**Files:**
- Create: `db/migrations/004_signals.sql`
- Modify: `db/schema.sql`

- [ ] **Step 1: Create migration file**

```sql
-- db/migrations/004_signals.sql
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

- [ ] **Step 2: Add tables to db/schema.sql**

Find the end of the existing CREATE TABLE blocks in `db/schema.sql` and append:

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
```

- [ ] **Step 3: Run migration against RDS**

```bash
psql $PGURL -f db/migrations/004_signals.sql
```

Expected output:
```
CREATE TABLE
CREATE TABLE
CREATE INDEX
INSERT 0 1
```

- [ ] **Step 4: Commit**

```bash
git add db/migrations/004_signals.sql db/schema.sql
git commit -m "feat(db): add strategy_config and signals tables (Phase 7)"
```

---

## Task 2: Strategy Base (`libs/strategies/base.py`)

**Files:**
- Create: `libs/strategies/__init__.py`
- Create: `libs/strategies/base.py`
- Create: `tests/libs/strategies/__init__.py`
- Create: `tests/libs/strategies/test_base.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/libs/strategies/test_base.py
from datetime import datetime
from libs.strategies.base import Signal, BaseStrategy
import pandas as pd
import pytest


def test_signal_defaults():
    s = Signal(
        strategy_id="test",
        symbol="SPY",
        side="buy",
        quantity=10.0,
        confidence=0.8,
        reason="test reason",
    )
    assert s.strategy_id == "test"
    assert s.symbol == "SPY"
    assert s.side == "buy"
    assert s.quantity == 10.0
    assert s.confidence == 0.8
    assert s.reason == "test reason"
    assert isinstance(s.generated_at, datetime)


def test_signal_custom_generated_at():
    ts = datetime(2026, 1, 1, 12, 0, 0)
    s = Signal(strategy_id="x", symbol="SPY", side="flat",
               quantity=0.0, confidence=0.0, reason="r", generated_at=ts)
    assert s.generated_at == ts


def test_base_strategy_is_abstract():
    with pytest.raises(TypeError):
        BaseStrategy({})


class ConcreteStrategy(BaseStrategy):
    @property
    def strategy_id(self) -> str:
        return "concrete"

    def generate_signal(self, bars, context):
        return None


def test_concrete_strategy_instantiates():
    s = ConcreteStrategy({"symbol": "SPY"})
    assert s.strategy_id == "concrete"
    assert s.config == {"symbol": "SPY"}
    assert s.generate_signal(pd.DataFrame(), {}) is None
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd C:/Users/dipeng.chen/OneDrive/ETRM/ib-platform
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/libs/strategies/test_base.py -v
```

Expected: `ModuleNotFoundError: No module named 'libs.strategies'`

- [ ] **Step 3: Create the files**

```python
# libs/strategies/__init__.py
```

```python
# tests/libs/strategies/__init__.py
```

```python
# libs/strategies/base.py
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
        bars: DataFrame with columns [ts_date, open, high, low, close, volume],
              containing only rows up to and including the current bar (no look-ahead).
        context: dict — at minimum {"vix_term": VixTermStructure | None}
        Returns Signal or None if insufficient data. Never raises.
        """
        ...
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/libs/strategies/test_base.py -v
```

Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add libs/strategies/__init__.py libs/strategies/base.py \
        tests/libs/strategies/__init__.py tests/libs/strategies/test_base.py
git commit -m "feat(strategies): add Signal dataclass and BaseStrategy ABC"
```

---

## Task 3: VIX Regime Strategy (`libs/strategies/vix_regime.py`)

**Files:**
- Create: `libs/strategies/vix_regime.py`
- Create: `tests/libs/strategies/test_vix_regime.py`

**Context:** `VixTermStructure` lives in `libs/signals/vix.py`. Its property `contango_m1_m2` returns a ratio (e.g. `0.07` for 7% contango). Multiply by 100 to get percentage points for comparison with `threshold_contango` (default `5.0`).

- [ ] **Step 1: Write the failing tests**

```python
# tests/libs/strategies/test_vix_regime.py
import pandas as pd
import pytest
from libs.signals.vix import VixTermStructure
from libs.strategies.vix_regime import VixRegimeStrategy


def _make_vix(m1: float, m2: float) -> VixTermStructure:
    return VixTermStructure(vix_index=m1, m1=m1, m2=m2, m3=m2 * 1.02, days_to_m1_expiry=21)


def _make_bars(n: int = 10) -> pd.DataFrame:
    dates = pd.date_range("2026-01-01", periods=n, freq="B")
    return pd.DataFrame({
        "ts_date": dates,
        "open": [100.0] * n,
        "high": [101.0] * n,
        "low": [99.0] * n,
        "close": [100.0] * n,
        "volume": [1_000_000] * n,
    })


def test_contango_regime_buy():
    # m1=18, m2=20 → contango_pct = (20-18)/18 * 100 = 11.1% > 5.0 threshold
    strategy = VixRegimeStrategy({"symbol": "SPY", "base_qty": 10,
                                   "threshold_contango": 5.0, "threshold_backwardation": -2.0})
    signal = strategy.generate_signal(_make_bars(), {"vix_term": _make_vix(18, 20)})
    assert signal is not None
    assert signal.side == "buy"
    assert signal.symbol == "SPY"
    assert signal.quantity > 0
    assert 0.0 <= signal.confidence <= 1.0
    assert signal.strategy_id == "vix_regime"


def test_backwardation_regime_sell():
    # m1=22, m2=20 → contango_pct = (20-22)/22 * 100 = -9.1% < -2.0 threshold
    strategy = VixRegimeStrategy({"symbol": "SPY", "base_qty": 10,
                                   "threshold_contango": 5.0, "threshold_backwardation": -2.0})
    signal = strategy.generate_signal(_make_bars(), {"vix_term": _make_vix(22, 20)})
    assert signal is not None
    assert signal.side == "sell"
    assert signal.quantity == 10.0


def test_neutral_regime_flat():
    # m1=20, m2=20.5 → contango_pct = 2.5% — between thresholds
    strategy = VixRegimeStrategy({"symbol": "SPY", "base_qty": 10,
                                   "threshold_contango": 5.0, "threshold_backwardation": -2.0})
    signal = strategy.generate_signal(_make_bars(), {"vix_term": _make_vix(20, 20.5)})
    assert signal is not None
    assert signal.side == "flat"
    assert signal.quantity == 0.0


def test_missing_vix_returns_none():
    strategy = VixRegimeStrategy({})
    signal = strategy.generate_signal(_make_bars(), {"vix_term": None})
    assert signal is None


def test_missing_vix_key_returns_none():
    strategy = VixRegimeStrategy({})
    signal = strategy.generate_signal(_make_bars(), {})
    assert signal is None


def test_config_overrides():
    # threshold_contango=2.0, m1=18, m2=19 → cpct=5.6% > 2.0 → buy
    strategy = VixRegimeStrategy({"symbol": "QQQ", "base_qty": 5,
                                   "threshold_contango": 2.0, "threshold_backwardation": -5.0})
    signal = strategy.generate_signal(_make_bars(), {"vix_term": _make_vix(18, 19)})
    assert signal is not None
    assert signal.symbol == "QQQ"
    assert signal.side == "buy"


def test_quantity_capped_at_3x_base():
    # Extreme contango: m1=10, m2=20 → cpct=100% → qty = 10 * 100/10 = 100, capped at 10*3=30
    strategy = VixRegimeStrategy({"base_qty": 10, "threshold_contango": 5.0,
                                   "threshold_backwardation": -2.0})
    signal = strategy.generate_signal(_make_bars(), {"vix_term": _make_vix(10, 20)})
    assert signal is not None
    assert signal.side == "buy"
    assert signal.quantity == 30.0


def test_exception_returns_none():
    strategy = VixRegimeStrategy({})
    # Pass invalid context (not a dict) — should return None, not raise
    signal = strategy.generate_signal(_make_bars(), "not_a_dict")
    assert signal is None
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/libs/strategies/test_vix_regime.py -v
```

Expected: `ModuleNotFoundError: No module named 'libs.strategies.vix_regime'`

- [ ] **Step 3: Implement VixRegimeStrategy**

```python
# libs/strategies/vix_regime.py
from __future__ import annotations
import logging
from libs.strategies.base import BaseStrategy, Signal
from libs.signals.vix import VixTermStructure

logger = logging.getLogger(__name__)


class VixRegimeStrategy(BaseStrategy):
    """
    Go long SPY in contango regime, flat/short in backwardation.

    Config keys (from trading.strategy_config.params JSONB):
      symbol                : default "SPY"
      base_qty              : default 10
      threshold_contango    : contango_pct (%) above which → buy; default 5.0
      threshold_backwardation: contango_pct (%) below which → sell; default -2.0
    """

    @property
    def strategy_id(self) -> str:
        return "vix_regime"

    def generate_signal(self, bars, context) -> Signal | None:
        try:
            vix_term: VixTermStructure | None = context.get("vix_term")
            if vix_term is None:
                return None

            symbol = str(self.config.get("symbol", "SPY"))
            base_qty = float(self.config.get("base_qty", 10))
            thr_c = float(self.config.get("threshold_contango", 5.0))
            thr_b = float(self.config.get("threshold_backwardation", -2.0))

            # contango_m1_m2 is a ratio (0.07 = 7%); convert to pct points
            cpct = vix_term.contango_m1_m2 * 100.0

            if cpct > thr_c:
                side = "buy"
                qty = min(base_qty * cpct / 10.0, base_qty * 3.0)
                reason = f"contango {cpct:.1f}% > threshold {thr_c}"
            elif cpct < thr_b:
                side = "sell"
                qty = base_qty
                reason = f"backwardation {cpct:.1f}% < threshold {thr_b}"
            else:
                side = "flat"
                qty = 0.0
                reason = f"neutral contango {cpct:.1f}%"

            confidence = min(abs(cpct) / 20.0, 1.0)

            return Signal(
                strategy_id=self.strategy_id,
                symbol=symbol,
                side=side,
                quantity=qty,
                confidence=confidence,
                reason=reason,
            )
        except Exception:
            logger.exception("VixRegimeStrategy.generate_signal failed")
            return None
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/libs/strategies/ -v
```

Expected: 11 passed

- [ ] **Step 5: Commit**

```bash
git add libs/strategies/vix_regime.py tests/libs/strategies/test_vix_regime.py
git commit -m "feat(strategies): add VixRegimeStrategy"
```

---

## Task 4: Backtest Metrics (`libs/backtest/metrics.py`)

**Files:**
- Create: `libs/backtest/__init__.py`
- Create: `libs/backtest/metrics.py`
- Create: `tests/libs/backtest/__init__.py`
- Create: `tests/libs/backtest/test_metrics.py`

**Context:**
- `libs/risk/performance.sharpe(returns: list[float]) -> float` — takes daily returns (not equity curve); annualises by default.
- `libs/risk/performance.max_drawdown(nav_series: list[float]) -> float` — takes equity curve values; returns a positive fraction (e.g. `0.15` for 15% drawdown).

- [ ] **Step 1: Write the failing tests**

```python
# tests/libs/backtest/test_metrics.py
from libs.backtest.metrics import compute_metrics


def _flat_equity(n=100, value=100_000.0):
    return [value] * n


def test_zero_trades_returns_zeros():
    eq = _flat_equity()
    result = compute_metrics([], eq)
    assert result["num_trades"] == 0
    assert result["win_rate"] == 0.0
    assert result["total_return_pct"] == 0.0


def test_total_return_pct():
    eq = [100_000.0, 110_000.0]
    result = compute_metrics([], eq)
    assert abs(result["total_return_pct"] - 10.0) < 0.01


def test_win_rate():
    trades = [{"pnl": 200.0}, {"pnl": -100.0}, {"pnl": 300.0}]
    result = compute_metrics(trades, _flat_equity())
    assert abs(result["win_rate"] - 2 / 3) < 0.01


def test_win_rate_no_trades():
    result = compute_metrics([], _flat_equity())
    assert result["win_rate"] == 0.0


def test_max_drawdown_positive():
    # Equity goes up then crashes — max_drawdown_pct should be positive
    eq = [100_000, 120_000, 90_000, 95_000]
    result = compute_metrics([], eq)
    assert result["max_drawdown_pct"] > 0.0


def test_num_trades():
    trades = [{"pnl": 100.0}] * 5
    result = compute_metrics(trades, _flat_equity())
    assert result["num_trades"] == 5


def test_short_equity_curve():
    result = compute_metrics([], [100_000.0])
    assert result["total_return_pct"] == 0.0
    assert result["sharpe"] == 0.0
    assert result["max_drawdown_pct"] == 0.0
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/libs/backtest/test_metrics.py -v
```

Expected: `ModuleNotFoundError: No module named 'libs.backtest'`

- [ ] **Step 3: Implement compute_metrics**

```python
# libs/backtest/__init__.py
```

```python
# tests/libs/backtest/__init__.py
```

```python
# libs/backtest/metrics.py
from __future__ import annotations
from libs.risk.performance import sharpe, max_drawdown


def compute_metrics(trades: list[dict], equity_curve: list[float]) -> dict:
    """
    Compute summary statistics from a completed backtest.

    trades      : list of {"pnl": float, ...} dicts
    equity_curve: list of portfolio values at each bar (same length as bars)
    """
    if len(equity_curve) < 2:
        return {
            "total_return_pct": 0.0,
            "sharpe": 0.0,
            "max_drawdown_pct": 0.0,
            "win_rate": 0.0,
            "num_trades": len(trades),
        }

    returns = [
        (equity_curve[i] - equity_curve[i - 1]) / equity_curve[i - 1]
        for i in range(1, len(equity_curve))
    ]
    total_return_pct = (equity_curve[-1] - equity_curve[0]) / equity_curve[0] * 100.0
    profitable = [t for t in trades if float(t.get("pnl", 0.0)) > 0]
    win_rate = len(profitable) / len(trades) if trades else 0.0

    return {
        "total_return_pct": total_return_pct,
        "sharpe": sharpe(returns),
        "max_drawdown_pct": max_drawdown(equity_curve) * 100.0,
        "win_rate": win_rate,
        "num_trades": len(trades),
    }
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/libs/backtest/test_metrics.py -v
```

Expected: 7 passed

- [ ] **Step 5: Commit**

```bash
git add libs/backtest/__init__.py libs/backtest/metrics.py \
        tests/libs/backtest/__init__.py tests/libs/backtest/test_metrics.py
git commit -m "feat(backtest): add compute_metrics"
```

---

## Task 5: Backtest Engine (`libs/backtest/engine.py`)

**Files:**
- Create: `libs/backtest/engine.py`
- Create: `tests/libs/backtest/test_engine.py`

**Context:**
- Iterate `bars` index `i` from `0` to `len(bars)-2`. On bar `i`, generate signal using `bars.iloc[:i+1]`; execute at `bars.iloc[i+1]["open"]` (no look-ahead).
- Position is long-only for the VIX regime strategy: "buy" opens position if flat; "sell" or "flat" closes position if open.
- `vix_bars` columns: `ts_date, vix_index, m1, m2, m3` (from `trading.vix_term_structure`). Match by `ts_date`. Use `days_to_m1_expiry=21` as constant approximation.

- [ ] **Step 1: Write the failing tests**

```python
# tests/libs/backtest/test_engine.py
import pandas as pd
import numpy as np
from unittest.mock import MagicMock
from libs.strategies.base import Signal, BaseStrategy
from libs.backtest.engine import run_backtest, BacktestResult


def _make_bars(n: int = 40) -> pd.DataFrame:
    dates = pd.date_range("2026-01-01", periods=n, freq="B")
    prices = 100.0 + np.arange(n) * 0.5  # gently trending up
    return pd.DataFrame({
        "ts_date": dates,
        "open":   prices,
        "high":   prices + 1,
        "low":    prices - 1,
        "close":  prices,
        "volume": [1_000_000] * n,
    })


def _make_vix_bars(bars: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        "ts_date":   bars["ts_date"],
        "vix_index": [18.0] * len(bars),
        "m1":        [18.0] * len(bars),
        "m2":        [20.0] * len(bars),  # contango ~11%
        "m3":        [21.0] * len(bars),
    })


class BuyAlwaysStrategy(BaseStrategy):
    @property
    def strategy_id(self) -> str:
        return "buy_always"

    def generate_signal(self, bars, context) -> Signal:
        return Signal(strategy_id="buy_always", symbol="SPY",
                      side="buy", quantity=10.0, confidence=1.0, reason="test")


class FlatAlwaysStrategy(BaseStrategy):
    @property
    def strategy_id(self) -> str:
        return "flat_always"

    def generate_signal(self, bars, context) -> Signal:
        return Signal(strategy_id="flat_always", symbol="SPY",
                      side="flat", quantity=0.0, confidence=0.0, reason="flat")


def test_result_type():
    bars = _make_bars(10)
    vix = _make_vix_bars(bars)
    result = run_backtest(BuyAlwaysStrategy({}), bars, vix)
    assert isinstance(result, BacktestResult)
    assert isinstance(result.signals, list)
    assert isinstance(result.trades, list)
    assert isinstance(result.equity_curve, list)
    assert isinstance(result.metrics, dict)


def test_equity_curve_length():
    bars = _make_bars(30)
    vix = _make_vix_bars(bars)
    result = run_backtest(BuyAlwaysStrategy({}), bars, vix)
    assert len(result.equity_curve) == len(bars)


def test_no_look_ahead():
    """Signal generated at bar i must execute at bar i+1 open, not bar i close."""
    executed_prices = []

    class RecordPriceStrategy(BaseStrategy):
        @property
        def strategy_id(self):
            return "record"

        def generate_signal(self, bars, context):
            # Record the last close in bars slice provided
            return Signal(strategy_id="record", symbol="SPY",
                          side="buy", quantity=1.0, confidence=1.0,
                          reason=f"close={bars.iloc[-1]['close']}")

    bars = _make_bars(5)
    vix = _make_vix_bars(bars)
    result = run_backtest(RecordPriceStrategy({}), bars, vix)
    # First signal fires at bar 0, executes at bar 1 open
    if result.trades:
        first_trade = result.trades[0]
        assert first_trade["entry_price"] == bars.iloc[1]["open"]


def test_flat_strategy_no_trades():
    bars = _make_bars(30)
    vix = _make_vix_bars(bars)
    result = run_backtest(FlatAlwaysStrategy({}), bars, vix)
    assert result.trades == []


def test_buy_and_close_records_trade():
    """Buy on bar 0, sell on bar 5 — should record one trade."""
    call_count = [0]

    class BuyThenSellStrategy(BaseStrategy):
        @property
        def strategy_id(self):
            return "buy_then_sell"

        def generate_signal(self, bars, context):
            i = len(bars) - 1
            call_count[0] += 1
            side = "buy" if i < 5 else "sell"
            return Signal(strategy_id="buy_then_sell", symbol="SPY",
                          side=side, quantity=10.0, confidence=1.0, reason="")

    bars = _make_bars(20)
    vix = _make_vix_bars(bars)
    result = run_backtest(BuyThenSellStrategy({}), bars, vix)
    assert len(result.trades) >= 1
    trade = result.trades[0]
    assert "pnl" in trade
    assert "entry_price" in trade
    assert "exit_price" in trade


def test_metrics_populated():
    bars = _make_bars(30)
    vix = _make_vix_bars(bars)
    result = run_backtest(BuyAlwaysStrategy({}), bars, vix)
    assert "total_return_pct" in result.metrics
    assert "sharpe" in result.metrics
    assert "max_drawdown_pct" in result.metrics
    assert "win_rate" in result.metrics
    assert "num_trades" in result.metrics


def test_empty_bars_returns_empty_result():
    bars = pd.DataFrame(columns=["ts_date", "open", "high", "low", "close", "volume"])
    vix = pd.DataFrame(columns=["ts_date", "vix_index", "m1", "m2", "m3"])
    result = run_backtest(BuyAlwaysStrategy({}), bars, vix)
    assert result.trades == []
    assert result.equity_curve == []
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/libs/backtest/test_engine.py -v
```

Expected: `ModuleNotFoundError: No module named 'libs.backtest.engine'`

- [ ] **Step 3: Implement the backtest engine**

```python
# libs/backtest/engine.py
from __future__ import annotations
from dataclasses import dataclass, field
import pandas as pd
from libs.strategies.base import BaseStrategy, Signal
from libs.signals.vix import VixTermStructure
from libs.backtest.metrics import compute_metrics


@dataclass
class BacktestResult:
    signals: list[Signal]
    trades: list[dict]         # {date, symbol, side, qty, entry_price, exit_price, pnl}
    equity_curve: list[float]
    metrics: dict


def _build_vix_term(vix_row) -> VixTermStructure | None:
    """Build VixTermStructure from a vix_bars row (pandas Series)."""
    try:
        return VixTermStructure(
            vix_index=float(vix_row["vix_index"]),
            m1=float(vix_row["m1"]),
            m2=float(vix_row["m2"]),
            m3=float(vix_row["m3"]),
            days_to_m1_expiry=21.0,  # constant approximation
        )
    except Exception:
        return None


def run_backtest(
    strategy: BaseStrategy,
    bars: pd.DataFrame,
    vix_bars: pd.DataFrame,
    initial_capital: float = 100_000.0,
) -> BacktestResult:
    """
    bars     : columns [ts_date, open, high, low, close, volume]; sorted ascending.
    vix_bars : columns [ts_date, vix_index, m1, m2, m3]; sorted ascending; may be empty.
    """
    if len(bars) < 2:
        return BacktestResult(signals=[], trades=[], equity_curve=[],
                              metrics=compute_metrics([], []))

    # Index vix_bars by ts_date for O(1) lookup
    vix_index: dict = {}
    if not vix_bars.empty and "ts_date" in vix_bars.columns:
        for _, row in vix_bars.iterrows():
            vix_index[str(row["ts_date"])[:10]] = row

    cash = initial_capital
    position_qty = 0.0
    position_cost = 0.0  # average cost basis
    signals: list[Signal] = []
    trades: list[dict] = []
    equity_curve: list[float] = [initial_capital] * len(bars)

    for i in range(len(bars) - 1):
        bar = bars.iloc[i]
        date_key = str(bar["ts_date"])[:10]

        # Build VIX context
        vix_row = vix_index.get(date_key)
        vix_term = _build_vix_term(vix_row) if vix_row is not None else None

        signal = strategy.generate_signal(bars.iloc[:i + 1], {"vix_term": vix_term})
        if signal is not None:
            signals.append(signal)

        # Execute at next bar open
        next_open = float(bars.iloc[i + 1]["open"])
        next_date = str(bars.iloc[i + 1]["ts_date"])[:10]

        if signal is not None:
            if signal.side == "buy" and position_qty == 0.0:
                # Open long
                qty = signal.quantity
                cost = qty * next_open
                if cost <= cash:
                    cash -= cost
                    position_qty = qty
                    position_cost = next_open

            elif signal.side in ("sell", "flat") and position_qty > 0.0:
                # Close long
                proceeds = position_qty * next_open
                pnl = (next_open - position_cost) * position_qty
                trades.append({
                    "date": next_date,
                    "symbol": signal.symbol,
                    "side": "sell",
                    "qty": position_qty,
                    "entry_price": position_cost,
                    "exit_price": next_open,
                    "pnl": pnl,
                })
                cash += proceeds
                position_qty = 0.0
                position_cost = 0.0

        # Mark equity at bar i close
        equity_curve[i] = cash + position_qty * float(bar["close"])

    # Final bar equity
    equity_curve[-1] = cash + position_qty * float(bars.iloc[-1]["close"])

    return BacktestResult(
        signals=signals,
        trades=trades,
        equity_curve=equity_curve,
        metrics=compute_metrics(trades, equity_curve),
    )
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/libs/backtest/ -v
```

Expected: all passed

- [ ] **Step 5: Commit**

```bash
git add libs/backtest/engine.py tests/libs/backtest/test_engine.py
git commit -m "feat(backtest): add run_backtest engine"
```

---

## Task 6: Signal Writer (`services/broker_service/signal_writer.py`)

**Files:**
- Create: `services/broker_service/signal_writer.py`
- Create: `tests/broker_service/test_signal_writer.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/broker_service/test_signal_writer.py
from unittest.mock import MagicMock, call
from datetime import datetime
from libs.strategies.base import Signal
from services.broker_service.signal_writer import write_signal, mark_executed


def _make_signal():
    return Signal(
        strategy_id="vix_regime",
        symbol="SPY",
        side="buy",
        quantity=10.0,
        confidence=0.75,
        reason="contango 7.0%",
        generated_at=datetime(2026, 6, 24, 9, 35, 0),
    )


def _make_conn(lastrowid=42):
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    cur.fetchone = MagicMock(return_value=(lastrowid,))
    conn = MagicMock()
    conn.cursor = MagicMock(return_value=cur)
    return conn, cur


def test_write_signal_executes_insert():
    conn, cur = _make_conn(42)
    result = write_signal(conn, _make_signal())
    assert result == 42
    conn.commit.assert_called_once()
    args = cur.execute.call_args[0]
    assert "INSERT INTO trading.signals" in args[0]
    params = args[1]
    assert params[0] == "vix_regime"
    assert params[1] == "SPY"
    assert params[2] == "buy"
    assert params[3] == 10.0
    assert params[4] == 0.75


def test_write_signal_returns_id():
    conn, cur = _make_conn(99)
    assert write_signal(conn, _make_signal()) == 99


def test_mark_executed_updates_row():
    conn, cur = _make_conn()
    mark_executed(conn, 42, "IB-12345")
    conn.commit.assert_called_once()
    args = cur.execute.call_args[0]
    assert "UPDATE trading.signals" in args[0]
    assert "executed" in args[0].lower() or "EXECUTED" in args[0]
    params = args[1]
    assert "IB-12345" in params
    assert 42 in params
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/broker_service/test_signal_writer.py -v
```

Expected: `ModuleNotFoundError: No module named 'services.broker_service.signal_writer'`

- [ ] **Step 3: Implement signal_writer**

```python
# services/broker_service/signal_writer.py
from __future__ import annotations
from libs.strategies.base import Signal


def write_signal(conn, signal: Signal) -> int:
    """Insert a signal into trading.signals. Returns the new row id."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO trading.signals
                (strategy_id, symbol, side, quantity, confidence, reason, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                signal.strategy_id,
                signal.symbol,
                signal.side,
                signal.quantity,
                signal.confidence,
                signal.reason,
                signal.generated_at,
            ),
        )
        row = cur.fetchone()
    conn.commit()
    return row[0]


def mark_executed(conn, signal_id: int, order_id: str) -> None:
    """Set executed=TRUE and record the order_id for a signal."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE trading.signals
            SET executed = TRUE, order_id = %s
            WHERE id = %s
            """,
            (order_id, signal_id),
        )
    conn.commit()
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/broker_service/test_signal_writer.py -v
```

Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add services/broker_service/signal_writer.py tests/broker_service/test_signal_writer.py
git commit -m "feat(broker_service): add signal_writer (write_signal, mark_executed)"
```

---

## Task 7: Algo Scheduler Replacement

**Files:**
- Replace: `services/broker_service/algo_scheduler.py`
- Replace: `tests/broker_service/test_algo_scheduler.py`

**Context:**
- `BackgroundScheduler` is used (not `BlockingScheduler`) because this runs inside FastAPI's event loop.
- Each job run opens its own DB connection via `psycopg2.connect(os.environ["PGURL"])` and closes it in `finally` — matches `trade_monitor.py` pattern.
- `OrderRequest` requires `side` as `"BUY"` or `"SELL"` (uppercase) per `services/broker/base.py`.
- The old `start_strategy`, `stop_strategy`, `get_status` functions are removed; `main.py` is updated in Task 9.

- [ ] **Step 1: Write the failing tests**

```python
# tests/broker_service/test_algo_scheduler.py
import os
from unittest.mock import MagicMock, patch, call
import pytest
from libs.strategies.base import Signal
from services.broker_service.algo_scheduler import build_scheduler, _run_all_strategies


def _make_cursor(rows=None):
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    cur.fetchall = MagicMock(return_value=rows or [])
    return cur


def _config_row(auto_execute=False):
    return ("vix_regime", "SPY", auto_execute,
            {"base_qty": 10, "threshold_contango": 5.0, "threshold_backwardation": -2.0})


def test_build_scheduler_returns_scheduler():
    broker = MagicMock()
    with patch("services.broker_service.algo_scheduler.psycopg2"):
        scheduler = build_scheduler(broker)
    assert scheduler is not None
    assert hasattr(scheduler, "add_job")


def test_run_all_strategies_no_enabled_rows():
    broker = MagicMock()
    cur = _make_cursor(rows=[])
    conn = MagicMock()
    conn.cursor.return_value = cur
    with patch("services.broker_service.algo_scheduler.psycopg2.connect", return_value=conn):
        _run_all_strategies(broker)
    broker.submit_order.assert_not_called()


def test_run_all_strategies_auto_execute_false_no_order():
    broker = MagicMock()
    cur = _make_cursor(rows=[_config_row(auto_execute=False)])
    conn = MagicMock()
    conn.cursor.return_value = cur

    buy_signal = Signal(strategy_id="vix_regime", symbol="SPY", side="buy",
                        quantity=10.0, confidence=0.8, reason="test")

    with patch("services.broker_service.algo_scheduler.psycopg2.connect", return_value=conn), \
         patch("services.broker_service.algo_scheduler._fetch_bars",
               return_value=MagicMock()), \
         patch("services.broker_service.algo_scheduler._fetch_vix_term",
               return_value=None), \
         patch("services.broker_service.algo_scheduler.VixRegimeStrategy.generate_signal",
               return_value=buy_signal), \
         patch("services.broker_service.algo_scheduler.write_signal", return_value=1):
        _run_all_strategies(broker)

    broker.submit_order.assert_not_called()


def test_run_all_strategies_auto_execute_true_places_order():
    broker = MagicMock()
    broker.get_account_summary.return_value = MagicMock(nav=100_000.0)
    broker.get_positions.return_value = []
    broker.submit_order.return_value = MagicMock(order_id="IB-999")

    cur = _make_cursor(rows=[_config_row(auto_execute=True)])
    conn = MagicMock()
    conn.cursor.return_value = cur

    buy_signal = Signal(strategy_id="vix_regime", symbol="SPY", side="buy",
                        quantity=10.0, confidence=0.8, reason="test")

    with patch("services.broker_service.algo_scheduler.psycopg2.connect", return_value=conn), \
         patch("services.broker_service.algo_scheduler._fetch_bars",
               return_value=MagicMock()), \
         patch("services.broker_service.algo_scheduler._fetch_vix_term",
               return_value=None), \
         patch("services.broker_service.algo_scheduler.VixRegimeStrategy.generate_signal",
               return_value=buy_signal), \
         patch("services.broker_service.algo_scheduler.write_signal", return_value=1), \
         patch("services.broker_service.algo_scheduler.mark_executed") as mock_mark:
        _run_all_strategies(broker)

    broker.submit_order.assert_called_once()
    mock_mark.assert_called_once_with(conn, 1, "IB-999")


def test_run_all_strategies_flat_signal_no_order():
    broker = MagicMock()
    cur = _make_cursor(rows=[_config_row(auto_execute=True)])
    conn = MagicMock()
    conn.cursor.return_value = cur

    flat_signal = Signal(strategy_id="vix_regime", symbol="SPY", side="flat",
                         quantity=0.0, confidence=0.0, reason="neutral")

    with patch("services.broker_service.algo_scheduler.psycopg2.connect", return_value=conn), \
         patch("services.broker_service.algo_scheduler._fetch_bars",
               return_value=MagicMock()), \
         patch("services.broker_service.algo_scheduler._fetch_vix_term",
               return_value=None), \
         patch("services.broker_service.algo_scheduler.VixRegimeStrategy.generate_signal",
               return_value=flat_signal), \
         patch("services.broker_service.algo_scheduler.write_signal", return_value=1):
        _run_all_strategies(broker)

    broker.submit_order.assert_not_called()


def test_strategy_exception_does_not_propagate():
    broker = MagicMock()
    cur = _make_cursor(rows=[_config_row()])
    conn = MagicMock()
    conn.cursor.return_value = cur

    with patch("services.broker_service.algo_scheduler.psycopg2.connect", return_value=conn), \
         patch("services.broker_service.algo_scheduler._fetch_bars",
               side_effect=RuntimeError("DB exploded")):
        # Should not raise
        _run_all_strategies(broker)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/broker_service/test_algo_scheduler.py -v
```

Expected: failures (old skeleton has different interface)

- [ ] **Step 3: Replace algo_scheduler.py**

```python
# services/broker_service/algo_scheduler.py
"""
services/broker_service/algo_scheduler.py

Strategy execution loop. Replaced in Phase 7.
Runs all enabled strategies on a Mon-Fri 09:35 ET cron trigger.
Writes signals to trading.signals; submits orders if auto_execute=TRUE.
"""
from __future__ import annotations

import logging
import os

import psycopg2
import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from libs.strategies.base import BaseStrategy, Signal
from libs.strategies.vix_regime import VixRegimeStrategy
from libs.signals.vix import VixTermStructure
from services.broker_service.signal_writer import write_signal, mark_executed
from services.broker_service.order_router import OrderRouter, RiskConfig
from services.broker.base import OrderRequest

logger = logging.getLogger(__name__)

STRATEGY_REGISTRY: dict[str, type[BaseStrategy]] = {
    "vix_regime": VixRegimeStrategy,
}

_order_router = OrderRouter(config=RiskConfig())


def _fetch_bars(conn, symbol: str) -> pd.DataFrame:
    """Fetch bars_1d from DB; fallback to yfinance if < 30 rows."""
    with conn.cursor() as cur:
        cur.execute(
            """SELECT ts_date, open, high, low, close, volume
               FROM trading.bars_1d WHERE symbol = %s ORDER BY ts_date ASC""",
            (symbol,),
        )
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=cols)
    if len(df) < 30:
        try:
            import yfinance as yf
            raw = yf.download(symbol, period="1y", auto_adjust=True, progress=False)
            if not raw.empty:
                raw = raw.reset_index()
                raw.columns = [c.lower() for c in raw.columns]
                raw = raw.rename(columns={"date": "ts_date"})
                df = raw[["ts_date", "open", "high", "low", "close", "volume"]]
        except Exception:
            logger.exception("yfinance fallback failed for %s", symbol)
    return df


def _fetch_vix_term(conn) -> VixTermStructure | None:
    """Return the most recent VixTermStructure from DB, or None."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT vix_index, m1, m2, m3
                   FROM trading.vix_term_structure
                   ORDER BY ts_date DESC LIMIT 1"""
            )
            row = cur.fetchone()
        if row is None:
            return None
        return VixTermStructure(
            vix_index=float(row[0]), m1=float(row[1]),
            m2=float(row[2]), m3=float(row[3]),
            days_to_m1_expiry=21.0,
        )
    except Exception:
        logger.exception("_fetch_vix_term failed")
        return None


def _run_all_strategies(broker) -> None:
    """Job function: run all enabled strategies and optionally submit orders."""
    conn = None
    try:
        conn = psycopg2.connect(os.environ["PGURL"])
        with conn.cursor() as cur:
            cur.execute(
                """SELECT strategy_id, symbol, auto_execute, params
                   FROM trading.strategy_config WHERE enabled = TRUE"""
            )
            rows = cur.fetchall()

        for strategy_id, symbol, auto_execute, params in rows:
            try:
                strategy_cls = STRATEGY_REGISTRY.get(strategy_id)
                if strategy_cls is None:
                    logger.warning("Unknown strategy_id: %s", strategy_id)
                    continue

                config = {**(params or {}), "symbol": symbol}
                strategy = strategy_cls(config)

                bars = _fetch_bars(conn, symbol)
                vix_term = _fetch_vix_term(conn)

                signal = strategy.generate_signal(bars, {"vix_term": vix_term})
                if signal is None:
                    continue

                signal_id = write_signal(conn, signal)

                if auto_execute and signal.side != "flat":
                    req = OrderRequest(
                        symbol=signal.symbol,
                        side=signal.side.upper(),
                        quantity=signal.quantity,
                        order_type="MKT",
                        strategy_id=strategy_id,
                    )
                    summary = broker.get_account_summary()
                    positions = broker.get_positions()
                    result = _order_router.check(
                        req, positions=positions, summary=summary, daily_pnl=0.0
                    )
                    if result.approved:
                        order = broker.submit_order(req)
                        mark_executed(conn, signal_id, order.order_id)
                    else:
                        logger.warning("[algo_scheduler] order rejected: %s", result.message)

            except Exception:
                logger.exception("[algo_scheduler] strategy %s failed", strategy_id)

    finally:
        if conn:
            conn.close()


def build_scheduler(broker) -> BackgroundScheduler:
    """
    Build and return (but do not start) a BackgroundScheduler.
    Call scheduler.start() in the FastAPI lifespan.
    """
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        _run_all_strategies,
        trigger=CronTrigger(
            day_of_week="mon-fri", hour=9, minute=35,
            timezone="America/New_York",
        ),
        id="run_all_strategies",
        args=[broker],
        replace_existing=True,
    )
    return scheduler
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/broker_service/test_algo_scheduler.py -v
```

Expected: all passed

- [ ] **Step 5: Commit**

```bash
git add services/broker_service/algo_scheduler.py tests/broker_service/test_algo_scheduler.py
git commit -m "feat(broker_service): replace algo_scheduler skeleton with real strategy execution loop"
```

---

## Task 8: DB Query Helpers (`apps/shared/db.py`)

**Files:**
- Modify: `apps/shared/db.py` (append three functions at the bottom)
- Modify: `tests/apps/shared/test_db.py` (append tests)

- [ ] **Step 1: Write the failing tests**

Open `tests/apps/shared/test_db.py` and append:

```python
# append to tests/apps/shared/test_db.py

from apps.shared.db import get_signals, get_strategy_configs, upsert_strategy_config
import json


def _make_cursor_with_rows(rows, cols):
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    cur.description = [(c,) for c in cols]
    cur.fetchall = MagicMock(return_value=rows)
    return cur


def test_get_signals_calls_correct_query():
    cols = ["id", "strategy_id", "symbol", "side", "quantity",
            "confidence", "reason", "executed", "order_id", "created_at"]
    cur = _make_cursor_with_rows([], cols)
    conn = MagicMock()
    conn.cursor.return_value = cur
    df = get_signals(conn, "vix_regime", limit=10)
    assert df.empty or "strategy_id" in df.columns
    sql = cur.execute.call_args[0][0]
    assert "trading.signals" in sql
    params = cur.execute.call_args[0][1]
    assert "vix_regime" in params
    assert 10 in params


def test_get_strategy_configs_returns_df():
    cols = ["strategy_id", "symbol", "auto_execute", "params", "enabled", "updated_at"]
    cur = _make_cursor_with_rows(
        [("vix_regime", "SPY", False, {}, True, None)], cols
    )
    conn = MagicMock()
    conn.cursor.return_value = cur
    df = get_strategy_configs(conn)
    assert "strategy_id" in df.columns
    assert df.iloc[0]["strategy_id"] == "vix_regime"


def test_upsert_strategy_config_calls_insert():
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    conn = MagicMock()
    conn.cursor.return_value = cur
    upsert_strategy_config(conn, "vix_regime", "SPY", False, True, {"base_qty": 5})
    sql = cur.execute.call_args[0][0]
    assert "INSERT" in sql or "insert" in sql.lower()
    assert "ON CONFLICT" in sql or "on conflict" in sql.lower()
    conn.commit.assert_called_once()
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/apps/shared/test_db.py -v -k "signals or configs or upsert"
```

Expected: `ImportError` (functions not yet defined)

- [ ] **Step 3: Append functions to apps/shared/db.py**

Append to the bottom of `apps/shared/db.py`:

```python
def get_signals(conn, strategy_id: str, limit: int = 50) -> pd.DataFrame:
    return _fetch(
        conn,
        """SELECT id, strategy_id, symbol, side, quantity, confidence,
                  reason, executed, order_id, created_at
           FROM trading.signals
           WHERE strategy_id = %s
           ORDER BY created_at DESC LIMIT %s""",
        (strategy_id, limit),
    )


def get_strategy_configs(conn) -> pd.DataFrame:
    return _fetch(
        conn,
        "SELECT strategy_id, symbol, auto_execute, params, enabled, updated_at "
        "FROM trading.strategy_config ORDER BY strategy_id",
    )


def upsert_strategy_config(
    conn,
    strategy_id: str,
    symbol: str,
    auto_execute: bool,
    enabled: bool,
    params: dict,
) -> None:
    import json as _json
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO trading.strategy_config
                (strategy_id, symbol, auto_execute, enabled, params, updated_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            ON CONFLICT (strategy_id) DO UPDATE SET
                symbol       = EXCLUDED.symbol,
                auto_execute = EXCLUDED.auto_execute,
                enabled      = EXCLUDED.enabled,
                params       = EXCLUDED.params,
                updated_at   = NOW()
            """,
            (strategy_id, symbol, auto_execute, enabled, _json.dumps(params)),
        )
    conn.commit()
```

- [ ] **Step 4: Run all db tests to verify they pass**

```bash
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/apps/shared/test_db.py -v
```

Expected: all passed

- [ ] **Step 5: Commit**

```bash
git add apps/shared/db.py tests/apps/shared/test_db.py
git commit -m "feat(db): add get_signals, get_strategy_configs, upsert_strategy_config"
```

---

## Task 9: Update `services/broker_service/main.py`

**Files:**
- Modify: `services/broker_service/main.py`

**Context:**
- The lifespan must start `BackgroundScheduler` from `build_scheduler(_broker)`.
- `/algo/start/{strategy_id}` and `/algo/stop/{strategy_id}` now enable/disable in DB instead of calling the old skeleton.
- `/algo/status` returns strategy configs + last signal per strategy from DB.
- `main.py` creates its own DB connection via `psycopg2.connect(os.environ["PGURL"])`.

- [ ] **Step 1: Read current main.py**

Read `services/broker_service/main.py` lines 1–30 to see the imports and globals. (Already read above — proceed to step 2.)

- [ ] **Step 2: Apply the changes**

Replace the entire file with the following:

```python
# services/broker_service/main.py
"""
FastAPI broker service. Runs on personal laptop alongside TWS.
Exposes broker operations as REST endpoints so analytics apps on any
machine can read positions and submit orders without a direct IB connection.
"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Optional

import psycopg2
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", "config", ".env"))

from services.broker.broker_factory import get_broker
from services.broker.base import BaseBroker, OrderRequest
from services.broker_service.order_router import OrderRouter, RiskConfig

_broker: Optional[BaseBroker] = None
_router = OrderRouter(config=RiskConfig())
_scheduler = None


def _get_broker() -> BaseBroker:
    global _broker
    if _broker is None:
        _broker = get_broker()
        _broker.connect()
    return _broker


class OrderRequestBody(BaseModel):
    symbol: str
    side: str
    quantity: float
    order_type: str = "MKT"
    limit_price: Optional[float] = None
    strategy_id: Optional[str] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _scheduler
    _get_broker()
    from services.broker_service.algo_scheduler import build_scheduler
    _scheduler = build_scheduler(_broker)
    _scheduler.start()
    yield
    if _broker:
        _broker.disconnect()
    if _scheduler and _scheduler.running:
        _scheduler.shutdown()


app = FastAPI(title="IB Platform — Broker Service", lifespan=lifespan)


@app.get("/status")
def get_status():
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
    broker = _get_broker()
    positions = broker.get_positions()
    return [
        {
            "symbol": p.symbol, "asset_class": p.asset_class,
            "quantity": p.quantity, "avg_cost": p.avg_cost,
            "unrealised_pnl": p.unrealised_pnl, "currency": p.currency,
            "expiry": p.expiry, "strike": p.strike,
            "right": p.right, "account_id": p.account_id,
        }
        for p in positions
    ]


@app.post("/orders")
def submit_order(body: OrderRequestBody):
    broker = _get_broker()
    req = OrderRequest(
        symbol=body.symbol, side=body.side, quantity=body.quantity,
        order_type=body.order_type, limit_price=body.limit_price,
        strategy_id=body.strategy_id,
    )
    summary = broker.get_account_summary()
    positions = broker.get_positions()
    risk_result = _router.check(req, positions=positions, summary=summary, daily_pnl=0.0)
    if not risk_result.approved:
        raise HTTPException(status_code=422, detail=risk_result.message)
    result = broker.submit_order(req)
    return {"order_id": result.order_id, "status": result.status, "message": result.message}


@app.delete("/orders/{order_id}")
def cancel_order(order_id: str):
    broker = _get_broker()
    cancelled = broker.cancel_order(order_id)
    if not cancelled:
        raise HTTPException(status_code=404, detail=f"Order {order_id!r} not found or not cancellable")
    return {"order_id": order_id, "status": "CANCELLED"}


@app.get("/orders")
def get_orders():
    broker = _get_broker()
    orders = broker.get_orders()
    return [
        {
            "order_id": o.order_id, "symbol": o.symbol, "side": o.side,
            "quantity": o.quantity, "order_type": o.order_type,
            "status": o.status, "fill_price": o.fill_price,
            "strategy_id": o.strategy_id,
        }
        for o in orders
    ]


def _get_db_conn():
    return psycopg2.connect(os.environ["PGURL"])


@app.post("/algo/start/{strategy_id}")
def start_algo(strategy_id: str):
    """Enable a strategy (sets enabled=TRUE in strategy_config)."""
    conn = _get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE trading.strategy_config SET enabled=TRUE, updated_at=NOW() WHERE strategy_id=%s",
                (strategy_id,),
            )
        conn.commit()
    finally:
        conn.close()
    return {"strategy_id": strategy_id, "status": "enabled"}


@app.post("/algo/stop/{strategy_id}")
def stop_algo(strategy_id: str):
    """Disable a strategy (sets enabled=FALSE in strategy_config)."""
    conn = _get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE trading.strategy_config SET enabled=FALSE, updated_at=NOW() WHERE strategy_id=%s",
                (strategy_id,),
            )
        conn.commit()
    finally:
        conn.close()
    return {"strategy_id": strategy_id, "status": "disabled"}


@app.get("/algo/status")
def algo_status():
    """Return strategy configs and last signal per strategy."""
    conn = _get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT strategy_id, symbol, auto_execute, enabled FROM trading.strategy_config"
            )
            configs = [
                {"strategy_id": r[0], "symbol": r[1], "auto_execute": r[2], "enabled": r[3]}
                for r in cur.fetchall()
            ]
            for cfg in configs:
                cur.execute(
                    """SELECT side, confidence, reason, created_at
                       FROM trading.signals WHERE strategy_id=%s
                       ORDER BY created_at DESC LIMIT 1""",
                    (cfg["strategy_id"],),
                )
                row = cur.fetchone()
                cfg["last_signal"] = (
                    {"side": row[0], "confidence": row[1], "reason": row[2], "at": str(row[3])}
                    if row else None
                )
    finally:
        conn.close()
    return configs
```

- [ ] **Step 3: Run the existing broker service tests**

```bash
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/broker_service/test_main.py -v
```

Expected: all passed (the endpoint structure is the same; only `/algo/*` internals changed)

- [ ] **Step 4: Commit**

```bash
git add services/broker_service/main.py
git commit -m "feat(broker_service): wire algo_scheduler into FastAPI lifespan and update /algo/* endpoints"
```

---

## Task 10: Strategies Dashboard Tab

**Files:**
- Create: `apps/portfolio/tabs/strategies.py`
- Create: `tests/apps/portfolio/test_strategies_tab.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/apps/portfolio/test_strategies_tab.py
from unittest.mock import MagicMock, patch
import pandas as pd


def _mock_conn():
    return MagicMock()


def test_render_does_not_raise():
    conn = _mock_conn()

    empty_configs = pd.DataFrame(columns=[
        "strategy_id", "symbol", "auto_execute", "params", "enabled", "updated_at"
    ])
    empty_signals = pd.DataFrame(columns=[
        "id", "strategy_id", "symbol", "side", "quantity",
        "confidence", "reason", "executed", "order_id", "created_at"
    ])
    empty_bars = pd.DataFrame(columns=["ts_date", "open", "high", "low", "close", "volume"])
    empty_vix = pd.DataFrame(columns=["ts_date", "vix_index", "m1", "m2", "m3"])

    with patch("apps.portfolio.tabs.strategies.get_strategy_configs", return_value=empty_configs), \
         patch("apps.portfolio.tabs.strategies.get_signals", return_value=empty_signals), \
         patch("apps.portfolio.tabs.strategies.get_bars_1d", return_value=empty_bars), \
         patch("apps.portfolio.tabs.strategies.get_vix_term_structure", return_value=empty_vix), \
         patch("apps.portfolio.tabs.strategies.st") as mock_st:
        mock_st.tabs.return_value = [MagicMock(), MagicMock(), MagicMock()]
        mock_st.selectbox.return_value = "vix_regime"
        mock_st.button.return_value = False
        mock_st.date_input.return_value = None
        mock_st.number_input.return_value = 100_000.0

        from apps.portfolio.tabs.strategies import render
        render(conn)
```

- [ ] **Step 2: Run test to verify it fails**

```bash
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/apps/portfolio/test_strategies_tab.py -v
```

Expected: `ModuleNotFoundError: No module named 'apps.portfolio.tabs.strategies'`

- [ ] **Step 3: Implement the strategies tab**

```python
# apps/portfolio/tabs/strategies.py
from __future__ import annotations
import json
from datetime import date, timedelta

from apps.shared.db import (
    get_signals, get_strategy_configs, upsert_strategy_config,
    get_bars_1d, get_vix_term_structure,
)


def render(conn) -> None:
    import streamlit as st
    import pandas as pd
    import plotly.graph_objects as go

    st.subheader("Strategy Execution")
    tab_config, tab_signals, tab_backtest = st.tabs(
        ["Strategy Config", "Recent Signals", "Backtest"]
    )

    # ── Config panel ──────────────────────────────────────────────────────────
    with tab_config:
        configs_df = get_strategy_configs(conn)
        if configs_df.empty:
            st.info("No strategies configured. Run the DB migration and seed row.")
        else:
            for _, row in configs_df.iterrows():
                sid = row["strategy_id"]
                with st.expander(sid, expanded=True):
                    col1, col2, col3 = st.columns(3)
                    symbol = col1.text_input("Symbol", value=str(row["symbol"]), key=f"sym_{sid}")
                    auto_ex = col2.checkbox("Auto Execute", value=bool(row["auto_execute"]), key=f"ae_{sid}")
                    enabled = col3.checkbox("Enabled", value=bool(row["enabled"]), key=f"en_{sid}")
                    params_str = st.text_area(
                        "Params (JSON)",
                        value=json.dumps(row["params"] if row["params"] else {}, indent=2),
                        key=f"params_{sid}",
                        height=120,
                    )
                    if st.button("Save", key=f"save_{sid}"):
                        try:
                            params_dict = json.loads(params_str)
                            upsert_strategy_config(conn, sid, symbol, auto_ex, enabled, params_dict)
                            st.success(f"Saved {sid}")
                            st.rerun()
                        except json.JSONDecodeError as e:
                            st.error(f"Invalid JSON: {e}")

    # ── Recent Signals panel ──────────────────────────────────────────────────
    with tab_signals:
        configs_df2 = get_strategy_configs(conn)
        strategy_ids = list(configs_df2["strategy_id"]) if not configs_df2.empty else ["vix_regime"]
        selected = st.selectbox("Strategy", strategy_ids, key="sig_strategy_select")
        signals_df = get_signals(conn, selected, limit=50)
        if signals_df.empty:
            st.info("No signals yet for this strategy.")
        else:
            def _color_side(val):
                if val == "buy":
                    return "color: green"
                if val == "sell":
                    return "color: red"
                return "color: grey"
            styled = signals_df.style.applymap(_color_side, subset=["side"])
            st.dataframe(styled, use_container_width=True)

    # ── Backtest panel ────────────────────────────────────────────────────────
    with tab_backtest:
        configs_df3 = get_strategy_configs(conn)
        strategy_ids3 = list(configs_df3["strategy_id"]) if not configs_df3.empty else ["vix_regime"]
        bt_strategy = st.selectbox("Strategy", strategy_ids3, key="bt_strategy_select")
        col_a, col_b, col_c = st.columns(3)
        start_date = col_a.date_input("Start", value=date.today() - timedelta(days=365), key="bt_start")
        end_date = col_b.date_input("End", value=date.today(), key="bt_end")
        capital = col_c.number_input("Initial Capital ($)", value=100_000.0, step=10_000.0, key="bt_cap")

        if st.button("Run Backtest", key="bt_run"):
            from libs.strategies.vix_regime import VixRegimeStrategy
            from libs.backtest.engine import run_backtest
            import pandas as pd

            # Load strategy config
            row = None
            if not configs_df3.empty:
                matches = configs_df3[configs_df3["strategy_id"] == bt_strategy]
                if not matches.empty:
                    row = matches.iloc[0]
            config = dict(row["params"] if row is not None and row["params"] else {})
            if row is not None:
                config["symbol"] = row["symbol"]
            symbol = config.get("symbol", "SPY")

            # Fetch bars
            bars = get_bars_1d(conn, symbol)
            if bars.empty or len(bars) < 30:
                try:
                    import yfinance as yf
                    raw = yf.download(symbol, start=str(start_date), end=str(end_date),
                                      auto_adjust=True, progress=False)
                    if not raw.empty:
                        raw = raw.reset_index()
                        raw.columns = [c.lower() for c in raw.columns]
                        raw = raw.rename(columns={"date": "ts_date"})
                        bars = raw[["ts_date", "open", "high", "low", "close", "volume"]]
                except Exception as e:
                    st.error(f"Could not fetch bars: {e}")
                    return

            # Filter date range
            bars["ts_date"] = pd.to_datetime(bars["ts_date"])
            bars = bars[(bars["ts_date"].dt.date >= start_date) &
                        (bars["ts_date"].dt.date <= end_date)].reset_index(drop=True)

            # Fetch VIX bars
            vix_df = get_vix_term_structure(conn)
            if not vix_df.empty:
                vix_df["ts_date"] = pd.to_datetime(vix_df["ts_date"])
                vix_df = vix_df[(vix_df["ts_date"].dt.date >= start_date) &
                                (vix_df["ts_date"].dt.date <= end_date)].reset_index(drop=True)
                if not vix_df.empty and "m1" in vix_df.columns:
                    vix_df = vix_df[["ts_date", "vix_index", "m1", "m2", "m3"]]
                else:
                    vix_df = pd.DataFrame(columns=["ts_date", "vix_index", "m1", "m2", "m3"])
            else:
                vix_df = pd.DataFrame(columns=["ts_date", "vix_index", "m1", "m2", "m3"])

            if len(bars) < 2:
                st.warning("Not enough bars for backtest. Try a wider date range.")
                return

            strategy_map = {"vix_regime": VixRegimeStrategy}
            strategy_cls = strategy_map.get(bt_strategy, VixRegimeStrategy)
            strategy = strategy_cls(config)

            with st.spinner("Running backtest..."):
                result = run_backtest(strategy, bars, vix_df, initial_capital=float(capital))

            # Metrics
            m = result.metrics
            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("Total Return", f"{m['total_return_pct']:.1f}%")
            c2.metric("Sharpe", f"{m['sharpe']:.2f}")
            c3.metric("Max Drawdown", f"{m['max_drawdown_pct']:.1f}%")
            c4.metric("Win Rate", f"{m['win_rate']*100:.0f}%")
            c5.metric("Trades", str(m["num_trades"]))

            # Equity curve
            if result.equity_curve:
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=list(bars["ts_date"][:len(result.equity_curve)]),
                    y=result.equity_curve,
                    mode="lines",
                    name="Portfolio Value",
                    line=dict(color="royalblue"),
                ))
                fig.update_layout(
                    title=f"{bt_strategy} Equity Curve",
                    xaxis_title="Date", yaxis_title="Portfolio Value ($)",
                    height=350,
                )
                st.plotly_chart(fig, use_container_width=True)

            # Trades table
            if result.trades:
                trades_df = pd.DataFrame(result.trades)
                st.dataframe(trades_df, use_container_width=True)
            else:
                st.info("No trades executed in this period.")
```

- [ ] **Step 4: Run test to verify it passes**

```bash
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/apps/portfolio/test_strategies_tab.py -v
```

Expected: 1 passed

- [ ] **Step 5: Commit**

```bash
git add apps/portfolio/tabs/strategies.py tests/apps/portfolio/test_strategies_tab.py
git commit -m "feat(portfolio): add Strategies tab (config / signals / backtest)"
```

---

## Task 11: Wire Strategies Tab into Portfolio App

**Files:**
- Modify: `apps/portfolio/app.py`

- [ ] **Step 1: Edit app.py**

Current file at `apps/portfolio/app.py` (32 lines). Replace with:

```python
import os, sys
import streamlit as st
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from apps.shared.db import connect
from apps.portfolio.tabs import (
    positions, pnl, risk, options_book, fixed_income, performance, cashflow,
    advisor_pretrade, advisor_daily, strategies,
)

st.set_page_config(page_title="Portfolio", layout="wide", page_icon="📊")

@st.cache_resource
def _get_conn():
    return connect()

conn = _get_conn()
account_id = os.environ.get("ACCOUNT_ID", "paper_default")
st.title("Portfolio Dashboard")
tab_labels = [
    "Positions", "P&L", "Risk", "Options Book", "Fixed Income",
    "Performance", "Cash Flow", "Pre-Trade", "Daily Briefing", "Strategies",
]
tabs = st.tabs(tab_labels)
with tabs[0]: positions.render(conn, account_id)
with tabs[1]: pnl.render(conn)
with tabs[2]: risk.render(conn, account_id)
with tabs[3]: options_book.render(conn, account_id)
with tabs[4]: fixed_income.render(conn, account_id)
with tabs[5]: performance.render(conn)
with tabs[6]: cashflow.render(conn)
with tabs[7]: advisor_pretrade.render(conn)
with tabs[8]: advisor_daily.render(conn)
with tabs[9]: strategies.render(conn)
```

- [ ] **Step 2: Run full test suite**

```bash
cd C:/Users/dipeng.chen/OneDrive/ETRM/ib-platform
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/ -q
```

Expected: ~424 passed, 0 failed

- [ ] **Step 3: Commit**

```bash
git add apps/portfolio/app.py
git commit -m "feat(portfolio): wire Strategies as 10th tab (Phase 7 complete)"
```

---

## Post-Build Checklist

- [ ] Apply migration: `psql $PGURL -f db/migrations/004_signals.sql`
- [ ] All tests pass: `pytest tests/ -q`
- [ ] Start broker service and verify scheduler logs at 09:35 ET: `python -m services.broker_service.main`
- [ ] Open portfolio app and confirm Strategies tab loads without error
- [ ] Verify `auto_execute` defaults to `FALSE` in the DB — no live orders until you toggle it on
