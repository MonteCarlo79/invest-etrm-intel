# IB Platform Phase 9A — Options Chain Ingestion + EOD P&L Attribution

**Date:** 2026-06-27
**Repo:** `git@github.com:MonteCarlo79/ib-platform.git`
**Prerequisite phases:** 1–8 complete (480 tests passing as of 2026-06-28)
**Status:** Approved design, ready for implementation

---

## 1. Motivation

After Phase 8, three DB tables remain permanently empty in production:

| Table | Why it's empty |
|---|---|
| `trading.options_chain` | No ingestion service exists |
| `trading.strategy_pnl` | No P&L calculator exists |
| `trading.portfolio_risk` | Deferred to Phase 9B |

Consequence: all three OptionsStrategy subclasses (`VixOptionsStrategy`, `SpxOverlayStrategy`, `ShortStrangleStrategy`) return `None` every cycle because `_fetch_options_chain` finds no rows. The strategies are structurally complete but data-starved.

Phase 9A closes the two most critical gaps:
- Populate `trading.options_chain` before the 09:35 strategy run.
- Compute realized + unrealized P&L after the market close, feed it into `trading.strategy_pnl`, and close the trade-monitor learning loop.

---

## 2. Architecture Overview

Two additions to the existing service graph:

```
services/
  options_chain/          ← NEW standalone service
    __init__.py
    fetcher.py            ← fetch_chain(symbol) → DataFrame
    ingest.py             ← APScheduler entry-point (two jobs: pre-open + close)

  broker_service/
    pnl_calculator.py     ← NEW: compute_daily_pnl(conn, date) → int rows upserted
    algo_scheduler.py     ← MODIFIED: add 2 new scheduler jobs; trade_monitor reads real P&L

db/migrations/
  007_strategy_pnl.sql    ← trading.strategy_pnl table + index
```

No new FastAPI service. Options chain ingest runs as its own APScheduler process (like `services/market_data/ingest.py`). P&L calculator is a library function called by a scheduler job inside the existing `broker_service`.

### Scheduling chain (all times ET, Mon–Fri)

| Time | Job | Service |
|---|---|---|
| 09:25 | `ingest_chains_open` — fetch chains for all active symbols | options_chain |
| 09:35 | `run_all_strategies` — strategies now find chain data | broker_service |
| 16:05 | `ingest_chains_close` — refresh chains with closing IVs | options_chain |
| 16:15 | `compute_pnl` — upsert today's realized + unrealized P&L | broker_service |
| 16:30 | `daily_report` — Feishu card (reads strategy_pnl) | broker_service |
| 18:00 | `trade_monitor` — reads real P&L from strategy_pnl | broker_service (separate process) |

---

## 3. Options Chain Ingestion

### 3.1 Data source

**Primary: yfinance** (`yf.Ticker(symbol).option_chain(expiry)`)
- Free, no API key required.
- Returns `calls` and `puts` DataFrames with columns: `strike`, `lastPrice`, `bid`, `ask`, `impliedVolatility`, `openInterest`, `volume`.
- Expiry dates available via `yf.Ticker(symbol).options` (list of strings `"YYYY-MM-DD"`).

**Stub: Polygon** — reserved for future upgrade. Not implemented in Phase 9A.

Symbols to fetch: read dynamically from `trading.strategy_config WHERE enabled = TRUE`. The same symbols the strategies act on are the ones we fetch chains for. No hardcoded symbol list.

### 3.2 `services/options_chain/fetcher.py`

```python
def fetch_chain(symbol: str) -> pd.DataFrame:
    """
    Returns a DataFrame with columns:
        symbol, expiry (YYYYMMDD str), right (C/P), strike (float),
        bid, ask, mid, iv, open_interest, volume, delta, gamma, theta, vega,
        ts_snapshot (datetime UTC)
    Fetches the 4 nearest expiries. Greeks computed via bs_greeks().
    Returns empty DataFrame on any error (caller handles gracefully).
    """
```

Expiry selection: take `ticker.options[:4]` (nearest 4 expiry dates). Convert `"YYYY-MM-DD"` → `"YYYYMMDD"` for consistency with Signal.expiry.

Greeks computation: for each row, call `libs.pricing.black_scholes.bs_greeks(S=spot, K=strike, T=tte, sigma=iv, r=risk_free, flag=right.lower())`. Use the last close as `spot`; derive `T` from days to expiry / 252. Use `r = 0.05` as a constant (same default used throughout the pricing libs).

On any per-row exception (e.g., IV=0 or T≤0): set Greeks to `None`, continue.

### 3.3 `services/options_chain/ingest.py`

```python
def _get_active_symbols(conn) -> list[str]:
    """SELECT DISTINCT symbol FROM trading.strategy_config WHERE enabled = TRUE"""

def ingest_all(conn) -> int:
    """
    For each active symbol:
      1. fetch_chain(symbol) → DataFrame
      2. DELETE FROM trading.options_chain WHERE symbol = %s AND ts_snapshot::date = CURRENT_DATE
      3. INSERT rows (copy_from or executemany)
    Returns total rows inserted.
    """
```

The DELETE + INSERT pattern (snapshot replacement) keeps the table lean — one snapshot per symbol per day. No accumulation of stale intraday snapshots.

### 3.4 `trading.options_chain` schema update

The table exists (created in Phase 7 migration). **No schema change required.** Existing columns match fetcher output.

### 3.5 `_fetch_options_chain` in `algo_scheduler.py`

Currently queries all rows for the symbol. Add `AND ts_snapshot::date = CURRENT_DATE` to avoid stale data from prior days:

```sql
SELECT strike, expiry, right FROM trading.options_chain
WHERE symbol = %s AND ts_snapshot::date = CURRENT_DATE
ORDER BY expiry, strike
```

---

## 4. EOD P&L Computation

### 4.1 `db/migrations/007_strategy_pnl.sql`

```sql
CREATE TABLE IF NOT EXISTS trading.strategy_pnl (
    id             SERIAL PRIMARY KEY,
    strategy_id    TEXT        NOT NULL,
    date           DATE        NOT NULL,
    realized_pnl   NUMERIC(18,4),
    unrealized_pnl NUMERIC(18,4),
    total_pnl      NUMERIC(18,4) GENERATED ALWAYS AS (realized_pnl + unrealized_pnl) STORED,
    sharpe_30d     NUMERIC(10,6),
    max_dd_30d     NUMERIC(10,6),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (strategy_id, date)
);
CREATE INDEX IF NOT EXISTS idx_strategy_pnl_sid_date
    ON trading.strategy_pnl (strategy_id, date DESC);
```

### 4.2 `services/broker_service/pnl_calculator.py`

```python
def compute_daily_pnl(conn, target_date: date | None = None) -> int:
    """
    Computes and upserts P&L for all strategies for target_date (default: today).
    Returns number of rows upserted.

    Realized P&L:
      Source: trading.paper_fills (paper/shadow strategies)
              trading.trades (live strategies)
      Method: for each (strategy_id, date), sum fill-level P&L:
              closing fills → qty × (fill_price - avg_cost of open position)
              opening fills → 0 realized (cost basis established)
      Note: avg_cost tracked as running weighted average across fills.

    Unrealized P&L:
      net_qty = sum(signed_qty for all fills to date)
      avg_cost = weighted average cost of net open position
      last_close = most recent close from trading.market_bars for the symbol
      unrealized = net_qty × (last_close - avg_cost)
      If net_qty == 0: unrealized = 0.

    Rolling metrics (last 30 calendar days of strategy_pnl rows):
      sharpe_30d = mean(total_pnl_series) / std(total_pnl_series) × sqrt(252)
      max_dd_30d = max drawdown of cumulative total_pnl over last 30 days

    Upsert: ON CONFLICT (strategy_id, date) DO UPDATE SET ...
    """
```

#### Signed quantity convention
- Buy/long fill: `signed_qty = +qty`
- Sell/short fill: `signed_qty = -qty`
- Paper fills: read `side` column (`"buy"` / `"sell"`)

#### Symbol resolution
`trading.paper_fills` has a `symbol` column directly — no join needed. `trading.trades` also has `symbol`.

### 4.3 Scheduler job

In `algo_scheduler.py`, add job:

```python
scheduler.add_job(
    _compute_pnl_job,
    CronTrigger(day_of_week="mon-fri", hour=16, minute=15, timezone="America/New_York"),
    id="compute_pnl",
    replace_existing=True,
)
```

`_compute_pnl_job` opens a DB connection, calls `compute_daily_pnl(conn)`, logs row count.

### 4.4 `trade_monitor.py` — close the learning loop

Currently `extract_from_trade_outcome` receives a hardcoded stub dict `{"realized_pnl": 0.0, "max_loss": 0.0}`.

Change: before calling `extract_from_trade_outcome`, query `trading.strategy_pnl` for today's row for each strategy. Pass real `realized_pnl` and `total_pnl`. If no row found (market was closed, calculator didn't run), fall back to zeros with a log warning — no error raised.

---

## 5. Error Handling

| Scenario | Behaviour |
|---|---|
| yfinance rate-limited or symbol not found | `fetch_chain` returns empty DataFrame; `ingest_all` logs warning, skips symbol; strategies return `None` (unchanged from current behaviour) |
| DB connection down during ingest | Exception propagates to APScheduler; job marked failed; next run retries |
| P&L calculator finds no fills for a strategy | Inserts a zero row (realized=0, unrealized=0) so rolling metrics have a data point |
| `last_close` missing for a symbol | unrealized_pnl = 0; log warning |
| trade_monitor finds no P&L row | Falls back to zeros; continues (no crash) |

---

## 6. Testing

New test files (all unit tests, no live network calls):

| File | Tests |
|---|---|
| `tests/services/options_chain/test_fetcher.py` | mock yfinance; valid DataFrame columns; empty on error; Greeks computed |
| `tests/services/options_chain/test_ingest.py` | _get_active_symbols; ingest_all calls DELETE+INSERT; returns row count |
| `tests/broker_service/test_pnl_calculator.py` | realized P&L correct for buy-then-sell; unrealized for open position; sharpe/maxdd from 30-day series; upsert conflict updates; zero row when no fills |
| `tests/broker_service/test_algo_scheduler.py` | build_scheduler returns 5 jobs (add compute_pnl to existing 4) |
| `tests/broker_service/test_trade_monitor.py` | reads real P&L from DB; falls back to zeros when no row |

Target: 480 → **~515 passing** (approximately 35 new tests).

---

## 7. Out of Scope (Phase 9B)

- `trading.portfolio_risk` aggregation (position-level Greeks across all live positions)
- Feishu/Telegram alert service (signal generated, fill confirmed, risk limit breached)
- Historical options chain storage for backtesting

---

## 8. Migration Checklist

```bash
# Apply new migration
psql $PGURL -f db/migrations/007_strategy_pnl.sql

# Start options chain service (new terminal)
python -m services.options_chain.ingest

# broker_service picks up new jobs automatically on restart
python -m services.broker_service.main
```

No changes to existing migrations. No data backfill required (P&L rows accumulate from first run forward).
