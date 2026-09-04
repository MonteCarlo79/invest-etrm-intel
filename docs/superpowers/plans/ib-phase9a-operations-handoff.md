# IB Platform Phase 9A — Operations Handoff

> **For a new Claude session:** This document covers Phase 9A specifically (options chain ingestion + EOD P&L).
> Read the main handoff first: `ib-platform-handoff.md` (Mac) or `ib-platform-handoff-windows.md` (Windows).
> **Latest commit:** `9598ae7` — pushed 2026-06-29
> **Tests:** 502 passing

---

## What Phase 9A Added

| Component | File | Purpose |
|---|---|---|
| DB migration | `db/migrations/007_strategy_pnl.sql` | `trading.strategy_pnl` table |
| Chain fetcher | `services/options_chain/fetcher.py` | `fetch_chain(symbol) → DataFrame` |
| Chain ingest | `services/options_chain/ingest.py` | Standalone APScheduler service |
| P&L calculator | `services/broker_service/pnl_calculator.py` | `compute_daily_pnl(conn)` |
| Scheduler job | `services/broker_service/algo_scheduler.py` | `compute_pnl` job at 16:15 ET |
| Trade monitor | `services/broker_service/trade_monitor.py` | Reads real P&L from `strategy_pnl` |

---

## First-Time Setup (apply once)

```bash
export $(grep -v '^#' config/.env | xargs)
psql $PGURL -f db/migrations/007_strategy_pnl.sql
# Expected output: CREATE TABLE, CREATE INDEX
```

---

## Running All Phase 9A Services

Phase 9A adds one new standalone process. Run it alongside the existing services:

```bash
source .venv/bin/activate   # or on Windows: load env via PowerShell (see Windows handoff)
export $(grep -v '^#' config/.env | xargs)

# Existing services (unchanged)
python -m services.broker_service.main           # FastAPI + APScheduler (now has 5 jobs incl. compute_pnl)
python -m services.market_data.ingest            # EOD bars, intraday, VIX, FX
python -m services.broker_service.trade_monitor  # Mon-Fri 18:00 ET — now reads real P&L

# NEW: Options chain ingest service
python -m services.options_chain.ingest          # Mon-Fri 09:25 + 16:05 ET
```

On Windows (no venv, dipeng.chen account):
```bash
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m services.options_chain.ingest
```

---

## Scheduler Timeline (Mon–Fri ET)

| Time | Job | Service process |
|---|---|---|
| 09:00 | `paper_promotions` — auto-promote paper→shadow | broker_service |
| 09:25 | `ingest_chains_open` — fetch options chains pre-open | **options_chain** |
| 09:35 | `run_all_strategies` — strategies now find chain data | broker_service |
| 16:05 | `ingest_chains_close` — refresh chains with closing IVs | **options_chain** |
| 16:10 | `eod_bars` — bar data | market_data |
| 16:15 | `compute_pnl` — upsert realized + unrealized P&L | broker_service |
| 16:30 | `daily_report` — Feishu card | broker_service |
| 17:00 | `weekly_report` (Fridays) — Feishu table | broker_service |
| 18:00 | `extract_trade_insights` — trade monitor → KB | trade_monitor |

---

## How the Data Flows

```
yfinance
  └─ fetch_chain(symbol)
       └─ trading.options_chain        (09:25 + 16:05 snapshot)
            └─ _run_all_strategies     (09:35 — strategies read from here)
                 └─ trading.paper_fills (fills written by PaperBroker)
                      └─ compute_daily_pnl  (16:15)
                           └─ trading.strategy_pnl
                                └─ trade_monitor     (18:00 — reads real P&L)
                                     └─ trading.kb_insights  (KB learning loop)
```

---

## DB Tables Written by Phase 9A

### `trading.strategy_pnl`
One row per (strategy_id, date). Written at 16:15 ET by `compute_pnl` job.

```sql
SELECT strategy_id, date, realized_pnl, unrealized_pnl, total_pnl, sharpe_30d, max_dd_30d
FROM trading.strategy_pnl
ORDER BY date DESC, strategy_id;
```

### `trading.options_chain`
Snapshot rows per symbol, replaced each day. Written at 09:25 and 16:05 ET.

```sql
SELECT symbol, expiry, "right", strike, bid, ask, iv, delta, gamma, theta, vega, ts_snapshot
FROM trading.options_chain
WHERE ts_snapshot::date = CURRENT_DATE
ORDER BY symbol, expiry, strike;
```

---

## Verifying Phase 9A Is Working

### Check chain data is being fetched
```sql
SELECT symbol, COUNT(*) as rows, MAX(ts_snapshot) as latest
FROM trading.options_chain
WHERE ts_snapshot::date = CURRENT_DATE
GROUP BY symbol;
```
Expected: rows for all enabled strategy symbols (SPY, VIX, etc.)

### Check P&L is being computed
```sql
SELECT strategy_id, date, realized_pnl, unrealized_pnl, total_pnl
FROM trading.strategy_pnl
ORDER BY date DESC LIMIT 10;
```
Expected: rows appearing after 16:15 ET each trading day.

### Check options strategies are generating signals (not None)
```sql
SELECT strategy_id, symbol, side, strike, expiry, "right", ts_signal
FROM trading.signals
WHERE strategy_id IN ('vix_options', 'spx_overlay', 'short_strangle')
ORDER BY ts_signal DESC LIMIT 10;
```
Expected: non-null strike/expiry/right fields after chain data is available.

---

## Known Gaps (Phase 9B — not yet designed)

1. **Portfolio Greeks aggregation** — `trading.portfolio_risk` is still empty. No service aggregates delta/gamma/theta/vega from live positions.

2. **Alert service** — No push notifications on signal generation, fill confirmation, or risk limit breaches. Only Feishu daily/weekly reports exist.

3. **Options backtest** — `libs/backtest/engine.py` cannot simulate options fills. Would need historical chain data in `trading.options_chain` (currently only today's snapshot is stored).

To design Phase 9B: use `superpowers:brainstorming`.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Options strategies return `None` signals | Chain table empty for today | Check options_chain service is running; verify `ts_snapshot::date = CURRENT_DATE` |
| `strategy_pnl` has no rows | compute_pnl job not running | Check broker_service is running; check logs for `[pnl_calculator]` |
| `InternalError: current transaction is aborted` in logs | Old pattern (pre-9598ae7); should not occur after the rollback fix | Confirm running >= 9598ae7 |
| yfinance rate-limited | Too many fetch calls | Fetch returns empty; chains not updated; strategies fall back to None |
| `PGURL` not set | Env not loaded | Run `export $(grep -v '^#' config/.env | xargs)` |
