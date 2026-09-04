# IB Platform Phase 9B — Portfolio Greeks Aggregation

**Date:** 2026-06-29
**Status:** Approved for implementation

---

## Goal

Compute aggregate portfolio Greeks (delta, gamma, theta, vega) across all open positions
— both paper fills and live IB positions — and write a daily snapshot to
`trading.portfolio_risk` at 09:30 ET. The existing Portfolio Risk tab reads this table
and will display real data immediately once the job runs.

---

## Architecture

**Option A selected:** new job inside the existing `broker_service` APScheduler.

The Greeks job is structurally identical to `compute_pnl` — reads from trading tables,
calls a library function, upserts a risk row. It belongs in the same scheduler that owns
`compute_pnl`, `run_all_strategies`, and `paper_promotions`.

Two files change:
1. **New:** `services/broker_service/greeks_calculator.py` — `compute_portfolio_greeks(conn) -> int`
2. **Modified:** `services/broker_service/algo_scheduler.py` — add `_compute_greeks_job()` + `compute_greeks` job at 09:30 ET

---

## New File: `services/broker_service/greeks_calculator.py`

### Public API

```python
def compute_portfolio_greeks(conn) -> int:
    """
    Compute portfolio Greeks for paper positions, live positions, and combined.
    Upserts three rows to trading.portfolio_risk:
        account_id = 'paper', 'live', 'total'
    Returns number of rows upserted (always 3).
    """
```

### Paper Positions

Query `trading.paper_fills` for net quantity per `(symbol, right, strike, expiry)`:

```sql
SELECT symbol,
       "right",
       strike,
       expiry,
       SUM(CASE WHEN side = 'BUY' THEN quantity ELSE -quantity END) AS net_qty
FROM trading.paper_fills
GROUP BY symbol, "right", strike, expiry
HAVING SUM(CASE WHEN side = 'BUY' THEN quantity ELSE -quantity END) != 0
```

For each row:
- **Equity** (`right IS NULL`): `{"delta": net_qty, "gamma": 0.0, "theta": 0.0, "vega": 0.0}`
- **Option** (`right IS NOT NULL`): look up `(symbol, expiry, right, strike)` in
  `trading.options_chain` (today's snapshot) for IV. Call
  `bs_greeks(S, K, T, r, iv, right)` where:
  - `S` = current mid from `trading.options_chain` (bid+ask)/2 as proxy, or last close from `trading.bars_1d`
  - `K` = strike
  - `T` = days to expiry / 365.0
  - `r` = 0.05 (fixed risk-free rate)
  - `iv` = from options_chain row
  Scale each Greek by `net_qty`. If no chain row found, log a warning and use
  `{"delta": 0.0, "gamma": 0.0, "theta": 0.0, "vega": 0.0}` for that position.

### Live Positions

Query the most recent snapshot per symbol from `trading.positions`:

```sql
SELECT DISTINCT ON (symbol, "right", strike, expiry)
       symbol, asset_class, "right", strike, expiry, quantity, avg_cost
FROM trading.positions
WHERE quantity != 0
ORDER BY symbol, "right", strike, expiry, ts_snapshot DESC
```

For each row:
- **Equity** (`asset_class = 'equity'` or `right IS NULL`):
  `{"delta": float(quantity), "gamma": 0.0, "theta": 0.0, "vega": 0.0}`
- **Option** (`right IS NOT NULL`): same chain lookup + `bs_greeks` call as paper options,
  scaled by `quantity`. Fallback to delta=quantity on missing chain data.

### Aggregation and Upsert

```python
paper_greeks = aggregate_greeks(paper_positions)
live_greeks  = aggregate_greeks(live_positions)
total_greeks = PortfolioGreeks(
    total_delta  = paper_greeks.total_delta  + live_greeks.total_delta,
    total_gamma  = paper_greeks.total_gamma  + live_greeks.total_gamma,
    total_theta  = paper_greeks.total_theta  + live_greeks.total_theta,
    total_vega   = paper_greeks.total_vega   + live_greeks.total_vega,
)
```

Upsert three rows into `trading.portfolio_risk`:

```sql
INSERT INTO trading.portfolio_risk
    (ts, account_id, total_delta, total_gamma, total_theta, total_vega,
     dv01, fx_delta_usd, var_1d_95, nav)
VALUES (%s, %s, %s, %s, %s, %s, 0, 0, NULL, NULL)
ON CONFLICT (ts, account_id) DO UPDATE SET
    total_delta  = EXCLUDED.total_delta,
    total_gamma  = EXCLUDED.total_gamma,
    total_theta  = EXCLUDED.total_theta,
    total_vega   = EXCLUDED.total_vega
```

`ts` = `NOW()` (current timestamp, not date-truncated — PKs are (ts, account_id)).
`dv01` and `fx_delta_usd` default to 0. `var_1d_95` and `nav` left NULL (Phase 9C scope).

---

## Modified File: `services/broker_service/algo_scheduler.py`

Add a `_compute_greeks_job()` function (same pattern as `_compute_pnl_job`):

```python
def _compute_greeks_job() -> None:
    """Job: compute and upsert portfolio Greeks for paper + live + total."""
    conn = None
    try:
        conn = psycopg2.connect(os.environ["PGURL"])
        from services.broker_service.greeks_calculator import compute_portfolio_greeks
        n = compute_portfolio_greeks(conn)
        logger.info("[greeks_calculator] upserted %d rows", n)
    except Exception:
        logger.exception("[greeks_calculator] job failed")
    finally:
        if conn:
            conn.close()
```

Add job to `build_scheduler()`:

```python
# Portfolio Greeks — Mon-Fri 09:30 ET
scheduler.add_job(
    _compute_greeks_job,
    trigger=CronTrigger(
        day_of_week="mon-fri", hour=9, minute=30,
        timezone="America/New_York",
    ),
    id="compute_greeks",
    args=[],
    replace_existing=True,
)
```

Total scheduler jobs after this change: **6**
(`paper_promotions`, `compute_greeks`, `run_all_strategies`, `compute_pnl`,
`daily_report`, `weekly_report`).

---

## Scheduler Timeline Impact

| Time (ET) | Job | Change |
|---|---|---|
| 09:00 | `paper_promotions` | unchanged |
| **09:30** | **`compute_greeks`** | **NEW** |
| 09:35 | `run_all_strategies` | unchanged |
| 16:15 | `compute_pnl` | unchanged |
| 16:30 | `daily_report` | unchanged |
| 17:00 (Fri) | `weekly_report` | unchanged |

---

## What the Risk Tab Sees

`apps/shared/db.py:get_portfolio_risk(conn, account_id)` already queries
`trading.portfolio_risk WHERE account_id = %s ORDER BY ts DESC LIMIT 90`.

Once the job runs, the Risk tab will render:
- **account_id='paper'** — Greeks from paper fill positions only
- **account_id='live'** — Greeks from IB live positions only
- **account_id='total'** — combined

The tab's existing `account_id` filter (if any) will need to be set or defaulted to
`'total'`. If the tab shows all rows, no change needed.

---

## Testing: `tests/broker_service/test_greeks_calculator.py`

Mirror the mock-cursor-factory pattern from `test_pnl_calculator.py`. No real DB.

| Test | Setup | Expected |
|---|---|---|
| `test_equity_paper_position` | 1 BUY 100 SPY fill, right=NULL, no chain needed | paper delta=100, gamma/theta/vega=0 |
| `test_options_paper_position_with_chain` | 2 BUY calls, chain row present (iv=0.3) | paper greeks = bs_greeks(...) * 2 |
| `test_options_paper_position_no_chain` | 1 BUY call, no chain row for that strike | logs warning; delta=0 for that pos |
| `test_net_zero_position_excluded` | BUY 100 + SELL 100 SPY | 0 positions → paper delta=0 |
| `test_live_equity_position` | 1 live equity row qty=50 | live delta=50 |
| `test_live_options_position_with_chain` | 1 live call row, chain present | live greeks scaled by qty |
| `test_paper_and_live_combined` | 1 paper equity + 1 live equity | total delta = sum of both |
| `test_empty_db` | no fills, no positions | 3 upsert calls all-zeros, returns 3 |

---

## Not In Scope (Phase 9C)

- `var_1d_95` computation (requires returns history)
- `nav` computation (requires account value from broker)
- Intraday Greeks refresh (currently once daily at 09:30 ET)
- `dv01` / `fx_delta_usd` population (fixed-income and FX positions not yet in paper fills)
