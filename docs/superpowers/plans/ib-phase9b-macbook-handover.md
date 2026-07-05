# IB Platform — MacBook Handover (2026-06-29, after Phase 9B)

> **For MacBook Claude:** `git pull` first — 7 new commits since `cb63bb7`.
> **Latest master:** `7e2541c` — 521 tests passing.

---

## What the Windows machine built (Phase 9B)

Portfolio Greeks aggregation — **complete and pushed**.

| Commit | What |
|---|---|
| `d02689b` | `greeks_calculator.py` skeleton + `_get_paper_positions` |
| `9f5fcea` | `_enrich_option_greeks` (options_chain IV → bs_greeks) |
| `6447357` | Tightened tests + docstring fix |
| `79c5252` | `_get_live_positions` (from trading.positions) |
| `f50a0c1` | Removed unused asset_class column, tightened tests |
| `1859a20` | `compute_portfolio_greeks` — 3-row upsert (paper/live/total) |
| `7e2541c` | `compute_greeks` job at 09:30 ET in broker_service scheduler |

**New file:** `services/broker_service/greeks_calculator.py`
**Modified:** `services/broker_service/algo_scheduler.py` (now 6 jobs)
**New tests:** `tests/broker_service/test_greeks_calculator.py` (14 tests)

The Portfolio app Risk tab now reads real data from `trading.portfolio_risk` instead of showing "No risk data available".

---

## Spec and plan docs (in bess-platform)

- Spec: `docs/superpowers/specs/2026-06-29-ib-phase9b-design.md`
- Plan: `docs/superpowers/plans/2026-06-29-ib-phase9b.md` (in ib-platform repo)
- Ops handoff: `docs/superpowers/plans/ib-phase9b-operations-handoff.md`

---

## Complete APScheduler job list (broker_service, 6 jobs)

| Time (ET) | Job ID | What |
|---|---|---|
| 09:00 Mon-Fri | `paper_promotions` | Auto-promote paper→shadow |
| 09:30 Mon-Fri | `compute_greeks` | Portfolio Greeks → trading.portfolio_risk |
| 09:35 Mon-Fri | `run_all_strategies` | Execute enabled strategies |
| 16:15 Mon-Fri | `compute_pnl` | EOD P&L → trading.strategy_pnl |
| 16:30 Mon-Fri | `daily_report` | Feishu daily brief |
| 17:00 Fridays | `weekly_report` | Feishu weekly table |

---

## Pending deployment action items (still not done)

These were identified in your MacBook session (`ib-phase9a-deployment-gaps-2026-06-29.md`) and **remain unresolved**:

### 1. Run `pip install -r requirements.txt`
MacBook added `scipy>=1.13` to requirements.txt (`3e4b8cc`). Re-run on any machine that hasn't done so.

```bash
uv pip install -r requirements.txt   # on Mac with uv venv
# or
python -m pip install -r requirements.txt
```

### 2. Apply missing DB migrations to shared RDS

`trading.strategy_config` and `trading.paper_fills` are **missing** from the shared RDS. Without them, `compute_pnl`, options_chain ingest, and all `/algo/*` endpoints fail.

Also verify `trading.portfolio_risk` exists (needed by Phase 9B — defined in `db/schema.sql`).

```bash
export $(grep -v '^#' config/.env | xargs)
psql $PGURL -f db/migrations/004_signals.sql      # creates strategy_config + seeds strategies
psql $PGURL -f db/migrations/006_paper_fills.sql  # creates paper_fills

# Verify Phase 9B table:
psql $PGURL -c "\dt trading.portfolio_risk"
# If missing, apply it from schema.sql:
psql $PGURL -c "
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
);"
```

### 3. Upgrade yfinance

`yfinance==0.2.44` is broken against Yahoo (JSONDecodeError). Options chain ingest returns zero data.

```bash
pip install --upgrade yfinance
# Verify:
python -c "import yfinance as yf; print(yf.Ticker('SPY').options[:2])"
# Re-pin in requirements.txt once you confirm the working version
```

### 4. Paper account activation

Paper account (`DU…`) was created and needs ~24h to activate. Once active:
- Log TWS into the paper session (`DU…` prefix, not `U1530449`)
- Set `BROKER_TYPE=ib` in `config/.env`
- Verify account prefix is `DU` before any run with auto_execute strategies

**Do not set `BROKER_TYPE=ib` while TWS is logged into the live account** (`U1530449`) — the scheduler starts on `broker_service` startup and could route real orders.

---

## Verifying everything works end-to-end

Once migrations applied and yfinance upgraded:

```bash
# 1. Run tests
python -m pytest tests/ -q   # expect 521 passed

# 2. Dry-run options chain ingest
python -c "
import os, psycopg2
from services.options_chain.ingest import ingest_all
conn = psycopg2.connect(os.environ['PGURL'])
n = ingest_all(conn)
print(f'Ingested {n} rows')
conn.close()
"

# 3. Dry-run P&L computation
python -c "
import os, psycopg2
from services.broker_service.pnl_calculator import compute_daily_pnl
conn = psycopg2.connect(os.environ['PGURL'])
n = compute_daily_pnl(conn)
print(f'Upserted {n} P&L rows')
conn.close()
"

# 4. Dry-run Greeks computation
python -c "
import os, psycopg2
from services.broker_service.greeks_calculator import compute_portfolio_greeks
conn = psycopg2.connect(os.environ['PGURL'])
n = compute_portfolio_greeks(conn)
print(f'Upserted {n} Greeks rows')
conn.close()
"

# 5. Check DB results
psql $PGURL -c "SELECT strategy_id, date, realized_pnl FROM trading.strategy_pnl ORDER BY date DESC LIMIT 5;"
psql $PGURL -c "SELECT ts, account_id, total_delta, total_gamma FROM trading.portfolio_risk ORDER BY ts DESC LIMIT 6;"
```

---

## What to build next (Phase 9C — not designed)

Start with `superpowers:brainstorming`. Candidate directions:
- `var_1d_95` computation (requires returns history)
- `nav` from broker account value
- Alert service — Feishu/Telegram push on signal, fill, risk breach, promotion
- Options backtest — historical IV simulation in backtest engine
