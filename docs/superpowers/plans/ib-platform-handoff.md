# IB Trading Platform — Session Handoff

> **For a new Claude session:** Read this document first, then proceed directly to the next task.
> **Repo:** `git@github.com:MonteCarlo79/ib-platform.git`
> **Primary working directory (Mac):** `~/projects/ib-platform` (or wherever you cloned it)
> **Primary working directory (Windows):** `C:\Users\dipeng.chen\OneDrive\ETRM\ib-platform`
> **Design spec:** `docs/superpowers/specs/2026-06-14-ib-trading-platform-design.md` (inside repo)
> **Latest commit:** `9598ae7` — pushed 2026-06-29
> **Tests:** 502 passing

---

## Mac Setup (first time only)

```bash
# 1. Clone
git clone git@github.com:MonteCarlo79/ib-platform.git
cd ib-platform

# 2. Python venv
python3.13 -m venv .venv          # brew install python@3.13 if needed
source .venv/bin/activate
pip install -r requirements.txt   # no --only-binary needed on Mac

# 3. Env file
cp config/.env.example config/.env
# Fill in real values (see "Environment Variables" section below)

# 4. Apply DB migrations (once, against shared RDS)
export $(grep -v '^#' config/.env | xargs)
psql $PGURL -f db/migrations/001_news_items.sql
psql $PGURL -f db/migrations/002_kb_tables.sql
psql $PGURL -f db/migrations/003_kb_insights_tags.sql
psql $PGURL -f db/migrations/004_signals.sql
psql $PGURL -f db/migrations/005_options_signals.sql
psql $PGURL -f db/migrations/006_paper_fills.sql

# 5. Verify
python -m pytest tests/ -q        # expect 502 passed
```

---

## Environment Variables (`config/.env`)

```bash
# Shared RDS (same instance as bess-platform)
PGURL=postgresql://...

# Claude (advisor + KB digest)
ANTHROPIC_API_KEY=sk-ant-...

# Broker mode: paper | ib | alpaca
BROKER_TYPE=paper          # use paper until TWS is wired

# IB TWS or Gateway (only when BROKER_TYPE=ib)
IB_HOST=127.0.0.1          # change to TWS machine IP if remote
IB_PORT=4002               # 4002=Gateway paper, 4001=Gateway live, 7497=TWS paper
IB_CLIENT_ID=1

# Optional — leave blank to skip
ALPACA_API_KEY=
ALPACA_SECRET_KEY=
ALPACA_BASE_URL=https://paper-api.alpaca.markets
PAPER_INITIAL_CASH=100000.0
POLYGON_API_KEY=
FRED_API_KEY=

# Feishu reporting (daily + weekly reports, auto-promote alerts)
# Leave blank to skip — all reporting paths fail silently without these
FEISHU_APP_ID=
FEISHU_APP_SECRET=
FEISHU_REPORT_OPEN_ID=
```

---

## Running Services

```bash
source .venv/bin/activate
export $(grep -v '^#' config/.env | xargs)

python -m services.broker_service.main           # FastAPI broker REST API (port 8600)
python -m services.market_data.ingest            # market data APScheduler
python -m services.news.ingest                   # news APScheduler (every 15 min)
python -m services.knowledge.ingest              # KB ingest + digest (Mon-Fri 06:00/06:30 ET)
python -m services.broker_service.trade_monitor  # trade outcome learning (Mon-Fri 18:00 ET)
streamlit run apps/portfolio/app.py              # 10-tab portfolio app (port 8501)
streamlit run apps/markets/app.py                # markets app
streamlit run apps/news/app.py                   # news app
```

The `broker_service` lifespan starts the APScheduler automatically. It runs:
- `run_all_strategies` — Mon-Fri 09:35 ET
- `paper_promotions` — Mon-Fri 09:00 ET (auto-promote paper→shadow)
- `daily_report` — Mon-Fri 16:30 ET (Feishu)
- `weekly_report` — Friday 17:00 ET (Feishu)

---

## TWS / IB Gateway Connection

**Preferred on Mac: IB Gateway** (lighter than full TWS)

1. Download IB Gateway from interactivebrokers.com → install on Mac
2. Log in with paper account credentials
3. Configure: Cog → Settings → API → Enable ActiveX and Socket Clients
   - Socket port: `4002` (paper) or `4001` (live)
   - Add `127.0.0.1` to trusted IPs
   - Uncheck "Read-Only API"
4. In `config/.env`: set `BROKER_TYPE=ib`, `IB_PORT=4002`, `IB_HOST=127.0.0.1`

**Connecting to TWS on the Windows machine remotely:**
1. In TWS on Windows: Edit → Global Configuration → API → Settings
   - Enable Socket Clients, port 7497
   - Trusted IPs: add the Mac's local IP (e.g. `192.168.1.x`)
2. In `config/.env`: `IB_HOST=<windows-machine-ip>`, `IB_PORT=7497`, `BROKER_TYPE=ib`

**TWS machine note (Yuzhu Chen's Windows account):**
- Python: `C:\Users\Yuzhu Chen\AppData\Local\Python\bin\python.exe`
- Use `--only-binary=:all:` for any pip installs on that machine
- No Visual Studio / compiler available there

---

## What Has Been Built

### Phase 1 — Foundation (complete)
- `services/broker/base.py` — `BaseBroker` ABC, `Position`, `Order`, `OrderRequest`, `AccountSummary` dataclasses
- `services/broker/ib_broker.py` — IBBroker (ib_insync)
- `services/broker/alpaca_broker.py` — AlpacaBroker (REST)
- `services/broker/paper_broker.py` — PaperBroker (in-memory fills, `_persist_fill` to `trading.paper_fills`)
- `services/broker/broker_factory.py` — `get_broker()` reads `BROKER_TYPE` env var
- `services/broker_service/main.py` — FastAPI (8 REST endpoints + APScheduler lifespan)
- `services/broker_service/order_router.py` — 4 pre-trade risk checks (daily loss, position size, duplicate guard, delta notional cap)
- `services/broker_service/algo_scheduler.py` — full strategy execution loop + paper/shadow/live mode + auto-promote + reporting jobs
- `services/broker_service/data_writer.py` — syncs positions/fills/bars → RDS
- `db/schema.sql` — full `trading.*` schema

### Phase 2 — Analytics Libs (complete)

| Module | Key exports |
|---|---|
| `libs/pricing/kirk_margrabe.py` | `kirk_spread_call`, `margrabe_exchange` |
| `libs/pricing/vol_surface.py` | `VolSurface` |
| `libs/pricing/pnl_explain.py` | `explain_pnl` → `PnlExplain` |
| `libs/pricing/black_scholes.py` | `bs_price`, `b76_price`, `bs_greeks`, `b76_greeks`, `implied_vol` |
| `libs/fixed_income/bonds.py` | `bond_price`, `ytm`, `duration`, `dv01`, `convexity` |
| `libs/fixed_income/yield_curve.py` | `NelsonSiegelCurve`, `bootstrap_curve` |
| `libs/fixed_income/swaps.py` | `irs_npv`, `par_rate`, `swap_dv01` |
| `libs/fixed_income/caps_floors.py` | `cap_black`, `floor_black` |
| `libs/fx/forwards.py` | `fx_forward`, `forward_points`, `cross_rate` |
| `libs/signals/vix.py` | `VixTermStructure`, `vix_regime`, `contango_pct` |

### Phase 3 — Risk Libs + Portfolio/Markets Apps (complete)

| Module | Key exports |
|---|---|
| `libs/risk/greeks.py` | `aggregate_greeks` |
| `libs/risk/var.py` | `historical_var`, `parametric_var`, `cvar`, `var_backtest` |
| `libs/risk/performance.py` | `sharpe`, `sortino`, `calmar`, `max_drawdown`, `win_stats` |
| `libs/risk/scenarios.py` | `spot_shock`, `vol_shock`, `spot_vol_matrix` |
| `libs/risk/cashflow.py` | `daily_cashflow_statement`, `margin_utilisation` |

**Portfolio app** (`apps/portfolio/`) — 10 tabs:
positions, pnl, risk, options_book, fixed_income, performance, cashflow, pre_trade, daily_briefing, strategies

**Markets app** (`apps/markets/`) — 7 tabs:
charts, vol_surface, options_cockpit, yield_curves, fx, macro, vix

### Phase 4 — Market Data Pipeline + News Service (complete)
- `services/market_data/` — yfinance + Polygon feeds; 4 APScheduler jobs (EOD bars, intraday, VIX, FX)
- `services/news/` — RSS + Polygon news; 15-min ingest; Haiku scorer
- `apps/news/` — 4-tab news app

### Phase 5 — Knowledge Base Pipeline (complete)
- `services/knowledge/` — FRED, Fed speeches, Treasury, BIS, RSS connectors
- `expert_memory.py` — `digest_kb_docs` (Channel 2), `extract_from_trade_outcome` (Channel 5)
- `db/migrations/002_kb_tables.sql` — `trading.kb_docs`, `trading.kb_insights`, `trading.kb_briefings`

### Phase 6 — Advisor App (complete)
- `expert_memory.py` additions — `inject_memory`, `extract_insights`, tags support
- `services/knowledge/daily_briefing.py` — 5-section macro briefing via Haiku
- Portfolio app gains Pre-Trade tab + Daily Briefing tab
- `services/broker_service/trade_monitor.py` — Mon-Fri 18:00 ET trade outcome → KB loop
- `db/migrations/003_kb_insights_tags.sql` — GIN index on tags

### Phase 7 — Strategy Signals + Backtest Engine (complete)
- `libs/strategies/base.py` — `Signal` dataclass (with expiry/strike/right) + `BaseStrategy` ABC
- `libs/strategies/vix_regime.py` — VixRegimeStrategy
- `libs/backtest/engine.py` — `run_backtest` → `BacktestResult`
- `services/broker_service/signal_writer.py` — `write_signal`, `mark_executed`
- Portfolio app: Strategies tab (Config / Signals / Backtest / Paper Performance)
- `db/migrations/004_signals.sql` — `trading.signals` + `trading.strategy_config`

### Phase 8 — Options Execution, Paper Shadow Mode, Reporting (complete)
- `libs/strategies/options_base.py` — `OptionsStrategy` ABC + `_select_strike` / `_nearest_expiry`
- `libs/strategies/vix_options.py` — VixOptionsStrategy
- `libs/strategies/spx_overlay.py` — SpxOverlayStrategy
- `libs/strategies/short_strangle.py` — ShortStrangleStrategy (returns list of 2 signals)
- `services/broker/paper_broker.py` — `_persist_fill` → `trading.paper_fills`
- `services/broker_service/order_router.py` — delta_notional_cap check added
- `services/broker_service/algo_scheduler.py` — mode-aware execution (paper/shadow/live); auto-promote job; reporting jobs
- `services/reporting/` — `feishu_client.py`, `daily_report.py`, `weekly_report.py`
- `apps/shared/db.py` — `get_paper_fills`, `upsert_strategy_config` (with mode)
- `db/migrations/005_options_signals.sql` — expiry/strike/right on signals; mode on strategy_config; 3 paper-mode options strategies seeded
- `db/migrations/006_paper_fills.sql` — `trading.paper_fills` table

### Phase 9A — Options Chain Ingestion + EOD P&L (complete)
- `services/options_chain/fetcher.py` — `fetch_chain(symbol)` → DataFrame (yfinance, bs_greeks)
- `services/options_chain/ingest.py` — APScheduler: ingest_chains_open (09:25 ET), ingest_chains_close (16:05 ET)
- `services/broker_service/pnl_calculator.py` — `compute_daily_pnl(conn)` → upserts to `trading.strategy_pnl`
- `services/broker_service/algo_scheduler.py` — `compute_pnl` job at 16:15 ET; date-filtered `_fetch_options_chain`
- `services/broker_service/trade_monitor.py` — reads real P&L from `strategy_pnl`, falls back to raw estimate
- `db/migrations/007_strategy_pnl.sql` — `trading.strategy_pnl` table

---

## Key Technical Patterns

### Tab render pattern
Each tab: single `render(conn, ...)` function. Streamlit imported inside `render()` only (never at module level). `app.py` wraps `connect()` in `@st.cache_resource`.

### DB query pattern
All `apps/shared/db.py` functions use `conn.cursor()` context manager (NOT `pd.read_sql`) → mockable in tests.

### Strategy execution modes
`trading.strategy_config.mode` controls routing in `algo_scheduler._run_all_strategies`:
- `paper` → fills paper broker only (no live execution)
- `live` → live broker only (no paper fills)
- `shadow` → both (paper fills recorded AND live orders submitted)

Auto-promote job runs at 09:00 ET: if paper strategy meets Sharpe + MaxDD thresholds after `paper_validation_days` → promotes to `shadow` + Feishu alert.

### Options chain context
`_run_all_strategies` detects `OptionsStrategy` instances and injects `vix_chain` / `spy_chain` into context from `trading.options_chain`. Equity strategies ignore extra context keys.

### Running tests
```bash
cd ~/projects/ib-platform
source .venv/bin/activate
python -m pytest tests/ -q    # 502 passed
```

---

## Git State

Branch: `master` (all work directly on master — no feature branches)
Remote: `git@github.com:MonteCarlo79/ib-platform.git`
Latest commit: `f1aa03f` — pushed 2026-06-29

All commits pushed. No in-progress work.

---

## What To Build Next

### Phase 9B — (not yet designed)
No design spec exists yet. Start with `superpowers:brainstorming`.

Potential directions:
- Portfolio-level Greeks aggregation service (populate `trading.portfolio_risk` from live positions)
- Alert service — Feishu/Telegram push on signal generated, order filled, risk limit breached, paper strategy promoted
- Options backtest — extend backtest engine to simulate options fills using historical IV from `trading.options_chain`
