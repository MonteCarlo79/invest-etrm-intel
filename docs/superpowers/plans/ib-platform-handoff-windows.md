# IB Trading Platform — Windows Session Handoff

> **For a new Claude session on this Windows machine:** Read this document first, then proceed.
> **Repo on disk:** `C:\Users\dipeng.chen\OneDrive\ETRM\ib-platform`
> **GitHub remote:** `git@github.com:MonteCarlo79/ib-platform.git` (SSH key at `~/.ssh/id_ed25519`)
> **Latest commit:** `7e2541c` — pushed 2026-06-29
> **Tests:** 521 passing

---

## Orientation

The ib-platform repo lives in OneDrive and is fully pushed to GitHub.
The bess-platform repo (shared RDS, feishu_client source, etc.) lives at
`C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform`.

There are **no cross-repo hardcoded paths remaining** as of 2026-06-28 — everything needed
is self-contained inside ib-platform.

---

## Running Tests

```powershell
# Load env (PowerShell)
Get-Content config\.env | ForEach-Object {
    if ($_ -match '^([^#=][^=]*)=(.*)$') {
        [System.Environment]::SetEnvironmentVariable($Matches[1].Trim(), $Matches[2].Trim())
    }
}

cd C:\Users\dipeng.chen\OneDrive\ETRM\ib-platform
C:\Users\dipeng.chen\AppData\Local\Programs\Python\Python313\python.exe -m pytest tests/ -q
# Expect: 502 passed
```

Or in bash (Git for Windows):
```bash
cd /c/Users/dipeng.chen/OneDrive/ETRM/ib-platform
export $(grep -v '^#' config/.env | xargs)
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/ -q
```

---

## Running Services

```bash
# In bash, from ib-platform root, with env loaded
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m services.broker_service.main
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m services.market_data.ingest
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m services.news.ingest
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m services.knowledge.ingest
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m services.broker_service.trade_monitor
streamlit run apps/portfolio/app.py
streamlit run apps/markets/app.py
streamlit run apps/news/app.py
```

---

## TWS Connection (Windows)

TWS is installed under **Yuzhu Chen's Windows account** (separate login on this machine).

When running broker_service on the TWS machine account:
```powershell
& "C:\Users\Yuzhu Chen\AppData\Local\Python\bin\python.exe" -m services.broker_service.main
```

TWS API setup (do once in TWS):
- Edit → Global Configuration → API → Settings
- Enable Socket Clients, port `7497`
- Trusted IPs: `127.0.0.1`
- Uncheck Read-Only API

`config/.env` for live TWS: `BROKER_TYPE=ib`, `IB_HOST=127.0.0.1`, `IB_PORT=7497`

TWS update issue: TWS fails to auto-update due to China network blocking IB's CDN.
Fix: launch TWS with a VPN active, or download offline installer from interactivebrokers.com.

**TWS machine pip installs** (no Visual Studio compiler):
```powershell
& "C:\Users\Yuzhu Chen\AppData\Local\Python\bin\python.exe" -m pip install -r requirements.txt --only-binary=:all:
```

---

## Git Workflow

```bash
cd /c/Users/dipeng.chen/OneDrive/ETRM/ib-platform

# Stage specific files only — never stage terraform.tfvars or .env
git add <files>
git commit -m "..."
git push
```

Do NOT use `git add -A` or `git add .` — may accidentally stage `.env` or untracked scratch files (`respond*.txt`, `setup.txt`).

---

## DB Migrations (all applied to shared RDS as of 2026-06-28)

```bash
psql $PGURL -f db/migrations/001_news_items.sql       # Phase 4 ✅
psql $PGURL -f db/migrations/002_kb_tables.sql        # Phase 5 ✅
psql $PGURL -f db/migrations/003_kb_insights_tags.sql # Phase 6 ✅
psql $PGURL -f db/migrations/004_signals.sql          # Phase 7 ✅
psql $PGURL -f db/migrations/005_options_signals.sql  # Phase 8 ✅
psql $PGURL -f db/migrations/006_paper_fills.sql      # Phase 8 ✅
psql $PGURL -f db/migrations/007_strategy_pnl.sql     # Phase 9A ✅
```

If connecting to the RDS from this machine fails, check the security group inbound rule for your current IP.

---

## What Has Been Built (Phases 1–8)

See the full spec at `docs/superpowers/specs/2026-06-14-ib-trading-platform-design.md`.

| Phase | Summary | Tests at end |
|---|---|---|
| 1 | Broker layer (IB/Alpaca/Paper), FastAPI service, order router, APScheduler | — |
| 2 | Pricing libs (Kirk-Margrabe, VolSurface, PnL explain, fixed income, FX, VIX) | 67 |
| 3 | Risk libs (Greeks, VaR, performance, scenarios, cashflow), Portfolio + Markets apps | 290 |
| 4 | Market data pipeline (yfinance + Polygon), News service + app | 337 |
| 5 | Knowledge Base pipeline (FRED, Fed speeches, Treasury, BIS, RSS) | 372 |
| 6 | Advisor App (inject_memory, extract_insights, daily briefing, trade monitor) | 394 |
| 7 | Strategy signals + backtest engine, Strategies tab in Portfolio app | 437 |
| 8 | Options strategies, paper shadow mode, Feishu reporting, options_cockpit fixed | **480** |
| 9A | Options chain ingestion (yfinance), EOD P&L calculator, trade_monitor P&L loop | **502** |
| 9B | Portfolio Greeks aggregation (paper + live + total), `compute_greeks` job at 09:30 ET | **521** |

### Key files added / changed in Phase 8
```
libs/pricing/black_scholes.py           self-contained BS pricing (no bess-platform dep)
libs/strategies/options_base.py         OptionsStrategy ABC + _select_strike/_nearest_expiry
libs/strategies/vix_options.py          VixOptionsStrategy
libs/strategies/spx_overlay.py          SpxOverlayStrategy
libs/strategies/short_strangle.py       ShortStrangleStrategy (returns list[Signal])
libs/strategies/base.py                 Signal.expiry/strike/right added
services/broker/base.py                 OrderRequest.expiry/strike/right added
services/broker/paper_broker.py         _persist_fill → trading.paper_fills
services/broker_service/order_router.py delta_notional_cap check (#4)
services/broker_service/algo_scheduler.py  mode-aware execution + 3 new scheduler jobs
services/reporting/__init__.py
services/reporting/feishu_client.py     copied from bess-platform/services/hermes/
services/reporting/daily_report.py      16:30 ET Mon-Fri Feishu card
services/reporting/weekly_report.py     17:00 ET Friday Feishu table
apps/shared/db.py                       get_paper_fills, upsert_strategy_config(mode=)
apps/portfolio/tabs/strategies.py       mode badge + Paper Performance sub-tab (4th tab)
apps/markets/tabs/options_cockpit.py    fixed: imports local black_scholes, no hardcoded path
db/migrations/005_options_signals.sql
db/migrations/006_paper_fills.sql
```

### Phase 9A — Options Chain Ingestion + EOD P&L (complete)
- `services/options_chain/fetcher.py` — `fetch_chain(symbol)` → DataFrame (yfinance, bs_greeks)
- `services/options_chain/ingest.py` — APScheduler: ingest_chains_open (09:25 ET), ingest_chains_close (16:05 ET)
- `services/broker_service/pnl_calculator.py` — `compute_daily_pnl(conn)` → upserts to `trading.strategy_pnl`
- `services/broker_service/algo_scheduler.py` — `compute_pnl` job at 16:15 ET; date-filtered `_fetch_options_chain`
- `services/broker_service/trade_monitor.py` — reads real P&L from `strategy_pnl`, falls back to raw estimate
- `db/migrations/007_strategy_pnl.sql` — `trading.strategy_pnl` table

### Phase 9B — Portfolio Greeks Aggregation (complete)
- `services/broker_service/greeks_calculator.py` — `compute_portfolio_greeks(conn)` → upserts paper/live/total rows to `trading.portfolio_risk`
- `services/broker_service/algo_scheduler.py` — `compute_greeks` job at 09:30 ET (6 jobs total)
- Paper options: `paper_fills LEFT JOIN signals` → `options_chain` IV → `bs_greeks`
- Live options: `trading.positions` (has expiry/strike/right) → `options_chain` IV → `bs_greeks`
- Portfolio app Risk tab now shows real Greeks data

### Strategy execution modes (in `trading.strategy_config.mode`)
| Mode | Paper broker | Live broker |
|---|---|---|
| `paper` | ✅ fills written to `trading.paper_fills` | ❌ |
| `live` | ❌ | ✅ if auto_execute=TRUE |
| `shadow` | ✅ | ✅ if auto_execute=TRUE |

Auto-promote job (09:00 ET): computes Sharpe + MaxDD from paper fills → if thresholds met after `paper_validation_days` → sets mode=`shadow` + Feishu alert.

### APScheduler jobs (started in FastAPI lifespan)
| Job ID | Schedule | Function |
|---|---|---|
| `paper_promotions` | Mon-Fri 09:00 ET | Auto-promote paper→shadow |
| `compute_greeks` | Mon-Fri 09:30 ET | Portfolio Greeks → trading.portfolio_risk |
| `run_all_strategies` | Mon-Fri 09:35 ET | Execute all enabled strategies |
| `compute_pnl` | Mon-Fri 16:15 ET | EOD P&L → trading.strategy_pnl |
| `daily_report` | Mon-Fri 16:30 ET | Feishu daily brief |
| `weekly_report` | Friday 17:00 ET | Feishu weekly table |
| `ingest_docs` (knowledge) | Mon-Fri 06:00 ET | KB doc ingest |
| `digest_docs` (knowledge) | Mon-Fri 06:30 ET | KB Haiku digest |
| `trade_monitor` (separate) | Mon-Fri 18:00 ET | Trade outcome → KB |

---

## Environment Variables (`config/.env`)

```bash
PGURL=postgresql://...            # shared RDS (same as bess-platform)
ANTHROPIC_API_KEY=sk-ant-...      # advisor + KB digest + news scorer
BROKER_TYPE=paper                 # paper | ib | alpaca
IB_HOST=127.0.0.1
IB_PORT=7497                      # 7497=TWS paper, 4002=Gateway paper, 4001=Gateway live
IB_CLIENT_ID=1
ALPACA_API_KEY=
ALPACA_SECRET_KEY=
ALPACA_BASE_URL=https://paper-api.alpaca.markets
PAPER_INITIAL_CASH=100000.0
POLYGON_API_KEY=                  # optional — falls back to yfinance if absent
FRED_API_KEY=                     # optional — connector returns [] if absent
FEISHU_APP_ID=                    # optional — reporting skips silently if absent
FEISHU_APP_SECRET=
FEISHU_REPORT_OPEN_ID=
```

---

## What To Build Next

### Phase 9C — (not yet designed)
No design spec exists yet. Start with `superpowers:brainstorming`.

Potential directions (from 9B known gaps):
- `var_1d_95` computation — requires returns history in `trading.portfolio_risk`
- `nav` computation — requires account value from broker
- Alert service — Feishu/Telegram push on signal generated, order filled, risk limit breached, paper strategy promoted
- Options backtest — extend backtest engine to simulate options fills using historical IV from `trading.options_chain`
