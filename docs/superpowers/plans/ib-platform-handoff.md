# IB Trading Platform — Session Handoff

> **For a new Claude session:** Read this document first, then proceed directly to the next task.
> Primary working directory: `C:\Users\dipeng.chen\OneDrive\ETRM\ib-platform`
> GitHub remote: `git@github.com:MonteCarlo79/ib-platform.git` (SSH, key at `~/.ssh/id_ed25519`)
> Design spec: `C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform\docs\superpowers\specs\2026-06-14-ib-trading-platform-design.md`

---

## What Has Been Built

### Phase 1 — Foundation (complete)
- `services/broker/base.py` — `BaseBroker` ABC, `Position`, `Order`, `OrderRequest`, `AccountSummary` dataclasses
- `services/broker/ib_broker.py` — IBBroker (ib_insync, TWS port 7497)
- `services/broker/alpaca_broker.py` — AlpacaBroker (REST)
- `services/broker/paper_broker.py` — PaperBroker (simulated fills from bars_1h)
- `services/broker/broker_factory.py` — `get_broker(type)` factory
- `services/broker_service/main.py` — FastAPI app (8 REST endpoints)
- `services/broker_service/order_router.py` — pre-trade risk checks (7 hard blocks)
- `services/broker_service/algo_scheduler.py` — APScheduler strategy loop
- `services/broker_service/data_writer.py` — syncs positions/fills/bars → RDS
- `db/schema.sql` — full `trading.*` schema (accounts, positions, trades, orders, bars, risk, KB, signals, VIX, news, agent_memory)

### Phase 2 — Analytics Libs (complete, 67 tests passing at phase end)
All under `libs/` and tested in `tests/libs/`:

| Module | Key exports |
|---|---|
| `libs/pricing/kirk_margrabe.py` | `kirk_spread_call`, `margrabe_exchange` |
| `libs/pricing/vol_surface.py` | `VolSurface(F, slices)` — `.get_vol(K, T)`, `.vol_grid(strikes, expiries)` |
| `libs/pricing/pnl_explain.py` | `explain_pnl` → `PnlExplain` (Δ/Γ/Vega/Θ attribution) |
| `libs/fixed_income/bonds.py` | `bond_price`, `ytm`, `macaulay_duration`, `modified_duration`, `dv01`, `convexity` |
| `libs/fixed_income/yield_curve.py` | `NelsonSiegelCurve.fit(tenors, rates)` → `.rate(t)`, `.discount_factor(t)`; `bootstrap_curve` |
| `libs/fixed_income/swaps.py` | `irs_npv`, `par_rate`, `swap_dv01` |
| `libs/fixed_income/caps_floors.py` | `caplet_black`, `cap_black`, `floor_black` |
| `libs/fx/forwards.py` | `fx_forward(spot, r_domestic, r_foreign, T)`, `forward_points(spot, r_domestic, r_foreign, T)`, `cross_rate` |
| `libs/fx/vol_surface_fx.py` | `FXVolSmile`, `build_fx_smile`, `delta_to_strike` |
| `libs/signals/vix.py` | `VixTermStructure`, `vix_regime`, `contango_pct`, `roll_yield_annualised`, `implied_vol_premium` |

### Phase 4 — Market Data Pipeline + News Service (complete, 337 tests passing)
Latest commit: `24f0e49` — pushed 2026-06-18.

**Market data service (`services/market_data/`):**
- `yfinance_feed.py` — `fetch_bars_1d`, `fetch_bars_1h`, `fetch_vix_term_structure`, `fetch_fx_rates`
- `polygon_feed.py` — same signatures; raises `SkipSource` if `POLYGON_API_KEY` unset; ingest falls back to yfinance
- `ingest.py` — APScheduler service (`python -m services.market_data.ingest`); 4 jobs: EOD bars (Mon–Fri 16:10 ET), intraday bars (hourly 09:30–16:00 ET), VIX (09:35 ET), FX rates (every 4h)

**News service (`services/news/`):**
- `sources.py` — `RSS_FEEDS` (Reuters, CNBC, FT) + `fetch_polygon_news` (returns `[]` if key unset)
- `ingest.py` — APScheduler service (`python -m services.news.ingest`); every 15 min 06:00–22:00 ET; SHA-256 url_hash dedup; calls `scorer.score_pending` after each batch
- `scorer.py` — Claude Haiku (`claude-haiku-4-5-20251001`), batch ≤20, 48h stale → 0.0 without API call, markdown-fence stripping, score clamped [0.0, 1.0]

**News app (`apps/news/`):**
- `app.py` — 4-tab Streamlit app (no `account_id`, news is account-agnostic)
- `tabs/top_stories.py` — sidebar relevance slider, `st.expander` per item, sentiment badges
- `tabs/by_symbol.py` — selectbox from `WATCHLIST` + positions, plotly scatter news timeline
- `tabs/full_feed.py` — keyword search + source multiselect, full dataframe
- `tabs/digest.py` — Claude Sonnet (`claude-sonnet-4-6`) daily briefing, cached in `trading.agent_memory`

**DB:** `db/migrations/001_news_items.sql` (run once: `psql $PGURL -f db/migrations/001_news_items.sql`)

**`apps/shared/db.py` additions:** `get_news_items(conn, min_relevance, symbols, limit)`, `get_news_by_symbol(conn, symbol, limit)`

**`requirements.txt` additions:** `feedparser==6.0.11`, `anthropic==0.34.2`

### Phase 3 — Apps + Risk Libs (complete, 290 tests passing)
Latest commit: `b331752` — pushed 2026-06-18.

**Risk libs (`libs/risk/`):**

| Module | Key exports |
|---|---|
| `libs/risk/greeks.py` | `PortfolioGreeks` dataclass, `aggregate_greeks(positions)` |
| `libs/risk/var.py` | `historical_var`, `parametric_var`, `cvar`, `component_var`, `var_backtest` (Kupiec LR test) |
| `libs/risk/performance.py` | `roace`, `sharpe`, `sortino`, `calmar` (CAGR), `max_drawdown`, `drawdown_analysis`, `win_stats`, `return_on_risk`, `capital_efficiency`, `attribution` |
| `libs/risk/scenarios.py` | `spot_shock`, `vol_shock` (absolute Δσ), `spot_vol_matrix` |
| `libs/risk/cashflow.py` | `daily_cashflow_statement`, `cumulative_cashflow`, `margin_utilisation` |

**Simulation lib:**
- `libs/simulation/options_scenarios.py` — `options_scenario_matrix(positions, spot_range, vol_range)` — delta-gamma-vega 2D grid, absolute vol shocks

**Market data connector:**
- `services/market_data/forexfactory.py` — `fetch_calendar(include_next_week=True)` — ForexFactory JSON feed

**Shared DB layer:**
- `apps/shared/db.py` — 13 cursor-based query functions (`get_positions`, `get_trades`, `get_strategy_pnl`, `get_capital_summary`, `get_cashflows`, `get_portfolio_risk`, `get_bars_1d`, `get_bars_1h`, `get_options_chain`, `get_yield_curve`, `get_fx_rates`, `get_vix_term_structure`, `get_vol_surface`); all return `pd.DataFrame`, mockable

**Portfolio app (`apps/portfolio/`)** — 7 tabs:
- `positions.py` — metrics + pie by asset class
- `pnl.py` — YTD/MTD/all-time stacked bar by strategy
- `risk.py` — Greeks metrics, VaR/CVaR, spot-vol heatmap
- `options_book.py` — chain per expiry + 3D VolSurface
- `fixed_income.py` — FI positions + NS yield curve fit
- `performance.py` — Sharpe/Sortino/Calmar (fractional returns), ROACE, drawdown
- `cashflow.py` — daily statement, YTD cumulative, margin gauge

**Markets app (`apps/markets/`)** — 7 tabs:
- `charts.py` — candlestick + MA20/MA50 + Bollinger + RSI(14)
- `vol_surface.py` — 3D surface / term structure / skew sub-tabs
- `options_cockpit.py` — Black-76/BS pricing via importlib (bess-platform); kwargs: `sigma=`, `flag="c"/"p"`
- `yield_curves.py` — NS fit + 3 historical overlays
- `fx.py` — spot table + forward/fwd-points for 5 tenors
- `macro.py` — ForexFactory calendar (colour-coded) + USD curve + FX panels
- `vix.py` — regime badge, M1-M8 bar, contango% history, IVP gauge

---

## Key Technical Notes (carried forward from Phase 2/3)

### bess-platform import pattern (for options_cockpit.py)
```python
import importlib.util, os, sys
_BESS = "C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform"
abs_path = os.path.join(_BESS, "libs/options/black_scholes.py")
spec = importlib.util.spec_from_file_location("_bess_black_scholes", abs_path)
mod = importlib.util.module_from_spec(spec)
sys.modules["_bess_black_scholes"] = mod
spec.loader.exec_module(mod)
b76_price = mod.b76_price  # signature: b76_price(F, K, T, r, sigma, flag="c")
bs_price  = mod.bs_price   # signature: bs_price(S, K, T, r, sigma, q=0.0, flag="c")
bs_greeks = mod.bs_greeks  # signature: bs_greeks(S, K, T, r, sigma, q=0.0, flag="c")
# NOTE: use sigma= not vol=, flag="c"/"p" not option_type="call"/"put"
```

### Tab render pattern
Each tab: single `render(conn, ...)` function. Streamlit imported inside `render()` only (never at module level). `app.py` wraps `connect()` in `@st.cache_resource`.

### DB query pattern
All `apps/shared/db.py` functions use `conn.cursor()` context manager (NOT `pd.read_sql`) → mockable in tests.

### var.py z-score
`math.erfinv` does not exist in Python 3.13 — uses `scipy.special.erfinv` instead.

### vol_shock convention
`vol_shock` and `options_scenario_matrix` use **absolute** vol shocks (e.g. 0.02 = 2 vol points), NOT relative.

### Running tests
```bash
cd C:/Users/dipeng.chen/OneDrive/ETRM/ib-platform
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/ -q
```
Python: `/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe`

### Environment
- Windows 11, bash shell via Git for Windows
- Primary working directory: `C:\Users\dipeng.chen\OneDrive\ETRM\ib-platform`
- Additional directory: `C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform` (shared infrastructure)
- RDS shared with bess-platform, env var `PGURL`
- Config: `config/.env` in ib-platform root

---

## Git State

Branch: `master` (all work directly on master — no feature branches)
Remote: `git@github.com:MonteCarlo79/ib-platform.git`
Latest commit: `24f0e49 fix(apps/news/digest): safe headline access in briefing prompt`

All commits pushed.

---

## What To Build Next

### Phase 5 — (Not yet designed)
No design spec exists yet for Phase 5. Start with `superpowers:brainstorming` to define the next phase.
