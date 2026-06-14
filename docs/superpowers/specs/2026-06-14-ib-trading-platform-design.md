# IB Trading Platform — Design Spec
*Date: 2026-06-14*
*Status: Awaiting user approval*

---

## 1. Overview

A personal multi-asset trading platform built as a separate repo (`ib-platform`) sharing the existing AWS RDS and common services from `bess-platform`. Covers four trading pillars — portfolio P&L/risk, market analytics, opportunity screening with AI, and algo execution — plus an AI news screener. Designed for single-user personal use today, auth-ready for team access later.

**Instruments in scope:** Equities, ETFs, options, futures, FX spot/forwards/options, fixed income (bonds, IRS, caps/floors).

**Primary broker:** Interactive Brokers (TWS/IB Gateway). Architecture is broker-agnostic via `BaseBroker` ABC — adding Alpaca, Tastytrade, or any other broker requires one new implementation file.

---

## 2. Physical Architecture

```
Personal laptop (non-corporate network)
  └── TWS / IB Gateway
  └── broker_service (FastAPI + uvicorn)
        ├── IBBroker: live positions, orders, market data
        ├── order_router: pre-trade risk checks
        ├── algo_scheduler: APScheduler strategy loop
        └── data_writer: syncs fills/positions → RDS (shared)

Corporate laptop (corporate network, IB blocked)
  └── Streamlit apps (portfolio, markets, screener, execution, news)
        └── read-only RDS queries + POST to broker_service API
              (broker_service reachable via local IP on same home LAN,
               or via ngrok/Tailscale tunnel when on different networks)

Shared infrastructure (AWS, existing bess-platform)
  └── RDS PostgreSQL — new schema: trading.*
  └── ECS Fargate — Streamlit app containers
  └── ECR — Docker image registry
  └── Cognito — auth (wired in later)

OneDrive sync
  └── ib-platform repo on both laptops (same code, different runtime roles)
```

---

## 3. Repository Structure

```
ib-platform/                              # separate repo
├── config/
│   └── .env                              # PGURL (shared RDS), ANTHROPIC_API_KEY,
│                                         # BROKER_TYPE, IB_HOST, IB_PORT
├── services/
│   ├── broker/                           # Multi-broker abstraction layer
│   │   ├── base.py                       # BaseBroker ABC
│   │   ├── ib_broker.py                  # IBBroker(BaseBroker) — ib_insync
│   │   ├── alpaca_broker.py              # AlpacaBroker(BaseBroker) — REST
│   │   ├── paper_broker.py               # PaperBroker(BaseBroker) — simulated fills
│   │   └── broker_factory.py             # get_broker(type) → BaseBroker
│   ├── broker_service/                   # FastAPI app (personal laptop only)
│   │   ├── main.py                       # uvicorn entrypoint, mounts routers
│   │   ├── data_writer.py                # position/fill/bar sync → RDS
│   │   ├── order_router.py               # pre-trade checks → broker.submit_order()
│   │   └── algo_scheduler.py             # APScheduler strategy loop
│   ├── market_data/                      # Runs on any machine
│   │   ├── yfinance_feed.py
│   │   ├── polygon_feed.py
│   │   └── ingest.py                     # Scheduled → trading.bars_1d/1h + vix_term_structure
│   ├── news/
│   │   ├── sources.py                    # RSS + Polygon/Alpha Vantage news registry
│   │   ├── ingest.py                     # Scheduled pull → trading.news_items
│   │   └── scorer.py                     # Claude relevance/sentiment scoring
│   └── knowledge/                        # KB ingestion + expert memory (new)
│       ├── config.py                     # TradingKBConfig, question sets
│       ├── base.py                       # BaseConnector, kb_docs table, FTS search
│       ├── expert_memory.py              # Insight extraction/retrieval + model-derived insights
│       ├── daily_briefing.py             # Generates structured daily market briefing
│       ├── trade_monitor.py              # Watches closed positions → post-trade P&L explain
│       │                                 #   → extract_from_trade_outcome()
│       └── connectors/                   # FRED, CBOE, SEC EDGAR, Fed, Treasury, BIS, RSS, local
├── apps/
│   ├── portfolio/                        # Pillar 1: P&L + risk dashboard
│   ├── markets/                          # Pillar 2: market analytics + vol tools
│   ├── screener/                         # Pillar 3: opportunity screener + AI agent
│   ├── execution/                        # Pillar 4: algo control panel + blotter
│   ├── news/                             # Pillar 5: AI news screener
│   └── advisor/                          # Pillar 6: Knowledge Base + Investment Advisor
├── libs/
│   ├── pricing/                          # Extends bess-platform libs/options/
│   ├── fixed_income/                     # Bond analytics, yield curve, swaps
│   ├── fx/                               # FX forwards, vol surface, CCY risk
│   ├── ml/                               # PCA, factor models, HMM, GARCH
│   ├── risk/                             # Unified cross-asset portfolio risk
│   ├── signals/                          # Rule-based + ML signal generators
│   ├── backtest/                         # Vectorized + event-driven engine
│   └── strategies/                       # Strategy ABC, registry, examples
├── agents/
│   └── trading_analyst/                  # Claude agent for screener
├── infra/
│   └── terraform/                        # ECS tasks for 5 apps (not broker_service)
├── tests/
├── requirements.txt
└── docker-compose.yml                    # Local: all 5 apps + market_data + news
```

**Shared bess-platform code** is imported via `sys.path`:
```python
# At top of any ib-platform file that needs it
import sys
sys.path.insert(0, "C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform")
from libs.options.black_scholes import bs_price, bs_greeks, implied_vol
from libs.options.smile import fit_svi, calibrate_from_quotes
from libs.options.structures import build_structure
```

---

## 4. Data Model — `trading` Schema (shared RDS)

### Reference & Positions

```sql
trading.accounts          -- account_id, broker, currency, account_type
trading.positions         -- account_id, symbol, asset_class, expiry, strike, right,
                          --   quantity, avg_cost, unrealised_pnl, ts_snapshot
trading.trades            -- trade_id, symbol, side, quantity, fill_price, commission,
                          --   strategy_id, order_id, ts_fill
trading.orders            -- order_id, symbol, order_type, side, quantity, limit_price,
                          --   status, strategy_id, ts_submitted, ts_last_update
```

### Market Data (time series)

```sql
trading.bars_1d           -- symbol, ts_date, open, high, low, close, volume, source
trading.bars_1h           -- symbol, ts, open, high, low, close, volume, source
trading.options_chain     -- symbol, expiry, strike, right, bid, ask, iv, delta,
                          --   gamma, theta, vega, ts_snapshot
trading.fx_rates          -- pair, ts, spot, bid, ask, source
trading.bond_quotes       -- isin, ts, clean_price, dirty_price, ytm, source
```

### Quant / Risk

```sql
trading.vol_surface       -- underlying, ts_date, expiry, strike, iv (historical)
trading.yield_curves      -- curve_id, ts_date, tenor_label, tenor_years, rate, source
trading.portfolio_risk    -- ts, account_id, total_delta, total_gamma, total_theta,
                          --   total_vega, dv01, fx_delta_usd, var_1d_95, nav
trading.strategy_pnl      -- strategy_id, ts_date, realized_pnl, unrealized_pnl, trades_n
trading.vix_term_structure -- ts_date, vix_index, m1..m8, contango_pct,
                           --   roll_yield_annualised, vvix, regime, source
```

### Knowledge Base

```sql
trading.kb_docs           -- id, source, doc_type, title, url, published_date,
                          --   content, fetched_at, search_vector (GIN tsvector)
trading.kb_insights       -- id, insight_text, insight_type, confidence,
                          --   source_session, source_doc_url, source_model_run,
                          --   source_backtest_id, source_trade_id,
                          --   validated_at, active, created_at
                          -- insight_type values:
                          --   market_regime | price_driver | vol_signal | macro_risk |
                          --   opportunity | strategy | model_insight |
                          --   strategy_backtest | trade_outcome
trading.kb_briefings      -- id, briefing_date, market_section, content,
                          --   model_outputs_json, generated_at
```

### Algo & Signals

```sql
trading.signals           -- signal_id, strategy_id, symbol, direction, strength,
                          --   source (rule/ml/agent), ts_generated, ts_acted, status
trading.algo_runs         -- run_id, strategy_id, status, params_json, ts_start, ts_end
```

### News

```sql
trading.news_items        -- item_id, source, headline, url, body_text, published_ts,
                          --   symbols_mentioned[], categories[], relevance_score,
                          --   sentiment, ai_summary, ts_ingested
```

### Agent Memory

```sql
trading.agent_memory      -- app_key, category, subject, content, ts_updated
```

---

## 5. Multi-Broker Abstraction (`services/broker/`)

### `base.py` — `BaseBroker` ABC

```python
class BaseBroker(ABC):
    def connect(self) -> None: ...
    def disconnect(self) -> None: ...
    def health(self) -> dict: ...                          # {"connected": bool, "latency_ms": int}
    def get_positions(self) -> list[Position]: ...
    def get_orders(self) -> list[Order]: ...
    def submit_order(self, req: OrderRequest) -> OrderResult: ...
    def cancel_order(self, order_id: str) -> bool: ...
    def get_bars(self, symbol: str, resolution: str, n: int) -> pd.DataFrame: ...
    def get_options_chain(self, symbol: str, expiry: str) -> pd.DataFrame: ...
    def get_account_summary(self) -> AccountSummary: ...
```

`Position` dataclass has `asset_class: str` field — one of `equity`, `option`, `future`, `bond`, `swap`, `fx_spot`, `fx_option` — so the unified risk aggregator handles all types.

### Implementations

| Class | Broker | Notes |
|---|---|---|
| `IBBroker` | Interactive Brokers | `ib_insync`, TWS port 7497 / Gateway 4001 |
| `AlpacaBroker` | Alpaca | REST API, US equities + crypto |
| `PaperBroker` | Simulated | Simulates fills from `trading.bars_1h`; runs on any machine |

`BROKER_TYPE` env var selects implementation at runtime via `broker_factory.py`.

---

## 6. Broker Service — FastAPI (`services/broker_service/`)

Runs on personal laptop only alongside TWS. Single `uvicorn` process.

### REST Endpoints

```
GET  /status                     — broker health + account summary
GET  /positions                  — live snapshot (direct broker call, not RDS)
POST /orders                     — submit: {symbol, side, qty, type, limit_price, strategy_id}
DELETE /orders/{order_id}        — cancel
GET  /orders                     — open orders
POST /algo/start/{strategy_id}   — activate strategy
POST /algo/stop/{strategy_id}    — deactivate strategy
GET  /algo/status                — running strategies + last signal per strategy
```

### Pre-Trade Risk Controls (`order_router.py`)

Hard blocks (reject order, return error):
- Max position size per symbol (configurable % of NAV)
- Max daily loss limit (halt all new orders if breached)
- Options delta-notional cap
- FX notional cap per currency pair
- Bond DV01 limit per position
- Duplicate order guard (same symbol + side within 30s)
- Paper mode intercept (TWS disconnected → route to PaperBroker)

### Data Sync Loop (`data_writer.py`)

- On connect: full position snapshot → `trading.positions`
- Every 60s: incremental position update
- On fill event: → `trading.trades` + `trading.orders`
- Every 5min: 1h bars for held positions → `trading.bars_1h`
- On market close: EOD 1d bars for watchlist → `trading.bars_1d`
- Every 15min (market hours): options chain for held options → `trading.options_chain`

---

## 7. The 5 Apps

### `apps/portfolio/` — P&L + Risk Dashboard

| Tab | Content |
|---|---|
| Positions | Live holdings table, unrealised P&L, cost basis, asset class breakdown |
| P&L | Daily/MTD/YTD realised + unrealised; strategy attribution waterfall |
| Risk | Portfolio Greeks (Δ, Γ, Θ, Vega), DV01, FX delta, VaR gauge, concentration heatmap |
| Options Book | Per-expiry Greek ladder, vol surface 3D chart, P&L explain waterfall |
| Fixed Income | Duration ladder, DV01 bucketing by tenor, yield curve position overlay |

### `apps/markets/` — Market Analytics

| Tab | Content |
|---|---|
| Charts | OHLCV candlestick + indicators (MA, RSI, BB), multi-timeframe |
| Vol Surface | IV rank, IV percentile, SVI-fitted surface 3D chart, term structure, skew |
| Options Cockpit | Full 4-tab options cockpit from bess-platform (embedded/extended) |
| Yield Curves | Live bootstrapped curve, historical curve overlay, Nelson-Siegel fit |
| FX | Spot rates dashboard, forward curve, FX vol surface (delta-space RR+BF) |
| Macro | Rates, FX, commodity context panels |

### `apps/screener/` — Opportunity Screener + AI Agent

| Tab | Content |
|---|---|
| Screen | Filter universe by IV rank, momentum, earnings date, sector, yield/duration criteria |
| Agent | Claude trading analyst with tools: `get_positions`, `get_vol_surface`, `get_news`, `run_screen`, `get_bars`, `get_yield_curve`, `get_fx_rates` |
| Watchlist | Monitored symbols with signal status (rule/ML/agent) and latest news |

### `apps/execution/` — Algo Control Panel

| Tab | Content |
|---|---|
| Blotter | Live order book (open/filled/cancelled), manual order entry form |
| Algos | Start/stop strategies, per-strategy P&L + signal log, param editor |
| Risk Controls | Edit pre-trade limits (max size, daily loss, DV01, FX notional) |
| Audit | Full trade log with strategy_id attribution |

**Safety:** Agent-generated signals show a confirmation panel before execution. Auto-approve is a per-strategy opt-in flag in `trading.algo_runs.params_json`.

### `apps/news/` — AI News Screener

| Tab | Content |
|---|---|
| Top Stories | AI-ranked feed filtered to positions + watchlist, one-line summaries |
| By Symbol | News timeline per holding |
| Full Feed | Unfiltered with search + source filter |
| Digest | Claude-generated daily briefing across all items |

---

## 8. Libs

### `libs/pricing/` — Extends bess-platform `libs/options/`

**Imported from bess-platform (no rebuild):**
- `black_scholes.py` — BS/B76 pricing, full Greeks, IV solver
- `smile.py` — SVI calibration from market quotes
- `structures.py` — vanilla/straddle/strangle/spreads/butterfly/condor

**New in ib-platform:**

| Module | Content |
|---|---|
| `quantlib_engine.py` | American options (binomial tree), term structure bootstrapping, day-count conventions, calendar handling |
| `kirk_margrabe.py` | Spread option: Kirk approximation (K≠0), Margrabe exchange option (K=0), per-leg Greeks including SkewVega |
| `vol_surface.py` | Multi-expiry surface: stitch per-expiry SVI slices into full (K, T) grid, interpolate IV at any point |
| `pnl_explain.py` | Daily P&L decomposition: Δ·ΔF + ½Γ·ΔF² + Vega·Δσ + SkewVega·Δskew + Θ·Δt + residual |
| `delta_hedge_sim.py` | Monte Carlo DH simulation: matched/mismatched vol scenarios, P&L distribution under different vol realization assumptions |

### `libs/fixed_income/` — Bond Analytics & Rates

| Module | Content |
|---|---|
| `bonds.py` | Clean/dirty price, YTM solver, modified duration, DV01, convexity — QuantLib BondFunctions backend |
| `yield_curve.py` | Bootstrap discount curve from deposit/swap/bond quotes; Nelson-Siegel + NSS smooth fitting; QuantLib PiecewiseYieldCurve |
| `swaps.py` | IRS pricing (fixed vs floating), par rate, swap DV01/PV01 — QuantLib VanillaSwap |
| `caps_floors.py` | Cap/floor pricing via Black model, caplet Greeks |
| `risk_fi.py` | DV01 bucketing (key-rate durations), duration hedging, z-spread, OAS |

### `libs/fx/` — FX Pricing & Risk

| Module | Content |
|---|---|
| `forwards.py` | Spot rate, forward rate (covered interest parity), forward points, cross rates |
| `vol_surface_fx.py` | FX vol surface: market quotes (10D/25D RR + BF in delta space) → convert delta to strikes → SVI calibration; Vanna-Volga approximation for exotics |
| `risk_fx.py` | Delta in base/quote CCY, gamma, vega, theta; CCY1 vs CCY2 delta decomposition |

**Note:** FX vanilla options pricing reuses `bess-platform/libs/options/black_scholes.py` with `q = foreign_rate` (Garman-Kohlhagen = BS with continuous dividend = foreign risk-free rate). No new pricer needed.

### `libs/ml/` — Quantitative Models

| Module | Content |
|---|---|
| `pca.py` | PCA on: return series (equity/FX), vol surface (3 factors: level/slope/curvature), yield curve (level/slope/curvature explains ~99%), correlation matrix regime detection |
| `factor_models.py` | Rolling OLS/Ridge/Lasso factor regression, time-varying betas |
| `regime.py` | Hidden Markov Model for market regime detection (risk-on/risk-off/trending/mean-reverting) |
| `vol_forecast.py` | GARCH/EGARCH for realized vol forecasting; Kalman filter for dynamic beta tracking |
| `price_direction.py` | RandomForest/XGBoost baseline signal; Kronos adapter for pre-trained candlestick model |
| `backtest_ml.py` | Walk-forward cross-validation for all ML models |

### `libs/risk/` — Unified Cross-Asset Portfolio Risk

| Module | Content |
|---|---|
| `greeks.py` | Aggregate book Greeks across all asset classes using pricing libs |
| `var.py` | Historical VaR, parametric VaR, Monte Carlo VaR (uses `ml/pca.py` for correlated scenario generation) |
| `scenarios.py` | Stress tests + PCA-driven shock generation; cross-asset scenario propagation |
| `margin.py` | SPAN margin estimator (futures), portfolio margin (options), initial margin for FX |

Unified `Position` dataclass with `asset_class` field routes to the correct pricing/risk function per instrument type.

### `libs/backtest/` — Strategy Validation

| Module | Content |
|---|---|
| `engine.py` | Vectorized engine (fast, pandas-based) + event-driven mode (realistic) |
| `cost_model.py` | IB commission schedule, bid/ask slippage model, funding costs |
| `metrics.py` | Sharpe, Sortino, max drawdown, profit factor, win rate, expectancy |
| `walk_forward.py` | Rolling train/test window validator |

### `libs/strategies/` — Strategy Definitions

| Module | Content |
|---|---|
| `base.py` | Strategy ABC: `generate_signals()`, `size_position()`, `on_fill()` |
| `registry.py` | Maps `strategy_id` → class, loads params from `trading.algo_runs` |
| `examples/iv_rank_straddle.py` | Sell straddle when IV rank > 80 |
| `examples/momentum_equity.py` | ML-signal-driven equity momentum |
| `examples/delta_hedge.py` | Auto delta-hedge options book |
| `examples/curve_trade.py` | Yield curve steepener/flattener via IRS or treasury spreads |
| `examples/fx_carry.py` | FX carry trade with vol filter |

---

## 9. Algo Trading — 3 Layers

```
Layer 1 — Rules (always on, fast)
  libs/signals/rule_signals.py → strategies/registry.py → order_router.py
  Examples: stop-loss, rebalance trigger, IV rank entry/exit, DV01 limit hedge
  Runs: every bar close in algo_scheduler.py

Layer 2 — ML Signals (scheduled, intraday)
  libs/ml/price_direction.py + regime.py → scores watchlist
  → writes trading.signals (source='ml')
  order_router.py checks signal table before every execution decision
  Initial models: pre-trained Kronos for equities; GARCH for vol entry timing;
                  PCA regime for FX carry filter

Layer 3 — AI Agent Oversight (on-demand + scheduled daily)
  agents/trading_analyst/ — Claude with tools:
    get_portfolio_risk(), get_signals(), get_news_digest(),
    get_vol_surface(), get_yield_curve(), get_fx_rates()
  Outputs: recommendations → trading.signals (source='agent', status='open')
  Execution gate: agent signals require manual confirmation in apps/execution/
    UNLESS strategy flag auto_approve=True (explicit opt-in per strategy)
```

**Signal flow:**
```
Rule / ML / Agent
    → trading.signals
    → algo_scheduler reads pending signals
    → order_router validates (pre-trade checks)
    → BaseBroker.submit_order() [live or paper]
    → trading.trades + trading.orders [audit trail]
```

---

## 10. Market Data Strategy

| Source | Used for | Machine |
|---|---|---|
| IB via TWS | Live positions, fills, real-time bars (held positions) | Personal laptop only |
| yfinance | EOD bars for broad universe, free | Any |
| Polygon.io | Intraday bars, options chain snapshots, news | Any |
| Alpha Vantage | FX rates, fundamentals | Any |
| FRED / central bank APIs | Yield curve data (US Treasuries, SOFR, EURIBOR) | Any |

Market data flows: IB data is written to RDS by `data_writer.py` on personal laptop. All other sources are written by `services/market_data/ingest.py` which runs on any machine.

---

## 11. News Service (`services/news/`)

- **Ingestion** (`ingest.py`): Pulls every 15min during market hours from Reuters RSS, FT RSS, CNBC RSS, Polygon news API, Alpha Vantage news. Deduplicates by URL hash.
- **Scoring** (`scorer.py`): Claude (Haiku for cost) evaluates each new item: relevance to held positions + watchlist (0–1 score), sentiment (bullish/bearish/neutral), tickers mentioned. Runs as batch job on the ingested queue.
- **Storage**: `trading.news_items` with `relevance_score`, `sentiment`, `ai_summary`, `symbols_mentioned[]`.

---

## 12. Auth-Ready Design

No auth UI built in v1 — single user, `st.secrets` for credentials.

Auth-ready pattern (identical to bess-platform):
```python
# In each app — present but bypassed until Cognito is wired
try:
    from auth.rbac import require_role
    role = require_role(["Admin", "Trader"])
except ImportError:
    role = "Admin"  # bypass in single-user mode
```

When auth is needed: add Cognito user pool to Terraform, wire in the `auth/rbac.py` module from bess-platform (shared via sys.path), remove the `except ImportError` bypass.

---

## 13. Deployment

### Local (development, either laptop)

```bash
docker-compose up   # starts all 5 apps + market_data + news ingestion
# broker_service runs separately: uvicorn services.broker_service.main:app
```

### Production (AWS ECS, using existing bess-platform Terraform patterns)

- Each of the 5 apps: one ECS Fargate task, one ECR image, one ALB target group
- `broker_service`: NOT deployed to ECS — always runs locally on personal laptop
- `services/market_data/ingest.py` and `services/news/ingest.py`: ECS scheduled tasks (EventBridge cron)
- RDS: existing bess-platform RDS, new `trading` schema only — no new DB instance
- Terraform: `infra/terraform/` in ib-platform repo, reuses bess-platform VPC, subnets, security groups

---

## 14. VIX-Specific Additions

### `libs/signals/vix.py`

```python
term_structure(m1..m8)         # futures curve → contango_pct, roll_yield_annualised
implied_vol_premium()          # VIX − 30d realised SPX vol (mean-reversion signal)
vvix_level()                   # vol-of-vol monitor and percentile rank
regime()                       # → "contango" | "backwardation" | "spike"
vix_etp_roll_cost(vxx_ratio)   # daily roll cost for VXX/UVXY from term structure
```

### `trading.vix_term_structure` (new table)

```sql
trading.vix_term_structure  -- ts_date, vix_index, m1..m8 (futures prices),
                            --   contango_pct, roll_yield_annualised,
                            --   vvix, regime, source
```
Populated daily by `services/market_data/ingest.py` from IB CFE futures data or CBOE free data feed.

### VIX panel in `apps/markets/`

| Panel | Content |
|---|---|
| VIX Curve | Futures term structure (M1–M8), contango %, historical curve overlays |
| Vol Premium | VIX vs 30d realised SPX vol — primary mean-reversion signal, historical spread |
| VVIX | Vol-of-vol level, historical percentile, term structure of VIX options |
| ETP Roll | VXX/UVXY implied daily roll cost from term structure |

**Operational note:** VIX options settle to the VIX Special Opening Quotation (SOQ), calculated once from SPX options at open on expiry day — not the regular VIX index. This creates pin risk near expiry. Managed operationally (close before expiry); no model change required. Black-76 with VIX futures as underlying remains correct.

---

## 15. Knowledge Base + Investment Advisor

This is the most strategically important component — the agent that learns, evolves, and generates reasoned investment recommendations from accumulated market intelligence and quantitative model outputs.

### Architecture Overview

Directly extends the `intl_market_common` pattern already built in bess-platform:
- `BaseConnector` ABC → market-specific document connectors
- `expert_memory_base.py` → Haiku-based insight extraction/retrieval
- `knowledge_docs` table with PostgreSQL FTS → `expert_insights` table

Key differences from the power market pattern:
1. **Document sources** span macro, rates, FX, equity, vol — broader than single-market power
2. **Agent has model tools** — can actually call pricing/risk/ML libs during reasoning, not just read data
3. **Model-derived insights** — when models produce notable results, insights are extracted and stored (new: `extract_from_model_run()`)
4. **Continuous scheduled analysis** — daily briefing generation, weekly PCA regime scan, event-triggered deep dives

---

### `services/knowledge/` (new service)

```
services/knowledge/
├── config.py              # TradingKBConfig — question sets for advisor,
│                          #   insight types, source weights
├── base.py                # Reuses intl_market_common BaseConnector + upsert_doc
│                          #   Table: trading.kb_docs (mirrors gb_knowledge_docs)
├── expert_memory.py       # Adapts expert_memory_base.py for trading domain:
│                          #   insight types: market_regime | price_driver |
│                          #   vol_signal | macro_risk | opportunity |
│                          #   strategy | model_insight
│                          #   NEW: extract_from_model_run(model_name, inputs, outputs)
│                          #   — extracts insights when model results are notable
├── daily_briefing.py      # Runs standard_questions battery against KB + models
│                          #   → structured daily briefing → stored in trading.kb_briefings
└── connectors/
    ├── fred.py            # FRED: rates, CPI, employment, GDP, yield curve data
    ├── cboe.py            # CBOE: VIX methodology docs, options market reports,
    │                      #   margin releases, regulatory notices
    ├── sec_edgar.py       # SEC EDGAR: 10-K/10-Q/8-K for held + watchlist stocks
    ├── fed_speeches.py    # Fed speech transcripts (federalreserve.gov),
    │                      #   FOMC minutes and statements
    ├── treasury.py        # US Treasury: auction results, yield curve releases
    ├── bis.py             # BIS quarterly review, working papers (rates, FX, credit)
    ├── news_rss.py        # Reuters/FT/CNBC/Bloomberg RSS — full text where available
    └── local_reports.py   # Manual PDF/Excel upload (same pattern as bess-platform)
```

### DB Tables

```sql
-- Knowledge base (mirrors intl_market pattern)
trading.kb_docs        -- id, source, doc_type, title, url, published_date,
                       --   content, fetched_at, search_vector (GIN tsvector)

trading.kb_insights    -- id, insight_text, insight_type, confidence,
                       --   source_session, source_doc_url, source_model_run,
                       --   validated_at, active, created_at
                       --   NEW col vs bess-platform: source_model_run (for model-derived)

trading.kb_briefings   -- id, briefing_date, market_section, content,
                       --   model_outputs_json, generated_at
                       --   one row per section per day (macro, rates, vol, equity, fx)
```

### Scheduled Knowledge Jobs (`services/knowledge/ingest.py`)

| Schedule | Job | Output |
|---|---|---|
| Daily 06:00 | Run all connectors → ingest new docs | `trading.kb_docs` |
| Daily 06:30 | `digest_kb_docs()` — undigested docs → Haiku insight extraction | `trading.kb_insights` |
| Daily 07:00 | `daily_briefing.py` — run standard questions + model scans | `trading.kb_briefings` |
| Weekly Mon | Run PCA regime detection on past 60d returns → extract regime insight | `trading.kb_insights` |
| Daily 08:00 | `trade_monitor.py` — scan newly closed positions → P&L explain → `extract_from_trade_outcome()` | `trading.kb_insights` |
| On backtest run | `extract_from_backtest()` called by `libs/backtest/engine.py` automatically | `trading.kb_insights` |
| On VIX spike (>20% 1d) | Trigger deep-dive: vol surface analysis + news + model run → insights | `trading.kb_insights` |

---

### `apps/advisor/` — Investment Advisor App (6th app)

The screener (`apps/screener/`) is tactical — what to trade today. The advisor is strategic — how to think about markets, build positions, and learn from history.

**Tabs:**

| Tab | Content |
|---|---|
| **Advisor** | Claude chat with full model tool access. Post-turn: Haiku extracts insights from conversation → `trading.kb_insights` |
| **Market Briefing** | Daily auto-generated briefing (stored in `trading.kb_briefings`): macro / rates / vol / equity / FX sections |
| **Knowledge Base** | Document browser: search docs by source/type/date; ingestion status; manual upload |
| **Insights Library** | Accumulated expert insights: searchable, filterable by type/confidence/date; mark as validated/inactive |
| **Model Lab** | Interactive model runner: run any lib (pricing, risk, ML) with custom inputs, view outputs, optionally extract insights |

### Claude Investment Advisor Agent Tools

The advisor has materially more tools than the power market agents — it can run models, not just read data:

```python
# Knowledge & memory
search_kb(query, sources=None)           # FTS over trading.kb_docs
get_insights(query, type=None)           # retrieve from trading.kb_insights
get_news_digest(hours=24)               # recent scored news from trading.news_items
get_briefing(section=None)              # today's kb_briefings

# Portfolio data
get_positions()                          # current holdings from trading.positions
get_portfolio_risk()                     # today's trading.portfolio_risk snapshot
get_strategy_pnl(days=30)              # strategy performance from trading.strategy_pnl
get_signals(status='open')             # open signals from trading.signals

# Market data
get_bars(symbol, resolution, n)         # from trading.bars_1d/1h
get_vol_surface(symbol)                 # from trading.vol_surface (latest)
get_options_chain(symbol, expiry)       # from trading.options_chain
get_yield_curve(curve='USD')            # from trading.yield_curves
get_fx_rates(pairs)                     # from trading.fx_rates
get_vix_term_structure()                # from trading.vix_term_structure

# Model execution (agent calls libs directly)
run_options_pricer(symbol, expiry, strike, flag)   # libs/pricing/ → price + Greeks
run_pnl_explain(position_id, date)                 # libs/pricing/pnl_explain
run_var(confidence=0.95, horizon=1)                # libs/risk/var
run_scenario(shock_dict)                            # libs/risk/scenarios
run_pca(asset_class, lookback_days=60)             # libs/ml/pca
detect_regime()                                     # libs/ml/regime (HMM)
forecast_vol(symbol, horizon_days=30)              # libs/ml/vol_forecast (GARCH)
run_backtest(strategy_id, lookback_days=90)        # libs/backtest/engine
run_kirk_margrabe(F1, F2, K, T, vol1, vol2, rho)  # libs/pricing/kirk_margrabe
run_bond_analytics(isin)                           # libs/fixed_income/bonds
run_yield_curve_fit(curve)                         # libs/fixed_income/yield_curve
```

### Learning & Evolution Mechanism

Three channels through which the advisor accumulates knowledge over time:

**1. Conversation learning** (same as bess-platform pattern)
After every advisor turn, Haiku extracts durable insights from the exchange and stores in `trading.kb_insights`. The next session loads relevant insights into the system prompt. The agent gets smarter with every conversation.

**2. Document digestion** (same as bess-platform pattern)
Daily `digest_kb_docs()` processes newly ingested documents → extracts structured insights. Knowledge base documents become queryable expert memory, not just raw text.

**3. Model-derived insight extraction** (new — not in bess-platform)
When notable model results occur, insights are extracted and stored:
```python
# Example: PCA weekly scan detects regime shift
pca_result = run_pca("equity", lookback_days=60)
if pca_result.regime_changed:
    extract_from_model_run(
        model_name="pca_regime",
        inputs={"asset_class": "equity", "lookback": 60},
        outputs=pca_result.summary,
        api_key=api_key,
        insight_type="market_regime"
    )
# Stored in trading.kb_insights with source_model_run="pca_regime"
# Next advisor session: agent knows "equity correlation regime shifted on [date]"
```

This closes the loop: the agent's recommendations improve not just from reading documents and conversations, but from what the quant models are observing in live market data.

**4. Strategy backtest learning** (new)
When a backtest completes, Haiku extracts durable strategy insights — what conditions drove performance, what regimes to avoid, what parameter sensitivities matter:

```python
extract_from_backtest(
    strategy_id="iv_rank_straddle",
    backtest_result=result,   # BacktestResult: metrics, trades, regime_breakdown
    api_key=api_key,
    insight_type="strategy_backtest"
)
# Example insight stored:
# "IV rank straddle produced Sharpe 1.8 in VIX contango regime but 0.3 in spike
#  regime — strategy should be suspended when vix_regime='spike'. Backtest 2023–2026."
```

Trigger: automatically called by `libs/backtest/engine.py` after every run. Also surfaced in Model Lab tab as "Extract insights from this backtest" button.

**5. Actual trade outcome learning** (new)
When a position closes, a post-trade analysis compares actual P&L against the signal's expectation and the model's attribution. Haiku extracts what the model got right, what it missed, and what to adjust:

```python
extract_from_trade_outcome(
    trade_id=trade_id,
    signal_source="ml",              # rule / ml / agent
    expected_pnl=signal.strength,   # directional expectation at signal time
    actual_pnl=realized_pnl,
    pnl_explain=explain_result,      # delta/gamma/vega/theta attribution
    market_context=context_snapshot, # VIX regime, yield curve shape, etc. at entry
    api_key=api_key,
    insight_type="trade_outcome"
)
# Example insight stored:
# "AAPL delta-hedge: 68% of P&L came from vega vs 15% expected — vol surface moved
#  more than GARCH forecast predicted. GARCH systematically underestimates vol after
#  FOMC announcements. Increase vega hedge size on Fed days."
```

Trigger: `services/knowledge/trade_monitor.py` — scheduled daily, watches `trading.trades` for newly closed positions (status flips from open to closed), runs P&L explain via `libs/pricing/pnl_explain.py`, calls `extract_from_trade_outcome()`.

**Complete 5-channel learning loop:**

```
Channel 1: Conversation    → Haiku extracts insights from advisor chat turns
Channel 2: Documents       → Haiku digests KB docs (FRED, CBOE, SEC, Fed, BIS...)
Channel 3: Model outputs   → PCA regime shifts, GARCH spikes → model_insight
Channel 4: Backtest runs   → Strategy condition/regime performance → strategy_backtest
Channel 5: Live trades     → Post-trade attribution vs expectation → trade_outcome

All → trading.kb_insights → injected into every advisor session system prompt
```

Over time the advisor builds a self-consistent picture: it knows which strategies work in which regimes (channel 4), whether its live recommendations are being validated by actual outcomes (channel 5), and how the current market regime relates to historical patterns (channels 2–3). The insight types `strategy_backtest` and `trade_outcome` are filtered at retrieval time — when the user asks about a strategy, relevant backtest and outcome history is surfaced automatically.

---

## 16. Data Contracts Pattern (from `options_platform_contracts.md`)



Four object types carried forward from the existing bess-platform contract vocabulary:

| Type | Examples in ib-platform | Pattern |
|---|---|---|
| Model Output | `OptionsPriceResult`, `BondAnalyticsResult`, `PnlExplainResult` | On-demand, keyed by `(run_id)` |
| Monitoring State | `trading.portfolio_risk`, `trading.vol_surface` daily snapshots | Scheduled write, agents query table |
| Recommendation | `trading.signals` with `status=open/acknowledged/resolved` | Lifecycle state, agent-created |
| Scenario | Named param sets in `trading.algo_runs.params_json` | Reusable configs |

---

## 17. Build Phases

| Phase | Scope | Outcome |
|---|---|---|
| 1 | Repo scaffold + DB schema + `services/broker/` + `broker_service` | IB connector syncing to RDS, paper mode working |
| 2 | `libs/pricing/` extensions + `libs/fixed_income/` + `libs/fx/` + VIX signals | Full cross-asset analytics lib |
| 3 | `apps/portfolio/` + `apps/markets/` (incl. VIX panel) | Dashboard + analytics live |
| 4 | `services/market_data/` + `services/news/` + `apps/news/` | Market data pipeline + news screener |
| 5 | `services/knowledge/` connectors + ingestion + expert_memory | KB pipeline running, insights accumulating |
| 6 | `apps/advisor/` — Advisor + Briefing + KB + Insights tabs | Investment advisor live, learning from day 1 |
| 7 | `libs/ml/` + `libs/backtest/` + `libs/strategies/` + Model Lab tab | ML signals + backtest + model-derived insights |
| 8 | `apps/screener/` + algo screener tools | Tactical opportunity screener |
| 9 | `apps/execution/` + algo scheduler (Layers 1–3) | Full algo trading |
| 10 | ECS deployment + Terraform | Production on AWS |

---

*Spec written by Claude Sonnet 4.6 via brainstorming skill, 2026-06-14.*
*Approved by: [pending user review]*
