# Market-Maker — Central Risk Book + Simulated Derivatives Venue: Design Spec

**Date:** 2026-11-14
**Status:** Draft for user approval
**Depends on:** `2026-11-14-asset-risk-restructure-design.md` steps 1–5 (position layer extracted from asset-risk and retail-risk)
**New app:** `apps/market_maker` (Streamlit front end) + `services/market_maker` (venue backend, ECS service)
**Pillar:** new, cross-pillar — consumes Trader (Pillar 3) dispatch data and strategy registry, Quant (Pillar 2) dispatch engine and option engine, Strategist (Pillar 1) spot archive

---

## 1. Intent

Two things in one app, because they share a position register and a risk engine:

1. **Central risk book.** One position register for every book on the platform, asset-side (wind, solar, BESS stations) and load-side (retail books). Exposure, MtM, VaR and Greeks are computed in one place, against one curve, with physical positions and financial positions netted. This is where the Positions & MtM and VaR & Greeks tabs from asset-risk and retail-risk go.

2. **Simulated derivatives venue.** A multi-user marketplace for forwards, futures, profile products and options on Chinese provincial power, with AI agent counterparties, running on an accelerated clock that replays real spot history. Purpose: build the trading, hedging and risk skills, and the decision models, before the Chinese power futures market opens (expected 2027). Humans and agents trade on equal footing; agents can be backed by real station data or by simulated assets.

**Why both in one app:** a human hedging 零碳46 on the venue must see the hedge land against the farm's forecast profile in the same exposure view, and agents must read the same exposure numbers a human would. Separate apps would mean two position registers.

## 2. Goals

1. One `rm_positions`-based register holds physical, bilateral and venue-traded positions for every book, with a sign and unit convention written down once.
2. Exposure per book = physical forward volume + financial positions, by delivery month and TOU block, marked against one curve.
3. MtM, VaR (historical and parametric) and Greeks (delta from everything, gamma/vega from options and BESS optionality) computed by `libs/risk`, extended, and shown per book and per portfolio.
4. A venue session: an owner creates it (province, replay start date, instrument set, roster), participants join, the owner advances the clock, orders match, expiries settle against the replayed spot index, the venue curve updates, risk re-marks.
5. AI agents with a per-agent switch: language model off = pure quant decision model or hedging strategy; on = the language model sits above the same quant model and may override, resize or hold its orders.
6. Agent books can be backed by a real station (dispatch policy = historical actual, historical nominated, or a registered strategy) or by a simulated asset (parameters + a registered strategy run against the replayed prices).
7. Runs locally end to end: local Postgres, replayed prices from the CSV extracts, LLM agents on a local Ollama endpoint (qwen3:8b) via the provider switch.

## 3. Non-goals

- Not a real exchange connection. No order routing to any external venue.
- No venue data flowing back into the realised layers (bills remain the truth for what happened).
- No 2027 futures contract specifics. The instrument model is parametric; the real contract becomes a row when the exchange publishes its spec.
- No intraday clock in v1. The tick is one simulated day.
- No RetailBook object model work; retail positions migrate as rows, the retail realised layer is untouched.
- No multi-session concurrency beyond "several sessions may exist, each is independent".

## 4. Anti-goals

- A venue price that is not anchored: if the curve can drift arbitrarily from the replayed spot it is a game, not training. Settlement against the replayed index is the anchor; see §9.
- Agents that cannot be compared: the LLM-on and LLM-off modes must run on the same quant model, same mandate, same data, so the delta is attributable to the model.
- A position register that silently mixes MW and MWh, or discharge sign conventions. See §6.
- Streamlit hosting the venue loop. It cannot; see §5.
- Per-tick LLM calls for every agent. Cost and latency would make a session unusable; the LLM layer is opt-in per agent and rate-limited per tick.

## 5. Architecture

```
apps/market_maker/            Streamlit UI (ECS service bess-market-maker, port 8530, /market-maker/*)
  app.py                      lazy tabs: Positions · Exposure & MtM · VaR & Greeks · Curves · Venue · Agents · Risk Agent
services/market_maker/        venue backend (ECS service bess-market-maker-venue, FastAPI, port 8531, internal only)
  api.py                      REST: sessions, orders, book, clock, agents
  engine/
    matching.py               price-time priority CLOB + call auction
    clock.py                  session clock, tick = one simulated day
    settlement.py             expiry settlement vs replayed index, cash flows to positions
    curve.py                  venue curve from trades/mids; fallback to reference curve
  agents/
    base.py                   Agent interface: observe → decide → orders
    quant/                    market_maker, hedger, speculator, bess_dispatch_hedger
    llm_overlay.py            optional language-model layer over a quant agent
    providers.py              LLM provider switch: anthropic | ollama (OpenAI-compatible) | none
  replay/
    index.py                  spot index per simulated day from spot_prices_hourly (or CSV extract)
    physical.py               physical profile per book-day: real (ops/nominations/strategy registry) or simulated
libs/positions/               position model + register (schema owner for rm_positions going forward)
libs/risk/                    existing mtm/var/greeks, extended for curves, profiles, options, physical legs
libs/curves/                  curve object: province × delivery month × TOU block → ¥/MWh, with sources
```

The UI talks to the backend over HTTP. State is Postgres, schema `venue`. The backend is one process per deployment; a session's tick is a single transaction so two owners cannot interleave ticks.

**Network placement (my call, corrected after builder check):** there is no internal ALB or service discovery in this account, so the venue API gets a path rule on the existing public ALB, `/market-maker-api/*`, **without** the Cognito authenticate action, guarded by (a) a shared secret in an `X-Venue-Key` header, generated once and injected into both task definitions as `VENUE_API_KEY`, and (b) the backend rejecting any request without it. This is one new ALB rule and target group, the same resources every other service has. Local mode uses `VENUE_API_URL=http://localhost:8531` and the same header. The "nothing new in infrastructure primitives" claim holds under this choice; an internal ALB would not.

**Why a backend:** multiple users see one order book and one clock; agents act between ticks; a tick must be atomic. Streamlit reruns per user per widget event and has no shared process state. The backend is the first non-Streamlit user-facing service on the platform.

## 6. Position model (`libs/positions`)

`rm_positions` is reused with additions, not replaced. Rows get:

- `source TEXT CHECK IN ('physical','bilateral','manual')`
- `tou_block TEXT CHECK IN ('base','peak','offpeak','valley', NULL)`; NULL = base

`venue.positions` has the same columns plus `session_id`, `venue_book_id`, `instrument_id`, and `source` additionally allows `'venue'`.

Conventions, written once in `libs/positions/conventions.py` and asserted in tests:

- Volume is MWh over the delivery period. `direction` is the **risk side**, not the trade a book intends to execute: `'buy'` = long power (gains when price rises), `'sell'` = short power. Delta = +volume for buy, −volume for sell, as `libs/risk/greeks.py` already computes. Net exposure = physical + financial, signed.
- Physical rows are generated, not entered: one row per book per delivery month per TOU block. **Assets are long:** wind/solar expected generation and BESS expected net discharge are `buy` rows. **Load is short:** a retail book's customer-contracted volume is a `sell` row, with `price_cny_mwh` = the fixed contract price for fixed-price customers and NULL for indexed/pass-through customers (so fixed-price retail MtM survives the move out of retail-risk; `services/retail_risk/mtm.py` already treats procurement buys as long, which is consistent). A wind farm's forward sale is then a `sell` row that nets its `buy` physical row toward zero, and a hedger's hedge ratio = −financial/physical per month. Regenerated by a job when forecasts or contracts change; `source='physical'` rows are never user-edited. (Corrected 2026-11-14 after builder check: the first draft had the sides inverted.)
- BESS optionality is **not** a volume row. It enters Greeks as per-month delta and vega of the peak/offpeak spread option, scaled by capacity, added to the book's Greeks. The physical volume row for BESS is expected net discharge only. **Which model supplies those Greeks is the user's decision (§16):** the DP option engine (`services/option_engine`) outputs value stacks but no Greeks; the only BESS Greeks in the repo are the Margrabe spread-call strip in `libs/decision_models/bess_spread_call_strip.py` (strip-level, finite-difference).
- Prices are ¥/MWh. Curve points are ¥/MWh. `price_cny_kwh` columns in older tables are converted at the boundary.

Retail positions already in `rm_positions` migrate with `source='bilateral'`; customer contracts from `rm_customer_contracts` become the load books' physical `sell` rows as above.

**One model, two stores (my call).** `libs/positions` is the single position model and register API. It has two backing stores: `marketdata.rm_positions` for the central (production) book, and `venue.positions` for session books. A session never writes to `rm_positions`. A session creates its own `venue.books` rows, each keyed to an `asset_code` (real backing) or a simulated-asset spec, and its physical rows are generated as of the session's `sim_date` into `venue.positions`. This is what lets two sessions replay the same station over different years without touching the production book, and it removes the `session_id` column from `rm_positions` (§6 first draft). The "same exposure view" in §1 is the same code and the same frame shape over a different store, selected by the UI's session context.

## 7. Curves (`libs/curves`)

A curve is `province × delivery_month × tou_block → ¥/MWh`, with a `source` per point: `spot_seasonal` (trailing same-month spot average from `spot_prices_hourly`, shaped by TOU), `traded` (volume-weighted prices of the monthly/annual channels in `rm_position_volumes` for the retail provinces 冀南, 安徽, 浙江, 山东, and of 零碳46's confirmations in `marketdata.wind_trades` for 蒙西), `manual` (upload), `venue` (session marks), and `exchange` reserved for monthly 竞价/挂牌 clearing results **once such data is ingested; none exists in the platform today** (corrected 2026-11-14: the first draft claimed it did). The v1 reference curve for a province is: `traded` where it exists, else `spot_seasonal`, with `manual` overrides. `rm_forward_curves` is kept as the storage table with `tou_block` added. Whether a 中长期 clearing-price source exists outside the platform is a question for the user (§16).

The venue curve for a session is the session's own marks (§9) and is used for MtM inside that session. Outside sessions, the central risk book marks against the reference curve.

## 8. Risk engine (`libs/risk`, extended)

- `exposure(book, curve, as_of)` → frame by delivery month × TOU: physical MWh, financial MWh, net MWh, mark, MtM.
- `mtm` extended to curve objects and profile products (a profile is a vector of block volumes).
- `var`: historical simulation over the replayed index history for the session (or the real history for the central book) and parametric delta-normal; horizon 1d and 10d; 95/99.
- `greeks`: delta = net MWh per month; gamma/vega from `libs/options` for option positions and from the option engine for BESS books; theta for options.
- All functions take explicit inputs and return frames; no DB access inside `libs/risk`.

## 9. Venue

**Instruments** (table `venue.instruments`, rows not code): `kind` (forward | future | profile | option), `province`, `delivery_start`, `delivery_end`, `tou_block`, `settlement` (cash vs replayed index | physical-notional), `tick_size`, `lot_mwh`, and for options `strike, option_type, expiry, underlying_instrument_id`. A session picks an instrument set at creation; a default set is the next 12 delivery months as base forwards plus 4 quarterly base, plus peak/offpeak monthly for the next 3 months, plus monthly ATM calls and puts on the next 3 months.

**Matching:** price-time priority continuous order book per instrument; limit and market orders; cancel. A call auction can be run at a tick boundary for a chosen instrument set (uniform clearing price maximising matched volume), mirroring 月度集中竞价. Fills create two `rm_positions` rows (`source='venue'`) and a `venue.trades` row.

**Clock:** `venue.sessions.sim_date`. `advance(session_id)` runs, in one transaction: (1) roll physical profiles for the new day for every book; (2) settle instruments whose delivery day is reached against the replayed index for that day (daily cash settlement over the delivery period, i.e. each day of a monthly forward settles 1/n of volume against that day's index); (3) agents observe and submit orders in roster order with a per-tick order budget; (4) continuous matching processes the submitted orders; (5) curve marks update (last trade, else mid, else carry); (6) risk snapshot per book written to `venue.risk_snapshots`. Only the owner may call `advance`. Humans trade between ticks; their orders rest in the book.

**Replayed index:** province chosen at session creation; the index for simulated day D is the hourly `spot_prices_hourly` RT series for real date D, aggregated to TOU blocks. Local mode reads the same series from the CSV extracts. If a day is missing, the tick refuses with the date named (no silent carry).

**Anchoring:** because every contract cash-settles against the replayed index, a venue mark that departs from the forward-realised index loses money to any agent that trades toward it. The default market-maker agent quotes around the reference curve for the session's province, which is the anchor between trades.

**Options (my call):** monthly options are Asian-style on the delivery month: they expire at the end of the delivery month and settle against the realised monthly average of the replayed index for their TOU block, so they are anchored like the forwards. Pricing and Greeks use Black-76 on the forward with the standard average-price volatility adjustment (builder's choice of Levy or Turnbull-Wakeman). European options on the venue curve are out of v1 because their settlement would not be anchored (§4).

## 10. Agents

`Agent.decide(obs: Observation) -> list[Order]` where `Observation` carries sim_date, the agent's book exposure frame, the order book top-N for its instrument set, the venue curve, the reference curve, its mandate, and its physical profile for the next N days.

Quant agents in v1: `market_maker` (two-sided quotes around reference ± spread, inventory skew), `hedger` (works toward a hedge-ratio target per month, limit orders at or inside mid), `speculator` (momentum/mean-reversion on the venue curve vs reference, position-limited), `bess_dispatch_hedger` (reads its dispatch policy's expected net discharge and hedges a configurable share). Each has a `Mandate`: limits (net MWh per month, VaR cap), targets (hedge ratio), style parameters.

**LLM overlay** (`llm_overlay.py`): when enabled on an agent, the quant agent's proposed orders plus the observation are rendered into a prompt; the model must return a JSON decision `{action: accept|modify|hold, orders: [...], rationale}` validated against a schema; invalid output falls back to the quant orders and is logged. Rate limit: at most one call per agent per tick. Provider via `LLM_PROVIDER=anthropic|ollama|none`: `anthropic` uses `shared.anthropic_client` (ECS only, geo-block); `ollama` uses the OpenAI-compatible endpoint `OLLAMA_BASE_URL` (default `http://localhost:11434/v1`) with `OLLAMA_MODEL` (default `qwen3:8b`). The decision schema is small enough for an 8B model; rationale text is stored for review, never acted on.

**Backing:** `venue.agents.backing = real | simulated`. Real: `asset_code` from the registry; physical profile per day from the dispatch policy for BESS: `actual` = `bess_actual_96` (fallback `ops_bess_dispatch_15min`), `nominated` = `bess_nomination_96`, `strategy:<model_version>` = the stored series in `nodal_strategy_daily` for that date, read as-is, never re-run; from `wind_dispatch_15min` for 零碳46; from `rm_position_volumes` hourly settled volumes for load books. Simulated: `asset_type, capacity_mw, duration_h, rte, province`; BESS profile from a dispatch **policy** run day by day against a forecast, not against the realised prices: `forecast_lp` = `build_forecast(model='ols_rt_time_v1')` on the replayed series to sim_date, then `compute_dispatch_from_15min_prices` on the forecast (same convention as asset-risk's forecast-optimal leg); `oracle` = perfect-foresight LP on the realised day, available but labelled as an upper bound and excluded from the LLM-on/off comparison. Wind/solar/load simulated profiles are a scaled historical shape from the same province. (`strategy_experiments` is a leaderboard, not callable code; my call 2026-11-14.)

**LLM-on vs LLM-off comparison** is a first-class view: two agents with identical mandate and backing, one with the overlay, same session; the Agents tab shows their P&L, VaR usage and fill quality side by side.

## 11. Users and roles

Cognito as elsewhere; roles `Admin`, `Trader`, `Quant`, `RiskOfficer`. Session owner = creator or Admin. Any role may join a session and trade its assigned book. Local mode: `AUTH_MODE=dev` as in the portal.

## 12. Local mode

`docker compose` with Postgres; `scripts/market_maker/seed_local.py` loads the spot CSV extracts for a chosen province and seeds the registry, instruments and a demo roster. `LLM_PROVIDER=ollama`. The venue backend and the Streamlit UI run as two local processes. Target: one session, 3 agents, 365 ticks in under 10 minutes on the MacBook with LLM off.

The existing offline extract (`data/offline/spot_prices_hourly_90d.csv.gz`) is 90 days. Phase C includes an ECS run-task extract script producing `spot_prices_hourly_<province>_<year>.csv.gz` for 蒙西 for the most recent full year, plus `bess_nomination_96` / `bess_actual_96` for the same window (already extracted). Other provinces are added on demand with the same script.

## 13. Build order

| Phase | Deliverable | Verify |
|---|---|---|
| A | `libs/positions` (conventions, register, physical-row generator) + `libs/curves` (reference curve from exchange + spot_seasonal) + `libs/risk` extensions | unit tests on conventions, exposure netting, curve build on a fixture province |
| B | `apps/market_maker` v1: Positions, Exposure & MtM, VaR & Greeks, Curves tabs over real books (asset + retail), Risk Agent tab; retail/asset position tabs retired (restructure step 5). The old `tab_var.py` logic (delta from net MWh, σ from `rm_position_volumes` daily means) is **replaced** by the new `libs/risk`, not preserved. Risk Agent tools v1: `get_asset_list`, `get_exposure(book_id)`, `get_mtm(book_id)`, `get_var(book_id)`, `get_greeks(book_id)`, `get_curve(province)` | real books show physical + bilateral exposure; VaR numbers match a notebook recompute of the same `libs/risk` functions on a frozen position snapshot |
| C | Venue backend: sessions, instruments, matching, clock, settlement, curve; UI Venue tab; no agents | 2 humans trade in one session locally; a month expires and settles to the correct cash |
| D | Quant agents + mandates + backing (real and simulated); Agents tab | 365-tick local run; agent P&L reconciles to settlement cash flows |
| E | LLM overlay + providers (ollama local, anthropic on ECS); comparison view | same-mandate pair runs; invalid model output falls back and is logged |
| F | ECS deploy of both services, Terraform, portal link | session works across two Cognito users |

Phases A–B are usable without the venue. Phase C is the first point at which the user's "play around locally" is possible without agents; D adds agents; E adds qwen3.

## 14. Left to the builder

- FastAPI vs a thin stdlib server for the venue API; any choice that gives atomic tick transactions is fine.
- Exact CLOB data structure; correctness over speed, tens of orders per tick.
- Agent order budget per tick and market-maker spread defaults.
- TOU block definitions per province (take from the existing spot-market TOU tables where present, else 蒙西 defaults).
- Risk snapshot retention.
- Whether `venue.*` lives in its own Postgres schema or in `marketdata` with a `venue_` prefix (recommend own schema).

## 15. Open questions deferred to later phases

- Intraday ticks and the DA/RT two-settlement structure inside a simulated day.
- Fleet-response feedback: should agents' venue trades move the replayed index? No in v1 (index is exogenous).
- Multi-province sessions and inter-provincial spreads.
- The real 2027 futures contract: exchange, underlying index, settlement method — unknown until published.

## 16. Decisions taken by the user (2026-11-14)

1. **BESS Greeks model** (§6, §8): adapt the Margrabe spread-call strip (`libs/decision_models/bess_spread_call_strip.py`) and split it by delivery month for v1; finite-differenced Greeks from the DP option engine are a later upgrade.
2. **Forward-curve anchor** (§7): no exchange clearing-price series exists outside the platform. The `traded` source is built from the two trade datasets that do exist: retail books' channel prices in `rm_position_volumes` (冀南, 安徽, 浙江, 山东) and 零碳46's medium/long-term trade confirmations in `marketdata.wind_trades` (蒙西, monthly delivery slices by 品种). Curves start from those provinces only; `exchange` stays reserved.
