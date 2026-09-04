# Handoff: Mengxi Trader — Mark-to-Market Metrics
**Date:** 2026-06-30  
**Branch:** `cost-optimisation`  
**Repo:** `C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform`

---

## What was just completed (do NOT redo)

Two features were fully built and pushed to `cost-optimisation` today:

1. **Nodal BESS Value Ranking** — adds `2h节点排名` / `4h节点排名` columns to the daily Mengxi BESS ranking PDF, plus a monthly nodal ranking page. Code is in `services/hermes/mengxi_ranking_report.py`. Hermes Docker image needs to be rebuilt and redeployed to activate (not done yet).

2. **WeCom webhook delivery** — daily ranking PDF also sent to `WECOM_RANKING_WEBHOOK_URL`. Done, included in the same Hermes image rebuild.

3. **Capacity compensation rate fix** — 大航都林/大航额日和图 = 0, 荣鑫地房子 = 280, new assets after 2026-06-27 = 280, default = 350.

**Before working on the trader, you may want to rebuild and redeploy Hermes first.** See `docs/superpowers/specs/2026-06-30-nodal-bess-value-ranking-design.md` for full spec.

---

## What to build next: Mark-to-Market on Trading Books

The user's request (verbatim): *"I want to build mark-to-market metrics on various trading books in this trader app"*

This has NOT been designed or specced yet. Start with brainstorming (`/brainstorm`).

---

## Context you need

### The Trader tab

**File:** `apps/mengxi-dashboard/app.py`  
**Tab:** Tab 8, `tab_trader` (line ~1593)  
**What it is:** A Claude agent chat UI embedded in the Streamlit app. The agent has 4 tools:

| Tool | What it does |
|---|---|
| `get_asset_pnl` | Queries `reports.bess_asset_daily_attribution` for daily P&L per asset |
| `get_dispatch_data` | Queries `marketdata.ops_bess_dispatch_15min` for 15-min dispatch |
| `get_rt_prices` | Queries `hist_mengxi_provincerealtimeclearprice_15min` for RT prices |
| `search_knowledge_base` | Vector search over uploaded docs in `staging.spot_knowledge_chunks` |

The agent system prompt is in `_build_trader_system()` (line ~1683). It auto-extracts and stores memories in `trader_memories` Postgres table.

### The 4 Inner Mongolia BESS assets

The assets managed are in `marketdata.station_master`. Their daily P&L is in `reports.bess_asset_daily_attribution`:

```sql
SELECT trade_date, asset_code,
       pf_unrestricted_pnl,      -- perfect-foresight benchmark
       pf_grid_feasible_pnl,     -- PF with grid constraints
       tt_forecast_optimal_pnl,  -- time-series forecast optimal
       tt_strategy_pnl,          -- actual strategy
       nominated_pnl,            -- what was nominated
       cleared_actual_pnl        -- what was actually cleared
FROM reports.bess_asset_daily_attribution
```

### Existing strategy comparison

`libs/decision_models/workflows/strategy_comparison.py` — 6-strategy comparison pipeline. Each strategy has a daily P&L series. The "Our BESS Portfolio" tab (Tab 3) already has P&L waterfall, strategy ranking, dispatch charts.

### Key DB tables for reference

| Table | Contents |
|---|---|
| `marketdata.md_id_cleared_energy` | 15-min Mengxi intraday cleared prices + energy (all nodes) |
| `marketdata.md_da_cleared_energy` | Day-ahead cleared prices |
| `reports.bess_asset_daily_attribution` | Daily P&L by strategy for 4 assets |
| `reports.bess_ops_pnl` | Ops actual P&L |
| `reports.nodal_pf_monthly` | NEW: monthly perfect-foresight nodal BESS value by node |
| `marketdata.station_master` | Plant metadata (name, MW, owner) |

---

## Key things to clarify with the user before building

The user said "trading books" — this is ambiguous in context. Before designing anything, ask:

1. **What are the "trading books"?** Options:
   - Spot energy arbitrage (charge/discharge spreads) — already partially tracked
   - Capacity compensation (容量补偿) — fixed monthly receipts
   - FR/regulation service — if assets provide frequency regulation
   - Physical position vs nominated position (nomination risk)
   - A "book" per asset vs one book per strategy

2. **What does "mark-to-market" mean here?**
   - Unrealised P&L vs current market prices (i.e. what would our position be worth if we liquidated now)?
   - Actual vs benchmark (strategy vs perfect-foresight)?
   - Cumulative P&L vs budget/target?

3. **Where to surface it?**
   - New analytics tab in the Trader agent (new tool for the agent)?
   - A new dashboard tab/page in the Streamlit app?
   - A panel within the existing "Our BESS Portfolio" tab?

---

## How to run the app locally

```bash
cd C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform
# Set env vars (PGURL, ANTHROPIC_API_KEY, etc.)
streamlit run apps/mengxi-dashboard/app.py --server.port 8511
```

Or via Docker (requires building the image first).

---

## Deployment

The app is deployed as `bess-mengxi-dashboard` on ECS Fargate. After changes:
```bash
# Build (no DOCKER_BUILDKIT=0 — it defeats dockerignore)
docker build -f apps/mengxi-dashboard/Dockerfile -t bess-mengxi-dashboard:vN .
# Tag and push to ECR, then update task definition
```

The task definition is `bess-mengxi-dashboard` on ECS. Always use `:latest` ECR tag (hardcoded version tags cause crash-loops).

---

## Files to read first

1. `apps/mengxi-dashboard/app.py` — the entire Streamlit app (2000 lines); Trader tab starts at line ~1593
2. `docs/superpowers/specs/2026-06-30-nodal-bess-value-ranking-design.md` — completed spec (for context on nodal data)
3. `reports.bess_asset_daily_attribution` schema — query DB to inspect
