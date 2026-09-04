# International Markets App — Handoff Document

**Branch:** `cost-optimisation`  
**Repo:** MonteCarlo79/invest-etrm-intel  
**Last commit:** `83784c8` — feat(intl-markets): add shared base phases 1–4

---

## Goal

Replicate the GB BESS Market Intelligence Streamlit app (`apps/gb-market/app.py`, 3 686 lines,
port 8508) for four new Modo Energy markets: **Australia (NEM)**, **ERCOT (Texas)**,
**PJM (US East)**, **CAISO (California)**. One ECS service per market, full feature parity
where Modo API data is available.

---

## What Is Done (phases 1–4, committed)

### Phase 1 — `services/intl_market_common/`

| File | Purpose |
|------|---------|
| `market_config.py` | `MarketConfig` dataclass — per-market parameter bag |
| `modo_ai_base.py` | `ModoAIConnector(cfg)` — Playwright distillation from modoenergy.com/home |
| `expert_memory_base.py` | `digest_kb_docs`, `get_insights`, `inject_memory`, `extract_insights` |
| `advanced_retrieval_base.py` | `retrieve_for_agent` — HyDE → OR-FTS → rerank |

### Phase 2 — Per-market knowledge packages (× 4)

`services/{au,ercot,pjm,caiso}_knowledge/`
- `config.py` — `MARKET_CONFIG` instance with 8 standard + 6 foundational + 6 research questions
- `ingest.py` — KB orchestrator: creates `{prefix}knowledge_docs` table, runs `ModoAIConnector`

### Phase 3 — `services/gb_knowledge/config.py`

GB `MarketConfig` instance (questions sourced from existing `modo_ai.py` constants).
Needed so GB app can also use the common `MarketConfig` dataclass.

### Phase 4 — Modo REST API ingestion (× 4)

`services/modo_energy/{au,ercot,pjm,caiso}_ingestion.py`
- Tables created: `{mkt}_bess_assets`, `{mkt}_bess_daily_index`, `{mkt}_bess_monthly_index`,
  `{mkt}_bess_leaderboard`, `{mkt}_spot_price`, `{mkt}_ancillary_results`, `{mkt}_ingestion_log`
- `run_ingestion(start, end, only=None)` — callable for backfill and scheduler
- `_try_get(client, path, params)` — graceful 404/403 fallback (returns `None`)

---

## What Remains (phases 5–8)

| Task | Description |
|------|-------------|
| **Phase 5** | `services/intl_market_common/app_template.py` — `run_market_app(cfg, _app_file)` |
| **Phase 6** | Refactor `apps/gb-market/app.py` to thin ~30-line wrapper |
| **Phase 7** | `apps/{au,ercot,pjm,caiso}-market/` — `app.py` + `daily_report.py` + `Dockerfile` × 4 |
| **Phase 8** | Terraform ECS resources × 4 + `apps/portal/app.py` cards × 4 |

**The critical blocker is Phase 5.** Once `app_template.py` exists, phases 6–7 take ~1 hour.

---

## Architecture: How the Agent + KB Works (GB as reference)

### Knowledge Pipeline

```
Modo AI (Playwright)           Public sources
    ↓                               ↓
ModoAIConnector.fetch()     elexon/entso_e/timera/modo/meteologica
    ↓                               ↓
          {prefix}knowledge_docs  (PostgreSQL, FTS index)
                    ↓
            digest_kb_docs()   ← Claude Haiku extracts 3–7 insights per doc
                    ↓
          {prefix}expert_insights  (structured, confidence-ranked)
```

**Tables involved:**
- `intl_market.{prefix}knowledge_docs` — raw KB docs (TEXT, GIN `search_vector` TSVECTOR column)
- `intl_market.{prefix}expert_insights` — digested insights (insight_text, insight_type, confidence, active)

**Key functions (`services/intl_market_common/expert_memory_base.py`):**
```python
digest_kb_docs(api_key, table_prefix, market_name, limit=50)
# Reads undigested docs → Claude Haiku → stores insights

get_insights(query, table_prefix, limit=5)
# OR-based FTS on expert_insights → returns top hits

inject_memory(insights, market_name) → str
# Formats insights into a system-prompt block

extract_insights(user_msg, agent_reply, api_key, table_prefix, market_name)
# Extracts reusable insights from a chat turn → stores to expert_insights
```

### Advanced Retrieval (HyDE + OR-FTS + Rerank)

`services/intl_market_common/advanced_retrieval_base.py`:

```
User query
    ↓
hyde_expand(query, api_key, cfg)       ← Claude Haiku generates hypothetical answer
    ↓
_search_or(conn, expanded_query, prefix)   ← OR-based FTS on {prefix}knowledge_docs
    ↓
rerank(query, candidates, api_key, top_k)  ← Claude Haiku cross-encoder scoring
    ↓
formatted context block (returned to agent as tool result)
```

Called by the Strategist agent's `search_knowledge_base` tool.

### Agent Architecture (Strategist + Quant)

Both agents use a **tool-use loop** (`_run_agent_turn`):
```python
while True:
    resp = claude.messages.create(model="claude-sonnet-4-6", tools=tools, messages=messages)
    if resp.stop_reason == "end_turn": return text
    for block in resp.content:
        if block.type == "tool_use":
            result = dispatch_fn(block.name, block.input)
            messages.append(tool_result)
```

**Strategist tools** (GB): `get_system_price`, `get_epex_prices`, `get_ancillary_results` (DX),
`get_market_summary`, `get_bess_leaderboard`, `get_bess_revenue_index`, `get_bess_assets`,
`search_knowledge_base`

**Strategist tools** (non-GB): `get_spot_price` (queries `{prefix}spot_price`),
`get_ancillary_results` (queries `{prefix}ancillary_results`), plus the 4 BESS tools + KB search

**Quant tools** (all markets): `get_bess_daily_index`, `get_bess_monthly_index`,
`get_leaderboard`, `get_asset_database`, `estimate_irr`

### Session Persistence

Chat history stored in `intl_market.{prefix}analyst_sessions` (session_id → JSONB messages).
On page load, users can resume a previous session. The session_id is a UUID in `st.session_state`.

### Knowledge Gap Interview

The Strategist has a "Teach the Agent" flow:
1. **Generate** — Claude Haiku audits expert_insights + knowledge_docs → produces 5 targeted gap questions
2. **Modo AI first** — `distill_gap_questions(questions, cfg)` fires each question at Modo AI via Playwright → stores answers as new knowledge_docs → digests into insights
3. **User Q&A** — any questions Modo couldn't answer are presented to the user in sequence
4. **Store** — user answers stored as `confidence='high'` insights in `{prefix}expert_insights`

---

## DB Table Schemas

### Common tables (all markets, schema by prefix)

```sql
-- Raw KB documents
intl_market.{prefix}knowledge_docs (
    id SERIAL PRIMARY KEY,
    source TEXT, doc_type TEXT, title TEXT, url TEXT UNIQUE,
    published_date DATE, content TEXT,
    fetched_at TIMESTAMPTZ DEFAULT NOW(),
    search_vector TSVECTOR GENERATED ALWAYS AS (
        to_tsvector('english', coalesce(title,'') || ' ' || left(content,100000))
    ) STORED
)

-- Expert insights (digested from KB docs or extracted from conversations)
intl_market.{prefix}expert_insights (
    id SERIAL PRIMARY KEY,
    insight_text TEXT, insight_type TEXT, confidence TEXT,
    source_session TEXT, active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
)

-- Chat session persistence
intl_market.{prefix}analyst_sessions (
    session_id TEXT PRIMARY KEY,
    messages JSONB NOT NULL DEFAULT '[]',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
)

-- Ingestion audit log
intl_market.{prefix}ingestion_log (
    id SERIAL PRIMARY KEY, run_at TIMESTAMPTZ DEFAULT NOW(),
    trigger TEXT, date_from DATE, date_to DATE,
    status TEXT, rows_ingested JSONB, error_msg TEXT, duration_seconds NUMERIC
)

-- BESS data (from Modo API)
intl_market.{prefix}bess_assets (asset TEXT, history_table TEXT, date_from DATE, date_to DATE, value TEXT)
intl_market.{prefix}bess_daily_index (settlement_date DATE, market TEXT, revenue_permw NUMERIC, revenue_permwh NUMERIC, duration NUMERIC, PRIMARY KEY(settlement_date, market))
intl_market.{prefix}bess_monthly_index (year_month TEXT, market TEXT, revenue_permw NUMERIC, ...)
intl_market.{prefix}bess_leaderboard (asset TEXT, settlement_date DATE, market TEXT, revenue NUMERIC, rated_power NUMERIC, energy_capacity NUMERIC)
intl_market.{prefix}spot_price (settlement_date DATE, settlement_period INT, region TEXT, spot_price NUMERIC)
intl_market.{prefix}ancillary_results (settlement_date DATE, service TEXT, region TEXT, clearing_price NUMERIC, volume_mw NUMERIC)
```

### GB-only tables (different schema from generic)

```sql
-- GB leaderboard has settlement_period in PK + revspermw/revspermwh columns
intl_market.gb_bess_leaderboard (settlement_date DATE, settlement_period INT, asset TEXT, market TEXT, revenue NUMERIC, revspermw NUMERIC, revspermwh NUMERIC, rated_power NUMERIC, ...)

-- GB monthly index uses DATE column named 'month' (not TEXT 'year_month')
intl_market.gb_bess_monthly_index (month DATE, market TEXT, duration TEXT, revenue_permw NUMERIC, ...)

-- GB assets has rich metadata (lat/long, gsp, developer, valid_from, etc.)
intl_market.gb_bess_assets (asset TEXT, history_table TEXT, value TEXT, valid_from DATE, ...)

-- GB-only market data tables
intl_market.gb_system_price (date DATE, settlement_period INT, system_price NUMERIC)
intl_market.gb_niv (date DATE, settlement_period INT, niv NUMERIC)
intl_market.gb_epex_da_hh (delivery_date DATE, settlement_period INT, price NUMERIC, daily_baseload NUMERIC, ...)
intl_market.gb_dx_results (efa_date DATE, efa INT, service TEXT, clearing_price NUMERIC, cleared_volume NUMERIC)
intl_market.gb_fuel_mix (settlement_date DATE, settlement_period INT, gas_mw NUMERIC, ...)
intl_market.gb_pricing_results (settlement_date DATE, asset_name TEXT, options_value_gbp_per_mw NUMERIC, pf_actual_da_pnl_gbp NUMERIC, ...)
```

---

## Market Configuration Table

| Market | code | table_prefix | port | currency | timezone | system_operator | ancillary_label | Modo prefix |
|--------|------|-------------|------|----------|----------|-----------------|-----------------|-------------|
| Great Britain | gb | gb_ | 8508 | £ | Europe/London | National Grid ESO | DC/DM/DR | /gb/modo |
| Australia (NEM) | au | au_ | 8509 | A$ | Australia/Sydney | AEMO | FCAS | /au/modo |
| ERCOT (Texas) | ercot | ercot_ | 8510 | $ | US/Central | ERCOT | Reg/RRS/ECRS | /ercot/modo |
| PJM (US East) | pjm | pjm_ | 8511 | $ | US/Eastern | PJM | Reg/Sync | /pjm/modo |
| CAISO (California) | caiso | caiso_ | 8512 | $ | US/Pacific | CAISO | Reg/Spin | /caiso/modo |

---

## How to Write `app_template.py` (Phase 5 — the remaining blocker)

This is `services/intl_market_common/app_template.py`. It must contain `run_market_app(cfg, _app_file=None)`.

### File structure

```python
"""Shared market intelligence app template."""
# 1. Imports (psycopg2, pandas, plotly, streamlit, anthropic, etc.)

# 2. Module-level @st.cache_resource / @st.cache_data functions
#    ALL must have `prefix: str` as first param for cache isolation between markets

@st.cache_resource(ttl=3600)
def _get_conn(): ...        # single shared DB connection

def _conn(): ...            # with reconnect logic

def _query(sql, params=None): ...

@st.cache_data(ttl=60)
def _load_memories(app_key): ...

@st.cache_data(ttl=60)
def _search_knowledge(prefix, query, sources=None, limit=8): ...
    # OR-based FTS on intl_market.{prefix}knowledge_docs

@st.cache_data(ttl=300)
def _knowledge_doc_counts(prefix): ...

@st.cache_data(ttl=300)
def _get_daily_index_range(prefix, start, end): ...
    # SELECT settlement_date, market, revenue_permw, revenue_permwh
    # FROM intl_market.{prefix}bess_daily_index
    # WHERE settlement_date BETWEEN %s AND %s AND duration = '*'

@st.cache_data(ttl=300)
def _get_monthly_index_range(prefix, start, end): ...
    # GB: column 'month', DATE comparison
    # Others: column 'year_month', TEXT comparison
    # → alias as 'month' so downstream code is uniform

@st.cache_data(ttl=300)
def _get_leaderboard_range(prefix, start, end, top_n=20): ...
    # GB: multi-CTE query joining gb_bess_leaderboard (has settlement_period) + gb_bess_assets (valid_from)
    # Others: simpler query on {prefix}bess_leaderboard + {prefix}bess_assets (date_from)

@st.cache_data(ttl=3600)
def _get_assets(prefix): ...
    # GB: rich schema (lat/lon, gsp, developer, etc.)
    # Others: simple schema (just asset, value, history_table, date_from)

@st.cache_data(ttl=300)
def _get_asset_revenue_map(prefix, start, end, market): ...

@st.cache_data(ttl=300)
def _get_spot_price_range(prefix, start, end): ...
    # SELECT settlement_date, settlement_period, region, spot_price
    # FROM intl_market.{prefix}spot_price WHERE ...

@st.cache_data(ttl=300)
def _get_ancillary_range(prefix, start, end): ...
    # SELECT settlement_date, service, clearing_price, volume_mw
    # FROM intl_market.{prefix}ancillary_results WHERE ...

# GB-only cached functions (only called when cfg.code == "gb"):
@st.cache_data(ttl=300)
def _get_system_price_daily(start, end): ...
@st.cache_data(ttl=3600)
def _get_system_price_hourly(start, end): ...
@st.cache_data(ttl=3600)
def _get_epex_hourly(start, end): ...
@st.cache_data(ttl=3600)
def _get_epex_range(start, end): ...
@st.cache_data(ttl=300)
def _get_dx_range(start, end): ...
@st.cache_data(ttl=3600, show_spinner=False)
def _get_fuel_mix_daily_dates(dates): ...
@st.cache_data(ttl=3600, show_spinner=False)
def _get_bidding_space_hh_dates(dates): ...
@st.cache_data(ttl=3600, show_spinner=False)
def _get_epex_hh_dates(dates): ...
@st.cache_data(ttl=1800, show_spinner=False)
def _get_pricing_table(start, end, top_n=20): ...
@st.cache_data(ttl=1800, show_spinner=False)
def _get_dispatch_comparison(asset, settlement_date): ...
@st.cache_data(ttl=300)
def _get_pricing_missing_dates(start, end): ...
@st.cache_data(ttl=300)
def _get_fuel_mix_missing_dates(start, end): ...

@st.cache_data(ttl=300)
def _table_counts(prefix): ...
    # Queries coverage for {prefix}bess_* tables
    # GB also adds gb_system_price, gb_niv, gb_epex_da_hh, gb_dx_results, gb_pricing_results

@st.cache_data(ttl=30)
def _get_ingestion_logs(prefix, limit=20): ...
    # SELECT ... FROM intl_market.{prefix}ingestion_log ORDER BY run_at DESC

@st.cache_resource
def _start_scheduler(code, name, prefix, app_tz, app_file):
    # BackgroundScheduler(timezone="Asia/Singapore")
    # Jobs for ALL markets:
    #   _daily_market_job (03:00): run_ingestion or run_gb_backfill
    #   _daily_knowledge_job (03:30): run_knowledge_ingest from {code}_knowledge.ingest
    #   _modo_ai_job (04:00): ModoAIConnector(cfg).run(conn)
    #   _kb_digest_job (03:45): digest_kb_docs from expert_memory_base
    #   _daily_report_job (06:00): load daily_report.py from app_file directory
    # GB-only jobs:
    #   _pricing_batch_job (04:30): load pricing_batch.py
    #   (fuel mix ingest is part of _daily_market_job for GB)

# 3. Constants
_KNOWN_MANUFACTURERS = {"BYD", "CATL", "Samsung SDI", "LG Energy Solution"}
_REV_COLOR_SCALE = [[0, "#d73027"], [0.5, "#fee08b"], [1, "#1a9850"]]

# 4. Main function
def run_market_app(cfg: MarketConfig, _app_file: str | None = None) -> None:
    prefix = cfg.table_prefix
    app_key = cfg.app_key
    currency = cfg.currency_sym
    ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
    client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
    _app_dir = pathlib.Path(_app_file).parent if _app_file else pathlib.Path(__file__).parent

    # 4a. Ensure tables exist (create if not)
    # 4b. Memory helpers (_save_memory, _delete_memory using marketdata.agent_memory)
    # 4c. Session helpers (_save_session, _load_session, _list_recent_sessions)
    # 4d. KB / upload helpers (_ingest_url, _ingest_uploaded_file, _log_ingestion_run)
    # 4e. Ingestion job runners
    #       def _run_ingestion_job(date_from, date_to, trigger):
    #           if cfg.code == "gb":
    #               from services.modo_energy.gb_ingestion import run_gb_backfill
    #               run_gb_backfill(date_from, date_to)
    #           else:
    #               mod = importlib.import_module(f"services.modo_energy.{cfg.code}_ingestion")
    #               mod.run_ingestion(date_from, date_to)
    #       def _run_knowledge_ingest_job(only=None, trigger="manual"):
    #           mod = importlib.import_module(f"services.{cfg.code}_knowledge.ingest")
    #           return mod.run_knowledge_ingest(only=only, verbose=False)
    # 4f. Expert memory + interview functions
    #       _generate_interview_questions() → Claude Haiku audits {prefix}expert_insights
    #       _store_interview_answer(q, a, topic) → high-confidence insight
    #       _extract_memories(user_msg, reply) → agent memory items
    # 4g. Strategist system prompt
    #       GB: uses _GB_STRATEGIST_BASE_SYSTEM (verbatim from existing app.py)
    #       Others: dynamic prompt built from cfg.name, cfg.system_operator, cfg.ancillary_label etc.
    #       Both: inject expert insights via get_insights(query, prefix) + inject_memory(insights, cfg.name)
    # 4h. Agent tools + dispatch
    #       Build tool list based on cfg.code
    #       dispatch_strategist / dispatch_quant functions use prefix in SQL queries
    # 4i. _start_scheduler(cfg.code, cfg.name, prefix, cfg.timezone, str(_app_file))
    # 4j. Sidebar: st.title(f"{cfg.flag_emoji} {cfg.name}"), date pickers
    # 4k. Tabs: st.tabs([...])  ← 9 tabs for GB, 8 for others (no Pricing tab)
    #       Tab list for GB:   ["Market Overview","Ancillary Markets","BESS Benchmarking","Pricing","Asset Map","Knowledge Base","Strategist","Quant","Data Management"]
    #       Tab list for others: ["Market Overview","Ancillary Markets","BESS Benchmarking","Asset Map","Knowledge Base","Strategist","Quant","Data Management"]
    # 4l. Render each tab (guarded where necessary)
```

### Key parameterisation rules

1. Every `intl_market.gb_*` table reference → `f"intl_market.{prefix}..."` except GB-only tables (guard with `if cfg.code == "gb"`)
2. All `£` → `cfg.currency_sym`; `GBP` → `cfg.currency_code`
3. `"GB BESS"` / `"GB Market"` / `"Great Britain"` → `cfg.name`
4. `"National Grid ESO"` → `cfg.system_operator`
5. `"DC/DM/DR"` → `cfg.ancillary_label`
6. `"EPEX DA"` / `"System Price"` → `cfg.wholesale_label`
7. `"gb_daily_market"` scheduler IDs → `f"{cfg.code}_daily_market"` etc.
8. `from services.gb_knowledge.expert_memory import extract_gb_insights` → `from services.intl_market_common.expert_memory_base import extract_insights` called with `(user_msg, reply, ANTHROPIC_KEY, prefix, cfg.name)`
9. `from services.gb_knowledge.advanced_retrieval import retrieve_for_gb_agent` → `from services.intl_market_common.advanced_retrieval_base import retrieve_for_agent` called with `(query, ANTHROPIC_KEY, cfg)`
10. Monthly index column: `"month" if prefix == "gb_" else "year_month"` (aliased to `month`)
11. Leaderboard query: GB has `settlement_period` in PK + `revspermw`; others use simpler schema
12. Assets query: GB has `valid_from` column + extra geo/metadata; others use `date_from` and simple `(asset, history_table, value)` schema
13. `_table_counts(prefix)` coverage list differs by market (GB includes extra tables)

### Tabs rendering summary

**Market Overview tab**
- GB: system price (hourly) + NIV (daily) + EPEX DA heatmap + fuel mix + bidding space + EPEX DA line charts
- Non-GB: spot price chart from `{prefix}spot_price` (daily avg, with graceful "no data" message if table is empty)

**Ancillary Markets tab**
- GB: DX results from `gb_dx_results` (clearing_price, cleared_volume by EFA service)
- Non-GB: ancillary results from `{prefix}ancillary_results` (clearing_price, volume_mw by service)

**BESS Benchmarking tab** — identical logic for all markets (parameterised by prefix)

**Pricing tab** — only rendered for GB (`if cfg.code == "gb"`)

**Asset Map tab** — all markets; graceful if no lat/long (show capacity-by-owner bar chart instead of map)

**Knowledge Base tab** — all markets; KB source guide is GB-specific for GB, generic for others

**Strategist tab** — all markets; system prompt and tool set differ by cfg.code

**Quant tab** — all markets; IRR tool uses cfg.currency_sym

**Data Management tab** — all markets; Pricing Batch + Fuel Mix Backfill sections only for GB

---

## How to Write Each Market's `app.py` (Phase 7)

Once `app_template.py` exists, each market app is ~30 lines:

```python
# apps/au-market/app.py
"""Australia (NEM) Market Intelligence — Streamlit app.  Port 8509."""
import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", "config", ".env"))

import streamlit as st
st.set_page_config(
    page_title="Australia (NEM) Market Intelligence",
    page_icon="🇦🇺",
    layout="wide",
    initial_sidebar_state="expanded",
)

from services.au_knowledge.config import MARKET_CONFIG
from services.intl_market_common.app_template import run_market_app

run_market_app(MARKET_CONFIG, _app_file=__file__)
```

Same pattern for ercot (🇺🇸, port 8510), pjm (🇺🇸, port 8511), caiso (🇺🇸, port 8512).

The GB app refactored:
```python
# apps/gb-market/app.py (thin wrapper, ~35 lines)
"""GB Market Intelligence — Streamlit app. Port 8508."""
import os, sys
sys.path.insert(0, ...)
load_dotenv(...)
import streamlit as st
st.set_page_config(page_title="GB Market Intelligence", page_icon="🇬🇧", layout="wide", ...)

from services.gb_knowledge.config import MARKET_CONFIG
from services.intl_market_common.app_template import run_market_app
run_market_app(MARKET_CONFIG, _app_file=__file__)
```

---

## Terraform (Phase 8)

Add for each market in `infra/terraform/main.tf` following the existing `gb_market` block (lines ~1536–1714):

```hcl
# ECR repo
resource "aws_ecr_repository" "au_market" { name = "bess-au-market" }

# Target group
resource "aws_lb_target_group" "au_market" {
  name     = "bess-au-market"
  port     = 8509
  protocol = "HTTP"
  vpc_id   = var.vpc_id
  health_check { path = "/au-market/_stcore/health" }
}

# Listener rule (priority 46)
resource "aws_lb_listener_rule" "au_market_path" {
  listener_arn = aws_lb_listener.https.arn
  priority     = 46
  action { type = "forward"; target_group_arn = aws_lb_target_group.au_market.arn }
  condition { path_pattern { values = ["/au-market/*"] } }
}

# ECS task + service (512 CPU / 1024 MB, desired_count = var.desired_count_au_market)
```

`variables.tf` — add `image_au_market`, `desired_count_au_market` (default 0) × 4.

`terraform.tfvars` — set desired_count to 0 until images are pushed.

## Portal cards (Phase 8)

In `apps/portal/app.py`, add 4 market cards analogous to the existing GB card.

---

## Key File Locations

```
services/intl_market_common/
    market_config.py           ← MarketConfig dataclass
    modo_ai_base.py            ← ModoAIConnector(cfg) — Playwright
    expert_memory_base.py      ← KB digest + insight retrieval
    advanced_retrieval_base.py ← HyDE + OR-FTS + rerank
    app_template.py            ← *** NOT YET WRITTEN — main blocker ***

services/{au,ercot,pjm,caiso}_knowledge/
    config.py                  ← MARKET_CONFIG with questions
    ingest.py                  ← KB orchestrator

services/gb_knowledge/
    config.py                  ← GB MARKET_CONFIG (new)
    modo_ai.py                 ← original GB Playwright (still used by GB scheduler)
    expert_memory.py           ← original GB expert memory (still used directly by GB app)
    advanced_retrieval.py      ← original GB retrieval (still used directly by GB app)

services/modo_energy/
    {au,ercot,pjm,caiso}_ingestion.py  ← run_ingestion(start, end)
    gb_ingestion.py            ← run_gb_backfill(start, end)  ← different function name!

apps/gb-market/app.py          ← 3686 lines, NOT YET REFACTORED
apps/{au,ercot,pjm,caiso}-market/     ← DO NOT EXIST YET
```

---

## Important Gotchas

1. **GB leaderboard has `settlement_period`** — the query in the app does a double-CTE aggregation (first sum per SP, then per asset). New markets have one row per (asset, date, market) — simpler query.

2. **Monthly index column name** — GB uses `month DATE`, new markets use `year_month TEXT` (e.g. `"2024-01"`). Alias to `month` in the SELECT for uniform downstream handling.

3. **GB assets table** has `valid_from` for ORDER BY and extra columns. New markets use `date_from`.

4. **`run_gb_backfill`** (GB ingestion function name) ≠ `run_ingestion` (new markets). Template must branch.

5. **`st.set_page_config` must be the FIRST Streamlit call** — it must stay in each market's `app.py`, not inside `run_market_app`. The function is called AFTER `st.set_page_config` has already run.

6. **`@st.cache_data` functions must be at module level** — they cannot be nested inside `run_market_app` or they lose their caching behaviour. Pass `prefix: str` as a parameter to isolate cache entries per market.

7. **`_start_scheduler` should be `@st.cache_resource` at module level** taking `code, name, prefix, app_file` as parameters — so it fires once per process per market.

8. **Modo AI uses Playwright** — requires `playwright install chromium` in the Docker container. Already present in `apps/gb-market/Dockerfile`.
