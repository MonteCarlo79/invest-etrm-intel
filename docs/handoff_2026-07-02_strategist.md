# Handoff — 2026-07-02 — Spot Market Strategist

## Context for new Claude session

Working directory: `C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform`  
Branch: `cost-optimisation` (pushed, up to date with origin)  
Last commit: `e2d7f92`

---

## What was completed in the previous session

### 1. Hermes — WeCom monthly report send (td:136 deployed)
- `services/hermes/market_report.py`: added `_monthly_pdf_cache` (module-level dict), `send_monthly_report_to_wecom`, `_wecom_upload_file`
- `services/hermes/app.py`: added `/sendwecom` chat command — bypasses Feishu card button (Feishu silently drops callbacks on large-body cards); uses WebSocket path to find cached monthly report and send markdown + PDF to all `WECOM_MONTHLY_REPORT_WEBHOOKS`
- **Usage**: send `电力月报` first to generate and cache the report, then `/sendwecom` to push to WeCom groups

### 2. Spot price PDF parser bugs fixed (commit `ae6a38a`)
File: `services/spot_ingest/pdf_parser.py`

**Bug 1 — wrong DA prices**: 2026 DA tables have `[change%, price, change%, price, ...]` column order; parser expected price first. Fix: skip leading `%` cells before `_pick_triplet_from_tail`.

**Bug 2 — missing provinces (last date in multi-day reports)**: `mode=None` reset on every date change caused pages after a footnote page (which contained the next date's number) to be skipped. Fix: removed `mode = None` from the date-change block — mode now only changes when `_detect_section_mode` finds an explicit section header.

Both PDFs re-ingested locally (108 rows for 6.27-6.29, 50 rows for 6.30).

### 3. bess-map FR capacity tab + capcomp KB screener
- `apps/bess-map/app.py`: FR capacity tab using 4-tuple province rules
- `services/hermes/capcomp_screener.py`: replaced broken internet agent with KB search against `staging.spot_knowledge_chunks`

---

## Deployment needed

### spot-market v41 (contains pdf_parser fix)

```bash
docker build -f apps/spot-market/Dockerfile -t bess-spot-markets:v41 .
docker tag bess-spot-markets:v41 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-spot-markets:v41
docker tag bess-spot-markets:v41 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-spot-markets:latest
aws ecr get-login-password --region ap-southeast-1 | docker login --username AWS --password-stdin 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-spot-markets:v41
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-spot-markets:latest
IMAGE_TAG=v41 py scripts/update_spot_markets_taskdef.py
```

Current live: v40 / ECS task def rev 55  
Cluster: `bess-platform` in `ap-southeast-1`

---

## Strategist tab — architecture

The entire Strategist lives inline in `apps/spot-market/app.py` starting at **line 2678** (`with tab_agent:`).

### Tools (8 total)
| Tool | Purpose |
|------|---------|
| `get_spot_prices` | DA/RT prices from `public.spot_daily` |
| `get_interprov_flow` | Inter-provincial spot trading volumes/prices |
| `get_market_summaries` | AI-generated daily narrative summaries |
| `run_pipeline` | Ingest a spot-market PDF (parses + writes to DB + KB) |
| `get_market_fundamentals` | Installed capacity, generation mix, peak load by province |
| `search_reference_docs` | Vector search KB (PDFs, Excel, PPTX, DOCX) |
| `ingest_kb_document` | Add a document to the KB (S3 key or file path) |
| `get_bess_pnl` | IM BESS P&L across 4 assets × 5 scenarios |

### Memory layers (3)
1. **`marketdata.agent_memory`** — flat analyst preferences extracted by Haiku after every turn
2. **`staging.kp_expert_insights`** — structured expert insights extracted by `expert_memory.extract_spot_insights`
3. **`staging.spot_analyst_sessions`** — full conversation persistence; user can resume previous chats

### KB retrieval
- HyDE + reranking via `services/knowledge_pool/advanced_retrieval.py` injected into system prompt on every turn
- Expert insights retrieved via FTS from `staging.kp_expert_insights` and injected as context

### Key file paths
```
apps/spot-market/app.py              # Strategist tab: lines 2678–3750
services/knowledge_pool/
  knowledge_docs.py                  # search_reference_docs, register_and_ingest
  advanced_retrieval.py              # HyDE + reranking
  expert_memory.py                   # kp_expert_insights CRUD
services/spot_mcp/tools.py           # get_spot_prices, get_interprov_flow, get_market_summaries, etc.
services/bess_mcp/tools.py           # get_bess_pnl
```

### Knowledge base stats (as of 2026-07-02)
- `staging.spot_knowledge_docs`: ~6,688 docs
- `staging.spot_knowledge_chunks`: ~2.1M chunks with embeddings (backfill still running)
- All docs with `active=TRUE` and `embedding IS NOT NULL` are searchable — `ingest_status` is not checked by search

---

## DB tables referenced by Strategist

| Table | Purpose |
|-------|---------|
| `public.spot_daily` | DA/RT prices by province+date |
| `public.spot_interprov_daily` | Inter-provincial trading |
| `public.spot_summaries` | AI daily summaries |
| `staging.spot_knowledge_docs` | KB document index |
| `staging.spot_knowledge_chunks` | Vector-embedded text chunks |
| `staging.kp_expert_insights` | Expert memory (structured insights) |
| `staging.spot_analyst_sessions` | Chat session persistence |
| `marketdata.agent_memory` | Flat analyst memory |
| `marketdata.market_fundamentals` | Installed capacity/generation |

---

## Hermes current state

- td:136 deployed 2026-07-01 on `bess-platform-hermes:latest`
- `/sendwecom` chat command: sends most recent cached monthly report to WeCom webhooks
- Monthly report PDF: ReportLab + NotoSans CJK, generated via `电力月报` chat command or POST /hermes/reports/monthly
- WeCom webhook env var: `WECOM_MONTHLY_REPORT_WEBHOOKS` (comma-separated URLs)
- Deploy: `IMAGE_TAG=tdNNN py scripts/update_hermes_taskdef.py` (hermes has its own script)

---

## How to run the spot-market app locally

```bash
# From bess-platform root
py -m streamlit run apps/spot-market/app.py --server.port=8502 -- --server.baseUrlPath=spot-markets
```

Requires: `ANTHROPIC_API_KEY`, `DATABASE_URL` (or `DB_*` env vars), `AWS_*` for S3 operations.

---

## Suggested next steps for Strategist

1. **Deploy v41** (pdf_parser fix is live in code, not yet in ECS)
2. **Strategist UX improvements** — possible areas:
   - Add a `get_news_articles` tool that queries `staging.spot_knowledge_docs` for recent news articles so agent can discuss latest market news
   - Add `get_capacity_comp` tool for capacity compensation + FR market data (capcomp + sysopfee tables)
   - Improve Knowledge Gap Interview: pre-fill KB answers more aggressively before asking user
   - Add a "Chart" tool that returns a plotly figure for the agent to display inline
3. **`backfill_embeddings.py`** — still running; once complete, all market-fundamentals docs will be searchable in the KB
