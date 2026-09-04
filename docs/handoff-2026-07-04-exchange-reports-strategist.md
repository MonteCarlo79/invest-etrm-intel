# Handoff — 2026-07-04 — Exchange Reports + Strategist App

**Date:** 2026-07-04  
**Branch:** `cost-optimisation`  
**Commit:** `eb9ec66`  
**Working dir:** `C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform`

---

## CRITICAL: Prod DB is still empty for 2025+ data

The dashboard shows "No structured metrics yet" and "No exchange monthly reports ingested yet"
because **all backfill runs went to the local dev DB**, not the prod RDS.

**Prod RDS** has only 2024 data (203 metrics rows).  
**Local dev DB** has 2024+2025+广西 data (324+ metrics rows).

### Fix — run both scripts with prod PGURL

```powershell
$env:PGURL = "postgresql://postgres:!BESSmap2026@bess-platform-pg.cjs000o4wn2w.ap-southeast-1.rds.amazonaws.com:5432/marketdata?sslmode=require"
$env:DEEPSEEK_API_KEY = "sk-..."   # platform.deepseek.com

# Step 1: ingest all files to prod KB (~330 files, ~10 min)
py scripts/ingest_exchange_reports.py

# Step 2: extract structured metrics for all ingested monthly reports (~300 rows, ~10-15 min)
py scripts/ingest_exchange_reports.py --extract-metrics-only
```

After this, the Data Management tab "File List" and "数据汇总表 Metrics" will both show data.

---

## What was built (all committed, not yet deployed as new image)

### Exchange Reports Pipeline

| File | What it does |
|------|-------------|
| `services/exchange_reports/ingestor.py` | Province inference, text extraction, KB + registry upsert |
| `services/exchange_reports/metrics_extractor.py` | LLM metrics extraction (17 fields + highlights) |
| `services/exchange_reports/summary_pdf.py` | Cross-province PDF summary table builder |
| `scripts/ingest_exchange_reports.py` | CLI: full ingest + `--extract-metrics-only` backfill mode |

### LLM Provider selection (first match wins)

| Env var | Provider |
|---------|----------|
| `DEEPSEEK_API_KEY` | DeepSeek `deepseek-chat` (OpenAI-compatible; works from China) |
| `BEDROCK_REGION` | AWS Bedrock Claude (geo-blocked from China) |
| `ANTHROPIC_API_KEY` | Direct Anthropic API (geo-blocked from China) |

### Provinces supported (10)
`上海, 冀南, 安徽, 山东, 广东, 江苏, 浙江, 福建, 蒙西, 广西`

### Annual / non-monthly reports (NEW in eb9ec66)
`ingest_folder()` now routes files where province is known but month cannot be inferred
(annual summaries, semi-annual reports, supply forecasts) to `ingest_annual_report()` instead
of skipping. These go directly into `staging.spot_knowledge_docs` (KB-only; no metrics row).

---

## Data Management tab (spot-market app)

The "交易所月报管理 Exchange Monthly Reports" expander lives at `apps/spot-market/app.py` line **5338**.

Two views toggled by radio:
1. **📋 文件列表 File List** — queries `staging.exchange_monthly_reports` (only shows monthly reports, not annual)
2. **📊 数据汇总表 Metrics** — queries `staging.exchange_monthly_metrics` via `get_metrics_table(year, month)`;
   shows cross-province table + 📝 highlights expander + 📄 download PDF button

Key functions:
- `get_available_months(pg_url)` → list of `"YYYY-MM"` strings (for selectbox)
- `get_metrics_table(year, month, pg_url)` → list of dicts (17 numeric fields + `key_highlights`)
- `build_summary_pdf(rows, month_label)` → PDF bytes (ReportLab + NotoSans CJK)

---

## DB tables

| Table | Purpose |
|-------|---------|
| `staging.exchange_monthly_reports` | File registry; `report_month NOT NULL`, SHA256 dedup |
| `staging.exchange_monthly_metrics` | Structured metrics; UNIQUE on `(province, report_month, report_type)` |
| `staging.spot_knowledge_docs` | KB document index (also used by Strategist search) |
| `staging.spot_knowledge_chunks` | KB text chunks with vector embeddings |

---

## Strategist tab current state

- **File:** `apps/spot-market/app.py` lines **2681–4608**
- **Model:** `claude-sonnet-4-6` with streaming
- **Deployed:** v42 / ECS task def rev 57 (cluster `bess-platform`, region `ap-southeast-1`)

### Tools (8)

| Tool | Source |
|------|--------|
| `get_spot_prices` | DA/RT prices from `public.spot_daily` |
| `get_interprov_flow` | Inter-provincial trading from `public.spot_interprov_daily` |
| `get_market_summaries` | AI daily summaries from `public.spot_summaries` |
| `run_pipeline` | Ingest a spot-market PDF |
| `get_market_fundamentals` | Installed capacity/generation mix from `marketdata.market_fundamentals` |
| `search_reference_docs` | HyDE+rerank KB search against `staging.spot_knowledge_chunks` |
| `ingest_kb_document` | Add doc to KB (S3 key or file path) |
| `get_bess_pnl` | IM BESS P&L across 4 assets × 5 scenarios |

### Memory / session persistence

| Layer | Table | Purpose |
|-------|-------|---------|
| Analyst preferences | `marketdata.agent_memory` | Flat KV; updated by Haiku after every turn |
| Expert insights | `staging.kp_expert_insights` | Structured; updated by `expert_memory.extract_spot_insights` |
| Session history | `staging.spot_analyst_sessions` | Full conversation; user can resume |

### KB retrieval
HyDE + reranking via `services/knowledge_pool/advanced_retrieval.py` — injected into system
prompt on every turn. Expert insights retrieved via FTS and also injected as context.

---

## Hermes current state

- **Deployed:** td:148 (2026-07-03)  
- **Auto-ingest:** `is_exchange_report(filename)` in `services/exchange_reports/ingestor.py` — any PDF/DOCX dropped in Feishu with exchange report keywords + detectable month triggers `_ingest_exchange_report()` in `services/hermes/app.py`
- **Missing:** `DEEPSEEK_API_KEY` is NOT in the Hermes ECS task def — so Feishu-triggered ingests write to KB but do NOT extract structured metrics. Add it to get full pipeline.

### Add DEEPSEEK_API_KEY to Hermes task def

1. Open `hermes_td_src.json` (or fetch from ECS)
2. Add to `containerDefinitions[0].environment`:
   ```json
   {"name": "DEEPSEEK_API_KEY", "value": "sk-..."}
   ```
3. Register new task def revision and update service:
   ```powershell
   IMAGE_TAG=td149 py scripts/update_hermes_taskdef.py
   ```

---

## Deploy new spot-market image

The `ingest_annual_report()` change and `ingest_folder()` update (from `eb9ec66`) are in code
but **not yet deployed** (still on v42/td:57). To deploy v43:

```powershell
docker build -f apps/spot-market/Dockerfile -t bess-spot-markets:v43 .
docker tag bess-spot-markets:v43 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-spot-markets:v43
docker tag bess-spot-markets:v43 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-spot-markets:latest
aws ecr get-login-password --region ap-southeast-1 | docker login --username AWS --password-stdin 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-spot-markets:v43
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-spot-markets:latest
IMAGE_TAG=v43 py scripts/update_spot_markets_taskdef.py
```

---

## Suggested next steps

Priority order:

1. **Run prod backfill** (commands at top of this doc) — fixes empty dashboard
2. **Add DEEPSEEK_API_KEY to Hermes ECS task def** — enables metrics extraction from Feishu uploads
3. **Deploy spot-market v43** — activates annual report routing in UI
4. **Strategist enhancements** (optional):
   - Add `get_exchange_metrics` tool so agent can query `staging.exchange_monthly_metrics` directly and compare across provinces/months
   - Add `get_news_articles` tool querying `staging.spot_knowledge_docs` for recent news
   - Add `get_capacity_comp` tool for capcomp + sysopfee data

---

## How to run locally

```bash
# spot-market app
py -m streamlit run apps/spot-market/app.py --server.port=8502

# Hermes
py -m streamlit run services/hermes/app.py --server.port=8503
```

Requires: `.env` with `PGURL`, `ANTHROPIC_API_KEY` (or `DEEPSEEK_API_KEY`), `AWS_*` for S3.
