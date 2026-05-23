# Spot Market Strategist App — v26 Handover

## Context

All v26 code is committed and pushed to `origin/cost-optimisation` (commit `993d639`).
**v26 is not yet deployed to AWS ECS** — build and deploy is in progress.
Last deployed version: **v24**.

---

## Deploy v26

```powershell
# From repo root — terraform.tfvars line 92 already set to v26
docker build -f apps/spot-market/Dockerfile -t 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-spot-markets:v26 .

aws ecr get-login-password --region ap-southeast-1 | docker login --username AWS --password-stdin 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-spot-markets:v26

cd infra/terraform
terraform apply -target="aws_ecs_task_definition.spot_markets" -target="aws_ecs_service.spot_markets" -auto-approve
```

**Build is now fast** — `data/market-fundamentals/` (16GB) removed from Dockerfile; ECS reads from S3.

---

## What changed in v26 (vs v25)

### 1. Docker / dependencies
| Change | Detail |
|---|---|
| Removed `COPY data/market-fundamentals/` | Build context ~100MB instead of ~16GB |
| Added `beautifulsoup4>=4.12` | Required for URL-to-KB fetch feature |

### 2. Data Management tab fix (`apps/spot-market/app.py`)
Root cause: every Streamlit page rerun was opening 2–3 fresh psycopg2 connections to RDS via
`knowledge_pool/db.py`'s `get_conn()` (separate from the app's cached `_conn()`), causing
4–6 second hangs before `with tab_mgmt:` could execute.

**Fixes applied:**

| Guard | Key | What it prevents |
|---|---|---|
| `_ensure_spot_sessions_table()` | `_spot_sessions_table_ok` | DDL round-trip on every rerun |
| `_kb_init()` (init_knowledge_tables) | `_kp_tables_init_done` | DDL round-trip on every rerun |
| `get_memory_stats()` | `_mstats_cache` + `_mstats_ts` | New RDS connection every rerun (60s TTL) |
| `_kb_list()` (list_knowledge_docs) | `_kb_docs_cache` + `_kb_docs_ts` | New RDS connection every rerun (60s TTL) |

Also fixed in `services/knowledge_pool/knowledge_docs.py`:
- `_TABLES_INITIALIZED` module-level flag — `init_knowledge_tables()` DDL runs once per process

### 3. Heatmap tab — now Province × Hour-of-Day
- **Before**: Province × Date (calendar heatmap from `spot_daily`)
- **After**: Province × Hour-of-Day (intraday average price pattern from `spot_prices_hourly`)
- X-axis: `00:00` to `23:00`; date pickers control the averaging period
- Uses same `_load_intraday_shape()` + EN↔ZH province name translation as Intraday Analysis tab

### 4. Knowledge Base expander — GB-market parity
- **File formats**: added `doc`, `ppt` (old Office formats) to uploader
- **Progress bar**: shown during multi-file uploads
- **URL fetch tab** (`🌐 Fetch from URL`): paste any public URL → text extracted via BS4 →
  chunked and indexed into `staging.spot_knowledge_chunks` → immediately digested into insights
- `register_url()` added to `services/knowledge_pool/knowledge_docs.py`

### 5. Strategist tab — Gap Interview now prominent
- **Before**: buried as 3rd expander below Memory Management
- **After**: first expander below the chat (right after `st.divider()`)
- New order: 🎓 Knowledge Gap Interview → 🗄️ Memory Management → 📚 Knowledge Base

---

## Architecture: Expert Learning System (v25 + v26)

The spot market Strategist now mirrors the GB market learning system end-to-end:

```
User uploads doc / fetches URL
  → register_and_ingest() / register_url()
  → staging.spot_knowledge_chunks (FTS-indexed, immediate)
  → digest_spot_kb_docs() → staging.kp_expert_insights

Strategist conversation turn
  → _build_spot_system(query):
      get_relevant_insights(query)          ← FTS on kp_expert_insights
      retrieve_for_agent(query)             ← HyDE + rerank on spot_knowledge_chunks
      → injected into system prompt
  → agent responds
  → extract_spot_insights(user_msg, reply) ← Haiku extracts durable facts
  → staging.kp_expert_insights grows

🎓 Knowledge Gap Interview (Strategist tab, first expander below chat):
  Stage 0: "Generate Knowledge Gap Questions" → Haiku audits kp_expert_insights
  Stage 1: "Search KB First" → _answer_from_kb() → medium-confidence insights
  Stage 2: User Q&A for unanswered gaps → high-confidence insights
  Stage 3: Summary (N KB-answered, M user-answered)
```

---

## DB tables (staging schema)

| Table | Purpose |
|---|---|
| `spot_knowledge_docs` | KB document registry |
| `spot_knowledge_chunks` | Text chunks (FTS via GIN index) |
| `kp_doc_synthesis` | ECS synthesis task output (async, optional) |
| `kp_expert_insights` | Structured insights (per-turn + KB digest + interview) |
| `spot_analyst_sessions` | Strategist chat session persistence (JSONB) |

---

## Verify after deploy

1. **Heatmap tab** — x-axis shows `00:00`–`23:00`, not dates
2. **Data Management tab** — loads immediately, no spinning
3. **Strategist tab** — scroll past chat, first expander is `🎓 Teach the Strategist — Knowledge Gap Interview`
4. **KB expander** — two tabs: `📂 Upload Files` and `🌐 Fetch from URL`
5. Upload a PDF → toast shows "N insights extracted"
6. Fetch a URL → page is indexed and digested
7. Run gap interview → generates 5 questions → KB search → user Q&A

---

## Key files

| File | Role |
|---|---|
| `apps/spot-market/app.py` | Main Streamlit app (~4000 lines) |
| `apps/spot-market/Dockerfile` | Image build (no 16GB data copy) |
| `services/knowledge_pool/expert_memory.py` | `extract_spot_insights`, `digest_spot_kb_docs`, `get_memory_stats`, `get_relevant_insights` |
| `services/knowledge_pool/knowledge_docs.py` | `register_and_ingest`, `register_url`, `search_reference_docs` |
| `services/knowledge_pool/advanced_retrieval.py` | HyDE + reranking |
| `services/knowledge_pool/db.py` | `get_conn()` with `connect_timeout=10` |
| `infra/terraform/terraform.tfvars` | Line 92: `image_spot_markets = "...bess-spot-markets:v26"` (gitignored) |

---

## Pending / known issues

- **v26 not deployed** — build + push + terraform apply needed (commands above)
- **`data/market-fundamentals/` excluded from image** — ECS uses S3; local dev still has the folder
- **Synthesis pipeline** — after bulk KB ingest, run ECS synthesis tasks then click "Digest KB → Insights"
- **`infra/terraform/terraform.tfvars`** — gitignored, confirm line 92 = `v26` before terraform apply
