# Handoff — 2026-06-19 — Strategist App

## Context for the new session

You are continuing work on the **bess-platform** repo at
`C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform`, branch **`cost-optimisation`**.

---

## What was completed this session (2026-06-18)

### 1. 河北南网 data gap fixed (pipeline.py)

**Root cause:** `apps/spot-watcher/pipeline.py` PROVINCES_MAP was missing `"河北南网": "Hebei-South"`.
The PDF parser does exact-match only, so every 河北南网 row in every PDF was silently skipped.
Data was null in DB from 2026-05-23 onwards.

**Fix:** Added `"河北南网": "Hebei-South"` to PROVINCES_MAP (commit `6494f71`).

**Backfill still needed if not yet run:**
```powershell
py -3 C:/Users/dipeng.chen/Downloads/backfill_hebei.py --write
```
This re-processes 15 PDFs (2026-05-23 → 2026-06-15), upserts 37 rows to `spot_daily`.

### 2. Local dev run fixed (run_all_local_dashboards.ps1)

The ps1 script was pointing to the old `apps/spot-agent/ui/spot_dashboard.py`.
Fixed to `apps/spot-market/app.py` with correct env vars.

**To run locally:**
```powershell
cd C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform
.\run_all_local_dashboards.ps1 -AwsProfile default
```
Spot-markets app will be on **http://localhost:8505** (not 8515 — that's a stale Docker container).

**Do NOT use the Docker container** (`bess-spot-markets:latest` on port 8515) — it's a stale ECR
image with no local data mount. Stop it if needed: `docker stop bess-platform-spot-markets-1`

### 3. Library tab PDF pre-fetch bug fixed (report_library_ui.py)

`_cached_get_pdf` was called unconditionally on every page rerun (downloading a PDF from DB
even when the user hadn't clicked anything). Fixed to only fetch on explicit button click.
Affects all market apps that use `services/common/report_library_ui.py`.

---

## Next task: Strategist tab / app handover

The **Strategist** is `tab_agent` inside `apps/spot-market/app.py` (line 2676 onwards).
It is NOT a separate deployed app — it lives inside the spot-market app.

### What the Strategist tab does

- Claude-powered chat agent with access to spot market DB tools (prices, interprov flows,
  daily summaries, PDF knowledge base)
- Persistent memory via `marketdata.agent_memory` table (app=`spot_market`)
- Knowledge base: `staging.spot_report_documents` / `staging.spot_report_chunks` (pdfplumber
  ingested daily PDFs) + `services/knowledge_pool/` retrieval stack
- KB app namespace: `"strategist"` (separate from `"shared"` and `"trader"`)
- Expert memory / interview feature: "Teach the Strategist" expander (line ~3445) — stores
  domain knowledge answers into `agent_memory`

### Key files

| File | Purpose |
|---|---|
| `apps/spot-market/app.py:2676` | Strategist tab — full agent UI |
| `services/knowledge_pool/advanced_retrieval.py` | RAG retrieval (semantic + keyword) |
| `services/knowledge_pool/expert_memory.py` | digest_spot_kb_docs — insight extraction |
| `services/knowledge_pool/knowledge_docs.py` | KB document registry + ingestion |
| `apps/spot-agent/agent/tools_llm.py` | LLM tool definitions used by the agent |
| `services/spot_mcp/tools.py` | MCP tool implementations (prices, summaries) |

### What "handover" likely means

The user wants to understand/improve the Strategist tab or extract it into a standalone app.
Start by:

1. Read `apps/spot-market/app.py` lines 2676–3580 (full Strategist tab) to understand the
   current implementation
2. Read `apps/spot-agent/agent/tools_llm.py` for the tool schema
3. Check `services/knowledge_pool/advanced_retrieval.py` for retrieval logic
4. Ask the user: **Are we (a) improving the existing Strategist tab, or (b) extracting it
   into a new standalone `apps/strategist/` app?**

---

## ECS / deployment state

| Item | Value |
|---|---|
| ECR image | `bess-spot-markets:v32` |
| Task def | `bess-platform-spot-markets:39` |
| Service | `bess-platform-cluster / bess-platform-spot-markets-svc` |
| Port | 8505, path `/spot-markets` |
| Baseline task def | **rev 39** (based on rev 36; revs 37-38 are broken — wrong CMD) |

**To deploy a new version:**
```bash
# 1. Build + push
docker build -t bess-spot-markets:v33 -f apps/spot-market/Dockerfile . --platform linux/amd64
docker tag bess-spot-markets:v33 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-spot-markets:v33
aws ecr get-login-password --region ap-southeast-1 | docker login --username AWS --password-stdin 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-spot-markets:v33

# 2. Register new task def based on rev 39 with image swapped to v33
# 3. Update service to use new task def
```

---

## DB

- Host: `bess-platform-pg.cjs000o4wn2w.ap-southeast-1.rds.amazonaws.com:5432`
- DB: `marketdata`
- Spot prices table: `spot_daily` (report_date, province_cn, province_en, da_avg/max/min, rt_avg/max/min)
- Agent memory: `marketdata.agent_memory` (app, category, subject, content)
- KB documents: `staging.spot_report_documents`, `staging.spot_report_chunks`
