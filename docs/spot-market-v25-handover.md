# Spot Market Strategist App — v25 Handover

## Context

The spot market app (`apps/spot-market/app.py`) has been upgraded from v24 to v25 on branch `cost-optimisation` (commit `71ac2c4`). The code is committed and pushed to GitHub but **v25 is not yet deployed to AWS ECS**.

---

## What was built (v25)

The Strategist tab was upgraded to match the GB market domain expert learning system. The following are all working in the committed code:

| Feature | Location | Status |
|---|---|---|
| Session persistence (resume chat on reload) | `staging.spot_analyst_sessions` | ✅ committed |
| Expert memory injection at query time | `_build_spot_system(query)` | ✅ committed |
| HyDE + reranking KB context injection | `advanced_retrieval.retrieve_for_agent()` | ✅ committed |
| Per-turn insight extraction via Haiku | `extract_spot_insights()` in expert_memory.py | ✅ committed |
| Insight pool count in subheader | `get_memory_stats()` | ✅ committed |
| Knowledge Gap Interview (KB-first → Q&A) | Strategist tab expander | ✅ committed |
| KB Digest button (batch + post-upload) | KB expander + upload handler | ✅ committed |
| Heatmap year 2001 bug fix + date pickers | `chart_heatmap()` + tab_heatmap | ✅ committed |
| Data Management tab loading fix | `_kb_init()` + `_db_coverage_detail()` wrapped | ✅ committed |
| `knowledge_pool/db.py` connect timeout | `connect_timeout=10` | ✅ committed |
| `ingest_knowledge_bulk.py` timeout fix | `running_since` dict | ✅ committed |

---

## Immediate next step: Deploy v25

### 1. Build the Docker image

```powershell
# From repo root
docker build -f apps/spot-market/Dockerfile -t 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-spot-markets:v25 .
```

**Note on build context size:** The `.dockerignore` was fixed to re-include `apps/spot-market/`, `services/spot_ingest/`, `services/spot_mcp/`, `services/knowledge_pool/`, `services/market_fundamentals/`, `apps/spot-watcher/`. If the build context is too large (~16GB due to `data/market-fundamentals/`), you can remove line 37 from `apps/spot-market/Dockerfile`:
```
COPY data/market-fundamentals/          ./data/market-fundamentals/
```
The AWS deployment reads market fundamentals from S3, not from this local folder. The folder is only useful for local development.

### 2. Push to ECR

```powershell
aws ecr get-login-password --region ap-southeast-1 | docker login --username AWS --password-stdin 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-spot-markets:v25
```

### 3. Update terraform.tfvars (already done but gitignored)

`infra/terraform/terraform.tfvars` line 92 is already set to `v25`:
```
image_spot_markets   = "319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-spot-markets:v25"
```

### 4. Deploy via Terraform

```powershell
cd infra/terraform
# Note: quote the -target values in PowerShell
terraform apply -target="aws_ecs_task_definition.spot_markets" -target="aws_ecs_service.spot_markets" -auto-approve
```

---

## Verify after deployment

1. Open the Spot Market app
2. **Heatmap tab** — should now show correct years (2024/2025/2026), date pickers visible
3. **Data Management tab** — should load without spinning; KB Sync expander shows "unavailable" (no local data folder on ECS, which is expected)
4. **Strategist tab** — should show insight count in caption (e.g. "N expert insights accumulated")
5. Send a message → after response, check `SELECT COUNT(*) FROM staging.kp_expert_insights` has grown
6. Clear chat → new session UUID; reload page → "Resume a previous conversation?" expander appears
7. Click "🎓 Teach the Strategist — Knowledge Gap Interview" → generate questions → KB-first pass works

---

## Architecture: Expert Memory System

```
User uploads doc → register_and_ingest() → kp_doc_synthesis (ECS synthesis task)
                                                    ↓
                                         digest_spot_kb_docs()     ← "Digest KB→Insights" button
                                                    ↓
                                         staging.kp_expert_insights
                                                    ↑
Strategist conversation turn → extract_spot_insights() (Haiku)
                                                    ↑
Knowledge Gap Interview → _answer_from_kb() (medium confidence)
                        → user Q&A (high confidence)

At query time: get_relevant_insights(query) → FTS on kp_expert_insights
             + retrieve_for_agent(query) → HyDE + rerank on spot_knowledge_chunks
             → injected into _build_spot_system(query) → system prompt
```

---

## DB tables involved (all in `staging` schema)

| Table | Purpose |
|---|---|
| `spot_knowledge_docs` | KB document registry |
| `spot_knowledge_chunks` | KB text chunks (FTS) |
| `kp_doc_synthesis` | Synthesized doc summaries (ECS synthesis task) |
| `kp_expert_insights` | Structured expert insights (insight_text, confidence, source_doc_id) |
| `spot_analyst_sessions` | Strategist chat session persistence (JSONB messages) |

---

## Known issues / pending work

- **`data/market-fundamentals/` in Docker image**: Currently the Dockerfile copies this ~16GB folder into the image. On ECS this is unused (S3 is used instead). Consider removing that COPY line for faster builds, then rebuild as v26.
- **synthesis tasks**: After bulk KB ingest, run ECS synthesis tasks to process new docs:
  ```powershell
  .\infra\synthesis\push_and_run.ps1 -RunOnly            # Phase 1 shared
  .\infra\synthesis\push_and_run.ps1 -App trader -RunOnly # Phase 1 trader
  .\infra\synthesis\push_and_run.ps1 -Phase "2 3" -RunOnly
  .\infra\synthesis\push_and_run.ps1 -Phase "4" -RunOnly
  ```
  Then click "Digest KB → Insights" in the Strategist KB expander to extract insights from newly synthesized docs.

---

## Files changed in v25 (commit 71ac2c4)

| File | Change |
|---|---|
| `apps/spot-market/app.py` | Major Strategist tab additions (851 lines net) |
| `apps/spot-market/Dockerfile` | Restored `data/market-fundamentals/` COPY |
| `services/knowledge_pool/expert_memory.py` | Added `extract_spot_insights()`, `digest_spot_kb_docs()` |
| `services/knowledge_pool/db.py` | Added `connect_timeout=10` |
| `scripts/ingest_knowledge_bulk.py` | Fixed per-file timeout logic |
| `.dockerignore` | Re-included spot-market service directories |
| `infra/terraform/terraform.tfvars` | `v24` → `v25` (gitignored, update manually) |
