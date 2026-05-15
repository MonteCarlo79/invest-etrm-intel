# Knowledge Intelligence Architecture
## Learning Domain Expert Agent — Design & Implementation

*BESS Platform · `services/knowledge_pool/` · Updated 2026-05-15*

---

## Overview

The knowledge intelligence layer turns a static document corpus (~4,000+ policy, market, and technical documents) into a self-improving domain expert agent. Each agent query is answered with a combination of live quantitative market data, synthesised document knowledge, and accumulated expert insights from prior analysis sessions.

The system learns over time: every conversation session contributes validated insights back into expert memory, making subsequent sessions progressively more informed.

---

## Architecture: Five Phases

```
  Raw Documents (PDF/PPTX/XLSX/DOCX)
         │
         ▼
  ┌──────────────────────────────────────┐
  │  Phase 1: Document Synthesis         │  synthesis.py
  │  Summaries + Q&A pairs + Entities    │
  └──────────────┬───────────────────────┘
                 │
                 ▼
  ┌──────────────────────────────────────┐
  │  Phase 2: Knowledge Graph            │  knowledge_graph.py
  │  Entity dedup + Relationships        │
  └──────────────┬───────────────────────┘
                 │
                 ▼
  ┌──────────────────────────────────────┐
  │  Phase 3: Policy Timeline            │  knowledge_graph.py
  │  Regulatory changes + effective dates│
  └──────────────┬───────────────────────┘
                 │
                 ▼
  ┌──────────────────────────────────────┐
  │  Phase 4: Expert Memory              │  expert_memory.py
  │  Session insights → persistent store │
  └──────────────┬───────────────────────┘
                 │
  ┌──────────────▼───────────────────────┐
  │  Phase 5: Advanced Retrieval         │  advanced_retrieval.py
  │  HyDE + Re-ranking → agent context   │
  └──────────────────────────────────────┘
```

---

## Phase 1 — Document Synthesis

**File:** `services/knowledge_pool/synthesis.py`  
**Runner:** `scripts/run_synthesis_pipeline.py --phase 1`  
**ECS task:** `infra/synthesis/push_and_run.ps1`

Each ingested document is passed to Claude with a synthesis prompt that extracts:
- A **structured summary** (purpose, key points, market impact, regulatory implications)
- **Q&A pairs** — 5–10 questions and answers that capture the document's key facts
- **Named entities** (provinces, policies, market mechanisms)

Results are stored in `staging.kp_doc_synthesis`. The synthesis is decoupled from ingestion — documents are ingested first (Phase 0), synthesised later by the ECS task.

**Why decouple?** Bulk ingestion of 9,000 files fires thousands of on-ingest synthesis threads simultaneously → burst-limit 403s. The ECS Fargate task processes synthesis sequentially at a controlled rate.

**Key design:** `register_and_ingest(..., synthesize=False)` during bulk ingestion; `synthesize=True` (default) for single-file uploads via the web UI where immediate synthesis is desired.

---

## Phase 2 — Knowledge Graph

**File:** `services/knowledge_pool/knowledge_graph.py`  
**Runner:** `scripts/run_synthesis_pipeline.py --phase 2`

Builds a lightweight knowledge graph over all synthesised entities:
- **Entity deduplication** — normalises variant spellings of provinces, policy names, etc.
- **Relationship extraction** — links entities across documents (e.g. "Shanxi → FM ancillary → Rule 2024-06")
- Stored in `staging.kp_entities` and `staging.kp_relations`

---

## Phase 3 — Policy Timeline

**File:** `services/knowledge_pool/knowledge_graph.py`  
**Runner:** `scripts/run_synthesis_pipeline.py --phase 3`

Extracts temporal regulatory context from synthesised documents:
- Identifies policy effective dates, expiry dates, transition periods
- Builds a chronological regulatory timeline per province
- Allows the agent to answer questions like "what rules applied in Shanxi in Q3 2024?"

---

## Phase 4 — Expert Memory (The Learning Layer)

**File:** `services/knowledge_pool/expert_memory.py`  
**Runner:** `scripts/run_synthesis_pipeline.py --phase 4`

This is the key differentiator that makes the agent a *learning* system.

### How it works

After each analyst session, an insight extractor runs over the conversation log:

```python
extract_and_store_insights(api_key=key)
# Reads today's conversation logs from staging.kp_agent_sessions
# Calls Claude to identify durable, non-obvious, validated insights
# Writes to staging.kp_expert_insights
```

### What counts as an insight

The extractor is deliberately conservative. An insight must be:
1. **Non-obvious** — not trivially findable by document search
2. **Validated** — the analyst confirmed, corrected, or accepted the analysis
3. **Durable** — relevant for weeks or months, not ephemeral (e.g. not "today's price")
4. **Domain-specific** — about China electricity markets, BESS operations, regulation, or investment

Insight types: `market_structure | price_driver | regulation | risk | opportunity | dispatch_economics | investment | operations`

### Injection at query time

```python
insights = get_relevant_insights(query=user_prompt, limit=5)
memory_context = inject_expert_memory(insights)
# → injected into agent context alongside quantitative data and document retrieval
```

The retrieval uses PostgreSQL FTS over insight text, scoped by province where applicable.

### Schema

```sql
staging.kp_expert_insights (
    id, insight, type, province, confidence,
    source_session, created_at, use_count
)
```

`use_count` is incremented each time an insight is retrieved, enabling future decay/promotion logic.

---

## Phase 5 — Advanced Retrieval

**File:** `services/knowledge_pool/advanced_retrieval.py`

Three retrieval upgrades over baseline FTS:

### 5a. HyDE — Hypothetical Document Embeddings (without vectors)

Standard FTS matches query keywords literally. HyDE first generates a *hypothetical expert answer* to the query, then uses the terminology in that answer to drive the search. This is dramatically better for Chinese policy questions where the analyst might ask in English but the documents use Chinese regulatory vocabulary.

```
Query: "What ancillary market rules apply to BESS in Shanxi?"
   ↓ Claude (claude-haiku-4-5)
Hypothetical answer: "Shanxi's 调频 (FM) market requires BESS to maintain ≥10 MW
   registered capacity... 备用 services are compensated at..."
   ↓ extract terms: ["调频", "备用", "山西", "储能", "容量补偿", ...]
   ↓ FTS with domain-specific terms
Much better recall
```

### 5b. Hierarchical / Parent-Child Retrieval

FTS retrieves small chunks (500 chars) for precision. The system then expands each hit to include neighbouring chunks, preventing the agent from receiving a fragment without context.

### 5c. Cross-Encoder Re-ranking

After FTS returns top-K candidates, Claude (haiku) scores each by true relevance to the original query and returns only the top 5-6. This filters out keyword-match noise and surfaces the most relevant passages.

### Unified entry point

```python
context = retrieve_for_agent(
    query=user_prompt,
    api_key=key,
    app="shared",       # or "trader"
    use_hyde=True,
    use_rerank=True,
    top_k=6,
)
```

---

## Strategy Agent — Putting It Together

**File:** `shared/agents/strategy_agent.py`

The strategy agent composes three context streams before calling Claude:

```
1. Quantitative Market Data    load_top_provinces() + spread_stats + capacity_bias + mengxi_rank
2. Knowledge Context           retrieve_for_agent() → HyDE + re-ranked synthesis/Q&A results
3. Expert Memory               get_relevant_insights() → validated prior session insights
         │
         ▼
   Claude claude-sonnet-4-6  (max_tokens=4096)
         │
         ▼
   Analyst-grade strategy memo
```

### Why 4096 tokens

The three context streams combined can approach 3,000 input tokens. With `max_tokens=2048`, responses were being truncated mid-answer. Raising to 4096 gives the model full room to reason and respond.

---

## Ingestion Pipeline

**File:** `scripts/ingest_knowledge_bulk.py`

### Checkpoint optimization

Processing 9,000+ files with a DB lookup per file is slow even when most are already ingested. The checkpoint system avoids redundant DB queries:

```
.ingest_checkpoint.log   (lives alongside --dir)
  → one absolute file path per line
  → loaded at startup into a set
  → files in the set are skipped before any DB or API call
  → written to after each successful [ADDED] or [SKIP] result
```

**Pre-building the checkpoint** (use after a large run that predates the checkpoint):
```powershell
py scripts/ingest_knowledge_bulk.py `
  --dir "data/market-fundamentals" `
  --prebuild-checkpoint
# Queries DB for all ingested filenames, matches to disk paths, writes checkpoint
# Then run normally — only genuinely new files are processed
```

### Synthesis decoupling

`synthesize=False` is always passed during bulk ingestion. The ECS Fargate synthesis task runs separately:

```powershell
# After ingestion completes:
.\infra\synthesis\push_and_run.ps1 -RunOnly           # Phase 1: shared docs
.\infra\synthesis\push_and_run.ps1 -App trader -RunOnly  # Phase 1: trader docs
.\infra\synthesis\push_and_run.ps1 -Phase "2 3" -RunOnly # Knowledge graph + timeline
.\infra\synthesis\push_and_run.ps1 -Phase "4" -RunOnly   # Expert memory extraction
```

---

## Database Tables

| Table | Phase | Contents |
|---|---|---|
| `staging.spot_knowledge_docs` | Ingestion | Doc registry (hash, category, app, status) |
| `staging.spot_knowledge_chunks` | Ingestion | Text chunks (500 chars, 100 overlap) |
| `staging.kp_doc_synthesis` | Phase 1 | Summaries, Q&A pairs, entities per doc |
| `staging.kp_entities` | Phase 2 | Deduplicated entity catalogue |
| `staging.kp_relations` | Phase 2 | Entity relationships |
| `staging.kp_policy_timeline` | Phase 3 | Policy events with effective dates |
| `staging.kp_expert_insights` | Phase 4 | Durable validated session insights |
| `staging.kp_agent_sessions` | Runtime | Conversation logs for Phase 4 input |

---

## Key Design Decisions

**No pgvector.** All retrieval is PostgreSQL FTS with GIN indexes. HyDE + re-ranking recovers most of the quality gap vs. dense vector search while keeping the infrastructure simple (single RDS instance, no additional services).

**Decoupled synthesis.** Ingestion (fast, parallelisable) is separated from synthesis (sequential, API-rate-limited). ECS Fargate runs synthesis overnight without blocking the ingestion pipeline.

**Conservative insight extraction.** Expert memory is valuable only if it's high quality. The extractor deliberately rejects ephemeral facts, process instructions, and unvalidated hypotheses. `confidence=high` insights are surfaced preferentially at query time.

**App scoping.** Documents and insights are tagged `shared` (available to all agents) or `trader` (trading-specific). The strategy agent queries `shared`; the trader agent queries `trader` + `shared`.
