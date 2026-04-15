# Spot Market Knowledge Pool — Phase 0 Reconnaissance

_Date: 2026-04-14 | Author: Claude Code_

---

## 1. Executive Summary

A functional PDF-parsing pipeline already exists outside the bess-platform repo. The core PDF→price→DB path (`tools_pdf.py` + `spot_ingest.py` + `tools_db.py`) is **complete and production-quality**. The gap is everything above that layer: document registry, page/chunk text storage, Obsidian note generation, and retrieval. All of this can be added additively without touching the existing pipeline logic.

**Bottom line:** ~40% of Phase 1 is already written. The build plan below reuses all of it.

---

## 2. PDF Corpus

| Year | Count | Filename pattern |
|------|-------|-----------------|
| 2024 | 66 | `电力现货市场价格与运行日报（MM.DD）.pdf` |
| 2025 | 245 | same (daily + a handful of monthly `月报`) |
| 2026 | 40 | same (括号style change: `(MM.DD)` vs `（MM.DD）`) |
| **Total** | **351** | — |

All PDFs are now in `data/spot reports/{year}/`. The old spot-agent YAML config pointed to a different OneDrive path; it needs to be updated to point here.

Multi-day PDFs exist (e.g. `（10.19-21）`, `（11.30-12.2）`). `dates_from_filename()` in `spot_ingest.py` already handles these correctly.

---

## 3. Existing Code Found

### 3.1 `apps/spot-agent/` — canonical source is OUTSIDE the repo

**Critical finding:** The `apps/spot-agent/agent/` directory in the bess-platform repo contains only `__pycache__/` — **no `.py` source files**. The working source is at:

```
C:\Users\dipeng.chen\OneDrive\Envision Energy\Asset Investment Platform\Data\spot markets\spot-agent\agent\
```

This must be **copied into the repo** as the first build step.

### 3.2 `tools_pdf.py` — Complete, reusable as-is

| Function | Status | What it does |
|----------|--------|-------------|
| `parse_daily_report_multi(pdf_path, cfg)` | ✅ Complete | Multi-page/multi-day parser; infers page dates; dispatches DA/RT sections; extracts narrative text |
| `_parse_table_rows(page, provinces_cn, mode)` | ✅ Complete | Extracts avg/max/min from pdfplumber tables |
| `_extract_reason_sentences(text, provinces_cn)` | ✅ Complete | Regex extraction of cause/driver sentences |
| `_infer_page_date(text, year)` | ✅ Complete | Infers date from `M月D日` in page text |

Uses `pdfplumber` (installed in `.venv`). No OCR — works on text-layer PDFs only (adequate for these reports).

**Does NOT currently store:** raw page text, chunk text, page count, full narrative paragraphs. Only extracts prices + short highlight sentences.

### 3.3 `spot_ingest.py` — Complete CLI, reusable as-is

- `main()` + `argparse` with `--header` YAML config
- `dates_from_filename()` — handles single-day and multi-day filenames
- Orchestrates: glob PDFs → parse → LLM highlights → DB upsert
- Skip-logic is **absent** — it re-parses all PDFs every run (fine for now; hash-based skip to be added)

### 3.4 `tools_db.py` — Complete, reusable as-is

- Creates `public.spot_daily` with `(report_date, province_en)` unique constraint
- `upsert_da_rows()`, `upsert_rt_rows()`, `upsert_highlights_rows()` — all idempotent ON CONFLICT upserts
- Uses `MARKETDATA_DB_URL` / `DATABASE_URL` / `DB_URL` (different from rest of repo which uses `PGURL`)

### 3.5 `tools_llm.py` — Functional, minor gap

- `summarize_highlights()` — GPT-4o-mini call per province, returns <50-char summary
- `audit_price_row()` — **stub only**, signature defined, no implementation

### 3.6 `agent_run.py` — Broken, do not use

- References `parse_pdf_tables` and `digitize_hourly_chart` which do not exist in `tools_pdf.py`
- OpenAI Agents SDK path (`from agents import Agent, Runner`) is unresolvable
- **Skip this file entirely**

### 3.7 `services/document_intake/` — Reusable skeleton

- `register_document.py` — S3 upload + `raw_data.file_registry` registration, complete
- `extract_mengxi_settlement.py` — has indentation bug (line 173), PDF extraction is a stub
- `extract_mengxi_compensation.py` — hardcoded data only

### 3.8 `services/data_ingestion/shared/` — Fully reusable

- `db.py` — `get_engine()`, `upsert_staging()`, `delete_append()`
- `logging.py` — structured JSON logger with secret masking
- `control.py` — `ops.ingestion_job_runs` run tracking
- `context.py` — `RunContext` with `--mode/--start-date/--dry-run` argparse

---

## 4. Existing DB Schemas

### Already exists and usable

| Table | Schema | Reuse plan |
|-------|--------|-----------|
| `public.spot_daily` | prices + highlights per province/date | Keep as-is, already populated by spot_ingest |
| `raw_data.file_registry` | File registration + S3 metadata | Reuse for spot report source documents |
| `raw_data.file_manifest` | Extracted content per file | Reuse for chunk text storage |
| `core.document_registry` | Document metadata + status tracking | Reuse directly |
| `ops.ingestion_job_runs` | Run tracking | Reuse for knowledge pool runs |

### New tables needed (additive)

| Table | Schema | Purpose |
|-------|--------|---------|
| `staging.spot_report_documents` | staging | Lightweight source registry (hash, status, parse version) |
| `staging.spot_report_pages` | staging | Per-page raw text |
| `staging.spot_report_chunks` | staging | Chunked text for retrieval |
| `staging.spot_report_facts` | staging | Structured extracted facts |
| `staging.spot_report_notes` | staging | Note registry (path, type, generated_at) |

---

## 5. Existing Infrastructure Patterns

| Pattern | Location | Notes |
|---------|----------|-------|
| DB connection | `PGURL` env var + `get_engine()` | Main platform convention; spot-agent uses `DB_URL` — need to harmonise |
| Structured logging | `services/data_ingestion/shared/logging.py` | Reuse `get_logger()` |
| Run tracking | `services/data_ingestion/shared/control.py` | Reuse `start_run()`/`finish_run()` |
| CLI pattern | argparse + `if __name__ == '__main__'` | Follow same convention |
| ECS task | Fargate + EventBridge | Template from `data-ingestion/main.tf` |
| pdfplumber | Installed in `apps/spot-agent/.venv` | Needs to be added to `services/knowledge_pool/requirements.txt` |
| OpenAI | `openai==2.8.1` in spot-agent venv | Already used for highlights |

---

## 6. What Is Missing Entirely

1. **Source files not in repo** — `apps/spot-agent/agent/*.py` must be copied in
2. **No document/chunk text storage** — `spot_ingest.py` discards page text after parsing prices
3. **No hash-based skip** — re-parses every PDF every run
4. **No Obsidian/markdown note generation** — nothing exists
5. **No retrieval layer** — no embeddings, no full-text search, no CLI query tool
6. **No RAG packages** — no `langchain`, `chromadb`, `faiss`, `sentence-transformers` anywhere in repo
7. **`spot_hourly` table missing DDL** — API queries it but it doesn't exist
8. **`tools_db.py` uses `DB_URL` not `PGURL`** — minor inconsistency with rest of platform

---

## 7. Recommended Additive Build Plan

### Step 1 — Copy spot-agent sources into repo

Copy `tools_pdf.py`, `tools_db.py`, `tools_llm.py`, `spot_ingest.py`, `spot_header.yaml`, `schema.py`, `validator_tool.py` from the external OneDrive path into `apps/spot-agent/agent/`. Do not modify logic.

### Step 2 — DDL for knowledge pool tables

Create `db/ddl/staging/spot_report_knowledge.sql` with the 5 new tables. Update `tools_db.py` to also call `init_knowledge_tables()` on startup.

### Step 3 — Extend `spot_ingest.py` to store page text and register documents

Add a `--store-text` flag (default true). When enabled, also:
- Register each PDF in `staging.spot_report_documents` (path, hash, status)
- Store per-page text in `staging.spot_report_pages`
- Chunk text and store in `staging.spot_report_chunks`
- Skip files where hash matches existing record (idempotent re-runs)

This is **additive** — the existing price/highlight path is unchanged.

### Step 4 — Structured fact extraction

Add `fact_extract.py` to extract structured facts from page text:
- Report date, provinces mentioned
- DA/RT price statements
- Max/min province statements
- Cause/driver phrases

Store in `staging.spot_report_facts`.

### Step 5 — Markdown note generation

Add `note_generator.py` with three note templates:
- Daily report note (one per report date)
- Province note (one per province, accumulates across reports)
- Concept note (recurring driver themes)

Write to `knowledge/spot_market/` vault structure.

### Step 6 — Retrieval CLI

Add `spot_market_query.py` with basic full-text search over chunks + notes.

### Step 7 — Update YAML config + README

Update `spot_header.yaml` to point to `data/spot reports/` in the repo. Add `README_knowledge_pool.md`.

---

## 8. What to Reuse vs Build

| Component | Decision |
|-----------|----------|
| `tools_pdf.py` | ✅ Reuse as-is — copy into repo |
| `tools_db.py` | ✅ Reuse as-is — copy into repo, add `init_knowledge_tables()` |
| `tools_llm.py` | ✅ Reuse `summarize_highlights()` — copy into repo |
| `spot_ingest.py` | ✅ Reuse as base — extend with `--store-text` path |
| `services/data_ingestion/shared/logging.py` | ✅ Reuse as-is |
| `services/data_ingestion/shared/control.py` | ✅ Reuse for run tracking |
| `raw_data.file_registry` | ✅ Reuse for source doc registration |
| `agent_run.py` | ❌ Skip — broken, references non-existent functions |
| `extract_mengxi_settlement.py` | ⚠️ Separate concern — fix later |
| Vector/RAG layer | 🔜 Defer to Phase 2 — not needed for MVP |

---

## 9. Phase 1 Scope (MVP)

Ingestion of all 351 PDFs with:
- Document registry (hash, status)
- Page text stored in DB
- Chunked text stored in DB
- DA/RT price facts extracted (already done by existing pipeline)
- Narrative driver facts extracted (structured)
- Obsidian-compatible markdown notes generated for:
  - Each unique report date
  - Each province (蒙西, 蒙东, 山西, 宁夏, 甘肃 etc.)
  - Top recurring concept themes
- Simple retrieval CLI over chunks + notes

Explicitly deferred:
- Embeddings / vector search
- ECS deployment (run locally first)
- Settlement / trading rule knowledge pools
- `spot_hourly` table

---

_End of Phase 0 Recon Report_
