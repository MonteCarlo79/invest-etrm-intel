# Spot Market Knowledge Pool

Structured storage and retrieval layer for China electricity spot market daily report PDFs.
Ingests raw PDFs, extracts structured facts, and generates Obsidian-compatible markdown notes.

**Status**: Phase 1 complete + hardened. 350 PDFs ingested (2024–2026), 15,728 chunks, 2,606 facts, 895 notes generated.
See `docs/spot_market_knowledge_pool_validation.md` for full validation report.

---

## Prerequisites

| Requirement | Version | Notes |
|------------|---------|-------|
| Python | 3.11+ | Anaconda 3.13 used in dev |
| PostgreSQL | 14+ (18.2 in dev) | `staging` schema, `ops` schema optional |
| pdfplumber | ≥0.10 | pip install |
| psycopg2-binary | ≥2.9 | pip install |
| python-dotenv | ≥1.0 | pip install |
| pyyaml | ≥6.0 | pip install |

Install all at once:
```bash
pip install -r services/knowledge_pool/requirements.txt
```

On Windows with Anaconda (shell path issue workaround):
```bash
/c/ProgramData/anaconda3/python.exe -m pip install pdfplumber psycopg2-binary python-dotenv pyyaml
```

---

## Environment Variables

The DB connection is resolved in priority order:

```
PGURL           postgresql://user:pass@host:5432/dbname   ← preferred
DB_URL          ...
DATABASE_URL    ...
MARKETDATA_DB_URL ...
```

`.env` files are loaded automatically from (first found wins):
1. `<repo root>/.env`
2. `<repo root>/apps/spot-agent/.env`

In development, `DB_URL` is in `apps/spot-agent/.env`. No action needed if that file exists.

---

## Database Setup

DDL file: `db/ddl/staging/spot_report_knowledge.sql`

**First-time setup** (creates all 5 staging tables + GIN index):
```bash
# Option A: via CLI flag
python scripts/spot_market_ingest.py --init-db

# Option B: direct SQL
psql $PGURL -f db/ddl/staging/spot_report_knowledge.sql
```

**Tables created:**

| Table | Purpose |
|-------|---------|
| `staging.spot_report_documents` | One row per PDF; SHA-256 hash, ingest_status, date range |
| `staging.spot_report_pages` | Raw page text with per-page date inference |
| `staging.spot_report_chunks` | 500-char overlapping text chunks (GIN FTS indexed) |
| `staging.spot_report_facts` | Typed facts: driver phrases, prices, markers |
| `staging.spot_report_notes` | Registry of generated markdown note files |

**Migration** (adding `source_method` column to existing installs):
```sql
ALTER TABLE staging.spot_report_facts
    ADD COLUMN IF NOT EXISTS source_method text NOT NULL DEFAULT 'pdf_regex';
```
This is idempotent and included in the DDL file.

---

## Ingest Command

```bash
# Ingest all years (scans data/spot reports/2024/, /2025/, /2026/)
python scripts/spot_market_ingest.py

# Ingest one year only
python scripts/spot_market_ingest.py --year 2025

# Smoke test: first 5 PDFs only
python scripts/spot_market_ingest.py --year 2025 --limit 5

# Single file
python scripts/spot_market_ingest.py --pdf "data/spot reports/2025/电力现货市场价格与运行日报 (7.16).pdf"

# First-time setup (create tables + ingest)
python scripts/spot_market_ingest.py --init-db --year 2025

# Force re-ingest already-parsed files
python scripts/spot_market_ingest.py --year 2025 --force
```

**What happens during ingest** (per PDF):
1. **Register**: SHA-256 hash checked against `spot_report_documents`. Skip if already `parsed` (unless `--force`).
2. **Pages**: pdfplumber extracts text layer per page. Date inferred from `(\d{1,2})月(\d{1,2})日` regex + year from parent directory name.
3. **Chunks**: 500-char overlapping chunks (100-char stride) written to `spot_report_chunks`. GIN index keeps FTS current.
4. **Facts (pdf_regex)**: Province-aware regex extraction — driver phrases (`原因为`), section markers, interprovincial mentions, inline prices.
5. **Facts (spot_daily_bridge)**: Price rows copied from `public.spot_daily` if that table exists. Silently skipped with `[WARN]` if absent.
6. **Status**: Document marked `parsed`. Bridge failure does NOT roll back parsed status.

**Year inference**: Year is derived from the PDF's containing directory name (e.g., `data/spot reports/2025/` → year=2025). The `--year` flag is a fallback only. This ensures 2024 and 2026 PDFs are dated correctly even in an all-years scan.

---

## Note Generation Command

```bash
# Generate all note types (daily + province + concept + index)
python scripts/spot_market_generate_notes.py

# One type
python scripts/spot_market_generate_notes.py --type daily
python scripts/spot_market_generate_notes.py --type province
python scripts/spot_market_generate_notes.py --type concept
python scripts/spot_market_generate_notes.py --type index

# Single date
python scripts/spot_market_generate_notes.py --date 2025-07-16

# Single province
python scripts/spot_market_generate_notes.py --province 蒙西
```

**Note vault location**: `knowledge/spot_market/`

```
knowledge/spot_market/
  01_daily_reports/    YYYY-MM-DD.md — one per report date
  02_provinces/        省份名.md     — rolling summary per province (27 provinces)
  03_concepts/         concept.md    — cross-report concept notes (7 concepts)
  04_indices/          _index.md     — master index with corpus stats
```

**Notes are deterministic**: same input data → identical MD5 hash on re-runs. Safe to regenerate at any time.

**Multi-document dates**: If two PDFs cover the same date (e.g., a single-day report and a multi-day aggregate), the higher `document_id` wins (last-processed). This is expected behavior.

---

## Query Command

```bash
# Full-text search
python scripts/spot_market_query.py search "新能源出力下降"

# Search with filters
python scripts/spot_market_query.py search "均价偏高" --province 山东 --from 2025-07-01 --to 2025-12-31

# Show structured facts
python scripts/spot_market_query.py facts --type driver --province 蒙西
python scripts/spot_market_query.py facts --type price_da --province 山东

# List ingested documents
python scripts/spot_market_query.py docs

# List generated notes
python scripts/spot_market_query.py notes
```

**FTS notes**: PostgreSQL `simple` config treats uninterrupted Chinese character sequences as single tokens. Queries of ≤4 characters automatically use `ILIKE %query%` for better recall. Longer queries use GIN index (faster but may miss phrases that span page breaks or use variant wording).

---

## Module Reference

| Module | Key exports | Purpose |
|--------|------------|---------|
| `db.py` | `get_conn()`, `init_knowledge_tables()` | DB connection with env fallback chain |
| `document_registry.py` | `register_document()`, `set_document_status()` | SHA-256 dedup, status tracking |
| `pdf_ingestion.py` | `extract_and_store_pages()`, `build_and_store_chunks()` | pdfplumber extraction + chunking |
| `fact_extraction.py` | `extract_facts_for_document()`, `pull_price_facts_from_spot_daily()` | Regex + bridge extraction |
| `markdown_notes.py` | `generate_daily_note()`, `generate_province_note()`, `generate_concept_note()`, `generate_index_note()` | Obsidian note generation |
| `retrieval.py` | `search_chunks()`, `get_facts()`, `get_note_index()` | FTS + structured queries |

---

## What Is PDF-Derived vs DB-Enriched vs Inferred

Understanding provenance is important for downstream confidence scoring:

| Data | Source | `source_method` / `confidence` | Notes |
|------|--------|-------------------------------|-------|
| Page text | pdfplumber text layer | — | Only text-layer PDFs work; scanned images return empty |
| Page dates | `月日` regex + directory year | — | 25.8% coverage; continuation pages lack date headers |
| Driver phrases (`原因为`) | `pdf_regex` | `medium` | Province allowlist; `主要原因是`/`由于` variants not captured |
| Section markers | `pdf_regex` | `high` | Literal string match (`现货实时市场` etc.); very reliable |
| Interprovincial mentions | `pdf_regex` | `low` | Context window ±20/80 chars around match |
| Inline prices | `pdf_regex` | `medium` | Numeric + `元/MWh` pattern; unit variants may miss |
| Price rank statements | `pdf_regex` | `low` | Broad CJK match for province in rank context |
| DA/RT avg/max/min prices | `spot_daily_bridge` | `high` | Copied from `public.spot_daily`; absent if price pipeline not run |

**To populate bridge price facts**: run `spot_ingest.py` (the existing spot price pipeline) first to populate `public.spot_daily`, then re-run with `--force` to trigger the bridge step.

---

## Common Failure Modes

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| `relation "public.spot_daily" does not exist` | Price pipeline not run | `[WARN]` only — document still ingests. Run spot price pipeline to enable bridge facts. |
| `relation "staging.spot_report_documents" does not exist` | Tables not created | Run with `--init-db` or `psql $PGURL -f db/ddl/staging/spot_report_knowledge.sql` |
| `PGURL not set` / connection error | Missing env var | Check `apps/spot-agent/.env` exists and contains `DB_URL=postgresql://...` |
| All PDFs show `[SKIP]` | Already parsed | Use `--force` to re-ingest. Check `SELECT count(*) FROM staging.spot_report_documents WHERE ingest_status='parsed'` |
| `page_count=0` / document marked `empty` | Scanned (image) PDF | pdfplumber cannot extract from image PDFs. No fix without OCR. |
| Python not found on Windows | Windows Store stub | Use full path: `/c/ProgramData/anaconda3/python.exe scripts/spot_market_ingest.py ...` |
| Driver facts: 0 provinces matched | Province name not in allowlist | Check `_KNOWN_PROVINCES` in `fact_extraction.py`. Add new province and re-run `--force`. |
| `--force` with year change leaves stale facts | re-registration returns early | Manually: `DELETE FROM staging.spot_report_facts WHERE document_id = %s` before re-run. |
| Notes not updating after re-ingest | Generation not re-run | Run `python scripts/spot_market_generate_notes.py` after any `--force` re-ingest. |

---

## Known Limitations (as of Phase 1)

1. **`report_year` column not updated on `--force` re-registration** — not used in queries; use `report_date_min/max` instead.
2. **Driver recall**: `原因为` pattern only. Reports using `主要原因是` or `由于` are missed. See validation report §9.
3. **`spot_daily_bridge` facts absent** until `public.spot_daily` is populated by the spot price pipeline.
4. **25.8% page date coverage**: inherent to PDF layout — continuation table pages have no date header.
5. **Chinese FTS**: phrases >4 chars require word-boundary tokenization for exact recall. Use shorter sub-phrases for better hit rates.

Full imperfections table: `docs/spot_market_knowledge_pool_validation.md §9`.

---

## Reuse for Settlement Knowledge Pool

Per `docs/spot_market_knowledge_pool_validation.md §10 recommendation 5`:

The following modules can be reused **as-is** for the settlement knowledge pool:
- `document_registry.py` — hash-based dedup is pipeline-agnostic
- `pdf_ingestion.py` — page extraction and chunking are generic
- `markdown_notes.py` — note templates need settlement-specific sections but the generation scaffold is reusable
- `db.py`, `retrieval.py` — no spot-specific logic

Only `fact_extraction.py` needs settlement-specific regex patterns and a new `provinces_map` (or a different grouping key).

DDL: add new tables to `db/ddl/staging/` following the same naming convention (`settlement_report_*`).
