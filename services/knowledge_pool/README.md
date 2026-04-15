# Spot Market Knowledge Pool

Stores and structures text extracted from China spot market daily report PDFs.

## Components

| Module | Purpose |
|--------|---------|
| `db.py` | DB connection (PGURL → DB_URL → DATABASE_URL fallback chain) |
| `document_registry.py` | SHA-256 hash-based deduplication and status tracking |
| `pdf_ingestion.py` | Full page text extraction + overlapping chunks |
| `fact_extraction.py` | Driver phrase extraction + price fact bridge from `public.spot_daily` |
| `markdown_notes.py` | Obsidian-compatible note generation (daily/province/concept/index) |
| `retrieval.py` | Full-text search and structured fact queries |

## Database tables (staging schema)

```
staging.spot_report_documents   — one row per PDF, SHA-256 hash, ingest_status
staging.spot_report_pages       — raw page text, page_date per page
staging.spot_report_chunks      — 500-char overlapping chunks, GIN FTS index
staging.spot_report_facts       — typed facts: price_da, price_rt, driver, interprovincial
staging.spot_report_notes       — registry of generated markdown note files
```

DDL: `db/ddl/staging/spot_report_knowledge.sql`

## CLI scripts

### Ingest PDFs
```bash
# Ingest all 2025 PDFs (hash-based skip for already-done files)
PGURL=postgresql://... python scripts/spot_market_ingest.py --year 2025

# First run: create tables if not yet created
PGURL=postgresql://... python scripts/spot_market_ingest.py --year 2025 --init-db

# Test with 5 files first
PGURL=postgresql://... python scripts/spot_market_ingest.py --year 2025 --limit 5

# Single file
PGURL=postgresql://... python scripts/spot_market_ingest.py --pdf "data/spot reports/2025/日报 (7.16).pdf"

# Force re-ingest (overwrite existing)
PGURL=postgresql://... python scripts/spot_market_ingest.py --year 2025 --force
```

### Generate markdown notes
```bash
# Generate all note types
PGURL=postgresql://... python scripts/spot_market_generate_notes.py

# One type only
PGURL=postgresql://... python scripts/spot_market_generate_notes.py --type province

# Single daily note
PGURL=postgresql://... python scripts/spot_market_generate_notes.py --date 2025-07-16
```

Notes are written to `knowledge/spot_market/` and are Obsidian-compatible with YAML frontmatter.

### Query the pool
```bash
# Full-text search
PGURL=postgresql://... python scripts/spot_market_query.py search "新能源出力下降"

# Filter by province and date range
PGURL=postgresql://... python scripts/spot_market_query.py search "均价偏高" --province 山东 --from 2025-07-01

# Show price facts
PGURL=postgresql://... python scripts/spot_market_query.py facts --type price_da --province 山东

# List ingested documents
PGURL=postgresql://... python scripts/spot_market_query.py docs

# List generated notes
PGURL=postgresql://... python scripts/spot_market_query.py notes
```

## Note vault structure

```
knowledge/spot_market/
  01_daily_reports/   — one note per report date (YYYY-MM-DD.md)
  02_provinces/       — rolling note per province (省份名.md)
  03_concepts/        — cross-report concept notes (concept_key.md)
  04_indices/         — master index note
```

Each note has clearly separated `## Source-backed Summary` and `## Analyst / LLM Interpretation` sections.

## Environment

Set one of (in priority order):
```
PGURL=postgresql://user:pass@host:5432/dbname
DB_URL=...
DATABASE_URL=...
MARKETDATA_DB_URL=...
```

A `.env` file in the repo root is loaded automatically if present.

## Dependencies

```
pdfplumber>=0.10
psycopg2-binary>=2.9
python-dotenv>=1.0
pyyaml>=6.0
openai>=1.0        # only needed for LLM-augmented note generation (future)
```

Install: `pip install -r services/knowledge_pool/requirements.txt`
