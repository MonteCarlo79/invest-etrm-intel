-- db/ddl/staging/spot_report_knowledge.sql
-- Knowledge pool tables for spot market daily report ingestion
-- Created: 2026-04-14
-- Part of: spot market knowledge pool (Phase 1)
-- Pattern: additive only — does not touch existing spot_daily or raw_data tables

CREATE SCHEMA IF NOT EXISTS staging;

-- ============================================================
-- 1. Source document registry
--    One row per PDF file. Hash-based dedup for idempotent runs.
-- ============================================================
CREATE TABLE IF NOT EXISTS staging.spot_report_documents (
    id              bigserial       PRIMARY KEY,
    source_path     text            NOT NULL,           -- absolute or repo-relative path
    file_name       text            NOT NULL,
    report_year     smallint        NOT NULL,
    report_date_min date,                               -- earliest date in this PDF
    report_date_max date,                               -- latest date (multi-day reports)
    file_hash       text            NOT NULL,           -- SHA-256 hex
    file_size_bytes bigint,
    page_count      int,
    ingest_status   text            NOT NULL DEFAULT 'pending',
                                                        -- pending | parsed | failed | skipped
    parser_version  text            NOT NULL DEFAULT 'v1',
    parse_error     text,
    created_at      timestamptz     NOT NULL DEFAULT now(),
    updated_at      timestamptz     NOT NULL DEFAULT now(),
    UNIQUE (file_hash)
);

CREATE INDEX IF NOT EXISTS idx_srd_year     ON staging.spot_report_documents(report_year);
CREATE INDEX IF NOT EXISTS idx_srd_date_min ON staging.spot_report_documents(report_date_min);
CREATE INDEX IF NOT EXISTS idx_srd_status   ON staging.spot_report_documents(ingest_status);

COMMENT ON TABLE staging.spot_report_documents IS
    'Source registry for ingested spot market daily report PDFs (电力现货市场价格与运行日报)';

-- ============================================================
-- 2. Per-page raw text
--    Full text of every page, preserved for retrieval and re-parsing.
-- ============================================================
CREATE TABLE IF NOT EXISTS staging.spot_report_pages (
    id              bigserial       PRIMARY KEY,
    document_id     bigint          NOT NULL REFERENCES staging.spot_report_documents(id) ON DELETE CASCADE,
    page_no         smallint        NOT NULL,           -- 1-indexed
    page_date       date,                               -- inferred date for this page (may be null)
    extracted_text  text,
    char_count      int,
    extraction_method text          NOT NULL DEFAULT 'pdfplumber',
    created_at      timestamptz     NOT NULL DEFAULT now(),
    UNIQUE (document_id, page_no)
);

CREATE INDEX IF NOT EXISTS idx_srp_doc     ON staging.spot_report_pages(document_id);
CREATE INDEX IF NOT EXISTS idx_srp_date    ON staging.spot_report_pages(page_date);

COMMENT ON TABLE staging.spot_report_pages IS
    'Per-page extracted text from spot report PDFs';

-- ============================================================
-- 3. Chunked text
--    500-char overlapping chunks for retrieval. Stable chunk_index
--    within a document allows idempotent re-generation.
-- ============================================================
CREATE TABLE IF NOT EXISTS staging.spot_report_chunks (
    id              bigserial       PRIMARY KEY,
    document_id     bigint          NOT NULL REFERENCES staging.spot_report_documents(id) ON DELETE CASCADE,
    page_no         smallint,
    chunk_index     int             NOT NULL,           -- sequential within document
    chunk_text      text            NOT NULL,
    chunk_type      text            NOT NULL DEFAULT 'body',
                                                        -- body | table | header | reason
    report_date     date,                               -- page_date carried forward if known
    created_at      timestamptz     NOT NULL DEFAULT now(),
    UNIQUE (document_id, chunk_index)
);

CREATE INDEX IF NOT EXISTS idx_src_doc     ON staging.spot_report_chunks(document_id);
CREATE INDEX IF NOT EXISTS idx_src_date    ON staging.spot_report_chunks(report_date);

-- Full-text search index over chunk text (Chinese-friendly GIN on tsvector)
CREATE INDEX IF NOT EXISTS idx_src_fts ON staging.spot_report_chunks
    USING gin(to_tsvector('simple', chunk_text));

COMMENT ON TABLE staging.spot_report_chunks IS
    'Chunked text blocks from spot report PDFs for retrieval';

-- ============================================================
-- 4. Structured extracted facts
--    Typed key/value pairs per report date + province.
-- ============================================================
CREATE TABLE IF NOT EXISTS staging.spot_report_facts (
    id              bigserial       PRIMARY KEY,
    document_id     bigint          NOT NULL REFERENCES staging.spot_report_documents(id) ON DELETE CASCADE,
    report_date     date            NOT NULL,
    province_cn     text,                               -- NULL = national-level fact
    province_en     text,
    fact_type       text            NOT NULL,
                                                        -- price_da | price_rt | driver | volume | interprovincial | summary
    metric_name     text,                               -- da_avg / rt_max / reason_text etc.
    metric_value    numeric,                            -- NULL for text facts
    metric_unit     text,                               -- yuan/MWh, MW, GWh, etc.
    fact_text       text,                               -- raw source sentence/phrase
    page_no         smallint,
    confidence      text            NOT NULL DEFAULT 'medium',
                                                        -- high | medium | low
    source_method   text            NOT NULL DEFAULT 'pdf_regex',
                                                        -- pdf_regex | spot_daily_bridge | filename_inference
    created_at      timestamptz     NOT NULL DEFAULT now(),
    UNIQUE (document_id, report_date, province_cn, fact_type, metric_name)
);

-- Migration: add source_method to any pre-existing staging.spot_report_facts table
ALTER TABLE staging.spot_report_facts
    ADD COLUMN IF NOT EXISTS source_method text NOT NULL DEFAULT 'pdf_regex';

CREATE INDEX IF NOT EXISTS idx_srf_doc       ON staging.spot_report_facts(document_id);
CREATE INDEX IF NOT EXISTS idx_srf_date      ON staging.spot_report_facts(report_date);
CREATE INDEX IF NOT EXISTS idx_srf_province  ON staging.spot_report_facts(province_cn);
CREATE INDEX IF NOT EXISTS idx_srf_type      ON staging.spot_report_facts(fact_type);

COMMENT ON TABLE staging.spot_report_facts IS
    'Structured facts extracted from spot reports (prices, drivers, volumes)';

-- ============================================================
-- 5. Knowledge note registry
--    Tracks generated Obsidian-compatible markdown notes.
-- ============================================================
CREATE TABLE IF NOT EXISTS staging.spot_report_notes (
    id              bigserial       PRIMARY KEY,
    document_id     bigint          REFERENCES staging.spot_report_documents(id) ON DELETE SET NULL,
                                                        -- NULL for aggregate notes (province / concept)
    note_type       text            NOT NULL,           -- daily_report | province | concept
    note_key        text            NOT NULL,           -- e.g. "2025-07-28" | "蒙西" | "新能源出力下降"
    note_path       text            NOT NULL,           -- repo-relative path to .md file
    note_title      text,
    report_date_min date,                               -- date range covered
    report_date_max date,
    generated_at    timestamptz     NOT NULL DEFAULT now(),
    updated_at      timestamptz     NOT NULL DEFAULT now(),
    UNIQUE (note_type, note_key)
);

CREATE INDEX IF NOT EXISTS idx_srn_type ON staging.spot_report_notes(note_type);
CREATE INDEX IF NOT EXISTS idx_srn_key  ON staging.spot_report_notes(note_key);

COMMENT ON TABLE staging.spot_report_notes IS
    'Registry of generated Obsidian-compatible markdown knowledge notes';
