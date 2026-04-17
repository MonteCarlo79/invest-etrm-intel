-- db/ddl/staging/settlement_report_knowledge.sql
-- Settlement knowledge pool tables
-- Additive only. Does not touch spot_report_* or any existing table.
-- Created: 2026-04-15

CREATE SCHEMA IF NOT EXISTS staging;

-- ============================================================
-- 1. Source document registry
-- ============================================================
CREATE TABLE IF NOT EXISTS staging.settlement_report_documents (
    id                  bigserial       PRIMARY KEY,
    source_path         text            NOT NULL,
    file_name           text            NOT NULL,
    asset_slug          text,           -- NULL for capacity_compensation (multi-asset)
    invoice_dir_code    text,           -- e.g. 'B-6'; display / reverse-lookup
    settlement_year     smallint        NOT NULL,
    settlement_month    smallint        NOT NULL CHECK (settlement_month BETWEEN 1 AND 12),
    period_half         text            NOT NULL DEFAULT 'full',
                        -- full|commissioning_supplement|issuer_trading_center|issuer_plant|other
    invoice_type        text            NOT NULL,
                        -- grid_injection|grid_withdrawal|rural_grid|capacity_compensation
    period_notes        text,
    report_date_min     date,
    report_date_max     date,
    file_hash           text            NOT NULL,
    file_size_bytes     bigint,
    page_count          int,
    ingest_status       text            NOT NULL DEFAULT 'pending',
                        -- pending|parsed|empty|unresolved_asset|error
    parser_version      text            NOT NULL DEFAULT 'v1',
    parse_error         text,
    core_document_id    uuid,           -- optional FK to core.document_registry
    created_at          timestamptz     NOT NULL DEFAULT now(),
    updated_at          timestamptz     NOT NULL DEFAULT now(),
    UNIQUE (file_hash)
);

CREATE INDEX IF NOT EXISTS idx_srd_settl_asset   ON staging.settlement_report_documents(asset_slug);
CREATE INDEX IF NOT EXISTS idx_srd_settl_period  ON staging.settlement_report_documents(settlement_year, settlement_month);
CREATE INDEX IF NOT EXISTS idx_srd_settl_type    ON staging.settlement_report_documents(invoice_type);
CREATE INDEX IF NOT EXISTS idx_srd_settl_status  ON staging.settlement_report_documents(ingest_status);

-- ============================================================
-- 2. Per-page raw text
-- ============================================================
CREATE TABLE IF NOT EXISTS staging.settlement_report_pages (
    id                  bigserial       PRIMARY KEY,
    document_id         bigint          NOT NULL REFERENCES staging.settlement_report_documents(id) ON DELETE CASCADE,
    page_no             smallint        NOT NULL,
    page_date           date,
    extracted_text      text,
    char_count          int,
    extraction_method   text            NOT NULL DEFAULT 'pdfplumber',
    created_at          timestamptz     NOT NULL DEFAULT now(),
    UNIQUE (document_id, page_no)
);

CREATE INDEX IF NOT EXISTS idx_srp_settl_doc  ON staging.settlement_report_pages(document_id);

-- ============================================================
-- 3. Chunked text (GIN FTS)
-- ============================================================
CREATE TABLE IF NOT EXISTS staging.settlement_report_chunks (
    id                  bigserial       PRIMARY KEY,
    document_id         bigint          NOT NULL REFERENCES staging.settlement_report_documents(id) ON DELETE CASCADE,
    page_no             smallint,
    chunk_index         int             NOT NULL,
    chunk_text          text            NOT NULL,
    chunk_type          text            NOT NULL DEFAULT 'body',
                        -- body|table|header|amount_line
    report_date         date,
    created_at          timestamptz     NOT NULL DEFAULT now(),
    UNIQUE (document_id, chunk_index)
);

CREATE INDEX IF NOT EXISTS idx_src_settl_doc ON staging.settlement_report_chunks(document_id);
CREATE INDEX IF NOT EXISTS idx_src_settl_fts ON staging.settlement_report_chunks
    USING gin(to_tsvector('simple', chunk_text));

-- ============================================================
-- 4. Structured extracted facts
-- ============================================================
CREATE TABLE IF NOT EXISTS staging.settlement_report_facts (
    id                  bigserial       PRIMARY KEY,
    document_id         bigint          NOT NULL REFERENCES staging.settlement_report_documents(id) ON DELETE CASCADE,
    asset_slug          text            NOT NULL,
    settlement_year     smallint        NOT NULL,
    settlement_month    smallint        NOT NULL,
    period_half         text            NOT NULL DEFAULT 'full',
    invoice_type        text            NOT NULL,
    fact_type           text            NOT NULL,
                        -- energy_mwh|energy_kwh|charge_component|total_amount|capacity_compensation|penalty
    component_name      text,           -- canonical normalized name
    component_group     text,           -- energy|ancillary|system|power_quality|subsidy|policy|adjustment|total
    metric_value        numeric,
    metric_unit         text,           -- yuan|kWh|MWh|yuan/MWh
    fact_text           text            NOT NULL,
    page_no             smallint        NOT NULL,
    confidence          text            NOT NULL DEFAULT 'medium',
    source_method       text            NOT NULL DEFAULT 'pdf_regex',
                        -- pdf_regex|table_extraction|manual_entry|prior_extract
    created_at          timestamptz     NOT NULL DEFAULT now(),
    UNIQUE (document_id, asset_slug, fact_type, component_name, period_half)
);

CREATE INDEX IF NOT EXISTS idx_srf_settl_asset  ON staging.settlement_report_facts(asset_slug);
CREATE INDEX IF NOT EXISTS idx_srf_settl_period ON staging.settlement_report_facts(settlement_year, settlement_month);
CREATE INDEX IF NOT EXISTS idx_srf_settl_type   ON staging.settlement_report_facts(fact_type);
CREATE INDEX IF NOT EXISTS idx_srf_settl_comp   ON staging.settlement_report_facts(component_name);

-- ============================================================
-- 5. Reconciliation
-- ============================================================
CREATE TABLE IF NOT EXISTS staging.settlement_reconciliation (
    id                  bigserial       PRIMARY KEY,
    asset_slug          text            NOT NULL,
    settlement_year     smallint        NOT NULL,
    settlement_month    smallint        NOT NULL,
    invoice_type        text            NOT NULL,
    fact_type           text            NOT NULL,
    component_name      text,
    version_a_doc_id    bigint          NOT NULL REFERENCES staging.settlement_report_documents(id),
    version_b_doc_id    bigint          NOT NULL REFERENCES staging.settlement_report_documents(id),
    value_a             numeric,
    value_b             numeric,
    delta               numeric GENERATED ALWAYS AS (value_b - value_a) STORED,
    delta_pct           numeric GENERATED ALWAYS AS (
                            CASE WHEN value_a <> 0
                            THEN ROUND((value_b - value_a) / ABS(value_a) * 100, 4)
                            ELSE NULL END
                        ) STORED,
    flagged             boolean         NOT NULL DEFAULT FALSE,
    flag_reason         text,
    flag_threshold_pct  numeric         NOT NULL DEFAULT 1.0,
    flag_threshold_abs  numeric         NOT NULL DEFAULT 500.0,
    created_at          timestamptz     NOT NULL DEFAULT now(),
    UNIQUE (asset_slug, settlement_year, settlement_month, invoice_type,
            fact_type, component_name, version_a_doc_id, version_b_doc_id)
);

CREATE INDEX IF NOT EXISTS idx_srecon_asset  ON staging.settlement_reconciliation(asset_slug);
CREATE INDEX IF NOT EXISTS idx_srecon_period ON staging.settlement_reconciliation(settlement_year, settlement_month);
CREATE INDEX IF NOT EXISTS idx_srecon_flag   ON staging.settlement_reconciliation(flagged);

-- ============================================================
-- 6. Note registry
-- ============================================================
CREATE TABLE IF NOT EXISTS staging.settlement_report_notes (
    id                  bigserial       PRIMARY KEY,
    document_id         bigint          REFERENCES staging.settlement_report_documents(id) ON DELETE SET NULL,
    note_type           text            NOT NULL,
                        -- monthly_asset|asset_summary|charge_component|reconciliation
    note_key            text            NOT NULL,
    note_path           text            NOT NULL,
    note_title          text,
    settlement_year     smallint,
    settlement_month    smallint,
    asset_slug          text,
    generated_at        timestamptz     NOT NULL DEFAULT now(),
    updated_at          timestamptz     NOT NULL DEFAULT now(),
    UNIQUE (note_type, note_key)
);
