-- Operating-assets knowledge base: ops_knowledge_docs / ops_knowledge_chunks
-- Parallel to staging.spot_knowledge_* (market knowledge pool), but scoped to
-- operating-asset documents: 复盘/backtest/incident/maintenance reports dropped
-- into assets/operating/复盘/ (ingested by services/knowledge_pool/ops_watcher.py).
-- Idempotent: safe to run repeatedly. Keep in sync with the inline _DDL in
-- services/knowledge_pool/ops_knowledge_docs.py.

CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.ops_knowledge_docs (
    id              SERIAL PRIMARY KEY,
    file_name       TEXT NOT NULL,
    file_hash       TEXT UNIQUE NOT NULL,          -- SHA-256 hex; dedup key
    category        TEXT NOT NULL DEFAULT 'other',
    asset_id        INTEGER REFERENCES marketdata.rm_assets(id),  -- NULL = multi-asset/unmatched
    title           TEXT,
    doc_date        DATE,
    source_path     TEXT,
    file_size_bytes INT,
    page_count      INT DEFAULT 0,
    ingest_status   TEXT NOT NULL DEFAULT 'pending',  -- pending|parsed|failed
    parse_error     TEXT,
    active          BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.ops_knowledge_chunks (
    id          SERIAL PRIMARY KEY,
    doc_id      INT NOT NULL REFERENCES staging.ops_knowledge_docs(id),
    page_no     INT,
    chunk_index INT NOT NULL,
    chunk_text  TEXT NOT NULL,
    UNIQUE(doc_id, chunk_index)
);

CREATE INDEX IF NOT EXISTS idx_okc_fts ON staging.ops_knowledge_chunks
    USING GIN(to_tsvector('simple', chunk_text));

CREATE INDEX IF NOT EXISTS idx_okd_asset ON staging.ops_knowledge_docs(asset_id);
CREATE INDEX IF NOT EXISTS idx_okd_category ON staging.ops_knowledge_docs(category);
