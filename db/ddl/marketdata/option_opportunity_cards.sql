-- db/ddl/marketdata/option_opportunity_cards.sql
-- Generative investment scout: opportunity cards (Task 1)
-- Only services/option_engine/scout/store.py writes this table
-- signature is the stable dedup key built as kind|province|asset_type|scan_key
CREATE TABLE IF NOT EXISTS marketdata.option_opportunity_cards (
    id                BIGSERIAL PRIMARY KEY,
    created_at        TIMESTAMPTZ DEFAULT NOW(),
    kind              TEXT CHECK (kind IN ('permit','mna','trading')),
    province          TEXT,
    asset_type        TEXT,
    thesis            TEXT,
    evidence_json     JSONB,
    validation        TEXT CHECK (validation IN ('confirmed','weakened','no_evidence','pending')),
    validation_detail JSONB,
    status            TEXT DEFAULT 'new' CHECK (status IN ('new','approved','rejected','snoozed','promoted','carried')),
    review_note       TEXT,
    reviewed_at       TIMESTAMPTZ,
    scan_params       JSONB,
    scan_date         DATE,
    signature         TEXT
);

CREATE INDEX IF NOT EXISTS idx_option_opportunity_cards_status_scan_date
    ON marketdata.option_opportunity_cards (status, scan_date DESC);
