CREATE TABLE IF NOT EXISTS intl_market.gb_ingestion_log (
    id               SERIAL PRIMARY KEY,
    run_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    trigger          TEXT NOT NULL,          -- 'scheduled' | 'manual'
    date_from        DATE,
    date_to          DATE,
    status           TEXT NOT NULL,          -- 'success' | 'error'
    rows_ingested    JSONB,
    error_msg        TEXT,
    duration_seconds NUMERIC
);
