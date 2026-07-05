-- db/ddl/hermes/005_thinking_log.sql
-- Run: psql $PGURL -f db/ddl/hermes/005_thinking_log.sql

CREATE TABLE IF NOT EXISTS hermes.thinking_log (
    id             BIGSERIAL PRIMARY KEY,
    mode           TEXT NOT NULL CHECK (mode IN ('health', 'design')),
    ts             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    files_read     TEXT[],
    tables_checked TEXT[],
    message_sent   TEXT,          -- full text sent to Feishu (used for dedup)
    model_used     TEXT
);

CREATE INDEX IF NOT EXISTS idx_thinking_log_mode_ts
    ON hermes.thinking_log (mode, ts DESC);
