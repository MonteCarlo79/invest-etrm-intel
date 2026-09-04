-- db/ddl/marketdata/deal_committee.sql
CREATE TABLE IF NOT EXISTS marketdata.deal_briefs (
    id           SERIAL PRIMARY KEY,
    deal_name    TEXT NOT NULL,
    brief        JSONB NOT NULL,
    confirmed    BOOLEAN NOT NULL DEFAULT FALSE,
    source_files TEXT[],
    created_at   TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS marketdata.deal_daf_library (
    id             SERIAL PRIMARY KEY,
    brief_id       INTEGER REFERENCES marketdata.deal_briefs(id),
    deal_name      TEXT NOT NULL,
    filename       TEXT NOT NULL,
    pdf_data       BYTEA NOT NULL,
    file_size_kb   INTEGER,
    recommendation TEXT,
    created_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_deal_daf_brief ON marketdata.deal_daf_library(brief_id);

-- Full analysis results (sections + KPIs + synthesis) for in-tab history viewing.
-- PDF bytes stay in deal_daf_library; daf_id links a result to its PDF when generated.
CREATE TABLE IF NOT EXISTS marketdata.deal_daf_results (
    id             SERIAL PRIMARY KEY,
    brief_id       INTEGER REFERENCES marketdata.deal_briefs(id),
    deal_name      TEXT NOT NULL,
    province       TEXT,
    asset_type     TEXT,
    brief          JSONB,
    sections       JSONB NOT NULL,
    economics      JSONB,
    synthesis      TEXT,
    recommendation TEXT,
    daf_id         INTEGER REFERENCES marketdata.deal_daf_library(id),
    created_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_deal_daf_results_brief ON marketdata.deal_daf_results(brief_id);
