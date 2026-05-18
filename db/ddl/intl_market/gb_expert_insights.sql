CREATE TABLE IF NOT EXISTS intl_market.gb_expert_insights (
    id              SERIAL PRIMARY KEY,
    insight_text    TEXT NOT NULL,
    insight_type    TEXT NOT NULL DEFAULT 'other',
    confidence      TEXT NOT NULL DEFAULT 'medium' CHECK (confidence IN ('high', 'medium', 'low')),
    source_session  DATE NOT NULL DEFAULT CURRENT_DATE,
    active          BOOLEAN NOT NULL DEFAULT TRUE,
    use_count       INTEGER NOT NULL DEFAULT 0,
    validated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS gb_expert_insights_fts
    ON intl_market.gb_expert_insights
    USING GIN(to_tsvector('english', insight_text));
