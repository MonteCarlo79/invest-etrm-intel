CREATE TABLE IF NOT EXISTS intl_market.gb_knowledge_docs (
    id              SERIAL PRIMARY KEY,
    source          TEXT NOT NULL,       -- elexon | entso_e | timera | modo | meteologica
    doc_type        TEXT NOT NULL,       -- article | report | notice | forecast | regulation
    title           TEXT,
    url             TEXT UNIQUE,
    published_date  DATE,
    content         TEXT NOT NULL,
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    search_vector   TSVECTOR GENERATED ALWAYS AS (
                        to_tsvector('english',
                            coalesce(title, '') || ' ' || left(content, 100000))
                    ) STORED
);

CREATE INDEX IF NOT EXISTS gb_knowledge_docs_fts
    ON intl_market.gb_knowledge_docs USING GIN(search_vector);

CREATE INDEX IF NOT EXISTS gb_knowledge_docs_source
    ON intl_market.gb_knowledge_docs (source, published_date DESC);
