-- staging.spot_knowledge_chunks — trigram GIN index for CJK bigram ILIKE search
--
-- Why: search_reference_docs() handles Chinese queries with OR'd bigram
-- `chunk_text ILIKE '%<bigram>%'` conditions (no CJK FTS dictionary in PG).
-- Without pg_trgm this is a full seq scan of 2.8M chunks — measured 114-283s
-- per search in prod (2026-09-06 deal-structurer probe), timing out the
-- 投委会 sections (市场背景 / 政策与规则环境 / 运营实证·蒙西储能, 420s cap).
-- The existing idx_skc_fts GIN only covers the Latin to_tsvector path.
--
-- Apply manually (CREATE INDEX CONCURRENTLY cannot run in a transaction):
--   psql "$DSN" -c "CREATE EXTENSION IF NOT EXISTS pg_trgm"
--   psql "$DSN" -c "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_skc_trgm ..."
-- Rollback: DROP INDEX CONCURRENTLY IF EXISTS idx_skc_trgm;

CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- NOTE: run CONCURRENTLY outside a transaction block.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_skc_trgm
    ON staging.spot_knowledge_chunks USING GIN (chunk_text gin_trgm_ops);

ANALYZE staging.spot_knowledge_chunks;
