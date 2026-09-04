"""
Knowledge Pool Intelligence — DDL Migration
============================================
Creates all new tables required for Phases 1-5 of the knowledge intelligence
upgrade. Safe to run multiple times (CREATE TABLE IF NOT EXISTS throughout).

Run:
    py scripts/apply_kp_intelligence_ddl.py
"""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO))

from dotenv import load_dotenv
for _env in [_REPO / "config" / ".env", _REPO / ".env"]:
    if _env.exists():
        load_dotenv(_env)
        break

from services.knowledge_pool.db import get_conn

_DDL = """
-- ─────────────────────────────────────────────────────────────────────────────
-- Phase 1: Document Synthesis artifacts
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS staging.kp_doc_summaries (
    id              SERIAL PRIMARY KEY,
    doc_id          INT NOT NULL REFERENCES staging.spot_knowledge_docs(id),
    summary_text    TEXT NOT NULL,
    key_entities    JSONB,
    synthesis_model TEXT,
    synthesized_at  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(doc_id)
);

CREATE INDEX IF NOT EXISTS idx_kp_summaries_fts
    ON staging.kp_doc_summaries
    USING GIN(to_tsvector('simple', summary_text));

CREATE TABLE IF NOT EXISTS staging.kp_qa_pairs (
    id          SERIAL PRIMARY KEY,
    doc_id      INT NOT NULL REFERENCES staging.spot_knowledge_docs(id),
    question    TEXT NOT NULL,
    answer      TEXT NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_kp_qa_fts
    ON staging.kp_qa_pairs
    USING GIN(to_tsvector('simple', question || ' ' || answer));

CREATE INDEX IF NOT EXISTS idx_kp_qa_doc_id
    ON staging.kp_qa_pairs(doc_id);

-- ─────────────────────────────────────────────────────────────────────────────
-- Phase 2: Knowledge Graph
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS staging.kp_entities (
    id              SERIAL PRIMARY KEY,
    entity_type     TEXT NOT NULL,   -- province, policy, asset, company, instrument, document
    canonical_name  TEXT NOT NULL,
    canonical_name_en TEXT,
    province        TEXT,
    aliases         TEXT[],
    metadata        JSONB,
    doc_count       INT DEFAULT 0,
    UNIQUE(entity_type, canonical_name)
);

CREATE INDEX IF NOT EXISTS idx_kp_entities_type
    ON staging.kp_entities(entity_type);

CREATE INDEX IF NOT EXISTS idx_kp_entities_name
    ON staging.kp_entities USING GIN(to_tsvector('simple', canonical_name));

CREATE TABLE IF NOT EXISTS staging.kp_entity_relations (
    id              SERIAL PRIMARY KEY,
    entity_from_id  INT REFERENCES staging.kp_entities(id),
    relation_type   TEXT NOT NULL,   -- mentioned_in, governs, supersedes, applies_to, related_to
    entity_to_id    INT REFERENCES staging.kp_entities(id),
    doc_id          INT REFERENCES staging.spot_knowledge_docs(id),
    context         TEXT,
    confidence      FLOAT DEFAULT 1.0,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_kp_relations_from
    ON staging.kp_entity_relations(entity_from_id);
CREATE INDEX IF NOT EXISTS idx_kp_relations_to
    ON staging.kp_entity_relations(entity_to_id);
CREATE INDEX IF NOT EXISTS idx_kp_relations_doc
    ON staging.kp_entity_relations(doc_id);

-- ─────────────────────────────────────────────────────────────────────────────
-- Phase 3: Policy Timeline
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS staging.kp_policy_timeline (
    id              SERIAL PRIMARY KEY,
    doc_id          INT REFERENCES staging.spot_knowledge_docs(id),
    policy_name     TEXT NOT NULL,
    policy_name_zh  TEXT,
    policy_type     TEXT,            -- market_rule | regulation | notice | standard | guideline | other
    province        TEXT,            -- NULL = national
    issuing_body    TEXT,
    issuing_body_zh TEXT,
    doc_ref_no      TEXT,
    effective_date  DATE,
    superseded_date DATE,            -- NULL = still active
    supersedes      TEXT,            -- name of policy this replaces
    key_changes     TEXT[],
    bess_relevance  TEXT,
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(doc_id)
);

CREATE INDEX IF NOT EXISTS idx_kp_policy_date
    ON staging.kp_policy_timeline(effective_date);
CREATE INDEX IF NOT EXISTS idx_kp_policy_province
    ON staging.kp_policy_timeline(province);
CREATE INDEX IF NOT EXISTS idx_kp_policy_type
    ON staging.kp_policy_timeline(policy_type);
CREATE INDEX IF NOT EXISTS idx_kp_policy_fts
    ON staging.kp_policy_timeline
    USING GIN(to_tsvector('simple', policy_name || ' ' || COALESCE(bess_relevance, '')));

-- ─────────────────────────────────────────────────────────────────────────────
-- Phase 4: Expert Memory
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS staging.kp_expert_insights (
    id              SERIAL PRIMARY KEY,
    insight_text    TEXT NOT NULL,
    insight_type    TEXT,            -- market_structure | price_driver | regulation | risk | opportunity | dispatch_economics | investment | operations
    province        TEXT,
    confidence      TEXT DEFAULT 'medium',  -- high | medium | low
    source_session  TEXT,            -- ISO date of originating session
    source_doc_ids  INT[],
    validated_at    TIMESTAMPTZ,
    active          BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_kp_insights_fts
    ON staging.kp_expert_insights
    USING GIN(to_tsvector('simple', insight_text));
CREATE INDEX IF NOT EXISTS idx_kp_insights_type
    ON staging.kp_expert_insights(insight_type);
CREATE INDEX IF NOT EXISTS idx_kp_insights_province
    ON staging.kp_expert_insights(province);
"""


def main() -> None:
    print("Applying knowledge intelligence DDL...")
    # Split into individual statements — psycopg2 execute() handles one at a time
    statements = [
        s.strip() for s in _DDL.split(";")
        if s.strip() and not s.strip().startswith("--")
    ]
    with get_conn() as conn:
        with conn.cursor() as cur:
            for stmt in statements:
                if stmt:
                    cur.execute(stmt)
        conn.commit()
    print("Done. Tables created (or already existed):")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'staging'
                  AND table_name LIKE 'kp_%'
                ORDER BY table_name
                """
            )
            for (t,) in cur.fetchall():
                print(f"  staging.{t}")


if __name__ == "__main__":
    main()
