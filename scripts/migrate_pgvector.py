"""
One-time migration: enable pgvector on RDS and add embedding column.

Run ONCE after ingest_market_fundamentals.py finishes (so no locks):
    python scripts/migrate_pgvector.py

Safe to re-run (all DDL uses IF NOT EXISTS / IF EXISTS).
"""
import os
import sys
import pathlib

_repo = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_repo))

try:
    from dotenv import load_dotenv
    for _env in [_repo / "config" / ".env", _repo / ".env"]:
        if _env.exists():
            load_dotenv(_env)
except ImportError:
    pass

import psycopg2

pgurl = os.environ.get("PGURL", os.environ.get("HERMES_DB_URL", ""))
if not pgurl:
    sys.exit("ERROR: Set PGURL before running.")

conn = psycopg2.connect(pgurl)
conn.autocommit = True
cur = conn.cursor()

print("Enabling pgvector extension...")
cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
cur.execute("SELECT extversion FROM pg_extension WHERE extname='vector';")
print(f"  pgvector version: {cur.fetchone()[0]}")

print("Adding embedding column to staging.spot_knowledge_chunks...")
cur.execute("""
    ALTER TABLE staging.spot_knowledge_chunks
    ADD COLUMN IF NOT EXISTS embedding vector(512);
""")
print("  Done.")

print("Creating HNSW index (background-safe, runs in place)...")
cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_skc_vec
    ON staging.spot_knowledge_chunks
    USING hnsw (embedding vector_cosine_ops);
""")
print("  Done.")

cur.execute("""
    SELECT COUNT(*) FROM staging.spot_knowledge_chunks;
""")
total = cur.fetchone()[0]
cur.execute("""
    SELECT COUNT(*) FROM staging.spot_knowledge_chunks WHERE embedding IS NULL;
""")
null_count = cur.fetchone()[0]

print(f"\nTotal chunks: {total}")
print(f"Chunks needing embeddings: {null_count}")
print("\nMigration complete. Run backfill_embeddings.py next.")

cur.close()
conn.close()
