"""
Backfill vector embeddings for all existing knowledge chunks that have
embedding IS NULL.

Run after migrate_pgvector.py:
    python scripts/backfill_embeddings.py

Uses BAAI/bge-small-zh-v1.5 via fastembed (ONNX CPU, ~90MB model).
Processes chunks in batches of 64. Prints progress every 500 chunks.
Safe to interrupt and re-run — skips already-embedded chunks.
"""
import os
import sys
import pathlib
import time

_repo = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_repo))

try:
    from dotenv import load_dotenv
    for _env in [_repo / "config" / ".env", _repo / ".env"]:
        if _env.exists():
            load_dotenv(_env)
except ImportError:
    pass

pgurl = os.environ.get("PGURL", os.environ.get("HERMES_DB_URL", ""))
if not pgurl:
    sys.exit("ERROR: Set PGURL before running.")

os.environ.setdefault("PGURL", pgurl)

import psycopg2
from services.knowledge_pool.embeddings import embed_texts, vec_to_pg, MODEL_NAME

print(f"Loading embedding model: {MODEL_NAME}")
# Warm up the model
embed_texts(["预热"])
print("Model ready.\n")

BATCH = 64
conn = psycopg2.connect(pgurl)

with conn.cursor() as cur:
    cur.execute(
        "SELECT COUNT(*) FROM staging.spot_knowledge_chunks WHERE embedding IS NULL"
    )
    total = cur.fetchone()[0]

print(f"Chunks to embed: {total}")
if total == 0:
    print("Nothing to do — all chunks already have embeddings.")
    conn.close()
    sys.exit(0)

done = 0
t0 = time.time()

while True:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, chunk_text FROM staging.spot_knowledge_chunks "
            "WHERE embedding IS NULL LIMIT %s",
            (BATCH,),
        )
        rows = cur.fetchall()

    if not rows:
        break

    ids = [r[0] for r in rows]
    texts = [r[1] for r in rows]
    vecs = embed_texts(texts)

    updates = [
        (vec_to_pg(v), cid)
        for v, cid in zip(vecs, ids)
        if v is not None
    ]

    if updates:
        with conn.cursor() as cur:
            cur.executemany(
                "UPDATE staging.spot_knowledge_chunks "
                "SET embedding = %s::vector WHERE id = %s",
                updates,
            )
        conn.commit()

    done += len(rows)
    if done % 500 < BATCH or done >= total:
        elapsed = time.time() - t0
        rate = done / elapsed if elapsed > 0 else 0
        remaining = (total - done) / rate if rate > 0 else 0
        print(f"  {done}/{total} ({100*done/total:.1f}%) — "
              f"{rate:.0f} chunks/s — "
              f"~{remaining/60:.1f} min remaining")

elapsed = time.time() - t0
print(f"\nDone: {done} chunks embedded in {elapsed:.1f}s")
conn.close()
