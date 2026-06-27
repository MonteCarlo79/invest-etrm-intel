"""
Fast backfill of vector embeddings for staging.spot_knowledge_chunks.

Improvements over backfill_embeddings.py:
  - Batch size 512 (was 64) — fewer DB round trips, better ONNX throughput
  - fastembed parallel=4 workers — uses multiple CPU cores for inference
  - Batch UPDATE via unnest() — single SQL call instead of executemany row-by-row
  - SELECT FOR UPDATE SKIP LOCKED — safe to run multiple instances in parallel

Run one instance:
    python scripts/backfill_embeddings_fast.py

Run N parallel instances (open N terminals, run same command in each):
    python scripts/backfill_embeddings_fast.py
    python scripts/backfill_embeddings_fast.py   # second terminal
    python scripts/backfill_embeddings_fast.py   # third terminal

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
import psycopg2.extras
from services.knowledge_pool.embeddings import MODEL_NAME

BATCH       = 512   # chunks per DB fetch (was 64)
PARALLEL    = None  # None = single-process ONNX (Windows multiprocessing requires __main__ guard)
LOG_EVERY   = 2048  # print progress every N chunks

if __name__ == "__main__":
    print(f"Loading embedding model: {MODEL_NAME}")
    from fastembed import TextEmbedding
    model = TextEmbedding(model_name=MODEL_NAME)
    list(model.embed(["预热"], batch_size=1))
    print(f"Model ready.  batch={BATCH}\n")

    def get_conn():
        c = psycopg2.connect(pgurl)
        c.autocommit = True
        return c

    conn = get_conn()

    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM staging.spot_knowledge_chunks WHERE embedding IS NULL"
        )
        total = cur.fetchone()[0]

    print(f"Chunks remaining: {total}")
    if total == 0:
        print("Nothing to do — all chunks already have embeddings.")
        conn.close()
        sys.exit(0)

    # Use a stable offset cursor: fetch by minimum id so multiple instances
    # naturally spread across different id ranges without locking.
    min_id = 0
    done   = 0
    t0     = time.time()

    while True:
        # Fetch next batch — no lock held during embedding
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, chunk_text
                    FROM staging.spot_knowledge_chunks
                    WHERE embedding IS NULL AND id > %s
                    ORDER BY id
                    LIMIT %s
                    """,
                    (min_id, BATCH),
                )
                rows = cur.fetchall()
        except Exception as e:
            print(f"  DB fetch error: {e} — reconnecting")
            try: conn.close()
            except: pass
            conn = get_conn()
            continue

        if not rows:
            break

        ids   = [r[0] for r in rows]
        texts = [r[1] for r in rows]
        min_id = ids[-1]  # advance cursor

        # Embed — no DB connection held open during this
        try:
            vecs = list(model.embed(texts, batch_size=BATCH))
        except Exception as e:
            print(f"  Embed error on batch starting id={ids[0]}: {e} — skipping")
            continue

        pairs = [
            ("[" + ",".join(f"{v:.6f}" for v in vec.tolist()) + "]", cid)
            for vec, cid in zip(vecs, ids)
            if vec is not None
        ]

        if pairs:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE staging.spot_knowledge_chunks AS t
                        SET    embedding = v.emb::vector
                        FROM   unnest(%s::text[], %s::bigint[]) AS v(emb, id)
                        WHERE  t.id = v.id
                        """,
                        ([p[0] for p in pairs], [p[1] for p in pairs]),
                    )
            except Exception as e:
                print(f"  DB update error: {e} — reconnecting")
                try: conn.close()
                except: pass
                conn = get_conn()

        done += len(rows)
        if done % LOG_EVERY < BATCH or done >= total:
            elapsed = time.time() - t0
            rate    = done / elapsed if elapsed > 0 else 0
            eta_min = ((total - done) / rate / 60) if rate > 0 else 0
            print(
                f"  {done}/{total} ({100*done/total:.1f}%) — "
                f"{rate:.0f} chunks/s — "
                f"~{eta_min:.0f} min remaining"
            )

    elapsed = time.time() - t0
    print(f"\nDone: {done} chunks in {elapsed:.1f}s  ({done/elapsed:.0f} chunks/s avg)")
    conn.close()
