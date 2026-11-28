"""Before re-running a batch: remove half-ingested copies of the files still left under its S3 prefix.

A half-ingested doc was registered (doc + chunks) but its child process died before tagging it with
source_name, so a re-run would hit the sha256 dedup and never finish it.
Usage (in the ECS task): python prestep.py tmp/<batch>/
"""
import sys

import boto3

from services.knowledge_pool.db import get_conn

B, P = "bess-uploader-data-chen-singp-2026", sys.argv[1]
names = [o["Key"].rsplit("/", 1)[1] for o in boto3.client("s3").list_objects_v2(Bucket=B, Prefix=P).get("Contents", [])
         if not o["Key"].rsplit("/", 1)[1].startswith("_")]
with get_conn() as c, c.cursor() as cur:
    cur.execute("select id from staging.spot_knowledge_docs where file_name = any(%s) and created_at > '2026-11-27' "
                "and source_name is null", (names,))
    ids = tuple(r[0] for r in cur.fetchall())
    print("PRESTEP partial docs:", ids, flush=True)
    if ids:
        cur.execute("""select conrelid::regclass::text, a.attname from pg_constraint k join pg_attribute a
            on a.attrelid=k.conrelid and a.attnum=k.conkey[1]
            where k.contype='f' and k.confrelid='staging.spot_knowledge_docs'::regclass""")
        for t, col in cur.fetchall():
            cur.execute(f"delete from {t} where {col} in %s", (ids,))
        cur.execute("delete from staging.spot_knowledge_docs where id in %s", (ids,))
        print("  docs deleted:", cur.rowcount, flush=True)
    c.commit()
