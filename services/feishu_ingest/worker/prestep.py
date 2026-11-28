"""Run before a batch ingests. For every file still waiting under the batch's S3 prefix:

1. remove half-ingested copies (registered today-ish but never tagged with source_name — a child that
   died before tagging; a re-run would otherwise hit the sha256 dedup and never finish them), and
2. remove the previous KB entry of the same document (same file name, source_name = feishu:<path>),
   so a re-exported, edited Feishu document REPLACES its old version instead of piling up.

Only files still in S3 are considered, so re-running a half-finished batch never touches documents the
first run already ingested. Usage (in the ECS task): python prestep.py tmp/<batch>/
"""
import json
import sys

import boto3

from services.knowledge_pool.db import get_conn

B, P = "bess-uploader-data-chen-singp-2026", sys.argv[1]
s3 = boto3.client("s3")
manifest = json.loads(s3.get_object(Bucket=B, Key=P + "_manifest.json")["Body"].read())
waiting = [o["Key"] for o in s3.list_objects_v2(Bucket=B, Prefix=P).get("Contents", [])
           if not o["Key"].rsplit("/", 1)[1].startswith("_")]
pairs = [(k.rsplit("/", 1)[1], "feishu:" + manifest[k]) for k in waiting if k in manifest]
names = [n for n, _ in pairs]

with get_conn() as c, c.cursor() as cur:
    cur.execute("select id from staging.spot_knowledge_docs where file_name = any(%s) "
                "and created_at > now() - interval '3 days' and source_name is null", (names,))
    partial = {r[0] for r in cur.fetchall()}
    previous = set()
    for name, src in pairs:
        cur.execute("select id from staging.spot_knowledge_docs where file_name = %s and source_name = %s",
                    (name, src))
        previous |= {r[0] for r in cur.fetchall()}
    ids = tuple(partial | previous)
    print(f"PRESTEP waiting={len(pairs)} partial={sorted(partial)} replaced={sorted(previous)}", flush=True)
    if ids:
        cur.execute("""select conrelid::regclass::text, a.attname from pg_constraint k join pg_attribute a
            on a.attrelid=k.conrelid and a.attnum=k.conkey[1]
            where k.contype='f' and k.confrelid='staging.spot_knowledge_docs'::regclass""")
        for t, col in cur.fetchall():
            cur.execute(f"delete from {t} where {col} in %s", (ids,))
        cur.execute("delete from staging.spot_knowledge_docs where id in %s", (ids,))
        print("  docs deleted:", cur.rowcount, flush=True)
    c.commit()
