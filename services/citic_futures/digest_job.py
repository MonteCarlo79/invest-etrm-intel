# services/citic_futures/digest_job.py
"""ECS-side CITIC weekly digest job (runs inside hermes on AWS).

The local Mac ingests weekly folders into the KB and records them in
staging.citic_ingest_log (folder → relevant doc_ids). This job — on a host
where Anthropic calls actually work (Bedrock, Singapore) — generates the
weekly digest per un-digested folder and sends the Feishu card, then records
it in staging.citic_digest_log.

Entry point: run_citic_digest(pg_url, feishu, owner_open_id, api_key) -> dict
"""
from __future__ import annotations

import json
import logging
from typing import Optional

logger = logging.getLogger(__name__)

_DDL = [
    """CREATE TABLE IF NOT EXISTS staging.citic_ingest_log (
        folder TEXT PRIMARY KEY,
        doc_ids JSONB NOT NULL DEFAULT '[]',
        n_new INT DEFAULT 0,
        n_dup INT DEFAULT 0,
        ingested_at TIMESTAMPTZ DEFAULT NOW()
    )""",
    """CREATE TABLE IF NOT EXISTS staging.citic_digest_log (
        folder TEXT PRIMARY KEY,
        sent_at TIMESTAMPTZ DEFAULT NOW(),
        n_docs INT DEFAULT 0
    )""",
]

_INGEST_UPSERT = """
INSERT INTO staging.citic_ingest_log (folder, doc_ids, n_new, n_dup, ingested_at)
VALUES (%s, %s, %s, %s, NOW())
ON CONFLICT (folder) DO UPDATE SET
    doc_ids = EXCLUDED.doc_ids,
    n_new = EXCLUDED.n_new,
    n_dup = EXCLUDED.n_dup,
    ingested_at = NOW()
"""

_PENDING_SQL = """
SELECT l.folder, l.doc_ids
FROM staging.citic_ingest_log l
LEFT JOIN staging.citic_digest_log d ON d.folder = l.folder
WHERE d.folder IS NULL
ORDER BY l.ingested_at
"""

_DOCS_SQL = "SELECT id, file_name FROM staging.spot_knowledge_docs WHERE id = ANY(%s)"

_LOG_INSERT = "INSERT INTO staging.citic_digest_log (folder, n_docs) VALUES (%s, %s) ON CONFLICT (folder) DO NOTHING"


def ensure_tables(pg_url: str) -> None:
    import psycopg2
    conn = psycopg2.connect(pg_url)
    try:
        with conn.cursor() as cur:
            for ddl in _DDL:
                cur.execute(ddl)
        conn.commit()
    finally:
        conn.close()


def record_ingest(pg_url: str, folder: str, doc_ids: list[int], n_new: int, n_dup: int) -> None:
    """Called by the LOCAL ingestor after each weekly folder."""
    import psycopg2
    ensure_tables(pg_url)
    conn = psycopg2.connect(pg_url)
    try:
        with conn.cursor() as cur:
            cur.execute(_INGEST_UPSERT, (folder, json.dumps(doc_ids), n_new, n_dup))
        conn.commit()
    finally:
        conn.close()


def _pending_folders(pg_url: str) -> list[tuple[str, list[int]]]:
    import psycopg2
    conn = psycopg2.connect(pg_url)
    try:
        with conn.cursor() as cur:
            cur.execute(_PENDING_SQL)
            out = []
            for folder, doc_ids in cur.fetchall():
                if isinstance(doc_ids, str):
                    doc_ids = json.loads(doc_ids)
                out.append((folder, [int(i) for i in doc_ids]))
            return out
    finally:
        conn.close()


def _collect_docs(pg_url: str, doc_ids: list[int]) -> list[tuple[int, str, str]]:
    """Map doc_ids back to (doc_id, file_name, relevant_group)."""
    from services.citic_futures.ingest_weekly import relevant_group
    import psycopg2
    if not doc_ids:
        return []
    conn = psycopg2.connect(pg_url)
    try:
        with conn.cursor() as cur:
            cur.execute(_DOCS_SQL, (doc_ids,))
            out = []
            for doc_id, file_name in cur.fetchall():
                g = relevant_group(file_name)
                if g:
                    out.append((doc_id, file_name, g))
            return out
    finally:
        conn.close()


def run_citic_digest(pg_url: str, feishu, owner_open_id: str, api_key: str = "") -> dict:
    """Generate + send digests for all ingested-but-undigested folders."""
    from services.citic_futures.weekly_digest import (
        collect_relevant_text, send_weekly_digest)

    ensure_tables(pg_url)
    sent = []
    for folder, doc_ids in _pending_folders(pg_url):
        relevant = _collect_docs(pg_url, doc_ids)
        if not relevant:
            logger.warning("citic_digest: %s has no relevant docs — logging as digested", folder)
            _log_folder(pg_url, folder, 0)
            continue
        texts = collect_relevant_text(relevant, pg_url)
        digest = send_weekly_digest(feishu, owner_open_id, folder, texts, api_key)
        if digest:
            _log_folder(pg_url, folder, len(relevant))
            sent.append(folder)
            logger.info("citic_digest: sent %s (%d docs)", folder, len(relevant))
    return {"sent": sent, "pending_checked": len(_pending_folders(pg_url))}


def _log_folder(pg_url: str, folder: str, n_docs: int) -> None:
    import psycopg2
    conn = psycopg2.connect(pg_url)
    try:
        with conn.cursor() as cur:
            cur.execute(_LOG_INSERT, (folder, n_docs))
        conn.commit()
    finally:
        conn.close()
