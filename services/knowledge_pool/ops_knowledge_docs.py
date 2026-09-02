"""Operating-assets knowledge base: document registration, ingestion, and FTS search.

Parallel to the spot-market knowledge pool (services/knowledge_pool/knowledge_docs.py)
but scoped to operating-asset documents: 复盘/backtest/incident/maintenance reports
dropped into assets/operating/复盘/ and ingested by ops_watcher.

Tables: staging.ops_knowledge_docs / staging.ops_knowledge_chunks
(canonical DDL: db/ddl/staging/ops_knowledge.sql — keep in sync with _DDL below).

Extractors and chunking are REUSED from knowledge_docs (PDF/PPTX-charts/DOCX/XLSX/
XLS/TXT/HTML/image-vision) — this module only re-implements categorization
(ops categories, with two fixes vs the source: working LLM fallback import +
haiku-class model), asset inference, and ops-table registration/search.
"""
from __future__ import annotations

import logging
import os
from typing import Optional

from services.knowledge_pool.knowledge_docs import (
    _cjk_bigrams,
    _chunk_text,
    _extract_pages,
    _has_cjk,
    sha256_bytes,
)

log = logging.getLogger(__name__)

_DDL = """
CREATE SCHEMA IF NOT EXISTS staging;
CREATE TABLE IF NOT EXISTS staging.ops_knowledge_docs (
    id              SERIAL PRIMARY KEY,
    file_name       TEXT NOT NULL,
    file_hash       TEXT UNIQUE NOT NULL,
    category        TEXT NOT NULL DEFAULT 'other',
    asset_id        INTEGER REFERENCES marketdata.rm_assets(id),
    title           TEXT,
    doc_date        DATE,
    source_path     TEXT,
    file_size_bytes INT,
    page_count      INT DEFAULT 0,
    ingest_status   TEXT NOT NULL DEFAULT 'pending',
    parse_error     TEXT,
    active          BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS staging.ops_knowledge_chunks (
    id          SERIAL PRIMARY KEY,
    doc_id      INT NOT NULL REFERENCES staging.ops_knowledge_docs(id),
    page_no     INT,
    chunk_index INT NOT NULL,
    chunk_text  TEXT NOT NULL,
    UNIQUE(doc_id, chunk_index)
);
CREATE INDEX IF NOT EXISTS idx_okc_fts ON staging.ops_knowledge_chunks
    USING GIN(to_tsvector('simple', chunk_text));
CREATE INDEX IF NOT EXISTS idx_okd_asset ON staging.ops_knowledge_docs(asset_id);
CREATE INDEX IF NOT EXISTS idx_okd_category ON staging.ops_knowledge_docs(category);
"""

_TABLES_INITIALIZED = False

CATEGORIES = {
    "operational_review": ["复盘", "运营统计", "月度总结", "半年度", "年度总结", "运营分析"],
    "incident_report": ["停机", "故障", "缺陷", "非计划", "跳闸", "告警"],
    "backtest_report": ["回测", "策略验证", "复盘回测", "完美收益", "perfect foresight"],
    "maintenance_record": ["检修", "维护", "涉网试验", "试验", "定检"],
    "dispatch_plan": ["调度计划表", "调度计划", "交易调度"],
}
CATEGORY_LABELS_ZH = {
    "operational_review": "运营复盘",
    "incident_report": "事故/停机报告",
    "backtest_report": "回测报告",
    "maintenance_record": "检修/试验记录",
    "dispatch_plan": "调度计划",
    "other": "其他",
}


def _get_conn():
    from shared.agents.db import get_conn
    return get_conn()


def init_ops_knowledge_tables() -> None:
    """Idempotent table creation (platform auto-migration pattern)."""
    global _TABLES_INITIALIZED
    if _TABLES_INITIALIZED:
        return
    with _get_conn() as conn, conn.cursor() as cur:
        cur.execute(_DDL)
        conn.commit()
    _TABLES_INITIALIZED = True


def _keyword_category(filename: str, text_sample: str) -> str:
    hay = (filename + " " + text_sample[:1000]).lower()
    for cat, keywords in CATEGORIES.items():
        if any(k.lower() in hay for k in keywords):
            return cat
    return "other"


def auto_categorize(filename: str, text_sample: str, api_key: str | None = None) -> str:
    """Keyword heuristic first; Haiku fallback when it returns 'other'.

    Fixes vs knowledge_docs.auto_categorize: make_client imported at module top
    (source had a function-local import that NameError'd the fallback), and uses
    a haiku-class model per repo convention for cheap tasks.
    """
    cat = _keyword_category(filename, text_sample)
    if cat != "other" or not api_key:
        return cat
    try:
        from shared.anthropic_client import make_client
        resp = make_client(api_key).messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=20,
            system=("Categorize this operating-asset document into exactly one of: "
                    + ", ".join(list(CATEGORIES) + ["other"])
                    + ". Reply with the category key only."),
            messages=[{"role": "user", "content": f"Filename: {filename}\n\n{text_sample[:800]}"}],
        )
        cand = next((b.text for b in resp.content if hasattr(b, "text")), "").strip().lower()
        return cand if cand in CATEGORIES else "other"
    except Exception:
        return "other"


def _infer_asset_id(filename: str, asset_names: dict[str, int] | None) -> Optional[int]:
    """First rm_assets.name found as a substring of the filename; None if none/multi."""
    if not asset_names:
        return None
    hits = [aid for name, aid in asset_names.items() if name and name in filename]
    return hits[0] if len(hits) == 1 else None


def _infer_title(filename: str) -> str:
    base = os.path.basename(filename)
    return os.path.splitext(base)[0]


def register_and_ingest(file_bytes: bytes, filename: str, *,
                        source_path: str | None = None,
                        category_override: str | None = None,
                        api_key: str | None = None,
                        asset_names: dict[str, int] | None = None) -> tuple[int, bool, str]:
    """Register + ingest one document. Returns (doc_id, is_new, category).

    Hash-dedup via file_hash; parse failures still register with ingest_status='failed'.
    """
    init_ops_knowledge_tables()
    file_hash = sha256_bytes(file_bytes)
    with _get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT id, category FROM staging.ops_knowledge_docs WHERE file_hash = %s",
                    (file_hash,))
        row = cur.fetchone()
        if row:
            return row[0], False, row[1]

        try:
            pages = _extract_pages(file_bytes, filename, api_key)
            ingest_status, parse_error = "parsed", None
        except Exception as exc:
            pages, ingest_status, parse_error = [], "failed", str(exc)[:500]

        sample = "\n".join(t for _, t in pages[:2]) if pages else ""
        category = category_override or auto_categorize(filename, sample, api_key)
        asset_id = _infer_asset_id(filename, asset_names)

        cur.execute("""
            INSERT INTO staging.ops_knowledge_docs
                (file_name, file_hash, category, asset_id, title, source_path,
                 file_size_bytes, page_count, ingest_status, parse_error)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (filename, file_hash, category, asset_id, _infer_title(filename),
              source_path, len(file_bytes), len(pages), ingest_status, parse_error))
        doc_id = cur.fetchone()[0]

        chunk_rows = []
        for page_no, page_text in pages:
            for i, chunk in enumerate(_chunk_text(page_text, chunk_size=500, overlap=100)):
                chunk_rows.append((doc_id, page_no, i, chunk))
        if chunk_rows:
            cur.executemany("""
                INSERT INTO staging.ops_knowledge_chunks (doc_id, page_no, chunk_index, chunk_text)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (doc_id, chunk_index) DO NOTHING
            """, chunk_rows)
        conn.commit()
        return doc_id, True, category


def search_ops_docs(query: str, category: str | None = None,
                    asset_id: int | None = None, limit: int = 5) -> list[dict]:
    """FTS over ops chunks: CJK bigram ILIKE, short-string ILIKE, else tsvector."""
    init_ops_knowledge_tables()
    filters = ["d.active = TRUE"]
    params: dict = {"lim": limit}
    if category:
        filters.append("d.category = %(cat)s")
        params["cat"] = category
    if asset_id is not None:
        filters.append("(d.asset_id = %(aid)s OR d.asset_id IS NULL)")
        params["aid"] = asset_id
    where = " AND ".join(filters)

    if _has_cjk(query):
        bigrams = _cjk_bigrams(query)[:12]
        if bigrams:
            ors = " OR ".join(f"c.chunk_text ILIKE %(bg{i})s" for i in range(len(bigrams)))
            for i, bg in enumerate(bigrams):
                params[f"bg{i}"] = f"%{bg}%"
            sql = f"""
                SELECT d.id AS doc_id, d.file_name, d.category, d.title, c.page_no, c.chunk_text,
                       ({" + ".join(f"(c.chunk_text ILIKE %(bg{i})s)::int" for i in range(len(bigrams)))}) AS score
                FROM staging.ops_knowledge_chunks c
                JOIN staging.ops_knowledge_docs d ON d.id = c.doc_id
                WHERE {where} AND ({ors})
                ORDER BY score DESC, d.id, c.page_no LIMIT %(lim)s
            """
        else:
            sql = f"""
                SELECT d.id AS doc_id, d.file_name, d.category, d.title, c.page_no, c.chunk_text, 1 AS score
                FROM staging.ops_knowledge_chunks c
                JOIN staging.ops_knowledge_docs d ON d.id = c.doc_id
                WHERE {where} AND c.chunk_text ILIKE %(q)s
                ORDER BY d.id, c.page_no LIMIT %(lim)s
            """
            params["q"] = f"%{query}%"
    else:
        sql = f"""
            SELECT d.id AS doc_id, d.file_name, d.category, d.title, c.page_no, c.chunk_text,
                   ts_rank(to_tsvector('simple', c.chunk_text),
                           plainto_tsquery('simple', %(q)s)) AS score
            FROM staging.ops_knowledge_chunks c
            JOIN staging.ops_knowledge_docs d ON d.id = c.doc_id
            WHERE {where}
              AND to_tsvector('simple', c.chunk_text) @@ plainto_tsquery('simple', %(q)s)
            ORDER BY score DESC, d.id, c.page_no LIMIT %(lim)s
        """
        params["q"] = query

    with _get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]
