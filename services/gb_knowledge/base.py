"""Shared base classes and DB utilities for GB knowledge ingestion."""
import os
from datetime import date
from typing import Iterator

import psycopg2


def get_db_conn():
    return psycopg2.connect(
        os.environ["PGURL"],
        keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=5,
    )


def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS intl_market.gb_knowledge_docs (
                id              SERIAL PRIMARY KEY,
                source          TEXT NOT NULL,
                doc_type        TEXT NOT NULL,
                title           TEXT,
                url             TEXT UNIQUE,
                published_date  DATE,
                content         TEXT NOT NULL,
                fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                search_vector   TSVECTOR GENERATED ALWAYS AS (
                                    to_tsvector('english',
                                        coalesce(title, '') || ' ' || left(content, 100000))
                                ) STORED
            )
        """)
        cur.execute(
            "CREATE INDEX IF NOT EXISTS gb_knowledge_docs_fts "
            "ON intl_market.gb_knowledge_docs USING GIN(search_vector)"
        )
    conn.commit()


def upsert_doc(conn, source: str, doc_type: str, title: str,
               url: str | None, published_date: date | None, content: str) -> bool:
    """Insert document; skip if URL already exists. Returns True if inserted."""
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO intl_market.gb_knowledge_docs "
            "(source, doc_type, title, url, published_date, content) "
            "VALUES (%s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (url) DO NOTHING",
            (source, doc_type, title, url, published_date, content),
        )
        inserted = cur.rowcount > 0
    conn.commit()
    return inserted


def search_docs(conn, query: str, sources: list[str] | None = None,
                limit: int = 8) -> list[dict]:
    """Full-text search over knowledge docs. Returns list of dicts."""
    import psycopg2.extras
    source_filter = ""
    params: list = [query, query]
    if sources:
        source_filter = f"AND source = ANY(%s)"
        params.append(sources)
    params.append(limit)
    sql = f"""
        SELECT source, doc_type, title, url, published_date,
               left(content, 2000) AS content_snippet,
               ts_rank(search_vector, plainto_tsquery('english', %s)) AS rank
        FROM intl_market.gb_knowledge_docs
        WHERE search_vector @@ plainto_tsquery('english', %s)
        {source_filter}
        ORDER BY rank DESC
        LIMIT %s
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]


class BaseConnector:
    """Abstract base for knowledge connectors."""
    source: str = ""

    def fetch(self) -> Iterator[dict]:
        """Yield dicts: doc_type, title, url, published_date, content."""
        raise NotImplementedError

    def run(self, conn) -> int:
        """Fetch and upsert all documents. Returns count of new docs inserted."""
        n = 0
        for doc in self.fetch():
            inserted = upsert_doc(
                conn, self.source,
                doc["doc_type"], doc.get("title", ""),
                doc.get("url"), doc.get("published_date"), doc["content"],
            )
            if inserted:
                n += 1
        return n
