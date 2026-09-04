"""PJM knowledge base ingestion orchestrator.

Sources:
  - Modo AI (Playwright distillation) — primary daily intelligence
  - PJM market notices (public)

Usage:
    python -m services.pjm_knowledge.ingest
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))


def run_knowledge_ingest(only: list[str] | None = None, verbose: bool = True) -> dict[str, int]:
    from dotenv import load_dotenv
    load_dotenv(
        os.path.join(os.path.dirname(__file__), "..", "..", "config", ".env"),
        override=False,
    )

    from services.pjm_knowledge.config import MARKET_CONFIG
    import psycopg2

    conn = psycopg2.connect(os.environ["PGURL"], keepalives=1, keepalives_idle=30)
    prefix = MARKET_CONFIG.table_prefix
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS intl_market.{prefix}knowledge_docs (
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
                        coalesce(title,'') || ' ' || left(content,100000))
                ) STORED
            )
        """)
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS {prefix}knowledge_docs_fts "
            f"ON intl_market.{prefix}knowledge_docs USING GIN(search_vector)"
        )
    conn.commit()

    connectors = []

    try:
        from services.intl_market_common.modo_ai_base import ModoAIConnector
        connectors.append(("modo_ai", "Modo Energy AI (PJM distillation)", ModoAIConnector(MARKET_CONFIG)))
    except ImportError as e:
        if verbose:
            print(f"  [skip] modo_ai: {e}")

    results = {}
    for key, label, connector in connectors:
        if only and key not in only:
            continue
        if verbose:
            print(f"  [{key}] {label}…", end="", flush=True)
        try:
            n = _run_connector(connector, conn, prefix)
            results[key] = n
            if verbose:
                print(f" {n} new docs")
        except Exception as exc:
            results[key] = 0
            if verbose:
                print(f" ERROR: {exc}")

    conn.close()
    return results


def _run_connector(connector, conn, prefix: str) -> int:
    n = 0
    for doc in connector.fetch():
        with conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO intl_market.{prefix}knowledge_docs "
                "(source, doc_type, title, url, published_date, content) "
                "VALUES (%s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (url) DO NOTHING",
                (
                    getattr(connector, "source", "unknown"),
                    doc["doc_type"], doc.get("title", ""),
                    doc.get("url"), doc.get("published_date"), doc["content"],
                ),
            )
            if cur.rowcount > 0:
                n += 1
    conn.commit()
    return n


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--only")
    args = parser.parse_args()
    print(run_knowledge_ingest(only=args.only.split(",") if args.only else None))
