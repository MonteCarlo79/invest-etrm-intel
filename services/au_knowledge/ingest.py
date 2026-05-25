"""AU (NEM) knowledge base ingestion orchestrator.

Sources:
  - Modo AI (Playwright distillation) — primary daily intelligence
  - AEMO market notices (NEMWeb API) — regulatory and operational updates

Usage:
    python -m services.au_knowledge.ingest            # all sources
    python -m services.au_knowledge.ingest --only modo_ai
    python -m services.au_knowledge.ingest --only aemo_notices
"""
import argparse
import logging
import os
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

logger = logging.getLogger(__name__)


class AEMONoticesConnector:
    """Fetches recent AEMO market notices from the NEMWeb API."""

    source = "aemo_notices"

    # AEMO's public market-notice search API (documented at aemo.com.au/about/contact-us/api)
    _API_URL = "https://www.aemo.com.au/api/market-notice"

    def fetch(self) -> list[dict]:
        import requests

        cutoff = date.today() - timedelta(days=14)
        docs = []

        try:
            resp = requests.get(
                self._API_URL,
                params={"markets": "NEM", "limit": 50},
                headers={"Accept": "application/json"},
                timeout=30,
            )
            resp.raise_for_status()
            raw = resp.json()
        except Exception as exc:
            logger.warning("[aemo_notices] API fetch failed: %s", exc)
            return []

        # API returns either a list directly or a dict with a "notices" key
        notices = raw if isinstance(raw, list) else raw.get("notices") or raw.get("items") or []

        for notice in notices:
            if not isinstance(notice, dict):
                continue

            # Parse publication date
            pub_date = None
            for field in ("createdDate", "publishedDate", "date", "noticeDate"):
                raw_dt = notice.get(field)
                if raw_dt:
                    try:
                        from datetime import datetime
                        pub_date = datetime.fromisoformat(str(raw_dt)[:10]).date()
                        break
                    except Exception:
                        pass

            if pub_date and pub_date < cutoff:
                continue

            title = (
                notice.get("title") or notice.get("subject")
                or notice.get("noticeTitle") or "AEMO Market Notice"
            )
            body = (
                notice.get("body") or notice.get("content")
                or notice.get("text") or notice.get("noticeBody") or ""
            )
            notice_id = notice.get("id") or notice.get("noticeId") or ""
            effective_date = (pub_date or date.today()).isoformat()
            url = f"aemo://market-notice/{notice_id}/{effective_date}"

            if not body:
                continue

            docs.append({
                "doc_type": "market_notice",
                "title": title,
                "url": url,
                "published_date": pub_date or date.today(),
                "content": f"AEMO Market Notice — {title}\nDate: {effective_date}\n\n{body}",
            })

        return docs


def run_knowledge_ingest(only: list[str] | None = None, verbose: bool = True) -> dict[str, int]:
    from dotenv import load_dotenv
    load_dotenv(
        os.path.join(os.path.dirname(__file__), "..", "..", "config", ".env"),
        override=False,
    )

    import psycopg2
    from services.au_knowledge.config import MARKET_CONFIG

    # Ensure au_knowledge_docs table exists
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

    connectors = [
        ("aemo_notices", "AEMO market notices", AEMONoticesConnector()),
    ]

    try:
        from services.intl_market_common.modo_ai_base import ModoAIConnector
        connectors.append(("modo_ai", "Modo Energy AI (AU distillation)", ModoAIConnector(MARKET_CONFIG)))
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
            n = connector.run(conn) if hasattr(connector, "run") else _run_connector(connector, conn, prefix)
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
    """Run a connector that yields dicts and upsert into the market's KB table."""
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
    parser.add_argument("--only", help="Comma-separated connectors to run")
    args = parser.parse_args()
    only = args.only.split(",") if args.only else None
    results = run_knowledge_ingest(only=only)
    print("Results:", results)
