"""HyDE-enhanced full-text retrieval for international market knowledge bases.

HyDE (Hypothetical Document Embeddings) improves FTS recall:
  1. Ask Claude to write a short hypothetical expert answer to the query.
  2. Combine the original query with key terms from the hypothetical answer.
  3. Run a PostgreSQL FTS search against {prefix}knowledge_docs.
"""
from __future__ import annotations

import logging
import os

import psycopg2

logger = logging.getLogger(__name__)


def retrieve_for_agent(
    query: str,
    api_key: str,
    cfg,
    sources: list | None = None,
    top_k: int = 6,
) -> str:
    """Return formatted knowledge-base snippets relevant to *query*.

    Uses HyDE to expand the query, then PostgreSQL FTS against
    intl_market.{cfg.table_prefix}knowledge_docs.

    Args:
        query:    The user or tool query string.
        api_key:  Anthropic API key for the HyDE step.
        cfg:      MarketConfig instance (needs .name and .table_prefix).
        sources:  Optional list of source names to filter on.
        top_k:    Maximum number of documents to return.

    Returns:
        A formatted string of ranked document snippets, or a no-results message.
    """
    # ── Step 1: HyDE expansion ────────────────────────────────────────────────
    search_query = query
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=200,
            messages=[
                {
                    "role": "user",
                    "content": (
                        f"Write a 100-word expert answer about {cfg.name} energy markets "
                        f"for this question: {query}\n"
                        "Use precise technical terms and market-specific language."
                    ),
                }
            ],
        )
        hyde_text = resp.content[0].text.strip()
        # Combine original query + first 200 chars of hypothetical doc for richer FTS input
        search_query = f"{query} {hyde_text[:200]}"
    except Exception as exc:
        logger.debug("[retrieve_for_agent] HyDE step skipped: %s", exc)

    # ── Step 2: PostgreSQL FTS ────────────────────────────────────────────────
    url = (
        os.environ.get("PGURL")
        or os.environ.get("DB_DSN")
        or "postgresql://postgres:root@127.0.0.1:5433/marketdata"
    )
    prefix = cfg.table_prefix

    conn = psycopg2.connect(url, connect_timeout=10)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            if sources:
                placeholders = ",".join(["%s"] * len(sources))
                source_clause = f"AND source IN ({placeholders})"
                params = [search_query, search_query, *sources, top_k]
            else:
                source_clause = ""
                params = [search_query, search_query, top_k]

            cur.execute(
                f"SELECT source, title, published_date, left(content, 1500) AS snippet, "
                f"ts_rank(search_vector, plainto_tsquery('english', %s)) AS rank "
                f"FROM intl_market.{prefix}knowledge_docs "
                f"WHERE search_vector @@ to_tsquery('english', "
                f"  regexp_replace(plainto_tsquery('english', %s)::text, ' & ', ' | ', 'g')) "
                f"{source_clause} "
                f"ORDER BY rank DESC LIMIT %s",
                params,
            )
            rows = cur.fetchall()
    except Exception as exc:
        logger.warning("[retrieve_for_agent] FTS query failed: %s", exc)
        rows = []
    finally:
        conn.close()

    if not rows:
        # Fallback: broad ILIKE search on title + first 20k of content using key terms
        try:
            # Extract the most meaningful terms (skip very short words)
            terms = [t for t in query.split() if len(t) > 3][:5]
            ilike_rows: list = []
            with psycopg2.connect(url, connect_timeout=10) as fb_conn:
                fb_conn.autocommit = True
                with fb_conn.cursor() as cur:
                    for term in terms:
                        cur.execute(
                            f"SELECT source, title, published_date, left(content, 1500) AS snippet, 0.5 AS rank "
                            f"FROM intl_market.{prefix}knowledge_docs "
                            f"WHERE title ILIKE %s OR left(content, 20000) ILIKE %s "
                            f"ORDER BY fetched_at DESC LIMIT %s",
                            (f"%{term}%", f"%{term}%", top_k),
                        )
                        ilike_rows.extend(cur.fetchall())
            # Deduplicate by title
            seen: set = set()
            rows = []
            for r in ilike_rows:
                key = r[1] or r[0]
                if key not in seen:
                    seen.add(key)
                    rows.append(r)
            rows = rows[:top_k]
        except Exception as exc:
            logger.debug("[retrieve_for_agent] ILIKE fallback failed: %s", exc)

    if not rows:
        return f"No documents found in the {cfg.name} knowledge base matching this query."

    parts = []
    for source, title, pub_date, snippet, _ in rows:
        date_str = pub_date.strftime("%Y-%m-%d") if pub_date else "n/a"
        parts.append(f"**[{source}] {title or 'Untitled'} ({date_str})**\n{snippet}")

    return "\n\n---\n\n".join(parts)
