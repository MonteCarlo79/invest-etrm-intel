"""Parameterised Expert Memory for any international BESS market.

Identical logic to gb_knowledge/expert_memory.py but all table references
are parameterised by `table_prefix` (e.g. "au_", "ercot_").

Usage:
    from services.intl_market_common.expert_memory_base import (
        extract_insights, get_insights, inject_memory, digest_kb_docs,
    )
    # Post-turn
    extract_insights(user_msg, reply, api_key, table_prefix="au_")
    # Pre-turn
    insights = get_insights(query, table_prefix="au_")
    context  = inject_memory(insights, market_name="Australia (NEM)")
"""
from __future__ import annotations

import json
import logging
from datetime import date

import anthropic
import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

_EXTRACT_MODEL = "claude-haiku-4-5"


# ── Shared system prompts ─────────────────────────────────────────────────────

def _extract_system(market_name: str) -> str:
    return f"""\
You are extracting durable expert insights from a {market_name} electricity market analyst conversation.

Extract ONLY insights that are ALL of the following:
1. Non-obvious — not trivially found by searching article titles or headlines
2. Validated — the user confirmed, corrected, or accepted the agent's analysis
3. Durable — likely to remain relevant for weeks or months (not today's single price reading)
4. Domain-specific — about {market_name} electricity markets, BESS storage operations,
   ancillary services, grid services, regulation, flexibility markets, or BESS investment economics

DO NOT extract:
- Ephemeral facts (today's price, this week's single event)
- Process instructions or UI navigation steps
- Questions without clear answers
- Generic observations already obvious from market documentation

Classify each insight type (choose one):
  market_structure | price_driver | regulation | risk | opportunity |
  bess_economics | grid_services | investment

Respond ONLY with valid JSON:
{{
  "insights": [
    {{
      "insight": "...",
      "type": "...",
      "confidence": "high|medium|low"
    }}
  ]
}}

If no durable insights are found, return {{"insights": []}}.
"""


def _digest_system(market_name: str) -> str:
    return f"""\
Extract 3-7 durable {market_name} electricity market insights from this document.

Each insight must be ALL of the following:
- Non-obvious: not trivially found by searching article titles or headlines
- Specific: contains concrete facts, figures, mechanisms, or named entities
- Actionable: useful for a BESS operator, trader, or investor making decisions
- Durable: will remain relevant for weeks or months

Focus on: market mechanics, BESS revenue drivers, regulatory developments, price patterns,
operational strategies, grid services, policy changes, capacity procurement, ancillary
service auction dynamics, grid constraint patterns, storage deployment trends.

DO NOT extract:
- Ephemeral daily price readings or single-day events
- Generic industry descriptions already obvious from public documentation
- Questions without clear answers

Classify each insight type (choose one):
  market_structure | price_driver | regulation | risk | opportunity |
  bess_economics | grid_services | investment

Respond ONLY with valid JSON:
{{"insights": [{{"insight": "...", "type": "...", "confidence": "high|medium|low"}}]}}

If no durable insights can be extracted, return {{"insights": []}}.
"""


# ── DB helpers ────────────────────────────────────────────────────────────────

def _get_conn():
    import os
    import psycopg2
    return psycopg2.connect(
        os.environ["PGURL"],
        keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=5,
    )


def _ensure_insights_table(conn, prefix: str) -> None:
    """Create {prefix}expert_insights table if it doesn't exist."""
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS intl_market.{prefix}expert_insights (
                id            SERIAL PRIMARY KEY,
                insight_text  TEXT NOT NULL,
                insight_type  TEXT NOT NULL DEFAULT 'other',
                confidence    TEXT NOT NULL DEFAULT 'medium',
                source_session DATE,
                source_doc_url TEXT,
                active        BOOLEAN NOT NULL DEFAULT TRUE,
                validated_at  TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        cur.execute(
            f"ALTER TABLE intl_market.{prefix}expert_insights "
            "ADD COLUMN IF NOT EXISTS source_doc_url TEXT"
        )
    conn.commit()


def _parse_insights_json(raw: str) -> list[dict]:
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("```", 2)[1]
        if raw.startswith("json"):
            raw = raw[4:]
    try:
        return json.loads(raw).get("insights", [])
    except Exception:
        return []


# ── Post-turn insight extraction ─────────────────────────────────────────────

def extract_insights(
    user_msg: str,
    agent_reply: str,
    api_key: str,
    table_prefix: str,
    market_name: str = "electricity",
) -> int:
    """Extract durable insights from a strategist turn and store them."""
    from shared.anthropic_client import make_client as _make_anthropic_client
    client = _make_anthropic_client(api_key)
    try:
        resp = client.messages.create(
            model=_EXTRACT_MODEL,
            max_tokens=800,
            system=_extract_system(market_name),
            messages=[{
                "role": "user",
                "content": f"User: {user_msg}\n\nAgent: {agent_reply[:2000]}",
            }],
        )
        insights = _parse_insights_json(resp.content[0].text)
    except Exception as exc:
        logger.debug("[expert_memory:%s] extraction failed: %s", table_prefix, exc)
        return 0

    if not insights:
        return 0

    conn = _get_conn()
    try:
        _ensure_insights_table(conn, table_prefix)
        conn.autocommit = False
        with conn.cursor() as cur:
            for item in insights:
                cur.execute(
                    f"INSERT INTO intl_market.{table_prefix}expert_insights "
                    "(insight_text, insight_type, confidence, source_session) "
                    "VALUES (%s, %s, %s, %s)",
                    (
                        item.get("insight", ""),
                        item.get("type", "other"),
                        item.get("confidence", "medium"),
                        date.today().isoformat(),
                    ),
                )
        conn.commit()
        return len(insights)
    except Exception as exc:
        logger.debug("[expert_memory:%s] store failed: %s", table_prefix, exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return 0
    finally:
        conn.close()


# ── Insight retrieval ─────────────────────────────────────────────────────────

def get_insights(query: str, table_prefix: str, limit: int = 5) -> list[dict]:
    """Retrieve expert insights relevant to a query using OR-based FTS."""
    conn = _get_conn()
    try:
        conn.autocommit = True
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"SELECT insight_text, insight_type, confidence, source_session "
                f"FROM intl_market.{table_prefix}expert_insights "
                "WHERE active = TRUE "
                "  AND to_tsvector('english', insight_text) @@ to_tsquery('english', "
                "    regexp_replace(plainto_tsquery('english', %s)::text, ' & ', ' | ', 'g')) "
                "ORDER BY "
                "  CASE confidence WHEN 'high' THEN 3 WHEN 'medium' THEN 2 ELSE 1 END DESC, "
                "  validated_at DESC NULLS LAST "
                "LIMIT %s",
                (query, limit),
            )
            return [dict(r) for r in cur.fetchall()]
    except Exception as exc:
        logger.debug("[expert_memory:%s] retrieval failed: %s", table_prefix, exc)
        return []
    finally:
        conn.close()


def inject_memory(insights: list[dict], market_name: str = "market") -> str:
    """Format insights as a context block for the agent system prompt."""
    if not insights:
        return ""
    lines = [f"## Expert Memory (accumulated {market_name} insights)\n"]
    for ins in insights:
        conf = f" ({ins['confidence']} confidence)" if ins.get("confidence") else ""
        lines.append(f"• [{ins['insight_type']}{conf}] {ins['insight_text']}")
    return "\n".join(lines)


# ── Interview answer storage ──────────────────────────────────────────────────

def store_interview_answer(
    question: str,
    answer: str,
    topic: str,
    table_prefix: str,
) -> None:
    """Store a user-provided interview answer as a high-confidence insight."""
    conn = _get_conn()
    try:
        _ensure_insights_table(conn, table_prefix)
        with conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO intl_market.{table_prefix}expert_insights "
                "(insight_text, insight_type, confidence, source_session) "
                "VALUES (%s, %s, 'high', %s)",
                (answer[:1000], topic, date.today().isoformat()),
            )
        conn.commit()
    except Exception as exc:
        raise RuntimeError(f"Failed to store answer: {exc}") from exc
    finally:
        conn.close()


# ── KB Digestion ──────────────────────────────────────────────────────────────

def _extract_insights_from_doc(client, doc: dict, market_name: str) -> list[dict]:
    prompt = (
        f"Source: {doc.get('source', '')} / {doc.get('doc_type', '')}\n"
        f"Title: {doc.get('title', 'Untitled')}\n"
        f"Published: {doc.get('published_date', 'unknown')}\n\n"
        f"{doc.get('content', '')}"
    )
    try:
        resp = client.messages.create(
            model=_EXTRACT_MODEL,
            max_tokens=800,
            system=_digest_system(market_name),
            messages=[{"role": "user", "content": prompt[:4000]}],
        )
        return _parse_insights_json(resp.content[0].text)
    except Exception as exc:
        logger.debug("[kb_digest] doc %s error: %s", doc.get("url"), exc)
        return []


def _store_doc_insights(conn, insights: list[dict], source_doc_url: str, table_prefix: str) -> int:
    if not insights:
        return 0
    today = date.today().isoformat()
    with conn.cursor() as cur:
        for item in insights:
            try:
                cur.execute(
                    f"INSERT INTO intl_market.{table_prefix}expert_insights "
                    "(insight_text, insight_type, confidence, source_session, source_doc_url) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (
                        item.get("insight", ""),
                        item.get("type", "other"),
                        item.get("confidence", "medium"),
                        today,
                        source_doc_url,
                    ),
                )
            except Exception as exc:
                logger.debug("[kb_digest] insert failed: %s", exc)
    conn.commit()
    return len(insights)


def digest_kb_docs(
    api_key: str,
    table_prefix: str,
    market_name: str = "electricity",
    limit: int = 50,
) -> int:
    """Process undigested KB docs → extract insights → store in {prefix}expert_insights.

    A doc is 'undigested' if its URL does not appear in {prefix}expert_insights.source_doc_url.
    Returns total count of new insights stored.
    """
    conn = _get_conn()
    try:
        _ensure_insights_table(conn, table_prefix)

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"SELECT id, source, doc_type, title, url, published_date, "
                f"left(content, 3500) AS content "
                f"FROM intl_market.{table_prefix}knowledge_docs "
                "WHERE url IS NOT NULL "
                "  AND url NOT IN ( "
                f"    SELECT DISTINCT source_doc_url "
                f"    FROM intl_market.{table_prefix}expert_insights "
                "    WHERE source_doc_url IS NOT NULL "
                "  ) "
                "ORDER BY fetched_at DESC "
                "LIMIT %s",
                (limit,),
            )
            docs = cur.fetchall()

        if not docs:
            logger.info("[kb_digest:%s] No undigested docs.", table_prefix)
            return 0

        logger.info("[kb_digest:%s] Digesting %d docs…", table_prefix, len(docs))
        from shared.anthropic_client import make_client as _make_anthropic_client
        client = _make_anthropic_client(api_key)
        total = 0
        for doc in docs:
            insights = _extract_insights_from_doc(client, doc, market_name)
            if insights:
                n = _store_doc_insights(conn, insights, source_doc_url=doc["url"], table_prefix=table_prefix)
                total += n
                logger.info("[kb_digest:%s] %s → %d insights", table_prefix, doc.get("url", "")[:60], n)

        logger.info("[kb_digest:%s] Done — %d insights from %d docs", table_prefix, total, len(docs))
        return total

    except Exception as exc:
        logger.error("[kb_digest:%s] Failed: %s", table_prefix, exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return 0
    finally:
        conn.close()
