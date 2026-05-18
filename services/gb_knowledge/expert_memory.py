"""GB Expert Memory
===================
Accumulates validated domain insights from GB Strategist conversations into
intl_market.gb_expert_insights (mirrors staging.kp_expert_insights for China).

After each strategist turn, an insight extractor identifies durable, non-obvious
GB market facts and writes them to the DB. At query time, relevant insights are
retrieved via FTS and injected into the agent system prompt.

Usage (in app.py):
    from services.gb_knowledge.expert_memory import (
        extract_gb_insights,
        get_gb_insights,
        inject_gb_memory,
    )
    # Post-turn
    extract_gb_insights(user_msg, agent_reply, api_key)
    # Pre-turn (build system prompt)
    insights = get_gb_insights(query)
    context = inject_gb_memory(insights)
"""
from __future__ import annotations

import json
import logging
from datetime import date
from typing import Optional

import anthropic
import psycopg2
import psycopg2.extras

from .base import get_db_conn

logger = logging.getLogger(__name__)

_EXTRACT_MODEL = "claude-haiku-4-5"

# ── Insight extraction ────────────────────────────────────────────────────────

_EXTRACT_SYSTEM = """\
You are extracting durable expert insights from a GB electricity market analyst conversation.

Extract ONLY insights that are ALL of the following:
1. Non-obvious — not trivially found by searching article titles or headlines
2. Validated — the user confirmed, corrected, or accepted the agent's analysis
3. Durable — likely to remain relevant for weeks or months (not today's single price reading)
4. Domain-specific — about GB electricity markets, BESS storage operations, ancillary
   services (DC/DM/DR), grid services, regulation (OFGEM/ESO/Elexon), flexibility markets,
   or BESS investment economics

DO NOT extract:
- Ephemeral facts (today's price, this week's single event)
- Process instructions or UI navigation steps
- Questions without clear answers
- Generic observations already obvious from market documentation

Classify each insight type (choose one):
  market_structure | price_driver | regulation | risk | opportunity |
  bess_economics | grid_services | investment

Respond ONLY with valid JSON:
{
  "insights": [
    {
      "insight": "...",          // 1-3 precise, actionable sentences
      "type": "...",
      "confidence": "high|medium|low"
    }
  ]
}

If no durable insights are found, return {"insights": []}.
"""


def extract_gb_insights(user_msg: str, agent_reply: str, api_key: str) -> int:
    """
    Extract durable insights from a strategist conversation turn and store them.

    Returns number of insights stored (0 if none or on error).
    """
    client = anthropic.Anthropic(api_key=api_key)
    try:
        resp = client.messages.create(
            model=_EXTRACT_MODEL,
            max_tokens=800,
            system=_EXTRACT_SYSTEM,
            messages=[{
                "role": "user",
                "content": f"User: {user_msg}\n\nAgent: {agent_reply[:2000]}",
            }],
        )
        raw = resp.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("```", 2)[1]
            if raw.startswith("json"):
                raw = raw[4:]
        insights = json.loads(raw).get("insights", [])
    except Exception as exc:
        logger.debug("GB insight extraction failed: %s", exc)
        return 0

    if not insights:
        return 0

    conn = get_db_conn()
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            for item in insights:
                cur.execute(
                    "INSERT INTO intl_market.gb_expert_insights "
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
        logger.debug("Failed to store GB insights: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return 0
    finally:
        conn.close()


# ── Insight retrieval ─────────────────────────────────────────────────────────

def get_gb_insights(query: str, limit: int = 5) -> list[dict]:
    """
    Retrieve expert insights relevant to a query using OR-based FTS.
    High-confidence insights are surfaced first.
    Returns [] on any error (table may not exist yet).
    """
    conn = get_db_conn()
    try:
        conn.autocommit = True
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT insight_text, insight_type, confidence, source_session "
                "FROM intl_market.gb_expert_insights "
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
        logger.debug("GB insight retrieval failed (table may not exist yet): %s", exc)
        return []
    finally:
        conn.close()


def inject_gb_memory(insights: list[dict]) -> str:
    """Format insights as a context block for injection into the agent system prompt."""
    if not insights:
        return ""
    lines = ["## Expert Memory (accumulated GB market insights)\n"]
    for ins in insights:
        conf = f" ({ins['confidence']} confidence)" if ins.get("confidence") else ""
        lines.append(f"• [{ins['insight_type']}{conf}] {ins['insight_text']}")
    return "\n".join(lines)


# ── KB Digestion ──────────────────────────────────────────────────────────────

_DIGEST_SYSTEM = """\
Extract 3-7 durable GB electricity market insights from this document.

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
- Statements about market activity with no specific facts

Classify each insight type (choose one):
  market_structure | price_driver | regulation | risk | opportunity |
  bess_economics | grid_services | investment

Respond ONLY with valid JSON:
{"insights": [{"insight": "...", "type": "...", "confidence": "high|medium|low"}]}

If no durable insights can be extracted, return {"insights": []}.
"""


def _parse_insights_json(raw: str) -> list[dict]:
    """Parse JSON from Claude response, handling markdown code fences."""
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("```", 2)[1]
        if raw.startswith("json"):
            raw = raw[4:]
    try:
        return json.loads(raw).get("insights", [])
    except Exception:
        return []


def _extract_insights_from_doc(client, doc: dict) -> list[dict]:
    """Call Claude haiku on one KB doc; return list of insight dicts."""
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
            system=_DIGEST_SYSTEM,
            messages=[{"role": "user", "content": prompt[:4000]}],
        )
        return _parse_insights_json(resp.content[0].text)
    except Exception as exc:
        logger.debug("Insight extraction failed for doc %s: %s", doc.get("url"), exc)
        return []


def _store_doc_insights(conn, insights: list[dict], source_doc_url: str) -> int:
    """Insert insights with source_doc_url provenance. Returns count inserted."""
    if not insights:
        return 0
    today = date.today().isoformat()
    with conn.cursor() as cur:
        for item in insights:
            try:
                cur.execute(
                    "INSERT INTO intl_market.gb_expert_insights "
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
                logger.debug("Insert insight failed: %s", exc)
    conn.commit()
    return len(insights)


def digest_kb_docs(api_key: str, limit: int = 50) -> int:
    """
    Process undigested KB docs → extract insights → store in gb_expert_insights.

    A doc is 'undigested' if its URL does not appear in gb_expert_insights.source_doc_url.
    Adds source_doc_url column if missing (schema migration).
    Returns total count of new insights stored.
    """
    conn = get_db_conn()
    try:
        # Ensure source_doc_url column exists (idempotent migration)
        with conn.cursor() as cur:
            cur.execute(
                "ALTER TABLE intl_market.gb_expert_insights "
                "ADD COLUMN IF NOT EXISTS source_doc_url TEXT"
            )
        conn.commit()

        # Find undigested docs (url not yet referenced in expert_insights)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, source, doc_type, title, url, published_date, "
                "left(content, 3500) AS content "
                "FROM intl_market.gb_knowledge_docs "
                "WHERE url IS NOT NULL "
                "  AND url NOT IN ( "
                "    SELECT DISTINCT source_doc_url "
                "    FROM intl_market.gb_expert_insights "
                "    WHERE source_doc_url IS NOT NULL "
                "  ) "
                "ORDER BY fetched_at DESC "
                "LIMIT %s",
                (limit,),
            )
            docs = cur.fetchall()

        if not docs:
            logger.info("[kb_digest] No undigested docs found.")
            return 0

        logger.info("[kb_digest] Digesting %d docs…", len(docs))
        client = anthropic.Anthropic(api_key=api_key)
        total = 0
        for doc in docs:
            insights = _extract_insights_from_doc(client, doc)
            if insights:
                n = _store_doc_insights(conn, insights, source_doc_url=doc["url"])
                total += n
                logger.info("[kb_digest] %s → %d insights", doc.get("url", "")[:60], n)

        logger.info("[kb_digest] Done — %d total insights extracted from %d docs", total, len(docs))
        return total

    except Exception as exc:
        logger.error("[kb_digest] Failed: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return 0
    finally:
        conn.close()
