"""Expert memory helpers shared across international market apps.

Functions:
  extract_insights  -- Claude extracts insights from a conversation turn and stores them
  get_insights      -- retrieve relevant stored insights by keyword match
  inject_memory     -- format insights into a system-prompt memory block
  digest_kb_docs    -- bulk-extract insights from knowledge_docs not yet processed
"""
from __future__ import annotations

import json
import logging
import os
import re

import psycopg2

logger = logging.getLogger(__name__)


def _conn():
    url = (
        os.environ.get("PGURL")
        or os.environ.get("DB_DSN")
        or "postgresql://postgres:root@127.0.0.1:5433/marketdata"
    )
    conn = psycopg2.connect(url, connect_timeout=10)
    conn.autocommit = True
    return conn


def _parse_json_list(text: str) -> list:
    m = re.search(r"\[.*\]", text, re.DOTALL)
    if not m:
        return []
    try:
        return json.loads(m.group())
    except json.JSONDecodeError:
        return []


def extract_insights(
    user_input: str,
    reply: str,
    api_key: str,
    prefix: str,
    market_name: str,
) -> int:
    """Extract 0-3 expert insights from a conversation turn and store them.

    Returns the number of new insights inserted.
    """
    import anthropic

    client = anthropic.Anthropic(api_key=api_key)
    prompt = (
        f"You are extracting expert market insights from a conversation about {market_name}.\n\n"
        f"User asked: {user_input[:500]}\n\n"
        f"Expert replied: {reply[:2000]}\n\n"
        "Extract 0-3 specific, factual insights worth remembering for future market analysis.\n"
        "Each insight must be a single sentence, concrete and specific (not generic advice).\n"
        'Return a JSON array: [{"insight_text": "...", "insight_type": "market|regulatory|investment|operational", "confidence": "high|medium|low"}]\n'
        "Return [] if nothing specific to extract."
    )
    try:
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}],
        )
        insights = _parse_json_list(resp.content[0].text.strip())
    except Exception as exc:
        logger.warning("[extract_insights] Claude call failed: %s", exc)
        return 0

    if not insights:
        return 0

    conn = _conn()
    n = 0
    try:
        with conn.cursor() as cur:
            for ins in insights:
                text = (ins.get("insight_text") or "").strip()
                if not text:
                    continue
                cur.execute(
                    f"INSERT INTO intl_market.{prefix}expert_insights "
                    "(insight_text, insight_type, confidence, source_session) "
                    "VALUES (%s, %s, %s, %s)",
                    (
                        text,
                        ins.get("insight_type", "general"),
                        ins.get("confidence", "medium"),
                        "conversation",
                    ),
                )
                n += 1
    except Exception as exc:
        logger.warning("[extract_insights] DB insert failed: %s", exc)
    finally:
        conn.close()
    return n


def get_insights(query: str, prefix: str, limit: int = 5) -> list:
    """Return relevant stored insights.

    Uses ILIKE keyword search on insight_text; falls back to most-recent
    if no match is found.
    """
    conn = _conn()
    try:
        with conn.cursor() as cur:
            kw = f"%{query[:100]}%"
            cur.execute(
                f"SELECT insight_text, insight_type, confidence, validated_at "
                f"FROM intl_market.{prefix}expert_insights "
                f"WHERE active=TRUE AND insight_text ILIKE %s "
                f"ORDER BY validated_at DESC LIMIT %s",
                (kw, limit),
            )
            rows = cur.fetchall()
            if not rows:
                cur.execute(
                    f"SELECT insight_text, insight_type, confidence, validated_at "
                    f"FROM intl_market.{prefix}expert_insights "
                    f"WHERE active=TRUE "
                    f"ORDER BY validated_at DESC LIMIT %s",
                    (limit,),
                )
                rows = cur.fetchall()
    except Exception as exc:
        logger.warning("[get_insights] DB query failed: %s", exc)
        rows = []
    finally:
        conn.close()

    return [
        {
            "insight_text": r[0],
            "insight_type": r[1],
            "confidence": r[2],
            "validated_at": r[3],
        }
        for r in rows
    ]


def inject_memory(insights: list, market_name: str) -> str:
    """Format a list of insights into a system-prompt memory block.

    Returns an empty string if there are no insights.
    """
    if not insights:
        return ""
    lines = [f"## Stored expert memory -- {market_name}:"]
    for ins in insights:
        itype = ins.get("insight_type") or "general"
        text = ins.get("insight_text") or ""
        if text:
            lines.append(f"- [{itype}] {text}")
    if len(lines) == 1:
        return ""
    return "\n".join(lines)


def digest_kb_docs(
    api_key: str,
    prefix: str,
    market_name: str,
    limit: int = 100,
) -> int:
    """Extract expert insights from knowledge docs not yet processed.

    Returns the number of new insights inserted across all processed docs.
    """
    import anthropic

    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT d.id, d.title, d.content "
                f"FROM intl_market.{prefix}knowledge_docs d "
                f"WHERE NOT EXISTS ("
                f"  SELECT 1 FROM intl_market.{prefix}expert_insights e "
                f"  WHERE e.source_doc_url = d.id::text"
                f") "
                f"ORDER BY d.fetched_at DESC LIMIT %s",
                (limit,),
            )
            docs = cur.fetchall()
    except Exception as exc:
        logger.warning("[digest_kb_docs] DB query failed: %s", exc)
        conn.close()
        return 0

    if not docs:
        conn.close()
        return 0

    client = anthropic.Anthropic(api_key=api_key)
    total = 0

    for doc_id, title, content in docs:
        prompt = (
            f"Extract 1-5 key expert insights from this {market_name} market document.\n\n"
            f"Title: {title or 'Unknown'}\n"
            f"Content: {(content or '')[:3000]}\n\n"
            "Extract specific, factual insights useful for investment analysis.\n"
            'Return JSON array: [{"insight_text": "...", "insight_type": "market|regulatory|investment|operational", "confidence": "high|medium|low"}]\n'
            "Return [] if nothing noteworthy."
        )
        try:
            resp = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=600,
                messages=[{"role": "user", "content": prompt}],
            )
            insights = _parse_json_list(resp.content[0].text.strip())
        except Exception as exc:
            logger.warning("[digest_kb_docs] Claude failed on doc %s: %s", doc_id, exc)
            continue

        try:
            with conn.cursor() as cur:
                for ins in insights:
                    text = (ins.get("insight_text") or "").strip()
                    if not text:
                        continue
                    cur.execute(
                        f"INSERT INTO intl_market.{prefix}expert_insights "
                        "(insight_text, insight_type, confidence, source_doc_url) "
                        "VALUES (%s, %s, %s, %s)",
                        (
                            text,
                            ins.get("insight_type", "general"),
                            ins.get("confidence", "medium"),
                            str(doc_id),
                        ),
                    )
                    total += 1
        except Exception as exc:
            logger.warning("[digest_kb_docs] DB insert failed for doc %s: %s", doc_id, exc)

    conn.close()
    return total
