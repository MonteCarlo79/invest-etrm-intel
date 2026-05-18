"""
Knowledge Pool Intelligence — Phase 4: Expert Memory
=====================================================
Accumulates validated domain insights from agent interactions into a
persistent expert memory store (staging.kp_expert_insights).

After each agent session, an insight extractor runs over the conversation
to pull out non-obvious domain facts, market observations, and regulatory
interpretations that aren't directly derivable from raw documents.

Over time this builds a curated "expert mind" that the strategy agent
injects as additional context, making each session smarter than the last.

Usage:
    # After a conversation turn is logged:
    from services.knowledge_pool.expert_memory import (
        extract_and_store_insights,
        get_relevant_insights,
        inject_expert_memory,
    )

    # End-of-session: extract insights from today's conversation
    extract_and_store_insights(api_key="...", session_date="2026-05-14")

    # Before answering a query: retrieve relevant prior insights
    insights = get_relevant_insights(query="Shanxi ancillary market rules")
    context = inject_expert_memory(insights)
"""
from __future__ import annotations

import json
import logging
from datetime import date
from typing import Optional

import anthropic

from .db import get_conn

logger = logging.getLogger(__name__)

_MEMORY_MODEL = "claude-sonnet-4-6"

# ── Insight extraction prompt ─────────────────────────────────────────────────

_EXTRACT_SYSTEM = """\
You are extracting durable expert insights from an energy market analyst
conversation log.

Review the conversation and extract ONLY insights that are:
1. Non-obvious — not trivially findable by searching the document titles
2. Validated — the user confirmed, corrected, or accepted the agent's analysis
3. Durable — likely to remain relevant for weeks or months
4. Domain-specific — about China electricity markets, BESS operations, regulation,
   pricing, dispatch economics, or investment

DO NOT extract:
- Ephemeral facts (today's price, a single day's result)
- Process instructions ("run this script", "check this table")
- Questions without clear answers

For each insight, classify its type:
  market_structure | price_driver | regulation | risk | opportunity |
  dispatch_economics | investment | operations

Respond ONLY with valid JSON:
{
  "insights": [
    {
      "insight": "...",          // 1-3 sentences, precise and actionable
      "type": "...",             // one of the types above
      "province": "...",         // province name or null if national/general
      "confidence": "high|medium|low",
      "source_session": "..."    // session date e.g. "2026-05-14"
    }
  ]
}

If no durable insights are found, return {"insights": []}.
"""


def extract_and_store_insights(
    api_key: str,
    session_date: Optional[str] = None,
    min_turns: int = 3,
) -> int:
    """
    Extract insights from a session's conversation log and persist them.

    Args:
        api_key: Anthropic API key
        session_date: ISO date string (default: today)
        min_turns: minimum conversation turns required before extracting

    Returns:
        Number of insights stored
    """
    target_date = session_date or date.today().isoformat()
    log_filename = f"conversation_log_{target_date}.md"

    # Fetch conversation turns for this session
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT c.chunk_text
                FROM staging.spot_knowledge_chunks c
                JOIN staging.spot_knowledge_docs d ON d.id = c.doc_id
                WHERE d.file_name = %s
                ORDER BY c.chunk_index
                """,
                (log_filename,),
            )
            turns = [r[0] for r in cur.fetchall()]

    if len(turns) < min_turns:
        return 0

    conversation_text = "\n\n---\n\n".join(turns)

    client = anthropic.Anthropic(api_key=api_key)
    try:
        resp = client.messages.create(
            model=_MEMORY_MODEL,
            max_tokens=1500,
            system=_EXTRACT_SYSTEM,
            messages=[{
                "role": "user",
                "content": (
                    f"Session date: {target_date}\n\n"
                    f"Conversation log:\n{conversation_text[:12000]}"
                ),
            }],
        )
        raw = resp.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("```", 2)[1]
            if raw.startswith("json"):
                raw = raw[4:]
        result = json.loads(raw)
    except Exception as exc:
        logger.error("Insight extraction failed for %s: %s", target_date, exc)
        return 0

    insights = result.get("insights", [])
    if not insights:
        return 0

    # Fetch doc IDs referenced in this session's conversation (as source context)
    with get_conn() as conn:
        with conn.cursor() as cur:
            for item in insights:
                cur.execute(
                    """
                    INSERT INTO staging.kp_expert_insights
                        (insight_text, insight_type, province, confidence,
                         source_session, validated_at)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                    """,
                    (
                        item.get("insight", ""),
                        item.get("type", "other"),
                        item.get("province"),
                        item.get("confidence", "medium"),
                        item.get("source_session", target_date),
                    ),
                )
        conn.commit()

    logger.info("Stored %d insights from session %s", len(insights), target_date)
    return len(insights)


def get_relevant_insights(
    query: str,
    province: Optional[str] = None,
    insight_type: Optional[str] = None,
    min_confidence: str = "medium",
    limit: int = 6,
) -> list[dict]:
    """
    Retrieve expert insights relevant to a query.

    Used by the strategy agent to inject curated expert memory as context.
    """
    confidence_order = {"high": 3, "medium": 2, "low": 1}
    min_conf_val = confidence_order.get(min_confidence, 2)

    conditions = [
        "active = TRUE",
        "CASE confidence WHEN 'high' THEN 3 WHEN 'medium' THEN 2 ELSE 1 END >= %s",
    ]
    params: list = [min_conf_val]

    if len(query) > 4:
        conditions.append(
            "to_tsvector('simple', insight_text) @@ plainto_tsquery('simple', %s)"
        )
        params.append(query)
        rank_expr = (
            "ts_rank(to_tsvector('simple', insight_text), plainto_tsquery('simple', %s))"
        )
        params.append(query)
    else:
        conditions.append("insight_text ILIKE %s")
        params.append(f"%{query}%")
        rank_expr = "1.0::float"

    if province:
        conditions.append("(province ILIKE %s OR province IS NULL)")
        params.append(f"%{province}%")

    if insight_type:
        conditions.append("insight_type = %s")
        params.append(insight_type)

    where = " AND ".join(conditions)
    sql = f"""
        SELECT id, insight_text, insight_type, province, confidence,
               source_session, validated_at, {rank_expr} AS rank
        FROM staging.kp_expert_insights
        WHERE {where}
        ORDER BY rank DESC, validated_at DESC NULLS LAST
        LIMIT %s
    """
    params.append(limit)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]


def inject_expert_memory(insights: list[dict]) -> str:
    """
    Format retrieved insights as a context block for injection into the agent prompt.
    """
    if not insights:
        return ""

    lines = ["## Expert Memory (accumulated validated insights)\n"]
    for ins in insights:
        province_tag = f" [{ins['province']}]" if ins.get("province") else ""
        conf_tag = f" ({ins['confidence']} confidence)" if ins.get("confidence") else ""
        lines.append(
            f"• [{ins['insight_type']}{province_tag}{conf_tag}] "
            f"{ins['insight_text']}"
        )
    return "\n".join(lines)


def get_memory_stats() -> dict:
    """Return summary statistics about the expert memory store."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE confidence = 'high') AS high_conf,
                    COUNT(*) FILTER (WHERE confidence = 'medium') AS med_conf,
                    COUNT(DISTINCT insight_type) AS type_count,
                    COUNT(DISTINCT province) AS province_count,
                    MAX(validated_at) AS last_updated
                FROM staging.kp_expert_insights
                WHERE active = TRUE
                """
            )
            row = cur.fetchone()
            cols = [d[0] for d in cur.description]
            return dict(zip(cols, row)) if row else {}
