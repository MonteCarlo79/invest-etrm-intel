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
from shared.anthropic_client import make_client as _make_anthropic_client

from .db import get_conn
from .knowledge_docs import _has_cjk, _cjk_bigrams

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

    client = _make_anthropic_client(api_key)
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

    if _has_cjk(query):
        bigrams = _cjk_bigrams(query)
        if bigrams:
            ilike_conds = " OR ".join("insight_text ILIKE %s" for _ in bigrams)
            conditions.append(f"({ilike_conds})")
            params.extend(f"%{bg}%" for bg in bigrams)
            case_parts = " + ".join(
                "(CASE WHEN insight_text ILIKE %s THEN 1 ELSE 0 END)" for _ in bigrams
            )
            rank_expr = f"({case_parts})::float"
            params.extend(f"%{bg}%" for bg in bigrams)
        else:
            conditions.append("insight_text ILIKE %s")
            params.append(f"%{query}%")
            rank_expr = "1.0::float"
    elif len(query) > 4:
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


# ── Per-turn insight extraction (Strategist chat) ────────────────────────────

_TURN_EXTRACT_MODEL = "claude-haiku-4-5-20251001"

_TURN_EXTRACT_SYSTEM = """\
Extract durable expert insights from this China electricity market analyst conversation turn.

Extract ONLY insights that are ALL of the following:
1. Non-obvious — not trivially found by searching policy document titles
2. Validated — the user confirmed, corrected, or accepted the agent's analysis
3. Durable — likely to remain relevant for weeks or months (not today's single price)
4. Domain-specific — about China electricity markets, BESS operations, regulation,
   provincial dispatch mechanics, FM/ancillary markets, or investment economics

DO NOT extract:
- Ephemeral facts (today's price, a single day's result)
- Process instructions or UI navigation steps
- Questions without clear answers
- Generic observations obvious from public market documentation

For each insight, provide:
- insight: 1-3 precise, actionable sentences
- type: market_structure | price_driver | regulation | risk | opportunity |
        dispatch_economics | investment | operations
- province: province name in English (e.g. "Shanxi") or null if national/general
- confidence: high | medium | low

Respond ONLY with valid JSON:
{"insights": [{"insight": "...", "type": "...", "province": "...", "confidence": "..."}]}

If no durable insights are found, return {"insights": []}.
"""


def extract_spot_insights(user_msg: str, agent_reply: str, api_key: str) -> int:
    """
    Extract durable insights from a single Strategist conversation turn and store them.

    Called after each agent response in the Strategist tab.
    Returns number of insights stored (0 if none or on error — never raises).
    """
    client = _make_anthropic_client(api_key)
    try:
        resp = client.messages.create(
            model=_TURN_EXTRACT_MODEL,
            max_tokens=800,
            system=_TURN_EXTRACT_SYSTEM,
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
        logger.debug("Spot insight extraction failed: %s", exc)
        return 0

    if not insights:
        return 0

    stored = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for item in insights:
                try:
                    cur.execute(
                        """
                        INSERT INTO staging.kp_expert_insights
                            (insight_text, insight_type, province, confidence,
                             source_session, validated_at)
                        VALUES (%s, %s, %s, %s, %s, NOW())
                        """,
                        (
                            item.get("insight", "")[:1000],
                            item.get("type", "other"),
                            item.get("province") or None,
                            item.get("confidence", "medium"),
                            date.today().isoformat(),
                        ),
                    )
                    stored += 1
                except Exception as exc:
                    logger.debug("Failed to store insight: %s", exc)
        conn.commit()

    return stored


# ── KB document digest → expert insights ─────────────────────────────────────

_DIGEST_MODEL = "claude-haiku-4-5-20251001"

_DIGEST_SYSTEM = """\
Extract 3-7 durable China electricity market insights from this synthesized document.

Each insight must be ALL of the following:
- Non-obvious: not trivially found by searching policy document titles
- Specific: contains concrete facts, figures, mechanisms, or named entities
- Actionable: useful for a BESS operator, trader, or investor making decisions
- Durable: will remain relevant for weeks or months

Focus on: market mechanics, BESS revenue drivers, provincial dispatch rules, regulatory
developments, price patterns, operational strategies, ancillary service dynamics,
capacity payment rules, curtailment patterns, settlement mechanisms, policy changes.

DO NOT extract:
- Ephemeral daily price readings or single-day events
- Generic descriptions obvious from public market documentation
- Questions without clear answers

For each insight, provide:
- insight: 1-3 precise, actionable sentences
- type: market_structure | price_driver | regulation | risk | opportunity |
        dispatch_economics | investment | operations
- province: province name in English or null if national/general
- confidence: high | medium | low

Respond ONLY with valid JSON:
{"insights": [{"insight": "...", "type": "...", "province": "...", "confidence": "..."}]}

If no durable insights can be extracted, return {"insights": []}.
"""


def digest_spot_kb_docs(
    api_key: str,
    limit: int = 50,
    doc_ids: Optional[list[int]] = None,
) -> int:
    """
    Digest unprocessed synthesis docs into structured expert insights.

    Reads from staging.kp_doc_summaries + staging.kp_qa_pairs for docs whose
    doc_id is not yet referenced in staging.kp_expert_insights.source_doc_id.

    Args:
        api_key: Anthropic API key
        limit:   Max number of docs to process in this run
        doc_ids: If provided, only process these specific doc IDs (for immediate
                 post-upload digest)

    Returns:
        Total number of new insights stored.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            # Ensure source_doc_id column exists (idempotent)
            cur.execute(
                "ALTER TABLE staging.kp_expert_insights "
                "ADD COLUMN IF NOT EXISTS source_doc_id INT"
            )
        conn.commit()

        with conn.cursor() as cur:
            if doc_ids:
                cur.execute(
                    """
                    SELECT s.doc_id, s.summary_text, d.file_name, d.category
                    FROM staging.kp_doc_summaries s
                    JOIN staging.spot_knowledge_docs d ON d.id = s.doc_id
                    WHERE s.doc_id = ANY(%s)
                      AND s.doc_id NOT IN (
                          SELECT DISTINCT source_doc_id
                          FROM staging.kp_expert_insights
                          WHERE source_doc_id IS NOT NULL
                      )
                    """,
                    (doc_ids,),
                )
            else:
                cur.execute(
                    """
                    SELECT s.doc_id, s.summary_text, d.file_name, d.category
                    FROM staging.kp_doc_summaries s
                    JOIN staging.spot_knowledge_docs d ON d.id = s.doc_id
                    WHERE s.doc_id NOT IN (
                        SELECT DISTINCT source_doc_id
                        FROM staging.kp_expert_insights
                        WHERE source_doc_id IS NOT NULL
                    )
                    ORDER BY s.doc_id DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
            rows = cur.fetchall()

    if not rows:
        logger.info("[kb_digest] No undigested synthesis docs found.")
        return 0

    logger.info("[kb_digest] Digesting %d docs…", len(rows))
    client = _make_anthropic_client(api_key)
    total = 0
    today = date.today().isoformat()

    for doc_id, summary_text, file_name, category in rows:
        # Fetch Q&A pairs for this doc
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT question, answer FROM staging.kp_qa_pairs "
                    "WHERE doc_id = %s LIMIT 10",
                    (doc_id,),
                )
                qa_pairs = cur.fetchall()

        if not summary_text and not qa_pairs:
            continue

        # Build prompt
        prompt_parts = [
            f"Document: {file_name}  [category: {category}]",
            f"\nSummary:\n{summary_text or '(no summary)'}",
        ]
        if qa_pairs:
            prompt_parts.append("\nKey Q&A pairs:")
            for q, a in qa_pairs:
                prompt_parts.append(f"Q: {q}\nA: {a}")
        prompt = "\n".join(prompt_parts)[:4000]

        try:
            resp = client.messages.create(
                model=_DIGEST_MODEL,
                max_tokens=800,
                system=_DIGEST_SYSTEM,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = resp.content[0].text.strip()
            if raw.startswith("```"):
                raw = raw.split("```", 2)[1]
                if raw.startswith("json"):
                    raw = raw[4:]
            insights = json.loads(raw).get("insights", [])
        except Exception as exc:
            logger.debug("[kb_digest] doc_id=%d extraction failed: %s", doc_id, exc)
            continue

        if not insights:
            # Mark as processed (no insights) with a sentinel row to avoid re-processing
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO staging.kp_expert_insights
                            (insight_text, insight_type, confidence, source_session,
                             source_doc_id, active)
                        VALUES ('(no insights extracted)', 'other', 'low', %s, %s, FALSE)
                        """,
                        (today, doc_id),
                    )
                conn.commit()
            continue

        with get_conn() as conn:
            with conn.cursor() as cur:
                for item in insights:
                    try:
                        cur.execute(
                            """
                            INSERT INTO staging.kp_expert_insights
                                (insight_text, insight_type, province, confidence,
                                 source_session, source_doc_id, validated_at)
                            VALUES (%s, %s, %s, %s, %s, %s, NOW())
                            """,
                            (
                                item.get("insight", "")[:1000],
                                item.get("type", "other"),
                                item.get("province") or None,
                                item.get("confidence", "medium"),
                                today,
                                doc_id,
                            ),
                        )
                        total += 1
                    except Exception as exc:
                        logger.debug("[kb_digest] insert failed: %s", exc)
            conn.commit()

        logger.info("[kb_digest] doc_id=%d → %d insights", doc_id, len(insights))

    logger.info("[kb_digest] Done — %d total insights from %d docs", total, len(rows))
    return total


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
