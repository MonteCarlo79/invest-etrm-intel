"""GB Knowledge — Advanced Retrieval
=====================================
HyDE + cross-encoder re-ranking over intl_market.gb_knowledge_docs.

Mirrors the China knowledge_pool/advanced_retrieval.py approach but adapted
for the GB knowledge base (single-table, no synthesis pipeline).

  1. HyDE: generate a hypothetical expert answer to extract better FTS terms
  2. OR-based FTS: any word match returns a result (ranked by best fit)
  3. Re-ranking: Claude haiku cross-encoder scores and filters to top-K

Entry point: retrieve_for_gb_agent(query, api_key, ...)
"""
from __future__ import annotations

import json
import logging
from typing import Optional

import anthropic
import psycopg2
import psycopg2.extras

from .base import get_db_conn

logger = logging.getLogger(__name__)

_HAIKU_MODEL = "claude-haiku-4-5"


# ── Phase 1: HyDE query expansion ────────────────────────────────────────────

_HYDE_SYSTEM = """\
You are an expert analyst in Great Britain's electricity markets and BESS storage operations.

Given a question, write a concise hypothetical expert answer (2-3 sentences) as if you have
perfect knowledge. Focus on domain-specific terminology: settlement periods, system price,
NIV, Dynamic Containment (DC), Dynamic Moderation (DM), Dynamic Regulation (DR), balancing
mechanism (BM), EPEX day-ahead, frequency response, Elexon, National Grid ESO, OFGEM,
flexibility markets, BESS revenue stacking, ancillary service auctions (DX), capacity market.

Then extract 8-12 key search terms from your hypothetical answer.

Respond ONLY with valid JSON:
{
  "hypothetical_answer": "...",
  "search_terms": ["...", ...]
}
"""


def hyde_expand_gb(query: str, api_key: str) -> tuple[str, list[str]]:
    """Generate hypothetical answer and extract search terms for better FTS recall."""
    client = anthropic.Anthropic(api_key=api_key)
    try:
        resp = client.messages.create(
            model=_HAIKU_MODEL,
            max_tokens=400,
            system=_HYDE_SYSTEM,
            messages=[{"role": "user", "content": query}],
        )
        raw = resp.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("```", 2)[1]
            if raw.startswith("json"):
                raw = raw[4:]
        result = json.loads(raw)
        return result.get("hypothetical_answer", query), result.get("search_terms", [query])
    except Exception as exc:
        logger.debug("HyDE expansion failed: %s", exc)
        return query, [query]


# ── Phase 2: OR-based FTS retrieval ──────────────────────────────────────────

def _search_gb_or(
    conn,
    query: str,
    sources: Optional[list[str]] = None,
    limit: int = 14,
) -> list[dict]:
    """OR-logic FTS: any word match returns results, ranked by best fit."""
    src_clause = "AND source = ANY(%s)" if sources else ""
    params: list = [query, query]
    if sources:
        params.append(sources)
    params.append(limit)
    sql = (
        "SELECT source, doc_type, title, url, published_date, "
        "left(content, 1200) AS content_snippet, "
        "ts_rank(search_vector, plainto_tsquery('english', %s)) AS rank "
        "FROM intl_market.gb_knowledge_docs "
        "WHERE search_vector @@ to_tsquery('english', "
        "  regexp_replace(plainto_tsquery('english', %s)::text, ' & ', ' | ', 'g')"
        f") {src_clause} "
        "ORDER BY rank DESC LIMIT %s"
    )
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]


# ── Phase 3: Cross-encoder re-ranking ────────────────────────────────────────

_RERANK_SYSTEM = """\
You are re-ranking retrieved document passages for relevance to a user query about
GB electricity markets or BESS battery storage operations.

For each passage, assign a relevance score 0-10:
  10 = directly answers the query with specific facts or data
   7 = highly relevant, provides important context
   4 = tangentially related
   1 = barely relevant
   0 = irrelevant

Respond ONLY with valid JSON:
{"scores": [{"index": 0, "score": 8}, ...]}
"""


def rerank_gb(
    query: str,
    candidates: list[dict],
    api_key: str,
    top_k: int = 6,
) -> list[dict]:
    """Re-rank candidates using Claude haiku as a cross-encoder. Falls back to FTS order."""
    if not candidates or len(candidates) <= top_k:
        return candidates[:top_k]

    passages = ""
    for i, c in enumerate(candidates):
        passages += (
            f"\n[{i}] [{c['source']}] {c.get('title', 'Untitled')} "
            f"({c.get('published_date', '—')})\n"
            f"{c['content_snippet'][:400]}\n"
        )

    client = anthropic.Anthropic(api_key=api_key)
    try:
        resp = client.messages.create(
            model=_HAIKU_MODEL,
            max_tokens=600,
            system=_RERANK_SYSTEM,
            messages=[{
                "role": "user",
                "content": f"Query: {query}\n\nPassages to re-rank:\n{passages}",
            }],
        )
        raw = resp.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("```", 2)[1]
            if raw.startswith("json"):
                raw = raw[4:]
        scores = {s["index"]: s["score"] for s in json.loads(raw).get("scores", [])}
        for i, c in enumerate(candidates):
            c["rerank_score"] = scores.get(i, 0)
        return sorted(candidates, key=lambda x: x.get("rerank_score", 0), reverse=True)[:top_k]
    except Exception as exc:
        logger.debug("Re-ranking failed, using FTS order: %s", exc)
        return candidates[:top_k]


# ── Unified entry point ───────────────────────────────────────────────────────

def retrieve_for_gb_agent(
    query: str,
    api_key: str,
    sources: Optional[list[str]] = None,
    top_k: int = 6,
) -> str:
    """
    Full retrieval pipeline for the GB Strategist agent.

      1. HyDE: generate hypothetical expert answer, extract search terms
      2. OR-based FTS over gb_knowledge_docs with original + expanded query
      3. Cross-encoder re-ranking with Claude haiku
      4. Format context block for agent injection

    Returns a formatted context string ready for injection into the system prompt.
    """
    # Step 1: HyDE expansion
    hyp_answer, search_terms = hyde_expand_gb(query, api_key)

    # Step 2: Retrieve with both the original query and the HyDE composite
    composite = " ".join(search_terms[:6])
    conn = get_db_conn()
    try:
        conn.autocommit = True
        seen_keys: set = set()
        candidates: list[dict] = []
        for q in [query, composite]:
            for doc in _search_gb_or(conn, q, sources=sources, limit=14):
                key = doc.get("url") or f"{doc['source']}:{doc.get('title', '')}"
                if key not in seen_keys:
                    seen_keys.add(key)
                    candidates.append(doc)
    finally:
        conn.close()

    if not candidates:
        return "No relevant knowledge found in the GB knowledge base."

    # Step 3: Re-rank
    top = rerank_gb(query, candidates, api_key, top_k=top_k)

    # Step 4: Format
    sections = [f"### Query Context (HyDE expansion)\n{hyp_answer}\n",
                "### Retrieved GB Knowledge\n"]
    for i, doc in enumerate(top, 1):
        score = doc.get("rerank_score", round(float(doc.get("rank", 0)), 2))
        sections.append(
            f"**[{i}] {doc.get('title') or 'Untitled'}** "
            f"[{doc['source']} / {doc['doc_type']}, {doc.get('published_date', '—')}, "
            f"relevance: {score}]\n"
            f"{doc['content_snippet']}\n"
        )
    return "\n".join(sections)
