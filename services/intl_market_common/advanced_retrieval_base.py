"""Parameterised advanced retrieval (HyDE + OR-FTS + cross-encoder rerank).

Identical logic to gb_knowledge/advanced_retrieval.py but table and
system-prompt references are parameterised by MarketConfig.

Entry point: retrieve_for_agent(query, api_key, cfg, ...)
"""
from __future__ import annotations

import json
import logging
from typing import Optional

import anthropic
import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

_HAIKU_MODEL = "claude-haiku-4-5"


def _get_conn():
    import os
    return psycopg2.connect(
        os.environ["PGURL"],
        keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=5,
    )


# ── Phase 1: HyDE ────────────────────────────────────────────────────────────

def _hyde_system(market_name: str, system_operator: str, ancillary_label: str) -> str:
    return f"""\
You are an expert analyst in {market_name}'s electricity markets and BESS storage operations
(system operator: {system_operator}, ancillary services: {ancillary_label}).

Given a question, write a concise hypothetical expert answer (2-3 sentences) as if you have
perfect knowledge. Use domain-specific terminology relevant to {market_name}.

Then extract 8-12 key search terms from your hypothetical answer.

Respond ONLY with valid JSON:
{{
  "hypothetical_answer": "...",
  "search_terms": ["...", ...]
}}
"""


def hyde_expand(query: str, api_key: str, cfg) -> tuple[str, list[str]]:
    """Generate hypothetical answer and extract search terms."""
    client = anthropic.Anthropic(api_key=api_key)
    try:
        resp = client.messages.create(
            model=_HAIKU_MODEL,
            max_tokens=400,
            system=_hyde_system(cfg.name, cfg.system_operator, cfg.ancillary_label),
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
        logger.debug("[retrieval:%s] HyDE failed: %s", cfg.code, exc)
        return query, [query]


# ── Phase 2: OR-FTS ──────────────────────────────────────────────────────────

def _search_or(conn, query: str, table_prefix: str, sources: Optional[list[str]] = None, limit: int = 14) -> list[dict]:
    src_clause = "AND source = ANY(%s)" if sources else ""
    params: list = [query, query]
    if sources:
        params.append(sources)
    params.append(limit)
    sql = (
        f"SELECT source, doc_type, title, url, published_date, "
        f"left(content, 1200) AS content_snippet, "
        f"ts_rank(search_vector, plainto_tsquery('english', %s)) AS rank "
        f"FROM intl_market.{table_prefix}knowledge_docs "
        "WHERE search_vector @@ to_tsquery('english', "
        "  regexp_replace(plainto_tsquery('english', %s)::text, ' & ', ' | ', 'g')"
        f") {src_clause} "
        "ORDER BY rank DESC LIMIT %s"
    )
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]


# ── Phase 3: Cross-encoder rerank ────────────────────────────────────────────

_RERANK_SYSTEM = """\
You are re-ranking retrieved document passages for relevance to a user query about
electricity markets or BESS battery storage operations.

For each passage, assign a relevance score 0-10:
  10 = directly answers the query with specific facts or data
   7 = highly relevant, provides important context
   4 = tangentially related
   1 = barely relevant
   0 = irrelevant

Respond ONLY with valid JSON:
{"scores": [{"index": 0, "score": 8}, ...]}
"""


def rerank(query: str, candidates: list[dict], api_key: str, top_k: int = 6) -> list[dict]:
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
            messages=[{"role": "user", "content": f"Query: {query}\n\nPassages:\n{passages}"}],
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
        logger.debug("[retrieval] rerank failed: %s", exc)
        return candidates[:top_k]


# ── Entry point ───────────────────────────────────────────────────────────────

def retrieve_for_agent(
    query: str,
    api_key: str,
    cfg,
    sources: Optional[list[str]] = None,
    top_k: int = 6,
) -> str:
    """Full retrieval pipeline: HyDE → OR-FTS → rerank → formatted context."""
    hyp_answer, search_terms = hyde_expand(query, api_key, cfg)
    composite = " ".join(search_terms[:6])

    conn = _get_conn()
    try:
        conn.autocommit = True
        seen: set = set()
        candidates: list[dict] = []
        for q in [query, composite]:
            for doc in _search_or(conn, q, cfg.table_prefix, sources=sources, limit=14):
                key = doc.get("url") or f"{doc['source']}:{doc.get('title', '')}"
                if key not in seen:
                    seen.add(key)
                    candidates.append(doc)
    finally:
        conn.close()

    if not candidates:
        return f"No relevant knowledge found in the {cfg.name} knowledge base."

    top = rerank(query, candidates, api_key, top_k=top_k)

    sections = [
        f"### Query Context (HyDE expansion)\n{hyp_answer}\n",
        f"### Retrieved {cfg.name} Knowledge\n",
    ]
    for i, doc in enumerate(top, 1):
        score = doc.get("rerank_score", round(float(doc.get("rank", 0)), 2))
        sections.append(
            f"**[{i}] {doc.get('title') or 'Untitled'}** "
            f"[{doc['source']} / {doc['doc_type']}, {doc.get('published_date', '—')}, "
            f"relevance: {score}]\n"
            f"{doc['content_snippet']}\n"
        )
    return "\n".join(sections)
