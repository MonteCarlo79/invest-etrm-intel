"""
Knowledge Pool Intelligence — Phase 5: Advanced Retrieval
==========================================================
Implements three retrieval upgrades over the base FTS pipeline:

1. HyDE (Hypothetical Document Embeddings without vectors):
   Generate a hypothetical expert answer to the query first, then use the
   keywords from that answer to drive FTS. Much better intent matching than
   raw query keywords — especially for Chinese policy questions.

2. Hierarchical / Parent-Child Retrieval:
   Retrieve small chunks for precision, then expand to neighbouring chunks
   for context. Prevents the agent from getting a fragment without context.

3. Cross-Encoder Re-ranking:
   Take top-K candidates from FTS (or synthesis search), send to Claude
   to score and re-rank by true relevance. Filters noise and surfaces
   the most relevant 5 chunks.

The unified entry point is `retrieve_for_agent()` which:
  - Runs synthesis-aware retrieval (summaries + Q&A pairs + raw chunks)
  - Applies HyDE query expansion
  - Re-ranks with cross-encoder
  - Returns a formatted context block ready for agent injection

Usage:
    from services.knowledge_pool.advanced_retrieval import retrieve_for_agent

    context = retrieve_for_agent(
        query="What ancillary market rules apply to BESS in Shanxi?",
        api_key=os.environ["ANTHROPIC_API_KEY"],
        app="strategist",
    )
    # Inject `context` into system prompt
"""
from __future__ import annotations

import logging
from typing import Optional

import anthropic
from shared.anthropic_client import make_client as _make_anthropic_client

from .db import get_conn
from .knowledge_docs import search_reference_docs, vector_search_reference_docs
from .synthesis import search_summaries, search_qa_pairs

logger = logging.getLogger(__name__)

_RERANK_MODEL = "claude-sonnet-4-6"   # haiku blocked on Bedrock; restore when use-case form approved
_HYDE_MODEL = "claude-sonnet-4-6"


# ── Phase 5a: HyDE query expansion ────────────────────────────────────────────

_HYDE_SYSTEM = """\
You are an expert analyst in China's electricity markets and BESS operations.

Given a question, write a concise hypothetical expert answer (2-3 sentences)
as if you have perfect knowledge. Focus on domain-specific terminology,
province names, policy names, and market mechanisms.

Then extract 8-12 key search terms from your hypothetical answer.

Respond ONLY with valid JSON:
{
  "hypothetical_answer": "...",
  "search_terms": ["...", "...", ...]
}
"""


def hyde_expand(query: str, api_key: str) -> tuple[str, list[str]]:
    """
    Generate a hypothetical answer and extract search terms.

    Returns (hypothetical_answer, search_terms).
    Falls back to (query, [query]) on error.
    """
    client = _make_anthropic_client(api_key)
    try:
        resp = client.messages.create(
            model=_HYDE_MODEL,
            max_tokens=400,
            system=_HYDE_SYSTEM,
            messages=[{"role": "user", "content": query}],
        )
        import json
        raw = resp.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("```", 2)[1]
            if raw.startswith("json"):
                raw = raw[4:]
        result = json.loads(raw)
        hyp_answer = result.get("hypothetical_answer", query)
        terms = result.get("search_terms", [query])
        return hyp_answer, terms
    except Exception as exc:
        logger.debug("HyDE expansion failed: %s", exc)
        return query, [query]


# ── Phase 5b: Multi-source retrieval ──────────────────────────────────────────

def _rrf_fuse(ranked_lists: list[list[dict]], k: int = 60) -> list[dict]:
    """
    Reciprocal Rank Fusion across multiple ranked result lists.
    Each entry is keyed by (doc_id, text[:80]) to merge duplicates.
    Returns a unified ranked list.
    """
    rrf_scores: dict[tuple, float] = {}
    entries: dict[tuple, dict] = {}

    for ranked_list in ranked_lists:
        for rank, hit in enumerate(ranked_list):
            key = (hit["doc_id"], hit["text"][:80])
            rrf_scores[key] = rrf_scores.get(key, 0.0) + 1.0 / (k + rank + 1)
            if key not in entries:
                entries[key] = hit

    merged = sorted(entries.keys(), key=lambda k: rrf_scores[k], reverse=True)
    result = []
    for key in merged:
        entry = entries[key].copy()
        entry["rank"] = rrf_scores[key]
        result.append(entry)
    return result


def _retrieve_with_hyde(
    query: str,
    search_terms: list[str],
    app: Optional[str],
    n_summaries: int = 4,
    n_qa: int = 6,
    n_chunks: int = 10,
) -> list[dict]:
    """
    Retrieve candidates from summaries, Q&A pairs, FTS chunks, and vector chunks.
    Fuses FTS + vector results with RRF for better recall.
    """
    composite_query = " ".join(search_terms[:6]) if search_terms else query

    # ── 1. Summaries (FTS, highest density) ──────────────────────────────────
    summaries: list[dict] = []
    seen_summary_ids: set = set()
    for hit in search_summaries(composite_query, app=app, limit=n_summaries):
        key = ("summary", hit["doc_id"])
        if key not in seen_summary_ids:
            seen_summary_ids.add(key)
            summaries.append({
                "source": "summary", "doc_id": hit["doc_id"],
                "file_name": hit["file_name"], "category": hit["category"],
                "text": hit["summary_text"], "rank": float(hit.get("rank", 0)),
            })
    for hit in search_summaries(query, app=app, limit=n_summaries):
        key = ("summary", hit["doc_id"])
        if key not in seen_summary_ids:
            seen_summary_ids.add(key)
            summaries.append({
                "source": "summary", "doc_id": hit["doc_id"],
                "file_name": hit["file_name"], "category": hit["category"],
                "text": hit["summary_text"], "rank": float(hit.get("rank", 0)),
            })

    # ── 2. Q&A pairs (FTS, intent-matched) ───────────────────────────────────
    qa_results: list[dict] = []
    seen_qa_ids: set = set()
    for hit in search_qa_pairs(composite_query, app=app, limit=n_qa):
        key = ("qa", hit["doc_id"], hit["question"][:50])
        if key not in seen_qa_ids:
            seen_qa_ids.add(key)
            qa_results.append({
                "source": "qa_pair", "doc_id": hit["doc_id"],
                "file_name": hit["file_name"], "category": hit["category"],
                "text": f"Q: {hit['question']}\nA: {hit['answer']}",
                "rank": float(hit.get("rank", 0)) * 1.2,
            })
    for hit in search_qa_pairs(query, app=app, limit=n_qa):
        key = ("qa", hit["doc_id"], hit["question"][:50])
        if key not in seen_qa_ids:
            seen_qa_ids.add(key)
            qa_results.append({
                "source": "qa_pair", "doc_id": hit["doc_id"],
                "file_name": hit["file_name"], "category": hit["category"],
                "text": f"Q: {hit['question']}\nA: {hit['answer']}",
                "rank": float(hit.get("rank", 0)) * 1.2,
            })

    # ── 3. FTS chunks ─────────────────────────────────────────────────────────
    fts_chunks: list[dict] = []
    seen_fts: set = set()
    for q in (composite_query, query):
        for hit in search_reference_docs(q, app=app, limit=n_chunks):
            key = (hit["doc_id"], hit.get("page_no"))
            if key not in seen_fts:
                seen_fts.add(key)
                fts_chunks.append({
                    "source": "chunk", "doc_id": hit["doc_id"],
                    "file_name": hit["file_name"], "category": hit["category"],
                    "text": hit["chunk_text"], "rank": float(hit.get("rank", 0)),
                })

    # ── 4. Vector chunks (semantic similarity) ────────────────────────────────
    vec_chunks: list[dict] = []
    try:
        for hit in vector_search_reference_docs(query, app=app, limit=n_chunks):
            vec_chunks.append({
                "source": "chunk", "doc_id": hit["doc_id"],
                "file_name": hit["file_name"], "category": hit["category"],
                "text": hit["chunk_text"], "rank": float(hit.get("rank", 0)),
            })
        # Also search with HyDE composite query
        for hit in vector_search_reference_docs(composite_query, app=app, limit=n_chunks):
            if not any(h["text"][:80] == hit["chunk_text"][:80] for h in vec_chunks):
                vec_chunks.append({
                    "source": "chunk", "doc_id": hit["doc_id"],
                    "file_name": hit["file_name"], "category": hit["category"],
                    "text": hit["chunk_text"], "rank": float(hit.get("rank", 0)),
                })
    except Exception as exc:
        logger.debug("Vector search skipped: %s", exc)

    # ── 5. RRF fusion of FTS + vector chunks ──────────────────────────────────
    fused_chunks = _rrf_fuse([fts_chunks, vec_chunks]) if vec_chunks else fts_chunks

    # ── Combine all sources, summaries first ──────────────────────────────────
    candidates = summaries + qa_results + fused_chunks
    # Deduplicate by text fingerprint
    seen_final: set = set()
    deduped = []
    for c in candidates:
        fp = (c["doc_id"], c["text"][:80])
        if fp not in seen_final:
            seen_final.add(fp)
            deduped.append(c)

    deduped.sort(key=lambda x: x["rank"], reverse=True)
    return deduped[:24]


# ── Phase 5c: Cross-encoder re-ranking ────────────────────────────────────────

_RERANK_SYSTEM = """\
You are re-ranking retrieved document passages for relevance to a user query.

For each passage, assign a relevance score 0-10:
  10 = directly answers the query with specific facts/rules
   7 = highly relevant, provides important context
   4 = tangentially related
   1 = barely relevant
   0 = irrelevant

Consider: specificity of answer, factual content, directness, recency.
Summaries and Q&A pairs score higher than raw text fragments if content is equivalent.

Respond ONLY with valid JSON:
{
  "scores": [
    {"index": 0, "score": 8, "reason": "..."},
    ...
  ]
}
"""


def rerank_candidates(
    query: str,
    candidates: list[dict],
    api_key: str,
    top_k: int = 6,
) -> list[dict]:
    """
    Re-rank candidates using Claude as a cross-encoder.
    Returns top_k candidates sorted by re-rank score.
    Falls back to original ranking on error.
    """
    if not candidates:
        return []
    if len(candidates) <= top_k:
        return candidates

    # Build the passages block for Claude
    passages_text = ""
    for i, c in enumerate(candidates):
        text_preview = c["text"][:500]
        passages_text += (
            f"\n[{i}] Source: {c['file_name']} ({c['source']})\n"
            f"{text_preview}\n"
        )

    import json
    client = _make_anthropic_client(api_key)
    try:
        resp = client.messages.create(
            model=_RERANK_MODEL,
            max_tokens=800,
            system=_RERANK_SYSTEM,
            messages=[{
                "role": "user",
                "content": (
                    f"Query: {query}\n\n"
                    f"Passages to re-rank:\n{passages_text}"
                ),
            }],
        )
        raw = resp.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("```", 2)[1]
            if raw.startswith("json"):
                raw = raw[4:]
        result = json.loads(raw)
        scores = {s["index"]: s["score"] for s in result.get("scores", [])}

        # Apply scores
        for i, c in enumerate(candidates):
            c["rerank_score"] = scores.get(i, 0)

        ranked = sorted(candidates, key=lambda x: x.get("rerank_score", 0), reverse=True)
        return ranked[:top_k]

    except Exception as exc:
        logger.debug("Re-ranking failed, using original order: %s", exc)
        return candidates[:top_k]


# ── Unified retrieval entry point ─────────────────────────────────────────────

def retrieve_for_agent(
    query: str,
    api_key: str,
    app: Optional[str] = None,
    use_hyde: bool = True,
    use_rerank: bool = True,
    top_k: int = 6,
    include_policy_timeline: bool = True,
) -> str:
    """
    Full retrieval pipeline for the strategy agent.

    Runs:
      1. HyDE query expansion (optional)
      2. Multi-source retrieval (summaries + Q&A + raw chunks)
      3. Cross-encoder re-ranking (optional)
      4. Policy timeline injection (for policy/regulatory queries)

    Returns a formatted context string ready for injection into the agent prompt.
    """
    # Step 1: HyDE expansion
    hyp_answer = ""
    search_terms = [query]
    if use_hyde:
        hyp_answer, search_terms = hyde_expand(query, api_key)

    # Step 2: Multi-source retrieval
    candidates = _retrieve_with_hyde(query, search_terms, app)

    # Step 3: Re-rank
    if use_rerank and candidates:
        top_results = rerank_candidates(query, candidates, api_key, top_k=top_k)
    else:
        top_results = candidates[:top_k]

    if not top_results:
        return "No relevant knowledge found."

    # Step 4: Format context block
    sections = []

    if hyp_answer:
        sections.append(
            f"### Query Context (HyDE expansion)\n{hyp_answer}\n"
        )

    sections.append("### Retrieved Knowledge\n")
    for i, hit in enumerate(top_results, 1):
        source_label = {
            "summary": "Expert Summary",
            "qa_pair": "Q&A Pair",
            "chunk": "Document Excerpt",
        }.get(hit["source"], hit["source"])
        score = hit.get("rerank_score", hit.get("rank", "—"))
        sections.append(
            f"**[{i}] {hit['file_name']}** ({source_label}, relevance: {score})\n"
            f"{hit['text']}\n"
        )

    # Step 5: Optionally inject policy timeline for regulatory queries
    if include_policy_timeline:
        _POLICY_TRIGGERS = [
            "rule", "regulation", "policy", "规则", "政策", "办法", "通知",
            "ancillary", "settlement", "market design", "reform",
        ]
        q_lower = query.lower()
        if any(t in q_lower for t in _POLICY_TRIGGERS):
            try:
                from .knowledge_graph import query_policy_timeline
                import datetime
                today = datetime.date.today().isoformat()
                policies = query_policy_timeline(
                    effective_on=today,
                    search_text=query[:50],
                    limit=3,
                )
                if policies:
                    sections.append("\n### Active Policy Context\n")
                    for p in policies:
                        sections.append(
                            f"• **{p['policy_name']}** "
                            f"({p['province']}, effective {p['effective_date']})\n"
                            f"  {p['bess_relevance']}\n"
                        )
            except Exception as exc:
                logger.debug("Policy timeline injection failed: %s", exc)

    return "\n".join(sections)


# ── Parent-child chunk expansion ──────────────────────────────────────────────

def expand_chunk_context(doc_id: int, chunk_index: int, window: int = 2) -> str:
    """
    Given a specific chunk, return it plus `window` neighbouring chunks
    on each side for fuller context.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT chunk_text
                FROM staging.spot_knowledge_chunks
                WHERE doc_id = %s
                  AND chunk_index BETWEEN %s AND %s
                ORDER BY chunk_index
                """,
                (doc_id, max(0, chunk_index - window), chunk_index + window),
            )
            chunks = [r[0] for r in cur.fetchall()]
    return "\n".join(chunks)
