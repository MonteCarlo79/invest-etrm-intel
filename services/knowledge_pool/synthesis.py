"""
Knowledge Pool Intelligence — Phase 1: Document Synthesis
==========================================================
Batch-processes ingested documents through Claude to produce three artifacts
per document, stored in dedicated tables:

  staging.kp_doc_summaries  — expert-level synthesis (300-500 words)
  staging.kp_qa_pairs       — 5-10 synthetic Q&A pairs per document
  staging.kp_doc_entities   — structured entity extraction

These artifacts replace raw chunk injection with dense, semantically rich
content. Q&A pairs are especially valuable: queries retrieve matching
Q&As rather than arbitrary raw chunks, so the injected context is already
shaped like an expert answer.

Usage (from runner script):
    from services.knowledge_pool.synthesis import SynthesisPipeline
    pipeline = SynthesisPipeline(api_key="...", workers=3)
    pipeline.run(app_filter="shared")   # or "trader", or None for all

Tables created by apply_kp_intelligence_ddl.py.
"""
from __future__ import annotations

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import anthropic

from .db import get_conn

logger = logging.getLogger(__name__)

# ── synthesis prompt ──────────────────────────────────────────────────────────

_SYNTHESIS_SYSTEM = """\
You are an expert analyst in China's electricity markets, energy storage (BESS),
power market regulation, and dispatch economics.

You will be given excerpts from a Chinese energy market document. Your task is to
produce three structured outputs:

1. EXPERT SUMMARY (300-500 words in English):
   A dense, authoritative synthesis written as if by a senior market analyst.
   Include: document type, scope (national/provincial), key market rules or
   findings, quantitative data, effective dates, and practical implications for
   BESS operators and traders. Do NOT pad with generic statements.

2. ENTITY EXTRACTION (JSON):
   Extract all named entities in this schema:
   {
     "provinces": ["..."],              // province names in Pinyin or English
     "policies": [{"name": "...", "ref_no": "...", "date": "..."}],
     "instruments": ["..."],            // market products: DAM, RTM, FM, AGC, etc.
     "assets": ["..."],                 // BESS sites, power plants, substations
     "companies": ["..."],              // utilities, grid operators, developers
     "dates": [{"event": "...", "date": "..."}],
     "prices": [{"metric": "...", "value": "...", "unit": "..."}]
   }

3. SYNTHETIC Q&A PAIRS (exactly 8 pairs):
   Generate 8 questions a domain expert would ask about this document, with
   precise answers drawn from the document content. Cover a mix of:
   - Factual lookups (dates, prices, thresholds)
   - Rule/procedure questions (how does X work?)
   - Comparative questions (what changed vs. prior regime?)
   - Implication questions (what does this mean for BESS?)
   Format: [{"question": "...", "answer": "..."}, ...]
   Answers must cite specific content from the document.

Respond ONLY with valid JSON in this exact structure:
{
  "summary": "...",
  "entities": { ... },
  "qa_pairs": [ ... ]
}
"""

_MAX_INPUT_CHARS = 6000   # chars of chunk text fed to Claude per doc
_SYNTHESIS_MODEL = "claude-sonnet-4-6"
_SYNTHESIS_MAX_TOKENS = 4096


def _gather_doc_text(doc_id: int) -> tuple[str, str, str]:
    """Return (filename, category, concatenated_chunk_text) for a doc."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT file_name, category FROM staging.spot_knowledge_docs WHERE id = %s",
                (doc_id,),
            )
            row = cur.fetchone()
            if not row:
                return "", "", ""
            filename, category = row

            cur.execute(
                """
                SELECT chunk_text FROM staging.spot_knowledge_chunks
                WHERE doc_id = %s ORDER BY chunk_index LIMIT 30
                """,
                (doc_id,),
            )
            chunks = [r[0] for r in cur.fetchall()]

    # Truncate to _MAX_INPUT_CHARS
    text = "\n\n".join(chunks)
    if len(text) > _MAX_INPUT_CHARS:
        text = text[:_MAX_INPUT_CHARS] + "\n\n[... document truncated ...]"
    return filename, category, text


_SYNTHESIS_TOOL = {
    "name": "store_synthesis",
    "description": "Store the synthesized document analysis",
    "input_schema": {
        "type": "object",
        "properties": {
            "summary": {
                "type": "string",
                "description": "300-500 word expert summary in English",
            },
            "entities": {
                "type": "object",
                "description": "Extracted named entities",
                "properties": {
                    "provinces": {"type": "array", "items": {"type": "string"}},
                    "policies": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string"},
                                "ref_no": {"type": "string"},
                                "date": {"type": "string"},
                            },
                        },
                    },
                    "instruments": {"type": "array", "items": {"type": "string"}},
                    "assets": {"type": "array", "items": {"type": "string"}},
                    "companies": {"type": "array", "items": {"type": "string"}},
                    "prices": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "metric": {"type": "string"},
                                "value": {"type": "string"},
                                "unit": {"type": "string"},
                            },
                        },
                    },
                },
            },
            "qa_pairs": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "question": {"type": "string"},
                        "answer": {"type": "string"},
                    },
                    "required": ["question", "answer"],
                },
                "description": "8 synthetic Q&A pairs covering key content",
                "minItems": 4,
                "maxItems": 8,
            },
        },
        "required": ["summary", "entities", "qa_pairs"],
    },
}


def _extract_json_text(raw: str) -> dict | None:
    """
    Robustly extract a JSON object from a model response.
    Tries: strip fences → parse → find first { } block → partial extraction.
    """
    # Strip markdown code fences
    text = raw.strip()
    if text.startswith("```"):
        parts = text.split("```", 2)
        text = parts[1] if len(parts) > 1 else text
        if text.startswith("json"):
            text = text[4:]
        text = text.strip()

    # Try direct parse
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Find first complete {...} block (handles trailing garbage)
    depth = 0
    start = text.find("{")
    if start == -1:
        return None
    for i, ch in enumerate(text[start:], start):
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(text[start:i + 1])
                except json.JSONDecodeError:
                    break

    # Partial extraction: pull out summary and qa_pairs with regex as last resort
    import re
    result: dict = {}

    summary_match = re.search(r'"summary"\s*:\s*"((?:[^"\\]|\\.)*)"', text, re.DOTALL)
    if summary_match:
        result["summary"] = summary_match.group(1).replace('\\"', '"')

    qa_matches = re.findall(
        r'"question"\s*:\s*"((?:[^"\\]|\\.)*)"\s*,\s*"answer"\s*:\s*"((?:[^"\\]|\\.)*)"',
        text,
        re.DOTALL,
    )
    if qa_matches:
        result["qa_pairs"] = [
            {"question": q.replace('\\"', '"'), "answer": a.replace('\\"', '"')}
            for q, a in qa_matches
        ]

    return result if result else None


def _synthesize_text_mode(
    doc_id: int,
    filename: str,
    category: str,
    text: str,
    api_key: str,
    retry: int = 2,
) -> dict | None:
    """
    Text-based synthesis with smart error handling:
    - 429 RateLimitError → wait 60s, retry up to `retry` times
    - 403 PermissionDeniedError → wait 30s, retry ONCE; skip on 2nd 403
      (could be transient burst limit or permanent content block — either
       way, don't burn minutes retrying indefinitely)
    - Other errors → retry up to `retry` times with 5s delay
    """
    user_msg = (
        f"Document: {filename}\nCategory: {category}\n\n"
        f"Content:\n{text}"
    )
    client = anthropic.Anthropic(api_key=api_key)
    permission_denied_count = 0

    for attempt in range(retry + 1):
        try:
            resp = client.messages.create(
                model=_SYNTHESIS_MODEL,
                max_tokens=_SYNTHESIS_MAX_TOKENS,
                system=_SYNTHESIS_SYSTEM,
                messages=[{"role": "user", "content": user_msg}],
            )
            raw = resp.content[0].text
            result = _extract_json_text(raw)
            if result and result.get("summary"):
                return result
            if attempt < retry:
                time.sleep(3)

        except anthropic.RateLimitError:
            wait = 60
            logger.warning("doc_id=%d 429 rate-limit (attempt %d), waiting %ds", doc_id, attempt, wait)
            time.sleep(wait)

        except anthropic.PermissionDeniedError:
            permission_denied_count += 1
            if permission_denied_count >= 2:
                # Two 403s in a row — skip this doc rather than blocking the pipeline
                logger.warning("doc_id=%d skipping after 2× PermissionDeniedError (burst limit or content block)", doc_id)
                return None
            wait = 30
            logger.warning("doc_id=%d 403 PermissionDenied (attempt %d), waiting %ds then retrying once", doc_id, attempt, wait)
            time.sleep(wait)

        except Exception as exc:
            logger.error("doc_id=%d error (attempt %d): %s", doc_id, attempt, exc)
            if attempt >= retry:
                return None
            time.sleep(5)

    return None


def _synthesize_doc(
    doc_id: int,
    api_key: str,
    retry: int = 3,
) -> dict | None:
    """
    Run synthesis for one document (text mode only — tool_use skipped).
    Treats 403 as a rate-limit signal and backs off before retrying.
    Returns parsed result dict or None on failure.
    """
    filename, category, text = _gather_doc_text(doc_id)
    if not text.strip():
        return None
    return _synthesize_text_mode(doc_id, filename, category, text, api_key, retry=retry)


def _store_synthesis(doc_id: int, result: dict) -> None:
    """Persist summary, entities, and Q&A pairs to DB."""
    summary = result.get("summary", "")
    entities = result.get("entities", {})
    qa_pairs = result.get("qa_pairs", [])

    with get_conn() as conn:
        with conn.cursor() as cur:
            # Upsert summary
            cur.execute(
                """
                INSERT INTO staging.kp_doc_summaries
                    (doc_id, summary_text, key_entities, synthesis_model)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (doc_id) DO UPDATE SET
                    summary_text = EXCLUDED.summary_text,
                    key_entities = EXCLUDED.key_entities,
                    synthesis_model = EXCLUDED.synthesis_model,
                    synthesized_at = NOW()
                """,
                (doc_id, summary, json.dumps(entities, ensure_ascii=False), _SYNTHESIS_MODEL),
            )

            # Insert Q&A pairs (clear old ones first for idempotency)
            cur.execute(
                "DELETE FROM staging.kp_qa_pairs WHERE doc_id = %s", (doc_id,)
            )
            if qa_pairs:
                cur.executemany(
                    """
                    INSERT INTO staging.kp_qa_pairs (doc_id, question, answer)
                    VALUES (%s, %s, %s)
                    """,
                    [
                        (doc_id, qa.get("question", ""), qa.get("answer", ""))
                        for qa in qa_pairs
                        if qa.get("question") and qa.get("answer")
                    ],
                )
        conn.commit()


def _get_unprocessed_doc_ids(
    app_filter: Optional[str] = None,
    limit: Optional[int] = None,
) -> list[int]:
    """Return doc IDs not yet in kp_doc_summaries that have at least one chunk."""
    conditions = ["d.active = TRUE"]
    params: list = []

    if app_filter:
        conditions.append("(d.app = %s OR d.app = 'shared')")
        params.append(app_filter)

    where = " AND ".join(conditions)
    lim_clause = f"LIMIT {limit}" if limit else ""

    sql = f"""
        SELECT d.id FROM staging.spot_knowledge_docs d
        LEFT JOIN staging.kp_doc_summaries s ON s.doc_id = d.id
        WHERE {where}
          AND s.doc_id IS NULL
          AND d.ingest_status = 'parsed'
          AND EXISTS (
              SELECT 1 FROM staging.spot_knowledge_chunks c
              WHERE c.doc_id = d.id
          )
        ORDER BY d.id
        {lim_clause}
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return [r[0] for r in cur.fetchall()]


def _process_one(doc_id: int, api_key: str) -> tuple[int, str]:
    """Worker: synthesize and store one doc. Returns (doc_id, status)."""
    try:
        result = _synthesize_doc(doc_id, api_key)
        if result is None:
            return doc_id, "error"
        _store_synthesis(doc_id, result)
        return doc_id, "ok"
    except Exception as exc:
        logger.error("doc_id=%d unexpected error: %s", doc_id, exc)
        return doc_id, f"error: {exc}"


class SynthesisPipeline:
    """
    Batch synthesis pipeline for the knowledge pool.

    Example:
        pipeline = SynthesisPipeline(api_key=os.environ["ANTHROPIC_API_KEY"])
        pipeline.run()
    """

    def __init__(
        self,
        api_key: str,
        workers: int = 1,
        delay_between_calls: float = 5.0,
    ) -> None:
        self.api_key = api_key
        self.workers = max(1, min(workers, 8))
        self.delay = delay_between_calls

    def run(
        self,
        app_filter: Optional[str] = None,
        limit: Optional[int] = None,
        verbose: bool = True,
    ) -> dict:
        """
        Synthesize all unprocessed documents.

        Args:
            app_filter: 'shared', 'trader', or None for all
            limit: max docs to process (useful for incremental runs)
            verbose: print progress lines

        Returns:
            {'ok': int, 'error': int, 'skipped': int}
        """
        doc_ids = _get_unprocessed_doc_ids(app_filter, limit)
        total = len(doc_ids)

        if total == 0:
            if verbose:
                print("All documents already synthesized.")
            return {"ok": 0, "error": 0, "skipped": 0}

        if verbose:
            scope = app_filter or "all"
            print(f"Synthesizing {total} documents (app={scope}, workers={self.workers})")

        counts = {"ok": 0, "error": 0}
        done = 0
        t0 = time.time()

        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            futures = {
                pool.submit(_process_one, doc_id, self.api_key): doc_id
                for doc_id in doc_ids
            }
            for future in as_completed(futures):
                doc_id, status = future.result()
                done += 1
                if status == "ok":
                    counts["ok"] += 1
                    icon = "[OK   ]"
                else:
                    counts["error"] += 1
                    icon = "[ERROR]"

                if verbose:
                    elapsed = time.time() - t0
                    rate = done / elapsed if elapsed > 0 else 0
                    remaining = (total - done) / rate if rate > 0 else 0
                    print(
                        f"{icon}  ({done:>5}/{total})  doc_id={doc_id}  "
                        f"{rate:.1f}/s  ETA {remaining/60:.0f}m  status={status}"
                    )

                # Throttle to avoid hammering the API
                time.sleep(self.delay)

        elapsed = time.time() - t0
        if verbose:
            print(
                f"\nDone in {elapsed:.0f}s — "
                f"ok: {counts['ok']}  errors: {counts['error']}"
            )
        return counts


# ── single-doc synthesis (used by knowledge_docs.py on ingest) ───────────────

def synthesize_on_ingest(doc_id: int, api_key: str) -> bool:
    """
    Synthesize a single just-ingested document. Non-blocking best-effort.
    Called from register_and_ingest() after successful chunk insertion.
    Returns True on success.
    """
    try:
        result = _synthesize_doc(doc_id, api_key)
        if result:
            _store_synthesis(doc_id, result)
            return True
    except Exception as exc:
        logger.warning("synthesize_on_ingest doc_id=%d failed: %s", doc_id, exc)
    return False


# ── search over synthesized artifacts ────────────────────────────────────────

def search_summaries(query: str, app: Optional[str] = None, limit: int = 5) -> list[dict]:
    """Full-text search over kp_doc_summaries."""
    conditions = ["d.active = TRUE"]
    params: list = []

    if len(query) > 4:
        conditions.append(
            "to_tsvector('simple', s.summary_text) @@ plainto_tsquery('simple', %s)"
        )
        params.append(query)
        rank_expr = "ts_rank(to_tsvector('simple', s.summary_text), plainto_tsquery('simple', %s))"
        params.append(query)
    else:
        conditions.append("s.summary_text ILIKE %s")
        params.append(f"%{query}%")
        rank_expr = "1.0::float"

    if app:
        conditions.append("(d.app = %s OR d.app = 'shared')")
        params.append(app)

    where = " AND ".join(conditions)
    sql = f"""
        SELECT d.id AS doc_id, d.file_name, d.category, d.app,
               s.summary_text, {rank_expr} AS rank
        FROM staging.kp_doc_summaries s
        JOIN staging.spot_knowledge_docs d ON d.id = s.doc_id
        WHERE {where}
        ORDER BY rank DESC
        LIMIT %s
    """
    params.append(limit)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]


def search_qa_pairs(query: str, app: Optional[str] = None, limit: int = 8) -> list[dict]:
    """Full-text search over kp_qa_pairs — returns matching Q&A as context."""
    conditions = ["d.active = TRUE"]
    params: list = []

    if len(query) > 4:
        conditions.append(
            "to_tsvector('simple', qa.question || ' ' || qa.answer) @@ plainto_tsquery('simple', %s)"
        )
        params.append(query)
        rank_expr = (
            "ts_rank(to_tsvector('simple', qa.question || ' ' || qa.answer), "
            "plainto_tsquery('simple', %s))"
        )
        params.append(query)
    else:
        conditions.append(
            "qa.question ILIKE %s OR qa.answer ILIKE %s"
        )
        params.extend([f"%{query}%", f"%{query}%"])
        rank_expr = "1.0::float"

    if app:
        conditions.append("(d.app = %s OR d.app = 'shared')")
        params.append(app)

    where = " AND ".join(conditions)
    sql = f"""
        SELECT d.id AS doc_id, d.file_name, d.category,
               qa.question, qa.answer,
               {rank_expr} AS rank
        FROM staging.kp_qa_pairs qa
        JOIN staging.spot_knowledge_docs d ON d.id = qa.doc_id
        WHERE {where}
        ORDER BY rank DESC
        LIMIT %s
    """
    params.append(limit)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
