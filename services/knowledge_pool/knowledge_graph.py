"""
Knowledge Pool Intelligence — Phase 2+3: Knowledge Graph & Policy Timeline
===========================================================================
Builds a relational knowledge graph from synthesized entity extractions and
a temporal policy timeline from policy documents.

Phase 2 — Knowledge Graph:
    staging.kp_entities          — canonical named entities
    staging.kp_entity_relations  — typed relationships between entities

Phase 3 — Policy Timeline:
    staging.kp_policy_timeline   — policies with effective/superseded dates

These structures enable the strategy agent to reason about relationships and
temporal regulatory state ("what rules applied on date X for province Y?").

Usage:
    from services.knowledge_pool.knowledge_graph import (
        build_knowledge_graph,
        build_policy_timeline,
        query_policy_timeline,
        search_entities,
    )
"""
from __future__ import annotations

import json
import logging
import time
from typing import Optional

import anthropic

from .db import get_conn

logger = logging.getLogger(__name__)

_GRAPH_MODEL = "claude-sonnet-4-6"

# ── Policy timeline extraction prompt ─────────────────────────────────────────

_TIMELINE_SYSTEM = """\
You are an expert in Chinese electricity market regulation.

Given a document summary and its metadata, extract policy timeline information.
Respond ONLY with valid JSON in this exact structure:

{
  "policy_name": "...",           // official name of the policy/rule
  "policy_name_zh": "...",        // Chinese name
  "policy_type": "...",           // market_rule | regulation | notice | standard | guideline | other
  "province": "...",              // province name in English, or "National" if national-level
  "issuing_body": "...",          // e.g. "NREC", "NEA", "Shanxi Energy Bureau"
  "issuing_body_zh": "...",
  "doc_ref_no": "...",            // official document reference number if present
  "effective_date": "YYYY-MM-DD", // null if not found
  "superseded_date": "YYYY-MM-DD",// null if still active
  "supersedes": "...",            // name of policy this replaces, if any
  "key_changes": [                // bullet points of key changes/rules
    "...",
    "..."
  ],
  "bess_relevance": "..."         // 1-2 sentences on implications for BESS operators
}

If the document is NOT a policy/regulation/rule, return {"not_policy": true}.
"""

# ── Entity deduplication prompt ───────────────────────────────────────────────

_DEDUP_SYSTEM = """\
You are resolving entity mentions to canonical names.

Given a list of entity strings (province names, policy names, etc.), identify
which refer to the same real-world entity and group them. Return canonical names.

Respond ONLY with valid JSON:
{
  "canonical_groups": [
    {
      "canonical": "...",      // English canonical name
      "canonical_zh": "...",   // Chinese name
      "aliases": ["...", ...]  // all variants that map here
    }
  ]
}
"""


# ── Phase 2: Knowledge Graph ──────────────────────────────────────────────────

def build_knowledge_graph(api_key: str, verbose: bool = True) -> dict:
    """
    Build the knowledge graph from synthesized entity extractions.

    Reads all kp_doc_summaries.key_entities, deduplicates entity names,
    upserts into kp_entities, and records doc→entity links in kp_entity_relations.

    Returns counts: {'entities': int, 'relations': int}
    """
    # Gather all entity extractions
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT s.doc_id, d.category, s.key_entities
                FROM staging.kp_doc_summaries s
                JOIN staging.spot_knowledge_docs d ON d.id = s.doc_id
                WHERE s.key_entities IS NOT NULL
                  AND s.key_entities != 'null'::jsonb
                """
            )
            rows = cur.fetchall()

    if not rows:
        if verbose:
            print("No synthesized entities found — run synthesis pipeline first.")
        return {"entities": 0, "relations": 0}

    if verbose:
        print(f"Building knowledge graph from {len(rows)} documents...")

    # Accumulate raw entity mentions by type
    raw_entities: dict[str, set[str]] = {
        "province": set(),
        "policy": set(),
        "instrument": set(),
        "asset": set(),
        "company": set(),
    }
    doc_entity_map: list[tuple[int, str, str]] = []  # (doc_id, entity_type, entity_name)

    for doc_id, category, entities_json in rows:
        if not entities_json:
            continue
        try:
            entities = entities_json if isinstance(entities_json, dict) else json.loads(entities_json)
        except Exception:
            continue

        for province in entities.get("provinces", []):
            if province and len(province) > 1:
                raw_entities["province"].add(province)
                doc_entity_map.append((doc_id, "province", province))

        for policy in entities.get("policies", []):
            name = policy.get("name", "") if isinstance(policy, dict) else str(policy)
            if name and len(name) > 2:
                raw_entities["policy"].add(name)
                doc_entity_map.append((doc_id, "policy", name))

        for instr in entities.get("instruments", []):
            if instr and len(instr) > 1:
                raw_entities["instrument"].add(instr)
                doc_entity_map.append((doc_id, "instrument", instr))

        for asset in entities.get("assets", []):
            if asset and len(asset) > 2:
                raw_entities["asset"].add(asset)
                doc_entity_map.append((doc_id, "asset", asset))

        for company in entities.get("companies", []):
            if company and len(company) > 2:
                raw_entities["company"].add(company)
                doc_entity_map.append((doc_id, "company", company))

    # Upsert canonical entities and collect name→id mapping
    entity_name_to_id: dict[tuple[str, str], int] = {}  # (entity_type, name) → id

    with get_conn() as conn:
        with conn.cursor() as cur:
            for entity_type, names in raw_entities.items():
                for name in names:
                    cur.execute(
                        """
                        INSERT INTO staging.kp_entities (entity_type, canonical_name)
                        VALUES (%s, %s)
                        ON CONFLICT (entity_type, canonical_name) DO UPDATE
                            SET doc_count = staging.kp_entities.doc_count + 1
                        RETURNING id
                        """,
                        (entity_type, name),
                    )
                    entity_id = cur.fetchone()[0]
                    entity_name_to_id[(entity_type, name)] = entity_id

            # Insert doc→entity relations (document_mentions type)
            relation_count = 0
            for doc_id, entity_type, entity_name in doc_entity_map:
                entity_id = entity_name_to_id.get((entity_type, entity_name))
                if entity_id is None:
                    continue
                cur.execute(
                    """
                    INSERT INTO staging.kp_entity_relations
                        (entity_from_id, relation_type, entity_to_id, doc_id)
                    VALUES (
                        (SELECT id FROM staging.kp_entities
                         WHERE entity_type='document' AND canonical_name=%s::text
                         LIMIT 1),
                        'mentioned_in',
                        %s, %s
                    )
                    ON CONFLICT DO NOTHING
                    """,
                    (str(doc_id), entity_id, doc_id),
                )
                relation_count += cur.rowcount

        conn.commit()

    entity_count = len(entity_name_to_id)
    if verbose:
        print(f"Knowledge graph: {entity_count} entities, {relation_count} relations")

    return {"entities": entity_count, "relations": relation_count}


def search_entities(
    query: str,
    entity_type: Optional[str] = None,
    limit: int = 10,
) -> list[dict]:
    """Search for entities by name."""
    conditions = ["canonical_name ILIKE %s"]
    params: list = [f"%{query}%"]

    if entity_type:
        conditions.append("entity_type = %s")
        params.append(entity_type)

    where = " AND ".join(conditions)
    sql = f"""
        SELECT id, entity_type, canonical_name, aliases, doc_count, metadata
        FROM staging.kp_entities
        WHERE {where}
        ORDER BY doc_count DESC
        LIMIT %s
    """
    params.append(limit)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]


def get_entity_documents(entity_name: str, entity_type: Optional[str] = None) -> list[dict]:
    """Return all documents that mention a given entity."""
    conditions = ["e.canonical_name ILIKE %s"]
    params: list = [f"%{entity_name}%"]
    if entity_type:
        conditions.append("e.entity_type = %s")
        params.append(entity_type)

    where = " AND ".join(conditions)
    sql = f"""
        SELECT DISTINCT d.id AS doc_id, d.file_name, d.category, d.app, d.created_at,
               e.entity_type, e.canonical_name
        FROM staging.kp_entities e
        JOIN staging.kp_entity_relations r ON r.entity_to_id = e.id
        JOIN staging.spot_knowledge_docs d ON d.id = r.doc_id
        WHERE {where}
          AND d.active = TRUE
        ORDER BY d.created_at DESC
        LIMIT 50
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]


# ── Phase 3: Policy Timeline ──────────────────────────────────────────────────

def _extract_policy_timeline(
    doc_id: int,
    summary_text: str,
    filename: str,
    api_key: str,
) -> dict | None:
    """Extract policy timeline data for one document."""
    client = anthropic.Anthropic(api_key=api_key)
    try:
        resp = client.messages.create(
            model=_GRAPH_MODEL,
            max_tokens=800,
            system=_TIMELINE_SYSTEM,
            messages=[{
                "role": "user",
                "content": (
                    f"Filename: {filename}\n\n"
                    f"Document summary:\n{summary_text[:3000]}"
                ),
            }],
        )
        raw = resp.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("```", 2)[1]
            if raw.startswith("json"):
                raw = raw[4:]
        result = json.loads(raw)
        if result.get("not_policy"):
            return None
        return result
    except Exception as exc:
        logger.warning("doc_id=%d policy extraction error: %s", doc_id, exc)
        return None


def _store_policy(doc_id: int, data: dict) -> None:
    """Persist extracted policy timeline record."""
    from datetime import date

    def _parse_date(s: str | None) -> date | None:
        if not s:
            return None
        try:
            return date.fromisoformat(s[:10])
        except ValueError:
            return None

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO staging.kp_policy_timeline (
                    doc_id, policy_name, policy_name_zh, policy_type,
                    province, issuing_body, issuing_body_zh, doc_ref_no,
                    effective_date, superseded_date, supersedes,
                    key_changes, bess_relevance
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (doc_id) DO UPDATE SET
                    policy_name      = EXCLUDED.policy_name,
                    policy_type      = EXCLUDED.policy_type,
                    province         = EXCLUDED.province,
                    effective_date   = EXCLUDED.effective_date,
                    superseded_date  = EXCLUDED.superseded_date,
                    key_changes      = EXCLUDED.key_changes,
                    bess_relevance   = EXCLUDED.bess_relevance,
                    updated_at       = NOW()
                """,
                (
                    doc_id,
                    data.get("policy_name", ""),
                    data.get("policy_name_zh", ""),
                    data.get("policy_type", "other"),
                    data.get("province"),
                    data.get("issuing_body"),
                    data.get("issuing_body_zh"),
                    data.get("doc_ref_no"),
                    _parse_date(data.get("effective_date")),
                    _parse_date(data.get("superseded_date")),
                    data.get("supersedes"),
                    data.get("key_changes", []),
                    data.get("bess_relevance"),
                ),
            )
        conn.commit()


def build_policy_timeline(
    api_key: str,
    workers: int = 3,
    verbose: bool = True,
) -> dict:
    """
    Build the policy timeline from synthesized policy/market_rule documents.

    Processes only docs with category in (policy_doc, market_rules) that
    have been synthesized but not yet added to the timeline.

    Returns {'processed': int, 'policies_found': int, 'errors': int}
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT s.doc_id, d.file_name, s.summary_text
                FROM staging.kp_doc_summaries s
                JOIN staging.spot_knowledge_docs d ON d.id = s.doc_id
                LEFT JOIN staging.kp_policy_timeline pt ON pt.doc_id = s.doc_id
                WHERE d.category IN ('policy_doc', 'market_rules')
                  AND d.active = TRUE
                  AND pt.doc_id IS NULL
                ORDER BY s.doc_id
                """
            )
            rows = cur.fetchall()

    if not rows:
        if verbose:
            print("No unprocessed policy/market_rules docs found.")
        return {"processed": 0, "policies_found": 0, "errors": 0}

    if verbose:
        print(f"Building policy timeline from {len(rows)} documents...")

    from concurrent.futures import ThreadPoolExecutor, as_completed

    def _process(row: tuple) -> tuple[int, str]:
        doc_id, filename, summary = row
        data = _extract_policy_timeline(doc_id, summary, filename, api_key)
        if data is None:
            return doc_id, "not_policy"
        try:
            _store_policy(doc_id, data)
            return doc_id, "ok"
        except Exception as exc:
            return doc_id, f"error: {exc}"

    counts = {"processed": 0, "policies_found": 0, "errors": 0}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_process, row): row[0] for row in rows}
        for future in as_completed(futures):
            doc_id, status = future.result()
            counts["processed"] += 1
            if status == "ok":
                counts["policies_found"] += 1
            elif "error" in status:
                counts["errors"] += 1
            if verbose:
                print(f"  [{status:>12}]  doc_id={doc_id}")
            time.sleep(0.5)

    if verbose:
        print(f"Policy timeline: {counts['policies_found']} policies added")
    return counts


# ── Query helpers ──────────────────────────────────────────────────────────────

def query_policy_timeline(
    province: Optional[str] = None,
    effective_on: Optional[str] = None,   # YYYY-MM-DD — what was active on this date?
    policy_type: Optional[str] = None,
    search_text: Optional[str] = None,
    include_superseded: bool = False,
    limit: int = 20,
) -> list[dict]:
    """
    Query the policy timeline with temporal filtering.

    effective_on: returns policies that were active on the given date
                  (effective_date <= date AND (superseded_date IS NULL OR superseded_date > date))
    """
    conditions = ["TRUE"]
    params: list = []

    if province:
        conditions.append("(province ILIKE %s OR province = 'National')")
        params.append(f"%{province}%")

    if effective_on:
        conditions.append(
            "(effective_date IS NULL OR effective_date <= %s)"
        )
        params.append(effective_on)
        if not include_superseded:
            conditions.append(
                "(superseded_date IS NULL OR superseded_date > %s)"
            )
            params.append(effective_on)
    elif not include_superseded:
        conditions.append("superseded_date IS NULL")

    if policy_type:
        conditions.append("policy_type = %s")
        params.append(policy_type)

    if search_text:
        conditions.append(
            "(policy_name ILIKE %s OR bess_relevance ILIKE %s)"
        )
        params.extend([f"%{search_text}%", f"%{search_text}%"])

    where = " AND ".join(conditions)
    sql = f"""
        SELECT pt.doc_id, pt.policy_name, pt.policy_name_zh, pt.policy_type,
               pt.province, pt.issuing_body, pt.doc_ref_no,
               pt.effective_date, pt.superseded_date, pt.supersedes,
               pt.key_changes, pt.bess_relevance,
               d.file_name
        FROM staging.kp_policy_timeline pt
        JOIN staging.spot_knowledge_docs d ON d.id = pt.doc_id
        WHERE {where}
        ORDER BY pt.effective_date DESC NULLS LAST
        LIMIT %s
    """
    params.append(limit)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]


def get_policy_context_for_date(
    province: str,
    as_of_date: str,
) -> str:
    """
    Return a formatted string of active policies for a province on a given date.
    Used to inject regulatory context into the strategy agent.
    """
    policies = query_policy_timeline(
        province=province,
        effective_on=as_of_date,
        include_superseded=False,
        limit=10,
    )
    if not policies:
        return f"No policy timeline data found for {province} as of {as_of_date}."

    lines = [f"Active policies for {province} as of {as_of_date}:\n"]
    for p in policies:
        changes = "\n  - ".join(p.get("key_changes") or [])
        lines.append(
            f"• {p['policy_name']} ({p['policy_type']})\n"
            f"  Effective: {p['effective_date']}  |  Issuer: {p['issuing_body']}\n"
            f"  BESS relevance: {p['bess_relevance']}\n"
            + (f"  Key changes:\n  - {changes}\n" if changes else "")
        )
    return "\n".join(lines)
