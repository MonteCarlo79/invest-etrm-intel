# KB Digest Scheduler — Design Spec
**Date:** 2026-07-11  
**Status:** Approved  
**Scope:** Automate the two-stage knowledge-pool synthesis + expert-insight pipeline inside Hermes

---

## Problem

The spot-markets knowledge base accumulates documents daily — exchange monthly reports, WeChat articles, uploaded research files, Excel data. Two processing stages must run before the Strategist agent can use them as expert memory:

1. **Synthesis** (`SynthesisPipeline.run()`) — converts raw doc text into structured summaries and Q&A pairs (`staging.kp_doc_summaries`, `staging.kp_qa_pairs`)
2. **Digest** (`digest_spot_kb_docs()`) — reads those summaries and writes durable domain insights into `staging.kp_expert_insights`, which the Strategist injects into every response

Currently both stages are triggered manually from the UI. New docs sit unprocessed until someone clicks a button, meaning the Strategist's expert memory lags real-world knowledge by days or weeks.

---

## Goal

Run synthesis + digest automatically every night so any document uploaded during the day is absorbed into expert memory by the next morning — with no manual action required.

---

## Architecture

```
Every day at 18:07 UTC (02:07 Beijing)
                │
                ▼
  Hermes APScheduler: _run_kb_digest()
                │
          ┌─────┴──────────────────┐
          │                        │
    Stage 1 (synthesis)      Stage 2 (digest)
    SynthesisPipeline        digest_spot_kb_docs()
    .run(limit=30)           (limit=30)
          │                        │
    kp_doc_summaries          kp_expert_insights
    kp_qa_pairs               (new insights added)
          │                        │
          └──────────┬─────────────┘
                     │
          log: [kb_digest] synthesized=N insights=M
```

On-demand path: `POST /hermes/knowledge/digest` calls `_run_kb_digest()` synchronously and returns `{"synthesized": N, "insights": M}`.

---

## Components

### `services/hermes/app.py` — 3 additions

**1. `_run_kb_digest(api_key, limit=30)` helper**

```python
def _run_kb_digest(api_key: str, limit: int = 30) -> dict:
    """Run synthesis + digest pipeline. Returns {"synthesized": int, "insights": int}."""
    result = {"synthesized": 0, "insights": 0}
    try:
        from services.knowledge_pool.synthesis import SynthesisPipeline
        r = SynthesisPipeline(api_key, workers=1).run(limit=limit, verbose=False)
        result["synthesized"] = r.get("ok", 0)
    except Exception as e:
        logger.error("[kb_digest] synthesis failed: %s", e)
    try:
        from services.knowledge_pool.expert_memory import digest_spot_kb_docs
        result["insights"] = digest_spot_kb_docs(api_key=api_key, limit=limit)
    except Exception as e:
        logger.error("[kb_digest] digest failed: %s", e)
    logger.info("[kb_digest] synthesized=%d insights=%d", result["synthesized"], result["insights"])
    return result
```

**2. APScheduler job** — registered in `create_app()` alongside the 8 existing jobs:

```python
scheduler.add_job(
    lambda: _run_kb_digest(os.environ.get("ANTHROPIC_API_KEY", "")),
    "cron", hour=18, minute=7,
    id="kb_digest_nightly",
    max_instances=1,
    misfire_grace_time=3600,
)
```

Timing: 18:07 UTC = 02:07 Beijing. After news screener (14:00 UTC) and before morning briefing (00:05 UTC next day).

**3. HTTP endpoint** for on-demand triggering:

```python
@app.post("/hermes/knowledge/digest")
def knowledge_digest():
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return jsonify({"error": "ANTHROPIC_API_KEY not set"}), 500
    result = _run_kb_digest(api_key)
    return jsonify(result)
```

### `services/knowledge_pool/synthesis.py` — no changes
`SynthesisPipeline(api_key).run(limit=N)` is the existing public API.

### `services/knowledge_pool/expert_memory.py` — no changes
`digest_spot_kb_docs(api_key, limit=N)` is the existing public API.

### `apps/spot-market/app.py` — 1 small addition (optional)
Add "▶ Run Digest Now" button in the "Teach the Strategist" expander. POSTs to `HERMES_URL + /hermes/knowledge/digest` and displays the returned `{"synthesized": N, "insights": M}` as a success message.

---

## Error Handling

- `_run_kb_digest()` catches all exceptions per stage — a synthesis failure still lets digest run on already-processed docs
- If `ANTHROPIC_API_KEY` is empty, logs a warning and returns `{"synthesized": 0, "insights": 0}` — no crash
- Both underlying functions have per-doc try/except — a single bad document never aborts the batch
- `max_instances=1` on the APScheduler job prevents overlapping runs
- `misfire_grace_time=3600` — if Hermes restarts during the scheduled window, the job fires within 1 hour rather than being skipped entirely

---

## Idempotency

Both pipeline stages track what's been processed:
- Synthesis: `_get_unprocessed_doc_ids()` checks `kp_doc_summaries` for missing entries
- Digest: checks `kp_expert_insights.source_doc_id` for already-digested docs

Re-running the job multiple times on the same day is safe — already-processed docs are skipped.

---

## Observability

- CloudWatch log group for Hermes ECS task shows `[kb_digest] synthesized=N insights=M` at INFO level after each run
- HTTP endpoint returns the same counts for manual runs
- Existing `get_memory_stats()` in `expert_memory.py` tracks total/high-conf insights and last-updated timestamp — already surfaced in the spot-markets UI stats panel

---

## Files Changed

| File | Change |
|---|---|
| `services/hermes/app.py` | Add `_run_kb_digest()`, APScheduler job, `POST /hermes/knowledge/digest` |
| `apps/spot-market/app.py` | Add "▶ Run Digest Now" button in Teach the Strategist expander |

No DB migrations. No new files. No new dependencies.

---

## Out of Scope

- Structured data → insights (exchange_excel_metrics trends): separate feature (Option A, not chosen)
- Notification on completion (WeChat/Feishu): can be added later if counts exceed a threshold
- Per-province insight quality metrics: future enhancement
