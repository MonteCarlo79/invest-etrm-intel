# KB Digest Scheduler Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a nightly APScheduler job to Hermes that automatically runs synthesis + expert-insight digest on any unprocessed KB documents, plus an HTTP endpoint for on-demand triggering and a UI button in spot-markets.

**Architecture:** `_run_kb_digest()` is a module-level helper in `services/hermes/app.py` that calls `SynthesisPipeline(api_key).run(limit=30)` then `digest_spot_kb_docs(api_key, limit=30)`, catching each stage independently. The helper is registered as an APScheduler cron job at 18:07 UTC and exposed via `POST /hermes/knowledge/digest` (uses `BackgroundTasks` — returns `{"status": "started"}` immediately). The spot-markets "Teach the Strategist" expander gets a "▶ Run Digest Now" button that calls this endpoint.

**Tech Stack:** FastAPI, APScheduler (`apscheduler`), `services/knowledge_pool/synthesis.SynthesisPipeline`, `services/knowledge_pool/expert_memory.digest_spot_kb_docs`, Streamlit (`apps/spot-market/app.py`), pytest + `unittest.mock`

---

## File Map

| File | Change |
|---|---|
| `services/hermes/app.py` | Add module-level `_run_kb_digest()`, APScheduler job at 18:07 UTC, `POST /hermes/knowledge/digest` endpoint |
| `services/hermes/tests/test_kb_digest.py` | New — unit tests for `_run_kb_digest()` with mocked synthesis/digest |
| `apps/spot-market/app.py` | Add "▶ Run Digest Now" button next to existing "Digest KB → Insights" button |

---

## Task 1: Write failing tests for `_run_kb_digest`

**Files:**
- Create: `services/hermes/tests/test_kb_digest.py`

- [ ] **Step 1.1: Create the test file**

```python
# services/hermes/tests/test_kb_digest.py
"""
Unit tests for _run_kb_digest() in services/hermes/app.py.
No DB, no API calls — all external dependencies mocked.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


def _import_helper():
    """Import _run_kb_digest from app.py."""
    from services.hermes.app import _run_kb_digest
    return _run_kb_digest


class TestRunKbDigest:

    def test_returns_dict_with_both_counts(self):
        """Happy path: both stages succeed, counts returned."""
        mock_pipeline = MagicMock()
        mock_pipeline.return_value.run.return_value = {"ok": 5, "error": 0, "skipped": 2}

        with patch("services.hermes.app._synthesis_pipeline_cls", mock_pipeline), \
             patch("services.hermes.app._digest_spot_kb_docs", return_value=3):
            from services.hermes.app import _run_kb_digest
            result = _run_kb_digest("test-api-key", limit=10)

        assert result == {"synthesized": 5, "insights": 3}

    def test_synthesis_failure_still_runs_digest(self):
        """If synthesis raises, digest still runs and partial result returned."""
        mock_pipeline = MagicMock()
        mock_pipeline.return_value.run.side_effect = RuntimeError("synthesis boom")

        with patch("services.hermes.app._synthesis_pipeline_cls", mock_pipeline), \
             patch("services.hermes.app._digest_spot_kb_docs", return_value=2):
            from services.hermes.app import _run_kb_digest
            result = _run_kb_digest("test-api-key")

        assert result["synthesized"] == 0
        assert result["insights"] == 2

    def test_digest_failure_still_returns_synthesis_count(self):
        """If digest raises, synthesis count is preserved."""
        mock_pipeline = MagicMock()
        mock_pipeline.return_value.run.return_value = {"ok": 4, "error": 0, "skipped": 0}

        with patch("services.hermes.app._synthesis_pipeline_cls", mock_pipeline), \
             patch("services.hermes.app._digest_spot_kb_docs",
                   side_effect=RuntimeError("digest boom")):
            from services.hermes.app import _run_kb_digest
            result = _run_kb_digest("test-api-key")

        assert result["synthesized"] == 4
        assert result["insights"] == 0

    def test_empty_api_key_returns_zeros_without_calling_apis(self):
        """Empty API key: neither stage is called, both counts are zero."""
        mock_pipeline = MagicMock()
        mock_digest = MagicMock(return_value=99)

        with patch("services.hermes.app._synthesis_pipeline_cls", mock_pipeline), \
             patch("services.hermes.app._digest_spot_kb_docs", mock_digest):
            from services.hermes.app import _run_kb_digest
            result = _run_kb_digest("")

        mock_pipeline.assert_not_called()
        mock_digest.assert_not_called()
        assert result == {"synthesized": 0, "insights": 0}

    def test_both_stages_fail_returns_zero_zero(self):
        """Both stages explode: result is zeros, no exception propagates."""
        mock_pipeline = MagicMock()
        mock_pipeline.return_value.run.side_effect = Exception("synthesis dead")

        with patch("services.hermes.app._synthesis_pipeline_cls", mock_pipeline), \
             patch("services.hermes.app._digest_spot_kb_docs",
                   side_effect=Exception("digest dead")):
            from services.hermes.app import _run_kb_digest
            result = _run_kb_digest("key")

        assert result == {"synthesized": 0, "insights": 0}
```

- [ ] **Step 1.2: Run tests to confirm they fail (function doesn't exist yet)**

```bash
cd C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform
py -m pytest services/hermes/tests/test_kb_digest.py -v 2>&1 | head -30
```

Expected: `ImportError` or `AttributeError` — `_run_kb_digest` not yet defined.

- [ ] **Step 1.3: Commit the failing tests**

```bash
git add services/hermes/tests/test_kb_digest.py
git commit -m "test: failing tests for _run_kb_digest helper"
```

---

## Task 2: Implement `_run_kb_digest` in `services/hermes/app.py`

**Files:**
- Modify: `services/hermes/app.py`

The tests mock `services.hermes.app._synthesis_pipeline_cls` and `services.hermes.app._digest_spot_kb_docs`. We expose these as module-level names so they're patchable, then `_run_kb_digest` uses them.

- [ ] **Step 2.1: Add module-level shims and `_run_kb_digest` to `app.py`**

Find the block of top-level imports (around line 57 where `from services.hermes.spot_ingest_bridge import ...` lives). Add immediately after the last import block, before `logger = logging.getLogger(...)`:

```python
# ── KB digest pipeline shims (module-level so tests can patch them) ──────────
def _synthesis_pipeline_cls(api_key: str, workers: int = 1):
    from services.knowledge_pool.synthesis import SynthesisPipeline
    return SynthesisPipeline(api_key, workers=workers)


def _digest_spot_kb_docs(api_key: str, limit: int = 30) -> int:
    from services.knowledge_pool.expert_memory import digest_spot_kb_docs
    return digest_spot_kb_docs(api_key=api_key, limit=limit)


def _run_kb_digest(api_key: str, limit: int = 30) -> dict:
    """
    Run the two-stage KB pipeline: synthesis then digest.

    Stage 1 — SynthesisPipeline: processes undigested docs into
        staging.kp_doc_summaries + staging.kp_qa_pairs.
    Stage 2 — digest_spot_kb_docs: reads summaries, writes durable
        insights to staging.kp_expert_insights.

    Each stage is fully isolated — a failure in Stage 1 does not prevent
    Stage 2 from running on already-synthesised docs.

    Returns {"synthesized": int, "insights": int}.
    """
    _log = logging.getLogger(__name__)
    result: dict = {"synthesized": 0, "insights": 0}

    if not api_key:
        _log.warning("[kb_digest] skipped — ANTHROPIC_API_KEY not set")
        return result

    try:
        r = _synthesis_pipeline_cls(api_key, workers=1).run(limit=limit, verbose=False)
        result["synthesized"] = r.get("ok", 0)
        _log.info("[kb_digest] synthesis done: %s", r)
    except Exception as exc:
        _log.error("[kb_digest] synthesis failed: %s", exc)

    try:
        result["insights"] = _digest_spot_kb_docs(api_key=api_key, limit=limit)
    except Exception as exc:
        _log.error("[kb_digest] digest failed: %s", exc)

    _log.info("[kb_digest] synthesized=%d insights=%d",
              result["synthesized"], result["insights"])
    return result
```

- [ ] **Step 2.2: Run the tests — expect them to pass**

```bash
py -m pytest services/hermes/tests/test_kb_digest.py -v
```

Expected output:
```
PASSED test_returns_dict_with_both_counts
PASSED test_synthesis_failure_still_runs_digest
PASSED test_digest_failure_still_returns_synthesis_count
PASSED test_empty_api_key_returns_zeros_without_calling_apis
PASSED test_both_stages_fail_returns_zero_zero
5 passed
```

- [ ] **Step 2.3: Commit**

```bash
git add services/hermes/app.py services/hermes/tests/test_kb_digest.py
git commit -m "feat: add _run_kb_digest helper to hermes app"
```

---

## Task 3: Register APScheduler job in `create_app()`

**Files:**
- Modify: `services/hermes/app.py`

- [ ] **Step 3.1: Add the cron job inside `create_app()`**

Find the block starting at line ~631:
```python
        # News screener: 06:00 UTC (14:00 Beijing) — scrape + score + ingest + send digest
        scheduler.add_job(
            _screen_news_sources,
```

Add the KB digest job immediately **after** the news screener block (which ends around line 642) and **before** the daily report block:

```python
        # KB digest: 18:07 UTC (02:07 Beijing next day) — synthesize + digest all new docs
        # Runs after news screener (06:00 UTC) and well before morning briefing (00:03 UTC)
        scheduler.add_job(
            lambda: _run_kb_digest(os.environ.get("ANTHROPIC_API_KEY", "")),
            "cron",
            hour=18, minute=7,
            id="kb_digest_nightly",
            max_instances=1,
            misfire_grace_time=3600,
        )
```

> Note: The job is inside the `if _mengxi_pg_url:` block — synthesis and digest both need DB access. This is consistent with other DB-dependent jobs (news screener, capacity scan, etc.).

- [ ] **Step 3.2: Verify the app still imports without error**

```bash
cd C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform
py -c "from services.hermes.app import create_app; print('ok')"
```

Expected: `ok`

- [ ] **Step 3.3: Commit**

```bash
git add services/hermes/app.py
git commit -m "feat: schedule nightly KB digest job in Hermes (18:07 UTC)"
```

---

## Task 4: Add `POST /hermes/knowledge/digest` endpoint

**Files:**
- Modify: `services/hermes/app.py`

- [ ] **Step 4.1: Add the endpoint inside `create_app()`**

Find the `@app.post("/hermes/news-screener/backfill")` block (around line 978). Add the knowledge digest endpoint immediately **before** it:

```python
    @app.post("/hermes/knowledge/digest")
    async def run_knowledge_digest(background: BackgroundTasks):
        """Trigger synthesis + expert-insight digest on unprocessed KB docs.

        Returns immediately with {"status": "started"}.
        The job runs in the background and logs results to CloudWatch.
        """
        _api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not _api_key:
            return Response(content="ANTHROPIC_API_KEY not set", status_code=503)
        background.add_task(_run_kb_digest, _api_key)
        return {"status": "started"}
```

- [ ] **Step 4.2: Smoke-test the endpoint locally**

```bash
# In one terminal: start Hermes (or just import-check)
py -c "
from services.hermes.app import create_app
app = create_app()
routes = [r.path for r in app.routes]
assert '/hermes/knowledge/digest' in routes, f'route missing, got: {routes}'
print('route registered ok')
"
```

Expected: `route registered ok`

- [ ] **Step 4.3: Commit**

```bash
git add services/hermes/app.py
git commit -m "feat: add POST /hermes/knowledge/digest endpoint to Hermes"
```

---

## Task 5: Add "▶ Run Digest Now" button in spot-markets

**Files:**
- Modify: `apps/spot-market/app.py`

- [ ] **Step 5.1: Find the existing "Digest KB → Insights" button**

It's around line 4017:
```python
        with _col_dig2:
            if st.button("Digest KB → Insights", key="kb_digest_btn"):
```

The columns `_col_dig1` and `_col_dig2` are declared just above. Add a third column and button. Find the column declaration (it will be something like `_col_dig1, _col_dig2 = st.columns([...])`) and change it to three columns, then add the new button:

First, find the exact column declaration by searching for `_col_dig1` in app.py:

```bash
grep -n "_col_dig1\|_col_dig2" apps/spot-market/app.py
```

- [ ] **Step 5.2: Replace the two-column layout with three columns and add the new button**

Change the column declaration from:
```python
        _col_dig1, _col_dig2 = st.columns([3, 2])
```
to:
```python
        _col_dig1, _col_dig2, _col_dig3 = st.columns([3, 2, 2])
```

Then, after the `with _col_dig2:` block (after line 4026), add:

```python
        with _col_dig3:
            st.caption(
                "Trigger the full synthesis + digest pipeline via Hermes "
                "(runs in background — check logs for results)."
            )
            if st.button("▶ Run Digest Now", key="kb_hermes_digest_btn"):
                _hermes_url = _os.environ.get("HERMES_URL", "")
                if not _hermes_url:
                    st.warning("HERMES_URL not configured.")
                else:
                    try:
                        import requests as _req
                        _r = _req.post(
                            _hermes_url.rstrip("/") + "/hermes/knowledge/digest",
                            timeout=10,
                            verify=False,
                        )
                        if _r.status_code == 200:
                            st.success("Digest job started — insights available in ~5 min.")
                        else:
                            st.error(f"Hermes returned {_r.status_code}: {_r.text[:120]}")
                    except Exception as _he:
                        st.error(f"Could not reach Hermes: {_he}")
```

> Note: `verify=False` matches the existing pattern used for all Hermes calls from spot-markets (internal ALB, cert not resolvable from container).

- [ ] **Step 5.3: Check `_col_dig1`, `_col_dig2` column count**

Look at the exact line:
```bash
grep -n "_col_dig1\|st.columns" apps/spot-market/app.py | grep -A1 "col_dig"
```

Adjust the `st.columns([...])` widths as needed (e.g. `[3, 2, 2]`).

- [ ] **Step 5.4: Commit**

```bash
git add apps/spot-market/app.py
git commit -m "feat: add Run Digest Now button in Teach the Strategist expander"
```

---

## Task 6: Deploy

**Files:** none — build + push + ECS update

- [ ] **Step 6.1: Run all tests**

```bash
py -m pytest services/hermes/tests/test_kb_digest.py -v
```

Expected: 5 passed

- [ ] **Step 6.2: Build and push Hermes image**

```bash
cd C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform
py scripts/deploy_hermes.ps1
```

Or manually:
```powershell
$ECR = "319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-hermes"
docker build -t bess-hermes -f apps/hermes-service/Dockerfile .
docker tag bess-hermes:latest "$ECR`:latest"
docker push "$ECR`:latest"
py scripts/update_hermes_taskdef.py
```

- [ ] **Step 6.3: Build and push spot-markets image**

```powershell
$ECR2 = "319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-spot-markets"
$env:IMAGE_TAG = "v53"
docker build -t "bess-spot-markets:v53" -f apps/spot-market/Dockerfile .
docker tag "bess-spot-markets:v53" "$ECR2`:v53"
docker push "$ECR2`:v53"
py scripts/update_spot_markets_taskdef.py
```

- [ ] **Step 6.4: Verify endpoint is live**

```bash
curl -s -X POST https://pjh-etrm.ai/hermes/knowledge/digest | python -m json.tool
```

Expected:
```json
{"status": "started"}
```

- [ ] **Step 6.5: Check CloudWatch logs the next morning**

Log group: `ecs-hermes` (or the Hermes ECS task log group).  
Filter: `[kb_digest]`  
Expected lines around 02:07 Beijing time:
```
[kb_digest] synthesis done: {'ok': N, 'error': 0, 'skipped': M}
[kb_digest] synthesized=N insights=M
```

---

## Self-Review Checklist

**Spec coverage:**
- ✅ `_run_kb_digest()` helper: Task 2
- ✅ APScheduler job at 18:07 UTC: Task 3
- ✅ `POST /hermes/knowledge/digest` endpoint: Task 4
- ✅ spot-markets "▶ Run Digest Now" button: Task 5
- ✅ Error handling (per-stage try/except, empty api_key guard): Task 2
- ✅ Idempotency: handled by existing `SynthesisPipeline` + `digest_spot_kb_docs` (no changes needed)
- ✅ `max_instances=1` + `misfire_grace_time=3600`: Task 3

**No placeholders:** All code blocks contain complete implementations.

**Type consistency:**
- `_run_kb_digest(api_key: str, limit: int = 30) -> dict` — consistent across Tasks 2, 3, 4
- `_synthesis_pipeline_cls` / `_digest_spot_kb_docs` — module-level shims patched identically in tests and used in implementation
- `BackgroundTasks` endpoint returns `{"status": "started"}` — spot-markets button handles this response in Task 5
