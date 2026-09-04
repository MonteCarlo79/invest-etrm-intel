# Handoff: Hermes ThinkingAgent — 2026-07-05

## What Was Built

Hermes now has proactive "thinking" capabilities. A new `ThinkingAgent` class runs on a schedule and independently checks data health and reviews app design, sending observations to the user via Feishu.

---

## Current State

| Item | Status |
|------|--------|
| Code | On branch `cost-optimisation` — pushed to GitHub |
| PR | [MonteCarlo79/invest-etrm-intel#9](https://github.com/MonteCarlo79/invest-etrm-intel/pull/9) — open, not yet merged to `main` |
| ECS | Deployed as task def `bess-platform-hermes:150` — **running the ThinkingAgent code** |
| DB migration | `hermes.thinking_log` table created in production RDS |
| Next scheduled run | Health check: 08:10 Beijing daily; Design review: Monday 08:30 Beijing |

**The service is live. No further deployment needed unless you merge to main.**

---

## Key Files

| File | What it does |
|------|-------------|
| `services/hermes/thinking_agent.py` | New — full `ThinkingAgent` class (~500 lines) |
| `services/hermes/agent.py` | Modified — added `WRITE_DEV_REQUEST` action |
| `services/hermes/app.py` | Modified — imports ThinkingAgent, 2 new scheduler jobs |
| `db/ddl/hermes/005_thinking_log.sql` | New — `hermes.thinking_log` DDL (already applied to prod) |
| `tests/hermes/test_thinking_agent.py` | New — 23 unit tests, all passing |

---

## Architecture

```
ThinkingAgent.run(mode)
  → build seed prompt (health or design)
  → Anthropic tool loop (max 8 iterations)
      tools: query_db, check_etl_freshness, read_source_file,
             list_app_files, send_feishu_message, write_dev_request
  → Claude calls send_feishu_message(text) when ready
  → log run to hermes.thinking_log (for dedup)
```

**Two modes:**
- `health` — daily, Haiku model, checks ETL freshness + DB gaps, alerts on stale data with upstream source reference
- `design` — weekly Monday, Sonnet model, reads app source files, produces 2–3 actionable observations, rotates apps A/B weekly

**Silence rule:** `send_feishu_message("")` = healthy, nothing sent.

**Dedup:** same message prefix (first 80 chars) within 7 days (health) or 14 days (design) is suppressed.

---

## Dev Request Pipeline

Users can ask Hermes to record a dev request via Feishu:
> "Hermes，记录一个需求：在 mengxi-dashboard 加 IRR 对比"

Hermes uses `WRITE_DEV_REQUEST` action → calls `ThinkingAgent.write_dev_request_from_message()` → writes structured `.md` to OneDrive `etrm/bess-platform/dev-requests/` → sends Feishu confirmation.

OneDrive syncs the file to:
`C:\Users\dipeng.chen\OneDrive\etrm\bess-platform\dev-requests\`

---

## Model Config

| Mode | Default model | Override env var |
|------|--------------|-----------------|
| health | `claude-haiku-4-5-20251001` | `HERMES_THINK_HEALTH_MODEL` |
| design | `claude-sonnet-4-6` | `HERMES_THINK_DESIGN_MODEL` |

Aliases: `haiku`, `sonnet`, `opus` — or pass full model ID.

---

## Infra

- **Cluster:** `bess-platform-cluster` (ap-southeast-1)
- **Service:** `bess-platform-hermes-svc`
- **ECR:** `319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-platform-hermes`
- **Current task def:** `bess-platform-hermes:150`
- **ECS execute-command:** enabled (for container access)
- **DB:** `hermes.thinking_log` in `marketdata` database on `bess-platform-pg` RDS

---

## What's Left To Do

1. **Merge PR #9** to `main` when ready
2. **After merge:** rebuild from `main` and redeploy (task def will increment)
3. **Verify first health check** fires at 08:10 Beijing — check CloudWatch:
   ```bash
   aws logs tail /ecs/bess-platform/bess-platform-hermes --follow --since 5m --region ap-southeast-1 | grep -i thinking
   ```
4. **Test dev request** — send "记录需求：xxx" to Hermes in Feishu, confirm `.md` appears in OneDrive

---

## Known Issues / Notes

- Binary files (`api/` PDF, `assets/` DOCX) were rejected by GitHub push rules — still only local
- `psql` is not installed in the container; use Python + psycopg2 for any future migrations:
  ```bash
  aws ecs execute-command --cluster bess-platform-cluster \
    --task <task-arn> --container hermes --interactive \
    --command "python -c \"import psycopg2; ...\""
  ```
- The `ops.ingestion_expected_freshness` and `ops.ingestion_dataset_status` tables must exist for `check_etl_freshness` to work — if they don't exist yet, the tool will return an error gracefully

---

## Relevant Specs and Plans

- Design spec: `docs/superpowers/specs/2026-07-05-hermes-thinking-agent-design.md`
- Implementation plan: `docs/superpowers/plans/2026-07-05-hermes-thinking-agent.md`

---

## To Redeploy (standard flow)

```bash
# From repo root
docker build -f apps/hermes-service/Dockerfile -t bess-platform-hermes:latest .
aws ecr get-login-password --region ap-southeast-1 | docker login --username AWS --password-stdin 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com
docker tag bess-platform-hermes:latest 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-platform-hermes:latest
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-platform-hermes:latest
py scripts/update_hermes_taskdef.py
```
