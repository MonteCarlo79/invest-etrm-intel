# Hermes Handover — 2026-08-08

> **Instructions for a new Claude session:** Read this file fully, then `CLAUDE.md`, then `ERRORS.md`, then the memory files listed under "Authoritative state" before touching anything. This document captures the state as of 2026-08-08 after the MacBook workstation transfer and the Hermes vault-hooks + bayesian-agent work.

---

## 1. What Hermes is

Hermes = the ops assistant bot (Feishu + Telegram + WeCom), FastAPI on ECS Fargate. Entry: `services/hermes/app.py` (`create_app()`); main reasoning loop: `services/hermes/agent.py` (JSON action envelope `{"action","params","reply"}`); domain questions dispatch via `services/hermes/market_agent_bridge.py` to per-domain headless agents (`services/*/headless_agent.py`). Deep-analysis questions go to `services/hermes/bayesian_agent.py` (prior → evidence → posterior loop).

- ECS: cluster `bess-platform-cluster`, service `bess-platform-hermes-svc`, task def family `bess-platform-hermes`, ECR repo `bess-platform-hermes` (tag convention: mutable `:latest`), log group `/ecs/bess-platform` (stream prefix `hermes/`)
- Deploy: `bash scripts/deploy_hermes.sh` (builds `--platform linux/amd64`, pushes, registers new td rev, force-deploys). On Mac, plain `docker push` worked 2026-08-07/08; if it dies with broken pipe to `192.168.65.x:3128`, use the crane workaround in `ERRORS.md` (crane at `~/.local/bin/crane`)
- Image Dockerfile: `apps/hermes-service/Dockerfile` (COPYs all of `services/` + `shared/` — new modules under `services/` ship automatically)

## 2. Deployed state (verified by image digest)

| When | What | Digest / td |
|---|---|---|
| 2026-08-06 | `lingfeng-ingest:v6` (collector JS-click + retry-loop fix) | td `bess-platform-lingfeng-ingest:7`, RUNNING |
| 2026-08-07 ~11:15 | Hermes: **vault read/write hooks** | `sha256:06f178d7…` |
| 2026-08-07 ~16:20 | Hermes: **bayesian unit normalization** (¥/MWh prefetch) | `sha256:ad61bca7…` |
| 2026-08-08 | Hermes: **province filter widened** (full corpus list + over-fetch) | `sha256:bf6762e8…`, RUNNING (current) |

Deploy verification pattern (use it every time): `aws ecs describe-tasks … --query "tasks[0].containers[0].imageDigest"` must equal the pushed manifest digest. `force-new-deployment` re-pulls `:latest`.

## 3. Git state — READ CAREFULLY

- **Shared branch `feat/deal-structurer-bedrock-migration`** — the user runs MULTIPLE Claude sessions against this branch in the same working tree. Expect parallel commits; never force-push; never `git reset`/`git restore --staged` (a T6 subagent once swept the user's staged `scripts/ingest_nodal_csvs.py` into a commit — that commit `b90712f` is on origin; local branch diverged around it).
- **Reconciliation pending:** local commits from this line of work were pushed to **`feat/hermes-vault-hooks`** (tip `cf5ecc4`). Once the parallel session's `ingest_nodal_csvs.py` edits are committed, merge `origin/feat/deal-structurer-bedrock-migration` into local, then merge/push — content overlap is byte-identical, so the merge is clean at commit level. Do NOT force-push the shared branch.
- Repo lives on OneDrive: full `git status` takes ~15 min cold — always use pathspecs (`git status -- services/`). `core.autocrlf=input` is set locally (Windows CRLF artifacts show as phantom modifications otherwise; commit diffstat may say "Bin"/0 insertions — cosmetic). Finder → "Always Keep on This Device" must stay pinned or OneDrive offloads `.git` and git breaks.
- `config/.env` is gitignored but OneDrive-synced; `infra/terraform/terraform.tfvars` must NEVER be staged.

## 4. What was built this week (pointers, not repetition)

**Vault hooks (Stage 4 first slice)** — spec `docs/superpowers/specs/2026-08-06-hermes-knowledge-hooks-design.md`, plan `docs/superpowers/plans/2026-08-06-hermes-knowledge-hooks.md`, ledger `.superpowers/sdd/2026-08-06-hermes-knowledge-hooks/progress.md`:
- `services/knowledge_pool/vault_reader.py` / `vault_writer.py` — OneDrive-direct read/write of the `knowledge/` vault (root `etrm/bess-platform/knowledge`)
- `services/hermes/onedrive_client.py` — `get_shared_onedrive_client()` / `set_shared_onedrive_client()`; app.py registers its startup client (single instance — MSA refresh tokens rotate; two clients would kill each other)
- Read injections: `agent.py` main loop + `bess_map`/`mengxi_trading` headless agents. Write paths: morning briefing + daily report → `knowledge/hermes/briefings/`; insights → `knowledge/hermes/inbox/` (`review_status: pending` — user promotes in Obsidian)
- Read scope excludes `inbox/` and `knowledge/policy/` PDFs by design

**Bayesian agent fixes** (`services/hermes/bayesian_agent.py`, tests `test_bayesian_agent_fixes.py` 7/7):
- Prefetch converts `spot_daily`/`interprov` to ¥/MWh with unit-labeled columns (was: unlabeled ¥/kWh next to ¥/MWh metrics → an LLM halved a Shanghai price forecast)
- System prompt: UNIT DISCIPLINE + CONSISTENCY CHECK (posterior >±30% from 3-month avg needs a named structural driver)
- Evidence trail sanitizes tool/DB errors for users (raw kept for the LLM)
- `search_exchange_reports` hard province filter by filename (+冀南/河北南网 alias, over-fetch 2×)

**Mac transfer** — tooling: uv venv `~/.venvs/bess-platform` (all deps incl. playwright 1.50), terraform 1.12.2, gh 2.97.0 (push auth works), Docker Desktop 4.85 (ECR login expires ~12h). LingFeng fallback scheduler now on Mac launchd (`ai.pjh-etrm.lingfeng-daily`, daily 04:00) — ECS is primary. Windows memory recovered into Mac memory (30 files).

## 5. Open items (priority order)

1. **`{"action":"REPLY"}` JSON envelope leaks on max_tokens continuation turns** — app.py reply/continuation send path (around the fc31ea8 fix). The parallel session owns that area; coordinate before editing. Symptom: raw action JSON shown to user on 继续 turns.
2. **Branch reconciliation** — see §3.
3. **FENGXING nodal scraper never ran on ECS** — `FENGXING_API_KEY` absent from every hermes task def revision; also needs vendor IP whitelist for the ECS NAT IP (error 40301 otherwise). Decide: add env + whitelist, or retire the job.
4. **Knowledge-pool PDF extraction quality** — vertical-text chunks (`司/公/源/科/技…`) pollute `search_reference_docs` results. Ingestion-side fix in `services/knowledge_pool/pdf_ingestion.py` (or chunk filtering).
5. **RDS blackholing from Mac/home network** — documented in ERRORS.md (2026-08-07 entry by the parallel session). Bulk loads need per-chunk connections + short timeouts; interactive queries sometimes just time out — retry later rather than debugging the DB.
6. **LingFeng Windows task** — disable `BESS-LingFeng-DailyCollection` on the old laptop if not already done.

## 6. Authoritative state — read these memory files

In `~/.claude/projects/-Users-chenzhuqi-…-bess-platform/memory/` (auto-loaded index is `MEMORY.md`):
- `project_hermes_service.md` — deployment state, chat commands, scheduled jobs, all recent fixes
- `project_hermes_vault_hooks.md` — the vault hooks feature record
- `project_lingfeng_pipeline.md` — LingFeng ECS + Mac launchd + credential incident + click fix
- `project_mac_transfer.md` — Mac environment quirks (git/OneDrive/Docker/paths)
- `feedback_git_staging.md` — never stage terraform.tfvars

## 7. Rules that bite if ignored

- **Deploys require explicit in-session user confirmation** — every time, even if discussed earlier
- **Never break production to test** — verify code in the image (`docker run --rm --entrypoint grep …`) before pushing; verify the running task's digest after deploying
- Agents answer from tool-returned data only — never let an agent state prices/facts from training knowledge
- Fail-silent knowledge I/O: vault/OneDrive failures must never break a chat loop
- Surgical edits; match existing style; flag cross-pillar impact early
- If a Bash call is denied with "Stage 2 classifier error", it's transient — retry once
