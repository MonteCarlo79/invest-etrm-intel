# CLAUDE.md — bess-platform

Read this file at the start of every session. Then read MEMORY.md and ERRORS.md before doing anything else.

---

## Who I Am

**Name:** Dipeng Chen
**Role:** Head of Power Markets + Head of Asset Management

**Business scope:**
- Power retail, wind/solar farm and BESS investment economics
- Asset-backed trading: coal-fired, renewable, and BESS stations
- O&M and value optimisation of in-house renewable and BESS assets
- Full asset lifecycle tracking (acquisition → operations → exit)
- Geography: China (primary), with awareness of European and other major markets

**Strong in:** Energy quant methods, power market analysis, Chinese/European power market structure and rules, renewable investment economics, numbers and model sensitivity. Do not over-explain power market fundamentals, dispatch logic, settlement mechanics, or investment return concepts.

**Still learning:** How to use AI tooling to amplify analytical skills and business value. Explain AI/software architecture decisions when they're non-obvious.

**Tone:** Professional. Direct. No filler. Match the precision of a quant analyst.

---

## The System Being Built

**Name:** Investment-Trading-Asset Intelligence and Decisions System

**Goal:** A renewable asset investment and trading master that tells where and what to invest in — starting with China.

**Audience:** Renewable asset investors who care about investment returns and asset operations.

**Architecture — 5 Pillars, 5 Agents:**

| Pillar | App | Based On | Focus |
|--------|-----|----------|-------|
| 1 | Market Map | `apps/spot-market` (china-spot) | Spot prices, inter-provincial flow, market fundamentals, system tightness |
| 2 | Asset Map | Similar framework to china-spot | Asset valuation and modelling by type and region |
| 3 | Asset Operations & Portfolio Optimisation | Inner Mongolia ops as foundation | Live asset data, dispatch strategy, invoice reconciliation |
| 4 | Knowledge Pool | `services/knowledge_pool` | National + provincial power market policies, trading rules, regulatory expert |
| 5 | Investment Committee | Orchestration layer | Aggregates opinions from all 4 agents to make investment decisions |

When any task touches more than one pillar, flag the cross-pillar impact before proceeding.

---

## Infrastructure

- **Cloud:** AWS, ap-southeast-1 (Singapore)
- **Compute:** ECS Fargate, ALB + Cognito auth
- **Database:** PostgreSQL RDS (`bess-platform-pg`)
- **Container registry:** ECR (`319383842493.dkr.ecr.ap-southeast-1.amazonaws.com`)
- **IaC:** Terraform in `infra/terraform/`
- **Domain:** `https://www.pjh-etrm.ai`
- **AI:** Anthropic Claude (sonnet-4-6 for agents, haiku-4-5 for cheap tasks like translation)

**Key services and paths:**

| Service | ECR repo | ALB path | Port |
|---------|----------|----------|------|
| Spot Market (Pillar 1) | `bess-spot-markets` | `/spot-markets/*` | 8505 |
| Inner Mongolia dashboard | `bess-inner-mongolia` | `/inner-mongolia/*` | — |
| Portal | `portal` | `/` | — |
| PnL Attribution | `bess-pnl-attribution` | `/pnl-attribution/*` | — |

---

## Deployment Protocol

**Standard deploy sequence:**
```bash
docker build -f <app>/Dockerfile -t <repo>:<vN> .
docker tag <repo>:<vN> 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/<repo>:<vN>
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/<repo>:<vN>
# Update tfvars image tag, then:
terraform apply
# If terraform shows "No changes", force ECS to use the latest task def:
$tdArn = aws ecs describe-task-definition --task-definition <family> --region ap-southeast-1 --query "taskDefinition.taskDefinitionArn" --output text
aws ecs update-service --cluster bess-platform-cluster --service <svc> --task-definition $tdArn --force-new-deployment --region ap-southeast-1
```

**If Terraform shows "No changes" despite image tag change:** State has drifted. Run `terraform refresh` then `terraform apply`.

**If Docker COPY layers cache old code despite `--no-cache`:** Disable BuildKit: `$env:DOCKER_BUILDKIT="0"; docker build ...`

**ECR token expires after ~12h:** Re-login with `$pass = aws ecr get-login-password --region ap-southeast-1; docker login --username AWS --password $pass 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com`

**All deployments require explicit in-session confirmation.** "You mentioned this earlier" is not confirmation.

---

## Persistent Files

| File | Purpose | When to update |
|------|---------|----------------|
| `MEMORY.md` | Decisions log + session summaries | After significant decisions; on "session end" |
| `ERRORS.md` | Failed approaches log | When something takes >2 attempts |
| `CLAUDE.md` | This file — session bootstrap | When project direction changes |

**Read MEMORY.md before every session.** Never contradict a logged decision without flagging it first.

---

## Coding Rules

1. **Ask, don't assume.** If intent, architecture, or requirements are unclear, ask before writing a line.
2. **Simplest solution first.** No speculative abstractions, no unrequested flexibility.
3. **Surgical edits only.** Only touch files, functions, and lines directly related to the current task.
4. **No stealth improvements.** If something elsewhere is worth fixing, note it. Do not touch it.
5. **Flag uncertainty.** If not confident about a library's behaviour or a technical detail, say so before proceeding.

---

## Irreversible Actions — Always Confirm First

Stop completely, list what will be affected, and wait for explicit "yes" in the current message before:
- Deploying or pushing to any environment
- Running DB migrations or schema changes
- Dropping files, tables, branches, or dependencies
- Sending external API calls, emails, or messages
- Any `git reset --hard`, `rm -rf`, force push, or equivalent

---

## End of Task — Always Close With

```
**Files changed:**
- path/to/file — what changed

**Files not touched:** (if relevant)

**Follow-up needed:** (decisions or attention required)
```

---

## Dual Environment — AWS + Local

Every app dashboard must run in two modes:

1. **AWS (production):** ECS Fargate, served via ALB at `https://www.pjh-etrm.ai`. This is the operating environment — always kept live and stable. Never break production to test a feature.

2. **Local (development):** Run directly with `streamlit run` (or equivalent) against a local `.env` or `docker-compose`. Every app must support local execution without AWS credentials where possible (e.g. use `DB_DSN` env var, fallback to local data files).

**Rules:**
- When building or modifying an app, confirm it runs locally before deploying to AWS.
- Local mode should degrade gracefully when AWS-only services (S3, Cognito) are unavailable — show a warning, don't crash.
- Document the local run command in the app's directory or in this file.

**Environment variables:** stored in `bess-platform/config/.env`. Load before running any app locally.

**Local run — spot-market:**
```bash
cd bess-platform
# PowerShell
Get-Content config\.env | ForEach-Object { if ($_ -match '^([^#][^=]+)=(.+)$') { [System.Environment]::SetEnvironmentVariable($matches[1].Trim(), $matches[2].Trim()) } }
streamlit run apps/spot-market/app.py --server.port 8505

# bash
set -a && source config/.env && set +a
streamlit run apps/spot-market/app.py --server.port 8505
```

---

## Infrastructure — AWS is the Operating Environment

- AWS ECS/RDS/S3/ECR is the production infrastructure. Do not tear it down, scale to zero, or reconfigure it without explicit confirmation.
- Terraform in `infra/terraform/` is the single source of truth for infrastructure. Do not make manual AWS console changes that bypass Terraform unless diagnosing an incident — and if you do, reconcile with `terraform refresh` immediately after.
- RDS (`bess-platform-pg`) holds live market data. Any migration or schema change requires explicit confirmation and a rollback plan.

---

## Git — Push All Changes

- All code changes must be committed and pushed to GitHub. No local-only work.
- Commit after every meaningful unit of work — don't batch unrelated changes into one commit.
- Commit message format: imperative, one line, e.g. `Add system tightness ranking to market fundamentals tab`.
- Never force-push to `main` without explicit confirmation.
- If a feature is incomplete, commit to a feature branch, not `main`.
- After every session, confirm all changes are pushed before closing.

---

## Session End

When I say **"session end"**, **"wrapping up"**, or **"let's stop here"**, write a session summary to `MEMORY.md`:

```markdown
## Session Summary, [Date]
**Worked on:** [focus of the session]
**Completed:** [finished items]
**In progress:** [started but not done]
**Decisions made:** [key choices]
**Next session:** [what to pick up first + important carry-forward context]
```
