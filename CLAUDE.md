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

## AI Development Path — Six Stages

Based on the 2026 AI planning framework (`AI规划方向_v0.41_20260422`). Progress against each stage informs what to build next.

### Stage 1 — AI Tool Usage ✅ Active
Using Claude Code, GitHub Copilot, and other AI tools for personal productivity. Skill libraries accelerate individual work.

**Current state:** Claude Code is the primary development tool for this entire project.

---

### Stage 2 — AI Agent Building ✅ Built
Agents with multi-turn tool-use loops, domain grounding, and business logic. Agents orchestrate data services and external tools to achieve business goals.

**Current state:** Two domain agents deployed:
- `apps/spot-market` — Agent tab with 5 tools (prices, inter-provincial flow, summaries, pipeline, fundamentals)
- `apps/bess-map` — Agent tab with 3 tools (economics, dispatch detail, IRR estimate)

Both agents have: explicit domain grounding rules (no external knowledge contamination), DB-backed conversation memory (`marketdata.agent_memory`), auto-extract via Haiku, memory injection into every session's system prompt. See **AI Agent Design Requirements** section for the full pattern.

---

### Stage 3 — RAG Knowledge Base ⚪ Partial
Build expert knowledge bases and Q&A pools using RAG (Retrieval-Augmented Generation): chunk documents, vectorise, store in a vector DB, retrieve by similarity, augment LLM reasoning.

**Current state:** `services/knowledge_pool` exists in the architecture and is referenced in Pillar 4 (Knowledge Pool agent). RAG pipeline not yet implemented. High operational cost — requires ongoing chunking, embedding, and vector DB maintenance.

**When to build:** When the domain knowledge base (power market policy, trading rules, regulatory documents) grows large enough that direct context injection is no longer feasible (>200k tokens).

---

### Stage 4 — Markdown Second Brain ❌ Not yet
For knowledge bases under 200k tokens: build all domain knowledge, skills, and experience as structured markdown files. LLM generates and maintains the knowledge base; humans review and correct. No RAG, no vector DB — LLM reads markdown directly from the context window. When accumulated markdown is large enough, use natural language to query the LLM, which reads its own markdown-generated index and summaries to answer faster and more accurately.

**Core principle:** Don't learn AI — use AI to build your own markdown digital knowledge base. Let AI help you remember, organise, and discover connections you missed, constructing your second brain outside your head.

**Gap between what is built and Stage 4:**

| Dimension | Current (DB memory) | Stage 4 target |
|---|---|---|
| Storage | DB rows: `category / subject / content` fragments | Structured markdown files, version-controlled in repo |
| Content scope | Conversation-level micro-facts | Comprehensive domain expertise: market mechanics, investment frameworks, operating experience |
| Authoring | Haiku extracts fragments automatically | LLM drafts full knowledge docs; human reviews, corrects, commits |
| Retrieval | SQL query → injected as bullet list | LLM reads markdown files directly — no query, no vector search |
| Evolution | Memories accumulate, no structural refinement | LLM detects gaps, proposes updates, human approves — knowledge progressively sharpens |

**What Stage 4 looks like in this project:**
- A `knowledge/` directory in the repo containing markdown files: e.g. `china_spot_market_mechanics.md`, `bess_investment_framework.md`, `provincial_market_rules.md`, `mengxi_asset_operations.md`
- At session end, Claude drafts updates to relevant files based on what was discussed; you review, correct, commit
- Agents load relevant markdown files into their system prompt at startup (replacing or supplementing `agent_memory`)
- `CLAUDE.md` itself is the seed — expand it into a full knowledge library

**Prerequisite:** The `agent_memory` DB system built in Stage 2 is a valid minimal scaffold. The next step is to extract accumulated memories into structured markdown files and build the authoring review loop.

---

### Stage 5 — Fine-tuning & Reinforcement Learning ❌ Not started
Build domain-specific L2 models: SFT or PEFT fine-tuning on a base model, then RL with custom loss functions (PPO/GRPO) to optimise output preference. Requires sufficient labelled training pairs.

**When to build:** After Stage 4 has produced enough reviewed, high-quality markdown knowledge that it can serve as fine-tuning training data.

---

### Stage 6 — Multimodal Training ❌ Not started
L2 domain models where input is natural language and output is structured models (CAD, simulation, optimisation models). Target domains: engineering design, digital simulation.

**When to build:** Long-term. Depends on Stage 5 foundation and specific multimodal output requirements.

---

### Current position: between Stage 2 and Stage 3, with Stage 4 as the next meaningful build target.

The `agent_memory` system is a bridging element that partially anticipates Stage 4. Prioritise building the markdown knowledge library (`knowledge/` directory) alongside the Pillar 3 and Pillar 4 agent work.

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
| BESS Asset Map (Pillar 2) | `bess-map` | `/bess-map/*` | 8503 |
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

## AI Agent Design Requirements

Every agent embedded in a dashboard app must satisfy three requirements:

### 1. Domain-specific — grounded on DB data only

The agent must answer from data returned by tool calls in the current conversation. It must not answer from Claude's training knowledge on domain-specific factual questions (prices, revenues, dispatch, etc.).

**Implementation:**
- System prompt must open with an explicit grounding rule:
  > *"Your knowledge comes exclusively from the data tools below — never from general training data or external information. Do not state any price level, trend, or market event unless it was returned by a tool call in this conversation."*
- Follow with domain definitions (units, naming conventions, thresholds) so the agent uses the project's own terminology, not generic market conventions.
- Follow with an analytical framework mapping question types to specific tools (e.g. "for IRR questions, call `get_irr_estimate`"), so the agent always fetches before answering.
- Tools must cover all data the agent is expected to query — if a question type can't be answered by a tool, the agent should say so rather than guessing.

**Limitation:** This is prompt-level instruction, not a technical lock. Claude generally obeys it on specific data questions. Spot-check by verifying a tool-call expander appears before factual answers.

### 2. Domain memory — learns from conversations

The agent must remember the analyst's views, methodology preferences, and domain insights across sessions.

**Implementation pattern (both Pillar 1 and Pillar 2 agents use this):**

**DB table** — `marketdata.agent_memory`:
```sql
id SERIAL PRIMARY KEY, app TEXT, category TEXT, subject TEXT,
content TEXT, source TEXT DEFAULT 'manual',
created_at TIMESTAMPTZ DEFAULT NOW(), active BOOLEAN DEFAULT TRUE
```
`app` column isolates memories per app (`bess_map` vs `spot_market`). `CREATE TABLE IF NOT EXISTS` + `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` for idempotent migration.

**Write path (auto-extract):** After every agent response, call `claude-haiku-4-5-20251001` with a structured prompt to extract `{category, subject, content}` items worth persisting. Show a suggestion panel (checkboxes) — the analyst saves or dismisses. Saved items go to DB with `source='auto'`.

**Read path (injection):** At the start of every turn, load active memories for this app from DB (cached 60s), format as a `## Analyst preferences & domain knowledge` block, and append to the base system prompt via `_build_system()` / `_build_spot_system()`.

**Delete:** Memory management expander at the bottom of the Agent tab lists all memories with per-row Delete buttons (soft-delete: `active=FALSE`).

**Categories used:** `market_view`, `methodology`, `province_note`, `red_flag`, `investment_thesis` (bess-map); `preference`, `market_view`, `methodology` (spot-market). Extend as needed.

### 3. No external contamination

- No web search, no external API calls, no retrieval from sources outside this project's DB.
- The only external call is to Anthropic API (Claude itself and Haiku for memory extraction).
- If future agents need document knowledge (PDFs, policy files), route through `services/knowledge_pool` — do not give the agent a generic web search tool.

### Current agent inventory

| App | Tab | System prompt | Tools | Memory table filter |
|-----|-----|--------------|-------|-------------------|
| `apps/spot-market/app.py` | Agent (tab 9) | `_SPOT_AGENT_BASE_SYSTEM` + `_build_spot_system()` | get_spot_prices, get_interprov_flow, get_market_summaries, run_pipeline, get_market_fundamentals | `app='spot_market'` |
| `apps/bess-map/app.py` | Agent (tab 6) | `_AGENT_BASE_SYSTEM` + `_build_system()` | get_bess_economics, get_dispatch_detail, get_irr_estimate | `app='bess_map'` |

When building a new agent, copy this pattern exactly. Name the base system constant `_<APP>_AGENT_BASE_SYSTEM`, the builder `_build_<app>_system()`, and scope all memory reads/writes to a unique `app` value.

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
