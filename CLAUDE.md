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

**Architecture — 4 Agent Pillars + Investment Committee:**

| Pillar | Agent | App | Focus |
|--------|-------|-----|-------|
| 1 | **Strategist** | `apps/spot-market` | China spot prices, inter-provincial flow, market fundamentals, system tightness |
| 2 | **Quant** | `apps/bess-map` | BESS investment economics, LP dispatch, IRR, province ranking |
| 3 | **Trader** | `apps/mengxi-dashboard` | IM BESS asset trading ops, P&L attribution, dispatch quality |
| 4 | **Knowledge Pool** | `services/knowledge_pool` | National + provincial market policies, trading rules, regulatory docs |
| 5 | **Deal Structurer** | Portal Quick Ask (full app TBD) | Investment committee: aggregates P1–P3 insights into investment recommendations |

**Portal** (`apps/portal`): Control tower with 4 agent sections, portfolio snapshot, Cognito user management (Admin only), and inline Quick Ask for all 4 personas.

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

**Current state:** Three domain agents deployed:

| Agent | App | Tab name | Tools | Memory app key |
|-------|-----|----------|-------|----------------|
| Strategist | `apps/spot-market` | "Strategist" / "策略分析师" | get_spot_prices, get_interprov_flow, get_market_summaries, run_pipeline, get_market_fundamentals, search_reference_docs | `spot_market` |
| Quant | `apps/bess-map` | "Quant" / "量化分析师" | get_bess_economics, get_dispatch_detail, get_irr_estimate | `bess_map` |
| Trader | `apps/mengxi-dashboard` | "Trader" | get_asset_pnl, get_dispatch_data, get_rt_prices | `mengxi_trader` |

All agents use: explicit domain grounding rules (no external knowledge contamination), DB-backed conversation memory (`marketdata.agent_memory`), auto-extract via Haiku (no confirmation panel since v21), memory injection into every session's system prompt. See **AI Agent Design Requirements** section for the full pattern.

**Portal Quick Ask:** Portal (`apps/portal`) has inline Quick Ask for all 4 personas (including Deal Structurer) — one-shot Claude calls, no tools, ≤300 tokens, for rapid conversational answers without opening a full app.

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

| Service | Agent | ECR repo | ALB path | Local port | Current image |
|---------|-------|----------|----------|------------|---------------|
| Spot Market (Pillar 1) | Strategist | `bess-spot-markets` | `/spot-markets/*` | 8505 | v22 |
| Quant Analyst (Pillar 2) | Quant | `bess-map` | `/bess-map/*` | 8503 | v38 (v39 pending) |
| Mengxi Dashboard (Pillar 3) | Trader | `bess-mengxi-dashboard` | `/mengxi-dashboard/*` | 8511 | v5 |
| Portal | 4 Quick Ask personas | `portal` | `/portal/*` | 8500 | v24 (v25 pending) |
| GB Market | Strategist + Quant | `bess-gb-market` | `/gb-market/*` | 8508 | pending first deploy |

**Local run (GB Market):**
```powershell
streamlit run apps/gb-market/app.py --server.port 8508
```

**Retired services (2026-05-10):** `bess-pnl-attribution` (ECR repo deleted, ALB rule removed) — superseded by bess-map Data Management. `bess-uploader` (ECS service + ALB rule removed, `enable_uploader_service = false`) — superseded by bess-map Data Management.

---

## LingFeng Data Pipeline

**Location:** `services/lingfeng/`

Automated daily data collection from LingFeng SaaS (`https://lingfeng-saas.tradingthink.cn`) → ingestion → capture pipeline.

| File | Purpose |
|------|---------|
| `collector.py` | Playwright browser automation: login, select market+indicator+date, export Excel |
| `run_daily.py` | Orchestrator: multi-market loop, chunked downloads, price+fundamentals ingest, capture |
| `ops_log.py` | Writes run status to `marketdata.data_ops_log` (start_op / finish_op) |
| `setup_schedule.ps1` | One-time Windows Task Scheduler registration (daily 06:00) |

**29 markets:** 河南, 新疆, 吉林, 海南, 湖北, 四川, 黑龙江, 福建, 浙江, 江苏, 广西, 安徽, 陕西, 贵州, 云南, 广东, 蒙东, 湖南, 宁夏, 辽宁, 河北南网, 甘肃, 蒙西, 山东, 山西, 冀北, 广州, 青海, 江西

**Scheduled run (06:00):** `python run_daily.py --markets all --models ols_rt_time_v1,naive_rt_ar17,ols_fundamentals_v1`

**Manual backfill example:**
```bash
python services/lingfeng/run_daily.py --markets 山东,山西 --start-date 2026-01-01 --end-date 2026-04-30 --chunk-days 30
```

**Re-register Task Scheduler (after any change to schedule or args):**
```powershell
.\services\lingfeng\setup_schedule.ps1
```

**Ops log table:** `marketdata.data_ops_log` — visible in Portal (Data Operations Status) and bess-map Data Management tab (Data Operations Log section).

---

## Data Operations Status

**Portal (`apps/portal/app.py`):** Replaces the old Portfolio Snapshot. Shows:
- Metric tiles for last run per op type (success/running/failed)
- Warning if any pipeline_job_status rows are running
- Collapsible table of last 48h ops from `data_ops_log`

**bess-map Data Management tab:** "Data Operations Log" section at bottom showing last 48h ops.

**Query helpers:** `shared/data_ops/status.py` — `get_recent_ops(engine)`, `get_pipeline_jobs(engine)`

**DB tables used:** `marketdata.data_ops_log` (LingFeng ops), `pipeline_job_status` (pipeline job tracker)

---

## BESS Capture Pipeline — Forecast Models

**Location:** `services/bess_map/`

The capture pipeline (`run_capture_pipeline.py`) simulates RT realized revenue by dispatching against a forecast price. It stores results in `marketdata.bess_capture_daily` with a `model` column — multiple models coexist in the DB.

**Theoretical profit** (LP optimal dispatch against actual RT prices) is model-agnostic. **Realized profit and capture rate** are model-specific.

| Model | Type | When to use |
|-------|------|-------------|
| `ols_rt_time_v1` | Rolling OLS + time-of-day | **Default. Use for all provinces.** RT-only, no DA price needed. |
| `naive_rt_ar17` | Rolling OLS + lag-1 + lag-7 same-hour | Combined AR(1,7) model for stable markets |
| `naive_rt_lag1` | Yesterday same hour | Baseline benchmark |
| `naive_rt_lag7` | Last-week same hour | Seasonal baseline |
| `ols_da_time_v1` | Rolling OLS + time-of-day on DA | **Legacy / invalid for RT.** DA prices published after RT trading — artificially inflates capture ~100%. Do not use for RT strategy evaluation. |
| `ols_fundamentals_v1` | OLS + D-1 fundamentals (bidding_space_d1_mw, load_d1_mw, renewable_d1_mw) | Requires `spot_fundamentals_hourly` data for province |

**UI:** Quant Analyst (bess-map) sidebar has a model selector. Province Ranking and Dispatch & Economics tabs filter realized/capture by selected model; theoretical always shows via LEFT JOIN.

**Re-run if data is stale or wrong model:** Data Management tab → Capture Pipeline Runner → select model → Run. Or:
```bash
python services/bess_map/run_capture_pipeline.py --province shandong --model ols_rt_time_v1 --force
```

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

| App | Tab label | System prompt constant | Tools | Memory app key |
|-----|-----------|----------------------|-------|----------------|
| `apps/spot-market/app.py` | "Strategist" / "策略分析师" | `_SPOT_AGENT_BASE_SYSTEM` | get_spot_prices, get_interprov_flow, get_market_summaries, run_pipeline, get_market_fundamentals, search_reference_docs | `spot_market` |
| `apps/bess-map/app.py` | "Quant" / "量化分析师" | `_AGENT_BASE_SYSTEM` | get_bess_economics, get_dispatch_detail, get_irr_estimate | `bess_map` |
| `apps/mengxi-dashboard/app.py` | "Trader" | `_TRADER_BASE_SYSTEM` | get_asset_pnl, get_dispatch_data, get_rt_prices | `mengxi_trader` |

**Auto-save memory pattern (v21+):** After every agent turn, call Haiku to extract `{category, subject, content}` items, auto-save to DB, show `st.toast()` notification. No confirmation panel. Use in all new agents.

When building a new agent, use the `/new-agent` skill. Name the base system constant `_<APP>_AGENT_BASE_SYSTEM`, the builder `_build_<app>_system()`, and scope all memory reads/writes to a unique `app` value.

### Knowledge Pool (Strategist only — v20+)
- `services/knowledge_pool/knowledge_docs.py` — DB-backed FTS knowledge base
- `staging.spot_knowledge_docs` / `staging.spot_knowledge_chunks` — tables
- Supported formats: PDF, PPTX, DOCX, XLSX, XLS, TXT, PNG, JPG, JPEG, WEBP
- Auto-categorization: keyword heuristic → Haiku fallback
- Images/charts: Claude vision at upload time
- Conversation logging: every Q&A turn saved as daily `conversation_log_YYYY-MM-DD.md`
- Bulk ingestion: `scripts/ingest_knowledge_bulk.py --dir /path/to/folder`

---

## Agent Development Kit

The project uses a 5-layer Claude Code dev team setup in `.claude/`.

### Layer structure

| Layer | Path | Purpose |
|-------|------|---------|
| L1 | `CLAUDE.md` | Project rules, architecture, agent patterns, deploy protocol |
| L2 | `.claude/commands/` | Custom slash commands (skills): `/deploy`, `/session-end`, `/new-agent` |
| L3 | `.claude/hooks/` | SessionStart hook: prints platform reminders on session open |
| L4 | `.claude/agents/` | Subagent definition files (6 agents) |
| L5 | `.claude/settings.json` | Hook registration |

### Subagents available

| Subagent | File | Use for |
|----------|------|---------|
| Strategist | `.claude/agents/strategist.md` | Pillar 1 code/data questions |
| Quant | `.claude/agents/quant.md` | Pillar 2 code/data questions |
| Trader | `.claude/agents/trader.md` | Pillar 3 code/data questions |
| Deal Structurer | `.claude/agents/deal-structurer.md` | Pillar 5 design/investment questions |
| code-reviewer | `.claude/agents/code-reviewer.md` | Review code changes vs CLAUDE.md rules |
| test-runner | `.claude/agents/test-runner.md` | Run tests, report results |

### Custom skills (slash commands)
- `/deploy` — generates deployment checklist for modified apps
- `/session-end` — writes session summary to MEMORY.md
- `/new-agent` — scaffolds a new agent tab following the v21+ pattern

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

## Data Upload — Prefer Local App Over S3

When ingesting data files (Excel, CSV, etc.) into the database, **always prefer the in-app upload path over S3** to minimise AWS storage and transfer costs.

| Method | When to use |
|--------|-------------|
| **In-app upload** (Streamlit file uploader → direct DB insert) | Default for all manual data ingestion. No S3 involved. |
| **S3 upload** | Only when files are too large for in-memory processing (>100 MB), or when the ingestion pipeline explicitly requires an S3 trigger. |

**Current in-app upload paths:**
- Mengxi market data (missing dates remediation): **Data Management tab → Section 5** in `apps/mengxi-dashboard`. Upload `YYYY-MM-DD.xlsx`, ingested directly to `marketdata.*` tables via `services/mengxi_ingestion/loader.py`.
- Ops dispatch data (nominated + actual curves): `services/ops_ingestion/inner_mongolia/` Excel upload flow.

When adding new data ingestion features, default to the in-app upload pattern. Only introduce an S3 path if there is a specific technical reason (batch size, pipeline trigger, cross-service sharing).

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

**Local run (PowerShell — load env first, then run each app in a separate terminal):**
```powershell
# Load env (run in each terminal)
Get-Content config\.env | ForEach-Object { if ($_ -match '^([^#][^=]+)=(.+)$') { [System.Environment]::SetEnvironmentVariable($matches[1].Trim(), $matches[2].Trim()) } }

# Portal (Terminal 1) — AUTH_MODE=dev bypasses Cognito, auto-logs in as Admin
$env:AUTH_MODE="dev"
streamlit run apps/portal/app.py --server.port 8500

# Spot Market / Strategist (Terminal 2)
streamlit run apps/spot-market/app.py --server.port 8505

# BESS Map / Quant (Terminal 3)
streamlit run apps/bess-map/app.py --server.port 8503

# Mengxi Dashboard / Trader (Terminal 4)
streamlit run apps/mengxi-dashboard/app.py --server.port 8511
```

Portal Open App links auto-resolve to localhost ports when `AUTH_MODE=dev` — no `APP_URL_MAP` needed.

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
