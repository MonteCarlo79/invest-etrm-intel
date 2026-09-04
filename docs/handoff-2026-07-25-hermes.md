# Hermes Handoff — 2026-07-25

> **For a new Claude session:** Read this document in full before making any changes.
> Working directory: `C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform`
> Branch: `feat/deal-structurer-bedrock-migration`
> Repo: `https://github.com/MonteCarlo79/invest-etrm-intel`

---

## Deployment State

| Item | Value |
|---|---|
| ECS cluster | `bess-platform-cluster` |
| ECS service | `bess-platform-hermes-svc` |
| ECR image | `319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-platform-hermes:latest` |
| Running task def | `bess-platform-hermes:158` |
| ALB DNS | `bess-platform-alb-1158505371.ap-southeast-1.elb.amazonaws.com` |
| Port / path prefix | `8000` / `/hermes/*` |

### Task def :158 environment variables (key ones)
| Name | Value | Notes |
|---|---|---|
| `BEDROCK_REGION` | `ap-southeast-1` | Added in this session — enables Bedrock routing |
| `ANTHROPIC_API_KEY` | `bedrock` | Placeholder — makes old `if not api_key:` guards pass |
| `HERMES_DB_URL` | `postgresql://postgres:...@bess-platform-pg...` | Primary DB connection |
| `AWS_DEFAULT_REGION` | `ap-southeast-1` | boto3 default |
| `FEISHU_APP_SECRET`, `TELEGRAM_BOT_TOKEN`, etc. | — | Messaging credentials |

---

## What Was Done in the 2026-07-24/25 Session

### 1. Added BEDROCK_REGION to task def

The hermes container (td:157 and before) had **no `BEDROCK_REGION`** env var, so all LLM calls fell through to the direct Anthropic API with an invalid/empty key. Every nightly job that uses LLM was silently failing.

Fix: registered td:158 with `BEDROCK_REGION=ap-southeast-1` and `ANTHROPIC_API_KEY=bedrock` (placeholder).

### 2. Fixed `_run_kb_digest` Bedrock guard

**File:** `services/hermes/app.py` line ~122

**Before:**
```python
if not api_key:
    _log.warning("[kb_digest] skipped — ANTHROPIC_API_KEY not set")
    return result
```

**After (committed, not yet in image):**
```python
if not _is_llm_available(api_key):
    _log.warning("[kb_digest] skipped — no LLM configured (set ANTHROPIC_API_KEY or BEDROCK_REGION)")
    return result
```

The `ANTHROPIC_API_KEY=bedrock` placeholder in td:158 makes the OLD guard pass (non-empty string). The proper fix lands when the new image is built and deployed.

---

## CRITICAL: Deploy New Hermes Image

The code fix is committed but the **running image still has the old code**. Build and push to get the proper `is_llm_available` guard into production:

```powershell
# From repo root in PowerShell
aws ecr get-login-password --region ap-southeast-1 | docker login --username AWS --password-stdin 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com

docker build -f apps/hermes-service/Dockerfile -t 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-platform-hermes:latest .

docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-platform-hermes:latest

aws ecs update-service --cluster bess-platform-cluster --service bess-platform-hermes-svc --force-new-deployment --region ap-southeast-1
```

---

## Remaining Bug: `_run_jizhi_scan` guard

Same old-style guard as `_run_kb_digest` had. Line ~172:

```python
if not api_key:
    _log.warning("[jizhi_scan] skipped — ANTHROPIC_API_KEY not set")
    return {"new_upcoming": 0, "provinces": []}
```

With `ANTHROPIC_API_KEY=bedrock` placeholder this passes, BUT once a new image is deployed it should be fixed properly. Change to:

```python
if not _is_llm_available(api_key):
    _log.warning("[jizhi_scan] skipped — no LLM configured (set ANTHROPIC_API_KEY or BEDROCK_REGION)")
    return {"new_upcoming": 0, "provinces": []}
```

---

## Bedrock Constraint (IMPORTANT)

`global.anthropic.*` Bedrock inference profiles **block calls from China-based IPs**, even when targeting `ap-southeast-1`. This affects:
- Local `py scripts/...` runs
- Any machine physically in China

**Only ECS tasks (inside AWS ap-southeast-1) can use Bedrock.** All LLM-dependent operations must either run as scheduled hermes jobs or be triggered via the hermes HTTP endpoints.

Model strings used: `"claude-sonnet-4-6"`, `"claude-haiku-4-5-20251001"` — auto-mapped to Bedrock IDs by `shared/anthropic_client.py`.

---

## Scheduler Jobs (all UTC)

| Job | UTC schedule | Beijing | Purpose |
|---|---|---|---|
| `send_due_reminders` | 00:05 daily | 08:05 | Task reminders via Feishu/WeCom |
| `send_morning_briefing` | 00:03 daily | 08:03 | Morning market briefing |
| `thinking_agent.run(health)` | 00:10 daily | 08:10 | Health check + self-review |
| `thinking_agent.run(design)` | 00:30 Mon | 08:30 Mon | Weekly architecture review |
| `_send_mengxi_ranking` | 23:00 daily | 07:00 +1 | Mengxi BESS ranking report |
| `_compute_nodal_pf_daily` | 22:30 daily | 06:30 +1 | Pre-compute plant MILP scores |
| `_scrape_nodal_daily` | 23:30 daily | 07:30 +1 | Fetch nodal prices from Fengxing |
| `_screen_new_bess` | 06:30 daily | 14:30 | New BESS project screener |
| `_screen_news_sources` | 06:00 daily | 14:00 | News scrape + score + Feishu digest |
| `_run_kb_digest` | 18:07 daily | 02:07 +1 | Synthesis + expert insight extraction |
| `_run_jizhi_scan` | 10:07 daily | 18:07 | 机制竞价 internet scan |
| `_send_daily_report` | 07:00 daily | 15:00 | Daily market PDF → Feishu |
| `_send_monthly_report` | 09:00 1st/month | 17:00 1st | Monthly market PDF → Feishu |
| `_screen_capacity` | 10:00 1st/month | 18:00 1st | Installed capacity screener |
| `_screen_sysopfee` | 11:00 1st/month | 19:00 1st | System operation fee screener |
| `_screen_daili` | 10:00 5th/month | 18:00 5th | 代理购电 screener |
| `_screen_capcomp` | 11:30 5th/month | 19:30 5th | 容量补偿+调频 screener |
| `_run_patrol` | 00:35 daily | 08:35 | Data quality patrol |
| `send_email_digest` | 01:03 daily | 09:03 | Outlook email digest |
| `_compute_nodal_pf_monthly` | 01:00 5th/month | 09:00 5th | Monthly nodal PF ranking |
| `_compute_nodal_pf_annual` | 02:00 1 Jan | 10:00 1 Jan | Annual nodal PF ranking |

---

## HTTP Endpoints (manual triggers)

All `POST` against `https://bess-platform-alb-1158505371.ap-southeast-1.elb.amazonaws.com`

```powershell
# Helper — call from local machine
function Invoke-Hermes($path) {
    py -c "import requests; r = requests.post('https://bess-platform-alb-1158505371.ap-southeast-1.elb.amazonaws.com$path', verify=False); print(r.status_code, r.text)"
}

Invoke-Hermes "/hermes/knowledge/digest"     # KB synthesis + expert insights (30 docs/call)
Invoke-Hermes "/hermes/jizhi/scan"           # 机制竞价 internet scan
Invoke-Hermes "/hermes/news-screener/run"    # News screener
Invoke-Hermes "/hermes/capacity/scan"        # Installed capacity screener
Invoke-Hermes "/hermes/sysopfee/scan"        # System operation fee screener
Invoke-Hermes "/hermes/capcomp/scan"         # 容量补偿+调频 screener
Invoke-Hermes "/hermes/daili/scan"           # 代理购电 screener
Invoke-Hermes "/hermes/patrol"               # Data patrol
Invoke-Hermes "/hermes/reports/daily"        # Daily market PDF report
Invoke-Hermes "/hermes/reports/monthly"      # Monthly market PDF report

# Health check (GET)
py -c "import requests; r = requests.get('https://bess-platform-alb-1158505371.ap-southeast-1.elb.amazonaws.com/hermes/health', verify=False); print(r.status_code, r.text)"
```

---

## KB Digest — Current Backlog

- **348 shared docs** pending synthesis (no `kp_doc_summaries` row yet)
- Nightly job: 30 docs/run → ~12 nights to clear
- To drain faster: call `/hermes/knowledge/digest` repeatedly (wait ~2 min between calls)

```powershell
# Drain 10 batches (300 docs) — ~20 minutes
1..10 | ForEach-Object {
    py -c "import requests; r = requests.post('https://bess-platform-alb-1158505371.ap-southeast-1.elb.amazonaws.com/hermes/knowledge/digest', verify=False); print('Batch $_ :', r.status_code, r.text)"
    if (`$_ -lt 10) { Start-Sleep 120 }
}
```

---

## Key Source Files

| File | Purpose |
|---|---|
| `services/hermes/app.py` | FastAPI app, scheduler, all HTTP routes (~1600 lines) |
| `services/hermes/agent.py` | `HermesAgent` — inbound message routing, Bedrock client |
| `services/hermes/scheduler.py` | Morning briefing, reminders, email digest |
| `services/hermes/thinking_agent.py` | Self-review agent (health + design modes) |
| `services/hermes/news_screener.py` | Daily news scrape + scoring + Feishu digest |
| `services/hermes/market_report.py` | Daily/monthly PDF market reports |
| `services/hermes/mengxi_ranking_report.py` | Mengxi BESS ranking + nodal PF |
| `services/hermes/capcomp_screener.py` | 容量补偿+调频 screener |
| `services/hermes/data_patrol.py` | Data quality patrol |
| `services/knowledge_pool/synthesis.py` | Doc synthesis (kp_doc_summaries) |
| `services/knowledge_pool/expert_memory.py` | KB digest → kp_expert_insights |
| `apps/hermes-service/Dockerfile` | Docker build |
| `apps/hermes-service/requirements.txt` | Python deps |
| `shared/anthropic_client.py` | Bedrock-aware client factory |

---

## Bedrock Client Pattern

```python
# shared/anthropic_client.py — already correct, all hermes modules use this
from shared.anthropic_client import make_client as _make_anthropic_client, is_llm_available as _is_llm_available

api_key = os.environ.get("ANTHROPIC_API_KEY", "")
client = _make_anthropic_client(api_key)   # uses Bedrock when BEDROCK_REGION is set

if not _is_llm_available(api_key):         # use THIS guard, not `if not api_key:`
    return  # skip LLM operation
```

---

## Open Items

| Priority | Item |
|---|---|
| **High** | Build + push new hermes image (code fix for `is_llm_available` guard) |
| **High** | Fix `_run_jizhi_scan` guard at line ~172 (same pattern as `_run_kb_digest`) |
| **Medium** | Drain KB synthesis backlog (348 docs, call `/hermes/knowledge/digest` × 12+) |
| **Low** | Audit remaining `if not api_key:` guards in scheduler callbacks at lines 823, 843, 860, 873, 886, 899, 924, 939, 952 — most route through `make_client` internally and are already Bedrock-safe |

---

## Hermes Architecture (quick reference)

```
Inbound channels:
  WeCom  → POST /hermes/inbound/wecom
  Feishu → POST /hermes/inbound/feishu     (card actions: /hermes/inbound/feishu-card)
  Telegram → POST /hermes/inbound/telegram

Message routing (services/hermes/agent.py):
  HermesAgent.handle_message()
    ├── is_spot_pdf?        → spot_ingest_bridge.ingest_pdf_bytes
    ├── is_exchange_report? → exchange_reports.ingestor.ingest_report
    ├── is_capacity_file?   → capacity_etl.upsert_capacity
    ├── is_capcomp_file?    → capcomp_manual_etl
    ├── classify_to_market_fundamentals? → KB ingest
    └── else → HermesAgent LLM conversation (Bedrock)

Knowledge base (shared with spot-market app):
  staging.spot_knowledge_chunks   — FTS + vector search
  staging.kp_doc_summaries        — synthesis (hermes nightly)
  staging.kp_expert_insights      — digest (hermes nightly)
  app tag: "strategist" | "trader" | "shared"
```
