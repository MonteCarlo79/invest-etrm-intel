# Spot-Market Handoff — 2026-07-28

> **For a new Claude session:** Read this document in full before making any changes.
> Working directory: `C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform`
> Branch: `feat/deal-structurer-bedrock-migration`
> Repo: `https://github.com/MonteCarlo79/invest-etrm-intel`

---

## Deployment State

| Service | Image | ECS Task Def | Status |
|---------|-------|--------------|--------|
| spot-markets | `bess-spot-markets:v91` | `bess-platform-spot-markets:124` | ✅ LIVE |
| hermes | `bess-platform-hermes:latest` | `bess-platform-hermes:158` | ✅ LIVE |

**Next version = v92** (rebuild when next code change lands in spot-market).

---

## What Was Done This Session (2026-07-28)

### Fix 1 — AI Agent `max_tokens` truncation → v91 (commit `a0874c6`)

**Problem:** When the Spot Market AI Agent hit `max_tokens=4096` mid-response, the app
discarded the streamed partial answer and showed `"Unexpected stop_reason: max_tokens"`.

**Fix:** `apps/spot-market/app.py` line 3423 — added explicit handler:
```python
if _final.stop_reason == "max_tokens":
    if _status_ph:
        _status_ph.empty()
    if text_placeholder is not None:
        text_placeholder.markdown(streamed_text)
    truncated = streamed_text + "\n\n*(回答因长度限制被截断。请发送「继续」以获取剩余内容。)*"
    return truncated, messages, tool_events
```
Partial answer is now shown; "继续" continues correctly (messages already had partial content).

**Deployed:** v91 → task def `:124` → ECS rollout complete, target healthy.

---

### Fix 2 — Exchange monthly reports: 13 provinces missing from lookup (commit `c357429`)

**Problem:** `scripts/ingest_exchange_reports.py` skipped 19 files with `province=None`
because `_NAME_TO_PROVINCE` and `_FOLDER_TO_PROVINCE` in
`services/exchange_reports/ingestor.py` only covered 10 provinces.

**Missing provinces added:**
云南, 吉林, 四川, 宁夏, 新疆, 江西, 河南, 海南, 湖南, 蒙东, 贵州, 辽宁, 青海

**Result after fix:**
- 18 June 2026 reports ingested → `staging.spot_knowledge_docs` (kb_doc_id 7650–7667)
- 1 permanent skip: `国网范围5月月报.pdf` — national scope, no province (expected)
- KB digest triggered via `POST /hermes/knowledge/digest` — synthesising in ECS now

**Caveat:** Metrics extraction (structured Claude-extracted data → `staging.exchange_monthly_metrics`)
failed with 403 for all files — Anthropic direct API blocked from China IPs.
Backfill metrics later from ECS or via:
```powershell
cd C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform
PGURL=$(py -c "from dotenv import load_dotenv; load_dotenv('config/.env'); import os; print(os.getenv('PGURL',''))") \
  py scripts/ingest_exchange_reports.py --extract-metrics-only
```
(Must be run from inside ECS or via a Hermes HTTP trigger — not from local China machine.)

---

## KB State After This Session

| Table | Approx rows | Notes |
|-------|-------------|-------|
| `staging.spot_knowledge_docs` | ~7,667 | 18 new June 2026 exchange reports added |
| `staging.spot_knowledge_chunks` | growing | FTS-indexed text chunks |
| `staging.kp_doc_summaries` | growing | Synthesis backlog ~30 docs/night |
| `staging.kp_expert_insights` | growing | Digest running now |
| `staging.exchange_monthly_reports` | 9 new June rows | 冀南/安徽/山东/广东×2/广西/江苏/蒙西×2 from first run |
| `staging.exchange_monthly_metrics` | 0 new (403 blocked) | Needs backfill from ECS |

To drain KB backlog faster (call with ~2 min gap, 30 docs/call):
```powershell
py -c "import requests, urllib3; urllib3.disable_warnings(); r = requests.post('https://bess-platform-alb-1158505371.ap-southeast-1.elb.amazonaws.com/hermes/knowledge/digest', verify=False); print(r.status_code, r.text)"
```

---

## Top Priorities (Open Work)

### 1. Wire `_load_forecast_fundamentals` into ARIMA — `apps/spot-market/app.py` ~line 5927

`_load_forecast_fundamentals` pulls from `marketdata.spot_fundamentals_hourly`
(province, datetime, load, wind, solar, net_export). Defined but **not connected** to
the PCA+ARIMA or merit-order forecast models.

Goal: ARIMAX with wind/solar/load as exogenous regressors.

**Start here:**
1. Read `_load_forecast_fundamentals` at ~line 5927
2. Read the PCA+ARIMA fitting block just below it
3. Note: `spot_prices_hourly` has mixed units — `nanmedian > 5` → divide by 1000

**Model architecture quick reference:**
```
PCA + ARIMA
  price matrix: days × 24h from spot_prices_hourly
  SVD → top 4 PCs → ARIMA(1,0,1) per PC, capped at 30 steps
  long horizon (>30d): seasonal mean-reversion, 3%/day decay toward monthly mean

Merit-Order Stack
  RE (0) → Nuclear (25) → Hydro (15) → Coal (coal_price×0.31+18) → Peaking coal

Bayesian Conjugate Gaussian
  posterior_var  = 1/(n0/σ0² + n1/σ1²)
  posterior_mean = posterior_var × (n0×μ0/σ0² + n1×μ1/σ1²)

Horizons: [1, 3, 7, 30, 90, 180, 365, days_to_eoy2027]
```

### 2. Metrics backfill for exchange monthly reports

18 June reports ingested to KB but `staging.exchange_monthly_metrics` rows are empty
(403 blocked locally). Needs Claude to extract structured metrics (clearing prices,
volumes, RE share, etc.) from each report.

Must run from inside ECS. No HTTP endpoint exists yet — options:
- Add `POST /hermes/exchange/extract-metrics` endpoint to trigger it remotely
- Or run as a one-off ECS task

### 3. Deploy updated hermes image (low urgency)

`services/hermes/app.py` `is_llm_available` guard fix is committed but running image
(td:158) still uses `ANTHROPIC_API_KEY=bedrock` workaround:
```bash
bash scripts/deploy_hermes.sh
```

### 4. 机制竞价 — Nova Pro prompt tuning (lower priority)

Edit `_BIDS_PROMPT` / `_UPCOMING_PROMPT` in
`services/knowledge_pool/jizhi_extractor.py`.
Model: `apac.amazon.nova-pro-v1:0` (ap-southeast-1). Edge case: multi-page PPTX.

---

## Exchange Reports — How Ingestion Works

**Automatic trigger:** Send file via Feishu → Hermes detects `is_exchange_report()`
→ `ingest_report()` → KB + `staging.exchange_monthly_reports` → Feishu confirms "已入库"

**Manual bulk trigger:**
```powershell
cd C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform
PGURL=$(py -c "from dotenv import load_dotenv; load_dotenv('config/.env'); import os; print(os.getenv('PGURL',''))") \
  py scripts/ingest_exchange_reports.py
```

**Nightly KB digest (18:07 UTC / 02:07 Beijing):** Synthesises already-ingested docs
into `kp_expert_insights`. Does NOT scan folders.

**Province lookup file:** `services/exchange_reports/ingestor.py`
— `_FOLDER_TO_PROVINCE` (folder name → province) and `_NAME_TO_PROVINCE` (filename → province)
— Now covers 23 provinces (10 original + 13 added today)
— Still missing: 冀北, 天津, 山西, 全国/国网 (national scope — intentionally skipped)

---

## Key File Reference

| File | Purpose | Notes |
|------|---------|-------|
| `apps/spot-market/app.py` | Main Streamlit app (~8240 lines) | All UI tabs + AI agent |
| `apps/spot-market/Dockerfile` | Docker build | v90+: `COPY shared/`, `fastembed>=0.3` |
| `services/exchange_reports/ingestor.py` | Exchange monthly report ETL | Province lookup fixed today |
| `scripts/ingest_exchange_reports.py` | Bulk ingest CLI | Walks `data/exchange-monthly-reports/` |
| `services/knowledge_pool/jizhi_extractor.py` | 机制竞价 Nova Pro extractor | |
| `services/hermes/app.py` | Hermes scheduler + KB digest | `_run_kb_digest` at line 107 |
| `shared/anthropic_client.py` | Bedrock-aware client factory | |

### `apps/spot-market/app.py` — key line numbers

| Line | What |
|------|------|
| ~712 | `__conn()` — `@st.cache_resource` DB connection |
| ~3044 | Agent system prompt / tool instructions |
| ~3139 | `_AGENT_TOOLS` definitions |
| ~3383 | `_run_agent_turn()` — agentic loop |
| **3391/3405** | `max_tokens=4096` — both streaming + non-streaming |
| **3423** | `max_tokens` handler (fixed today) |
| ~5080 | `tab_forecast` — 价格预测 tab |
| **~5927** | **`_load_forecast_fundamentals` — defined, NOT YET WIRED** |

---

## Deploy Next Version (v92+)

```powershell
# 1. Build
docker build -f apps/spot-market/Dockerfile `
  -t 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-spot-markets:v92 .

# 2. Push
aws ecr get-login-password --region ap-southeast-1 | docker login --username AWS --password-stdin 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-spot-markets:v92

# 3. Patch task def
MSYS_NO_PATHCONV=1 aws ecs describe-task-definition --task-definition bess-platform-spot-markets:124 --region ap-southeast-1 --query 'taskDefinition' --output json > C:/tmp/td_spot.json
py -3 -c "
import json
with open('C:/tmp/td_spot.json', encoding='utf-8') as f: td = json.load(f)
for k in ['taskDefinitionArn','revision','status','requiresAttributes','compatibilities','registeredAt','registeredBy']:
    td.pop(k, None)
td['containerDefinitions'][0]['image'] = td['containerDefinitions'][0]['image'].replace(':v91', ':v92')
with open('C:/tmp/td_spot_new.json', 'w') as f: json.dump(td, f, indent=2)
"
MSYS_NO_PATHCONV=1 aws ecs register-task-definition --cli-input-json file://C:/tmp/td_spot_new.json --region ap-southeast-1 --query 'taskDefinition.revision'

# 4. Deploy (replace NNN with new revision)
MSYS_NO_PATHCONV=1 aws ecs update-service --cluster bess-platform-cluster --service bess-platform-spot-markets-svc --task-definition bess-platform-spot-markets:NNN --force-new-deployment --region ap-southeast-1
```

---

## AWS / ECR Quick Reference

| Item | Value |
|------|-------|
| Account | `319383842493` |
| Region | `ap-southeast-1` |
| Spot-markets ECR | `bess-spot-markets` |
| Hermes ECR | `bess-platform-hermes` |
| ECS cluster | `bess-platform-cluster` |
| Spot-markets service | `bess-platform-spot-markets-svc` |
| Hermes service | `bess-platform-hermes-svc` |
| ALB DNS | `bess-platform-alb-1158505371.ap-southeast-1.elb.amazonaws.com` |
| Task role | `bess-platform-task-role` (`bedrock:InvokeModel`) |
| MSYS prefix | `MSYS_NO_PATHCONV=1 aws ...` (Windows Git Bash) |

## Version History

| Version | Task Def | Change |
|---------|----------|--------|
| v90 | :123 | Dockerfile fix (`COPY shared/`, `fastembed>=0.3`); Bedrock live |
| **v91** | **:124** | **AI Agent shows partial answer on `max_tokens` instead of error** |
| v92 | — | Next code change (ARIMA wiring or metrics endpoint) |
