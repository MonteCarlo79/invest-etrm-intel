# Handoff: Philippines + Poland Market Apps — Continue From Here

**Date:** 2026-06-02  
**Branch:** `feat/ph-po-market-apps` on `MonteCarlo79/invest-etrm-intel`  
**Last commit:** `74379be` — all 7 files pushed ✅

---

## What Is Done (as of this session)

| File | Status |
|------|--------|
| `services/ph_knowledge/__init__.py` | ✅ committed |
| `services/ph_knowledge/config.py` | ✅ committed |
| `services/ph_knowledge/ingest.py` | ✅ committed |
| `apps/ph-market/app.py` | ✅ committed (≈700 lines, 8 tabs) |
| `apps/ph-market/Dockerfile` | ✅ committed (port 8510) |
| `services/po_knowledge/__init__.py` | ✅ committed |
| `services/po_knowledge/config.py` | ✅ committed |
| `services/po_knowledge/ingest.py` | ✅ committed |
| `apps/po-market/app.py` | ✅ committed (≈750 lines, 8 tabs) |
| `apps/po-market/Dockerfile` | ✅ committed (port 8511) |
| `knowledge/handoff_ph_po_market_apps.md` | ✅ committed (architecture spec) |

**Nothing left to write.** Next step is **build → push ECR → deploy ECS**.

---

## Architecture Summary

### Philippines (`apps/ph-market/app.py`, port 8510)

Standalone Streamlit app (does NOT use `run_market_app` template). 8 tabs:

1. **Market Structure** — static KPIs (19.1 GW peak, 29,962 MW installed), 3-grid bar chart, generation mix pie, routes-to-market table, key players
2. **Green Energy Auctions** — GEA-1 through GEA-5 data, target vs awarded bar chart, GET price comparison, COE-GET mechanism explanation
3. **BESS Opportunity** — NGCP AS framework (regulating/contingency/dispatchable), reserve market (Jan 2024), ASPA, BESS revenue stack (indicative PHP/MW/h)
4. **Investment Analysis** — IRR calculator with Solar/Wind/BESS-2h/BESS-4h/IRESS presets; outputs: unlevered IRR, equity IRR, LCOE, NPV, 3×3 sensitivity
5. **Investment Advisor** — Claude sonnet-4-6 agent, 7 tools, expert memory, session persistence, quick-start questions
6. **Knowledge Base** — KB stats, search, file upload (PDF/Excel/PPTX), URL fetch, auto-ingest local reports button
7. **Data Management** — table counts, KB digest to expert memory, expert memory viewer, agent memory add/delete
8. **Grid Analysis (PyPSA)** — placeholder with CSV upload; builds network + runs lopf when buses+lines CSVs provided

**7 Agent tools:** `search_knowledge_base`, `get_geap_data`, `get_wesm_price_context`, `get_ancillary_services_context`, `estimate_re_irr`, `get_market_structure`, `get_policy_snapshot`

**Scheduler:** APScheduler Asia/Manila — 03:30 ingest, 03:45 KB digest

**IRR model currency:** PHP (USD × 58)

### Poland (`apps/po-market/app.py`, port 8511)

Same pattern as PH. 8 tabs:

1. **Market Structure** — installed capacity (~65 GW), generation mix (coal 38%, lignite 14%, solar 15%, wind 13%), TGE price trend chart, key players
2. **Balancing & AS Markets** — FCR/aFRR/mFRR detail, Rynek Mocy (capacity market), BESS revenue stack table
3. **BESS Opportunity** — why Poland, Aurora 2026 economics, optimal duration analysis (1h/2h/4h)
4. **Investment Analysis** — BESS IRR calculator (EUR/kW CAPEX, PLN/MW/yr revenue); Solar/Wind also supported
5. **Investment Advisor** — 7 tools, expert memory, quick-start questions, session persistence
6. **Knowledge Base** — same as PH; Aurora PDF/Excel local reports
7. **Data Management** — same as PH
8. **Grid Analysis (PyPSA)** — 4-zone built-in Poland model (N/S/E/W) with one-click LOPF; also CSV upload mode

**7 Agent tools:** `search_knowledge_base`, `get_aurora_forecast_data`, `get_balancing_market_context`, `get_capacity_market_context`, `estimate_bess_irr`, `get_market_structure`, `get_policy_snapshot`

**Scheduler:** APScheduler Europe/Warsaw — 03:30 ingest, 03:45 KB digest

**IRR model currency:** PLN (EUR × 4.25)

---

## Local Run (test before Docker build)

```bash
cd bess-platform

# Philippines
streamlit run apps/ph-market/app.py --server.port=8510

# Poland
streamlit run apps/po-market/app.py --server.port=8511
```

Requires `.env` at `config/.env` with `PGURL` and `ANTHROPIC_API_KEY`.

First run: go to **Knowledge Base tab → Auto-ingest Local Reports** to seed KB from:
- `data/market-fundamentals-ph/` (Philippines)
- `data/market-fundamentals-po/` (Poland — Aurora PDFs + Excel)

---

## Next Steps (What Claude Must Do)

### Step 1 — Build & push Docker images to ECR

```bash
# Philippines
aws ecr get-login-password --region ap-southeast-1 | \
  docker login --username AWS --password-stdin <ACCOUNT>.dkr.ecr.ap-southeast-1.amazonaws.com

docker build -f apps/ph-market/Dockerfile -t bess-ph-market:v1 .
docker tag bess-ph-market:v1 <ACCOUNT>.dkr.ecr.ap-southeast-1.amazonaws.com/bess-ph-market:v1
docker push <ACCOUNT>.dkr.ecr.ap-southeast-1.amazonaws.com/bess-ph-market:v1

# Poland
docker build -f apps/po-market/Dockerfile -t bess-po-market:v1 .
docker tag bess-po-market:v1 <ACCOUNT>.dkr.ecr.ap-southeast-1.amazonaws.com/bess-po-market:v1
docker push <ACCOUNT>.dkr.ecr.ap-southeast-1.amazonaws.com/bess-po-market:v1
```

> Check existing ECR repos: `aws ecr describe-repositories --region ap-southeast-1`  
> Likely need to create repos first: `aws ecr create-repository --repository-name bess-ph-market ...`

### Step 2 — Deploy to ECS

Pattern mirrors existing apps (au-market, gb-market). Use `terraform/` or AWS CLI:

```bash
# Check existing ECS cluster + task definitions for reference
aws ecs list-task-definitions --region ap-southeast-1 | grep bess

# Register new task definitions (copy from au-market task def, update image URI + port + env)
# Then create/update ECS services
```

Key config per service:
- **PH:** image `bess-ph-market:v1`, port `8510`, ALB path `/ph-market*`, env `PGURL` + `ANTHROPIC_API_KEY`
- **PO:** image `bess-po-market:v1`, port `8511`, ALB path `/po-market*`, env `PGURL` + `ANTHROPIC_API_KEY`

### Step 3 — Ingest Aurora data for Poland

After app is running, go to **Knowledge Base tab** → **Auto-ingest Local Reports**.

Source files are in `data/market-fundamentals-po/`:
- `Aurora_Q1_26_POL_Power_Renewables_Market_Forecast_Report.pdf`
- `Aurora_Q2_26_POL_Power_Renewables_Market_Forecast_Report.pdf`
- `Aurora_Q1_26_POL_Power_Renewables_Market_Forecast_Data.xlsx`
- `Aurora_Q2_26_POL_Power_Renewables_Market_Forecast_Data_v1.2.xlsx`
- `Aurora_Apr26_POL_Monthly_Flexible_Energy_Market_Summary.pdf`
- `Aurora_Mar26_POL_Monthly_Flexible_Energy_Market_Summary.pdf`
- 3 WXWork screenshot PNGs (no text extraction; stored as image placeholders)

### Step 4 — Optional: PR merge

Create PR from `feat/ph-po-market-apps` → `main` after ECS deploy verified.

---

## Key File Paths

```
apps/
  ph-market/
    app.py          ← standalone Streamlit, 8 tabs, 7 tools
    Dockerfile      ← port 8510, python-pptx + pypsa
  po-market/
    app.py          ← standalone Streamlit, 8 tabs, 7 tools
    Dockerfile      ← port 8511, python-pptx + pypsa

services/
  ph_knowledge/
    __init__.py
    config.py       ← MarketConfig(code="ph", port=8510, currency="₱")
    ingest.py       ← LocalReports + DOENews + IEMOPNotices connectors
  po_knowledge/
    __init__.py
    config.py       ← MarketConfig(code="po", port=8511, currency="zł")
    ingest.py       ← LocalReports (Aurora) + PSEPublications connectors
  intl_market_common/
    advanced_retrieval_base.py  ← HyDE + OR-FTS + rerank (used by both apps)
    expert_memory_base.py       ← extract/store/retrieve expert insights

data/
  market-fundamentals-ph/     ← seed KB for Philippines
  market-fundamentals-po/     ← Aurora PDFs + Excel for Poland

knowledge/
  handoff_ph_po_market_apps.md   ← original architecture spec
  handoff_ph_po_2026-06-02.md    ← THIS file
```

---

## DB Tables Created Automatically on First Run

Both apps call `_ensure_tables()` at startup:

```sql
intl_market.ph_knowledge_docs       -- KB with FTS
intl_market.ph_expert_insights      -- durable insights
intl_market.ph_analyst_sessions     -- chat history
intl_market.po_knowledge_docs
intl_market.po_expert_insights
intl_market.po_analyst_sessions
marketdata.agent_memory             -- cross-app memory (already exists)
```

---

## Known Issues / Watch Points

1. **`pypsa` + `highs` solver** — the 4-zone Poland model calls `n.lopf(solver_name="highs")`. Confirm `highs` is available in the Docker image (it's bundled with recent PyPSA). If not, fall back to `solver_name="glpk"`.

2. **`services/common/` copy in Dockerfile** — both Dockerfiles copy `services/common/`. Confirm this directory exists in the repo (it does for au-market). If empty or missing, the COPY command will fail — remove that line if so.

3. **ALB listener rules** — need `/ph-market*` and `/po-market*` path-based routing rules added to the ALB. Check Terraform `alb.tf` or add via AWS console.

4. **ECR repo names** — check what naming convention existing repos use (e.g., `bess-au-market`, `bess-gb-market`). Use `bess-ph-market` and `bess-po-market` to be consistent.
