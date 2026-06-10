# PH Market App — Handoff 2026-06-10

## Repos

| Repo | Remote | Branch | Latest Commit |
|------|--------|--------|---------------|
| bess-platform | `https://github.com/MonteCarlo79/invest-etrm-intel.git` | `feat/ph-po-market-apps` | `ae06103` |

Primary workspace: `C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform`

---

## AWS Infrastructure

| Service | URL | ECS Service | Task Def | ECR Image |
|---------|-----|-------------|----------|-----------|
| PH Market | `/ph-market/` | `bess-platform-ph-market-svc` | `:17` | `bess-ph-market:v15` |
| PO Market | `/po-market/` | `bess-platform-po-market-svc` | `:13` | `bess-po-market:v12` |

- **Cluster**: `bess-platform-cluster` (ap-southeast-1)
- **ECR**: `319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-ph-market`
- **RDS**: `bess-platform-pg.cjs000o4wn2w.ap-southeast-1.rds.amazonaws.com:5432/marketdata`
- **Cognito**: User pool `ap-southeast-1_4oXuJDwF2`, client `2sicn66brcgq78bk44q20mlb52`

---

## PH Market App — `apps/ph-market/app.py` (~2400 lines)

### 9 Tabs
| Tab | Key Content |
|-----|-------------|
| Market Structure | Static KPIs, Luzon/Visayas/Mindanao grid split, generation mix |
| Green Energy Auctions | GEAP rounds GEA-1 to GEA-5, technology breakdown, GET pricing |
| BESS Opportunity | NGCP AS framework + **BESS P&L Analysis — Perfect-Forecast Dispatch** section |
| Investment Analysis | Parametric IRR calculator + **Market-Data Driven BESS IRR (WESM)** section |
| Investment Advisor | Claude agent chat (7 tools + KB search + expert memory) |
| Knowledge Base | KB stats, search, file/URL upload, auto-ingest local reports |
| Data Management | WESM price scrape/backfill, doc sources, KB digest, expert memory |
| Grid Analysis (PyPSA) | Upload NGCP CSVs → LOPF → dispatch + congestion chart |
| Report Library | Save/browse/download PDF/PPT/Word reports |

### Key Data Table
```
intl_market.ph_wesm_prices
  (trading_date, hour 0-23, region, price_type, price_php_kwh)
  regions: Luzon / Visayas / Mindanao
  price_types: LWAP_orig (1-day lag), LWAP_final (monthly, ~15th of next month)
```
Populated by `services/ph_knowledge/wesm_scraper.py` → IEMOP LWAP API, daily at 03:45 MNL.

### Scheduler (APScheduler, Asia/Manila)
| Job | Cron | Function |
|-----|------|----------|
| `ph_ingest` | 03:30 daily | `run_knowledge_ingest()` |
| `ph_digest` | 03:45 daily | `digest_kb_docs()` |
| `ph_wesm_price` | 03:45 daily | IEMOP LWAP scrape |
| `ph_wesm_docs` | 04:05 Mon | WESM market report docs |

### Key Functions
```python
_query(sql, params=None) -> pd.DataFrame          # no conn arg
_run_bess_dispatch(region, power_mw, duration_h,
                   roundtrip_eff, price_type) -> pd.DataFrame
    # Returns: trading_date, pf_profit_php, naive_profit_php,
    #          options_value_php, charge_mwh, discharge_mwh
    # FIXED in v15: profits × 1000 (PHP/kWh×MW → PHP)

_run_irr_model(technology, capacity_mw, ...) -> dict
optimise_day(prices_24h, power_mw, duration_h, rte) -> DispatchResult
    # in services/bess_map/optimisation_engine.py
```

### Agent (Investment Advisor tab)
- Model: `claude-sonnet-4-6`, `max_tokens=4096`
- System prompt: senior Philippines RE Investment Expert; uses embedded market context + tools
- 7 tools: `search_knowledge_base`, `get_geap_data`, `get_wesm_price_context`,
  `get_ancillary_services_context`, `estimate_re_irr`, `get_market_structure`, `get_policy_snapshot`
- Also: `get_wesm_dispatch_data` (queries `ph_wesm_prices` — last N days of price stats)
- Expert memory injected at `_build_system()`; sessions persisted to `intl_market.ph_analyst_sessions`

---

## What Was Done Since 2026-06-06

### v14 — `c98fdc5` (2026-06-06)
Added two new sections to the app:

**BESS Opportunity tab — "BESS P&L Analysis"**
- `_run_bess_dispatch()` loads WESM prices, runs LP dispatch per trading day
- UI: 4-column config (region/MW/duration/RTE) → Compute button
- Outputs: Avg Daily Arbitrage, Reserve Value, Options Value metrics
- Charts: daily P&L line, monthly bar, per-day dispatch profile (dual-axis)

**Investment Analysis tab — "Market-Data Driven BESS IRR"**
- Multi-region dispatch → annualised revenue → 15-year DCF → IRR/NPV comparison table
- Config: regions multiselect, MW, duration, RTE, price type, CAPEX USD/kW, WACC, leverage

### v15 — `9f61064` (2026-06-07)
**Bug fix:** `_run_bess_dispatch()` profits were 1000× too low.
`optimise_day()` returns `profit` in `price_php_kwh × MW` units (not strict PHP).
Fixed by multiplying all three profit columns by 1000:
```python
"pf_profit_php":     res.profit * 1000,
"naive_profit_php":  max(naive_profit * 1000, 0.0),
"options_value_php": max((res.profit - max(naive_profit, 0.0)) * 1000, 0.0),
```

### td:17 (2026-06-07)
ANTHROPIC_API_KEY refreshed in ECS task def (old key was revoked, Investment Advisor was returning 401).

---

## Deploy Process (for any future change)

```bash
# 1. Build (from repo root)
docker build -f apps/ph-market/Dockerfile -t bess-ph-market:vN .

# 2. Push to ECR
MSYS_NO_PATHCONV=1 aws ecr get-login-password --region ap-southeast-1 \
  | docker login --username AWS --password-stdin \
    319383842493.dkr.ecr.ap-southeast-1.amazonaws.com
docker tag bess-ph-market:vN \
  319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-ph-market:vN
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-ph-market:vN

# 3. Register new task def (fetch :current, strip non-registrable fields,
#    update image to vN, register → gets revision :M)

# 4. Deploy
MSYS_NO_PATHCONV=1 aws ecs update-service \
  --cluster bess-platform-cluster \
  --service bess-platform-ph-market-svc \
  --task-definition bess-platform-ph-market:M \
  --force-new-deployment \
  --region ap-southeast-1
```

**Important:**
- Always build from **repo root**, not `apps/ph-market/`
- `MSYS_NO_PATHCONV=1` prefix needed for all AWS CLI calls with `/` paths on Windows Git Bash
- Never commit `infra/terraform/terraform.tfvars`
- Always `git add <file>` explicitly (never `git add -A`)
- For task def updates with secrets, write a Python boto3 script to a `.py` file and run it — don't inline secrets in Bash commands

---

## Known Issues / Pending Work

| Item | Detail |
|------|--------|
| WESM data coverage | Check Data Management → WESM Spot Price Data for current coverage. Scraper runs daily at 03:45 MNL. |
| AU May backfill | `py -m services.aemo.nem_ingest --start 2026-05-24 --end 2026-06-04 --only spot_price,fcas_price` once archive appears on nemweb |
| AU May BESS revenue | Run after MMSDM May 2026 published (~Jun 15): `py -m services.aemo.nem_ingest --start 2026-05-01 --end 2026-05-31` |
| CB client user | `DELETE FROM fortune.cb_users WHERE email='chen_dpeng@hotmail.com';` then re-register |

---

## Instructions for New Claude Session

> You are continuing work on the **Philippines Market Investment Advisory app** (`/ph-market/`)
> in the BESS trading platform.
>
> **Primary workspace:** `C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform`
> **Branch:** `feat/ph-po-market-apps` (latest `ae06103`) — pushed to origin
> **Memory:** `C:\Users\dipeng.chen\.claude\projects\C--Users-dipeng-chen--local-bin\memory\MEMORY.md`
>
> Read this file first, then check memory.
>
> **Current live state:**
> - `bess-ph-market:v15` on ECS `bess-platform-ph-market-svc` (task def `:17`)
> - App is fully functional — 9 tabs, BESS P&L + IRR sections, Investment Advisor with 7 agent tools
> - All recent bugs fixed (profit ×1000, ANTHROPIC_API_KEY refreshed)
>
> **Key technical facts:**
> - `_query(sql, params=None) → pd.DataFrame` — no conn arg
> - `optimise_day(prices_24h, power_mw, duration_h, rte)` in `services/bess_map/optimisation_engine.py`
> - Docker builds from **repo root**: `docker build -f apps/ph-market/Dockerfile -t bess-ph-market:vN .`
> - `MSYS_NO_PATHCONV=1` prefix for all AWS CLI commands
> - Never commit `infra/terraform/terraform.tfvars`; always `git add <file>` explicitly
> - For task def updates involving secrets: write a boto3 `.py` script, run it, then delete it
>
> **Next steps depend on user testing** — likely further WESM data coverage checks,
> new PH market features, or Investment Advisor enhancements.
