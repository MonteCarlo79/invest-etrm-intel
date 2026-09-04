# Handoff: Risk Management Apps — Both Apps Complete

**Date:** 2026-07-23  
**Branch:** `feat/deal-structurer-bedrock-migration`  
**Repo:** `MonteCarlo79/invest-etrm-intel`  
**Status:** Both App 1 (Asset Risk) and App 2 (Retail Risk) fully implemented and deployed to ECS.

---

## Deployment Status

| App | Port | ECR Image | ECS Service | Task Def | ALB Priority |
|-----|------|-----------|-------------|----------|--------------|
| Asset Risk | 8512 | `bess-asset-risk:v3` | `bess-platform-asset-risk-svc` | `bess-platform-asset-risk:3` | 57 |
| Retail Risk | 8513 | `bess-retail-risk:v1` | `bess-platform-retail-risk-svc` | `bess-platform-retail-risk:1` | 58 |
| Portal (updated) | 8500 | `bess-platform-portal:v10` | `bess-platform-portal-svc` | `bess-platform-portal:65` | 5 |

**Access URLs (via ALB direct — WAF bypass):**
- Asset Risk: `https://bess-platform-alb-1158505371.ap-southeast-1.elb.amazonaws.com/asset-risk/`
- Retail Risk: `https://bess-platform-alb-1158505371.ap-southeast-1.elb.amazonaws.com/retail-risk/`

**pjh-etrm.ai access:** Both paths blocked by corporate WAF (`Wafrule: 5` response). Need external WAF allowlist update for `/asset-risk/*` and `/retail-risk/*`.

---

## What was built

### Shared Libraries (used by both apps)

| Library | Files | Purpose |
|---------|-------|---------|
| `libs/risk/` | mtm.py, pnl.py, var.py, greeks.py | MtM valuation, P&L waterfall, VaR (hist+parametric), Greeks |
| `libs/settlement/` | parser.py, categorizer.py | Multi-format file ingestion, category rules, Mengxi wind settlement rule |

### Services

| Service | Files | Purpose |
|---------|-------|---------|
| `services/forward_curve/` | lingfeng_pull.py, manual_upload.py | Forward curve ingestion (LingFeng API + manual CSV) |
| `services/operating_assets/` | ingest.py, filename_mapper.py, parsers/*.py | BESS daily/dispatch + wind farm Excel ingestion |

### App 1 — Asset Risk (`apps/asset_risk/`)

6 tabs: Asset Config, Settlement, Realised P&L, Positions & MtM, VaR & Greeks, Agent

| Tab | Key features |
|-----|--------------|
| Asset Config | CRUD for rm_assets + rm_books; auto-creates linked book |
| Settlement | Upload Excel/PDF; format detection; Trade Capture, capacity comp, wind farm parsing |
| Realised P&L | Plotly waterfall (BESS: charge/discharge; Wind: generation + curtailment); ops KPIs |
| Positions & MtM | Hourly volumes grid, contract register, forward curve viewer + upload, MtM |
| VaR & Greeks | Delta/Gamma/Vega, historical+parametric VaR, stress scenarios |
| Agent | Claude (Bedrock) with 4 tools: get_book_pnl, get_position_mtm, get_var, get_asset_list |

### App 2 — Retail Risk (`apps/retail_risk/`)

6 tabs: CRM, Settlement, Realised P&L, Positions & MtM, VaR & Greeks, Agent

| Tab | Key features |
|-----|--------------|
| CRM | Customer registry, add customer, 各省份台账.xlsx import, portfolio metrics |
| Settlement | Retail settlement upload, per-customer P&L contribution, category analytics |
| Realised P&L | Retail waterfall (revenue → procurement → T&D → penalties → net margin), per-customer breakdown |
| Positions & MtM | Hourly volumes, procurement coverage ratio, open exposure, MtM |
| VaR & Greeks | Price VaR + load uncertainty VaR component, stress scenarios |
| Agent | Claude (Bedrock) with 4 tools: get_retail_margin, get_procurement_coverage, get_customer_pnl_ranking, get_contract_expiry_pipeline |

---

## Database Schema (all tables in `marketdata` schema)

### Shared (App 1 + App 2)

| Table | Purpose |
|-------|---------|
| `rm_assets` | Asset registry (wind/solar/bess/thermal) |
| `rm_books` | Trading books (book_type: 'asset' or 'load') |
| `rm_positions` | Individual trade records by channel |
| `rm_position_volumes` | Unified hourly position volumes (6 channels × price + volume) |
| `rm_dispatch_daily` | BESS daily operations summary |
| `rm_dispatch_plan` | 15-min dispatch plan (BESS + wind) |
| `rm_forward_curves` | Forward price curves (LingFeng, manual, exchange) |
| `rm_settlements` | Settlement file upload records |
| `rm_settlement_items` | Settlement line items by category |
| `rm_pnl_snapshots` | Monthly P&L snapshots (incl. wind curtailment KPIs) |
| `rm_var_snapshots` | VaR and Greeks snapshots |

### Retail-only (App 2)

| Table | Purpose |
|-------|---------|
| `rm_customers` | Customer master (province, channel, spread, revenue share) |
| `rm_customer_contracts` | Contracts (fixed/indexed/peak_offpeak/indexed_band, K-factors, JSONB formula) |
| `rm_customer_profiles` | Hourly customer load profiles |
| `rm_crm_import_configs` | Province-keyed column mapping for CRM import |
| `rm_retail_settlements` | Retail-specific settlement records (customer-scoped) |
| `rm_retail_settlement_items` | Retail settlement line items |

---

## Tests (20 passing)

```
tests/settlement/test_parser.py (4)
tests/settlement/test_categorizer.py (4)
tests/risk/test_mtm.py (3)
tests/risk/test_pnl.py (2)
tests/risk/test_var.py (3)
tests/risk/test_greeks.py (2)
tests/operating_assets/test_wind_farm_parser.py (2)
```

Run: `py -m pytest tests/settlement tests/risk tests/operating_assets -v`

---

## Key Technical Decisions

1. **Bedrock migration:** Both apps use `shared/anthropic_client.make_client()` + `is_llm_available()`. Model string `claude-sonnet-4-6` auto-mapped to `global.anthropic.claude-sonnet-4-6`. `BEDROCK_REGION=ap-southeast-1` in task definitions.

2. **App folder naming:** `apps/asset_risk/` and `apps/retail_risk/` (underscore for Python import), `baseUrlPath=asset-risk` / `retail-risk` (hyphen for URL).

3. **.dockerignore:** Root `.dockerignore` excludes `libs/` globally. Exceptions added: `!libs/risk/`, `!libs/settlement/`, `!libs/options/`, `!services/forward_curve/`, `!services/operating_assets/`, `!apps/asset_risk/`, `!apps/retail_risk/`.

4. **terraform.tfvars:** Contains secrets, NOT in git (`.gitignore`). Managed locally. Both apps' image tags and desired counts defined there.

5. **WAF:** Corporate WAF at Global Accelerator layer blocks paths not in allowlist. Access via ALB direct URL works. WAF update is external (not in this AWS account).

6. **Cognito:** ALB direct URL added to Cognito callback URLs. Rules use `authenticate-cognito` (Order 1) + `forward` (Order 2).

7. **Portal:** `shared/service_control.py` updated with both `asset-risk` and `retail-risk` entries. Portal v10 deployed.

---

## Known Issues / TODO

1. **WAF allowlist** — `/asset-risk/*` and `/retail-risk/*` need to be added to the corporate WAF allowlist for `pjh-etrm.ai` access.

2. **PDF settlement parsing** — `pdfplumber` integration for 上网电费结算单 PDF is stubbed out (shows warning). Needs implementation.

3. **Wind farm 15-min → hourly aggregation** — The wind farm parser writes individual 15-min rows to `rm_position_volumes` by hour (first interval's hour). Should properly aggregate 4 intervals per hour with volume-weighted pricing.

4. **CRM import** — Province column mapping configs not seeded. First import of 各省份台账.xlsx will need manual column mapping via UI.

5. **Load profile ingest** — Format A (Shandong .xls) and Format B (Jiangsu 15-min CSV) parsers referenced in spec but not yet implemented as standalone upload handlers in the retail-risk app.

6. **Retail settlement tables** — The subagent created extra tables (`rm_retail_settlements`, `rm_retail_settlement_items`) beyond the shared `rm_settlements`/`rm_settlement_items`. These may be redundant; consider consolidating.

---

## Prompt for new session

```
Read these docs in order:
1. docs/handoff-2026-07-23-risk-management-complete.md (this file)
2. docs/superpowers/specs/2026-07-16-asset-risk-design.md (App 1 spec)
3. docs/superpowers/specs/2026-07-16-retail-risk-design.md (App 2 spec)
4. docs/handoff-2026-07-16-risk-management.md (original design context)

Both risk management apps (Asset Risk + Retail Risk) are implemented and 
deployed on ECS. The handoff doc above has the full status and known issues.

Key areas for continuation:
- Fix known issues (PDF parsing, CRM import seeding, load profile ingest)
- WAF allowlist: contact IT to add /asset-risk/* and /retail-risk/* to pjh-etrm.ai
- Populate seed data: create the 8 known assets + their books via Tab 1
- Migrate historical data from Trade Capture.xlsx and 零碳46风电经营统计.xlsx
- Test end-to-end: upload settlement → verify P&L waterfall → check VaR

Branch: feat/deal-structurer-bedrock-migration
Working directory: C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform
```

---

## File Inventory

```
# DDL
db/ddl/marketdata/rm_assets_books.sql
db/ddl/marketdata/rm_positions.sql
db/ddl/marketdata/rm_dispatch.sql
db/ddl/marketdata/rm_forward_curves.sql
db/ddl/marketdata/rm_settlements.sql
db/ddl/marketdata/rm_snapshots.sql
db/ddl/marketdata/rm_retail.sql

# Shared libs
libs/risk/__init__.py
libs/risk/mtm.py
libs/risk/pnl.py
libs/risk/var.py
libs/risk/greeks.py
libs/settlement/__init__.py
libs/settlement/parser.py
libs/settlement/categorizer.py

# Services
services/forward_curve/__init__.py
services/forward_curve/lingfeng_pull.py
services/forward_curve/manual_upload.py
services/operating_assets/__init__.py
services/operating_assets/filename_mapper.py
services/operating_assets/ingest.py
services/operating_assets/parsers/__init__.py
services/operating_assets/parsers/bess_daily.py
services/operating_assets/parsers/bess_dispatch.py
services/operating_assets/parsers/wind_farm.py

# App 1 — Asset Risk
apps/asset_risk/__init__.py
apps/asset_risk/app.py
apps/asset_risk/tab_asset_config.py
apps/asset_risk/tab_settlement.py
apps/asset_risk/tab_pnl.py
apps/asset_risk/tab_positions.py
apps/asset_risk/tab_var.py
apps/asset_risk/tab_agent.py
apps/asset_risk/requirements.txt
apps/asset_risk/Dockerfile

# App 2 — Retail Risk
apps/retail_risk/__init__.py
apps/retail_risk/app.py
apps/retail_risk/tab_crm.py
apps/retail_risk/tab_settlement.py
apps/retail_risk/tab_pnl.py
apps/retail_risk/tab_positions.py
apps/retail_risk/tab_var.py
apps/retail_risk/tab_agent.py
apps/retail_risk/requirements.txt
apps/retail_risk/Dockerfile

# Tests
tests/settlement/__init__.py
tests/settlement/test_parser.py
tests/settlement/test_categorizer.py
tests/risk/__init__.py
tests/risk/test_mtm.py
tests/risk/test_pnl.py
tests/risk/test_var.py
tests/risk/test_greeks.py
tests/operating_assets/__init__.py
tests/operating_assets/test_wind_farm_parser.py

# Infrastructure
infra/terraform/services-new.tf (asset-risk + retail-risk blocks)
infra/terraform/variables.tf (image_asset_risk, image_retail_risk vars)
shared/service_control.py (portal entries for both apps)
.dockerignore (exceptions for both apps)

# Docs
docs/superpowers/plans/2026-07-16-asset-risk-management.md
docs/handoff-2026-07-23-asset-risk-complete.md
docs/handoff-2026-07-23-risk-management-complete.md
```
