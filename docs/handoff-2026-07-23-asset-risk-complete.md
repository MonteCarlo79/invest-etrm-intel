# Handoff: Asset Risk Management App — Implementation Complete

**Date:** 2026-07-23  
**Branch:** `feat/deal-structurer-bedrock-migration`  
**Status:** App 1 fully implemented + deployed. App 2 (Retail Risk) ready to start.

---

## What was completed

### Implementation (6 commits)

| Commit | Description |
|--------|-------------|
| `feat(rm): add all rm_ DDL tables` | 6 SQL files, 10 tables (rm_assets, rm_books, rm_positions, rm_position_volumes, rm_dispatch_daily, rm_dispatch_plan, rm_forward_curves, rm_settlements, rm_settlement_items, rm_pnl_snapshots, rm_var_snapshots) |
| `feat(settlement): add parser and categorizer with tests` | `libs/settlement/parser.py` (format detection, Trade Capture, capacity compensation) + `categorizer.py` (Mengxi wind settlement rule). 8 tests. |
| `feat(risk): add mtm, pnl, var, greeks libraries with tests` | `libs/risk/` — mtm.py, pnl.py, var.py, greeks.py. 10 tests. |
| `feat(services): add forward curve and operating assets services` | `services/forward_curve/` (LingFeng pull + manual upload) + `services/operating_assets/` (BESS daily, BESS dispatch, wind farm parsers, filename mapper, ingest orchestrator). 2 tests. |
| `feat(app): add asset-risk Streamlit app with 6 tabs` | `apps/asset_risk/` — app.py + 6 tab modules (Asset Config, Settlement, P&L, Positions & MtM, VaR & Greeks, Agent) |
| `feat(app): add Dockerfile and requirements` | Dockerfile + requirements.txt |

### Deployment

| Item | Value |
|------|-------|
| ECR repo | `319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-asset-risk:v3` |
| ECS service | `bess-platform-asset-risk-svc` (cluster: `bess-platform-cluster`) |
| Task definition | `bess-platform-asset-risk:3` |
| Port | 8512 |
| ALB path | `/asset-risk/*` (priority 57, Cognito auth + forward) |
| baseUrlPath | `asset-risk` |
| DB | All 10 `rm_` tables applied to `marketdata` schema |
| Bedrock | ✅ Uses `shared/anthropic_client.make_client()`, `BEDROCK_REGION=ap-southeast-1` |
| Health check | `/asset-risk/_stcore/health` |
| Portal | Added to `shared/service_control.py` as "Asset Risk" card |

### Tests (20 passing)

```
tests/settlement/test_parser.py (4)
tests/settlement/test_categorizer.py (4)
tests/risk/test_mtm.py (3)
tests/risk/test_pnl.py (2)
tests/risk/test_var.py (3)
tests/risk/test_greeks.py (2)
tests/operating_assets/test_wind_farm_parser.py (2)
```

### Key deployment notes

1. **WAF blocks `/asset-risk/` via pjh-etrm.ai** — The corporate WAF (`Wafrule: 5` header) hasn't been updated to allow the new path. Access works via ALB direct URL. WAF allowlist needs updating externally.
2. **Cognito callback** — Added `https://bess-platform-alb-...amazonaws.com/oauth2/idpresponse` to Cognito client callback URLs for direct ALB access.
3. **.dockerignore** — Added `!libs/risk/`, `!libs/settlement/`, `!libs/options/`, `!services/forward_curve/`, `!services/operating_assets/`, `!apps/asset_risk/` as exceptions.
4. **terraform.tfvars** — Contains secrets; removed from git, stays in `.gitignore`. Manage locally only.

---

## What's next: Retail Risk (App 2)

The design spec is approved: `docs/superpowers/specs/2026-07-16-retail-risk-design.md`

### Shared libs already built (from App 1)

- `libs/risk/` — mtm.py, pnl.py, var.py, greeks.py ✅
- `libs/settlement/` — parser.py, categorizer.py ✅

### New work for App 2

1. **Retail-specific DDL** — 4 new tables:
   - `rm_customers` (customer master with channel_name, fixed_spread, revenue_share_ratio)
   - `rm_customer_contracts` (type: fixed/indexed/peak_offpeak/indexed_band; price_formula JSONB; K1/K2/K3)
   - `rm_customer_profiles` (daily×hourly customer load)
   - `rm_crm_import_configs` (province-keyed column mapping for 各省份台账.xlsx)

2. **Extended settlement logic** — Retail settlement categorization (province-specific pricing rules: Hunan, Hubei, Zhejiang, Shandong, Jiangsu)

3. **Streamlit app** at `apps/retail-risk/` (port 8513, ECR: `bess-retail-risk`):
   - 6 tabs: CRM, Settlement, Realised P&L, Positions & MtM, VaR & Greeks, Agent
   - CRM tab: customer import from 各省份台账.xlsx, per-province column mapping
   - Settlement: retail-side with 售电分成, 服务费, 滚搓, 代理用电量 categories
   - P&L: retail procurement waterfall (annual + monthly + DA + RT)
   - Positions: customer load profiles, procurement vs consumption matching

4. **Docker + ECS deploy** — Same pattern as App 1

---

## Prompt for new session

```
Read these docs in order:
1. docs/handoff-2026-07-23-asset-risk-complete.md (this file — what's done)
2. docs/superpowers/specs/2026-07-16-retail-risk-design.md (App 2 spec, Approved)
3. docs/handoff-2026-07-16-risk-management.md (original design context)
4. docs/handoff-2026-07-20-risk-management-plan.md (codebase patterns)

App 1 (Asset Risk) is fully implemented and deployed. Now implement App 2 
(Retail Risk Management) following the same patterns:

- Invoke writing-plans skill → save to docs/superpowers/plans/2026-07-16-retail-risk-management.md
- After plan approval, execute using subagent-driven-development
- Follow Bedrock migration pattern (shared/anthropic_client.make_client())
- Port: 8513, ECR: bess-retail-risk, baseUrlPath: retail-risk
- Reuse libs/risk/ and libs/settlement/ — extend categorizer for retail provinces

Key context:
- Branch: feat/deal-structurer-bedrock-migration (or cost-optimisation)
- .dockerignore requires !exceptions for new paths
- WAF allowlist is external — new paths need manual approval
- terraform.tfvars is local-only (secrets, not in git)
- All DDL is raw SQL in db/ddl/marketdata/, pattern: CREATE TABLE IF NOT EXISTS marketdata.rm_*
```

---

## File inventory (App 1)

```
db/ddl/marketdata/rm_assets_books.sql
db/ddl/marketdata/rm_positions.sql
db/ddl/marketdata/rm_dispatch.sql
db/ddl/marketdata/rm_forward_curves.sql
db/ddl/marketdata/rm_settlements.sql
db/ddl/marketdata/rm_snapshots.sql
libs/settlement/__init__.py
libs/settlement/parser.py
libs/settlement/categorizer.py
libs/risk/__init__.py
libs/risk/mtm.py
libs/risk/pnl.py
libs/risk/var.py
libs/risk/greeks.py
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
docs/superpowers/plans/2026-07-16-asset-risk-management.md
```
