
# AGENTS.md

## Project identity

This repository is **not** a greenfield AI project.

It is an expansion of an existing live / partially live `bess-platform` into a broader **multi-asset power investment, trading, and asset intelligence platform**.

The target system must eventually support:
- BESS
- wind farms
- retail trading books
- coal-fired power plants
- later VPPs and other flexibility assets

The platform should become an integrated:
- market intelligence system
- trading and execution intelligence system
- portfolio and risk intelligence system
- investment intelligence system
- asset operations intelligence system
- reliability / data quality control system

The core business objective is a positive flywheel:

1. trading and operations generate first-hand market, dispatch, settlement, and behavioral data
2. that data improves forecasts, risk models, operating decisions, and investment decisions
3. improved investment decisions expand asset and market participation
4. wider participation generates more proprietary data and deeper intelligence
5. AI agents turn this into scalable decision advantage

The platform is **AI-assisted decision-making first**, and only later narrow supervised automation.

---

## Current foundation: existing `bess-platform`

The existing `bess-platform` is already live / partially live and must be treated as the **first production module** of the future system, not something to be replaced.

It already includes, at minimum:
- Streamlit-based apps/pages
  - portal
  - uploader
  - BESS dashboards/pages
  - Inner Mongolia app / regional analytics
  - BESS map / asset visualization
- existing data and analytics logic
  - market data ingestion jobs
  - BESS spread calculations
  - capturable spread calculations
  - theoretical spread calculations
  - operating / asset analytics
  - local scripts and pipelines for BESS use cases
- existing infrastructure patterns
  - Python pipelines
  - Postgres
  - Terraform
  - ECS / Lambda / scheduled job patterns
  - RBAC / auth
  - deployment / scheduling logic
- existing `marketdata` database usage
  - `marketdata` and existing historical market data structures are important and must remain compatible

### How to think about the current BESS platform

Treat `bess-platform` as:
- Module 0 / first live asset module
- first real trading + operations intelligence module
- first data source and first dashboard family
- first testbed for common services
- first source of reusable ingestion / analytics / reporting patterns

The future system should add layers **around** the current project rather than replace it.

---

## Critical implementation principle

**Do not propose rebuilding the BESS platform from scratch.**

Preferred philosophy:
- preserve existing working apps and jobs
- wrap old components where possible
- lightly refactor where needed
- extract reusable pieces gradually
- add thin shared layers around the existing project
- keep backward compatibility where practical
- use BESS as the first implementation pattern for the broader platform

Always ask first:

**Can this be wrapped, reused, or lightly refactored instead of rewritten?**

If yes, prefer that.

---

## Repo / system shape to assume

Current repo is roughly:

- `auth/`
- `shared/`
- `agents/`
- `reports/`
- `apps/`
  - `portal/`
  - `uploader/`
  - `it-dev-agent/`
  - `execution-agent/`
  - `portfolio-risk-agent/`
  - `strategy-agent/`
  - `bess-inner-mongolia/`
    - `im/`
    - `pipeline/`
    - `shared/`
  - `dashboard/`
- `bess-marketdata-ingestion/`
- `services/`
  - `bess_map/`
  - `bess-inner-mongolia/`
  - `common/`
  - `loader/`
  - `portal/`
- `data/`
- `backup/`
- `infra/`
  - `terraform/`
    - `mengxi-ingestion/`
- `db/`
- `config/`

Visible key files/modules include:
- `marketdata.dump`
- docker-compose
- root requirements
- Dockerfiles inside app/service folders
- `services/bess_map` with dispatch / optimisation scripts
- `apps/portal` app + Dockerfile + requirements
- `apps/uploader` app + Dockerfile + requirements
- `apps/bess-inner-mongolia/im` app + Dockerfile + requirements

Design new modules **additively around this structure** with minimal disruption.

---

## Existing infrastructure and deployment rules

Preserve and reuse current patterns:
- Streamlit app modules
- ECS / Fargate deployment
- EventBridge schedules
- RDS / Postgres
- Terraform
- ALB / Cognito / RBAC flow
- existing deployment / scheduling conventions

Any new module should fit the existing platform rather than create a disconnected parallel stack.

---

## `marketdata` / existing data-layer rules

`marketdata` and existing historical market data storage are already important.

Do not assume:
- the user wants to rename `marketdata`
- the user wants a greenfield database
- the user wants a full redesign of the current data layer

Instead:
- preserve compatibility with existing price tables and ingestion jobs
- integrate new modules with current `marketdata` first
- use wrappers, views, standardized downstream tables, and registries where needed
- keep schema changes additive and backward-compatible where feasible

New components should assume `marketdata` is an existing source-of-truth foundation for market-price-related workflows.

---

## Broader target architecture

Preferred layered architecture:

### 1. Existing live module layer
- current BESS apps
- current uploader
- current market ingestion
- current portal
- current Inner Mongolia / BESS dashboards

### 2. Shared enterprise registries and metadata
- `core.asset_master`
- `core.portfolio_master`
- `core.book_master`
- `core.data_source_registry`
- `core.document_registry`
- `raw_data.file_registry`
- `raw_data.file_manifest`
- report registry
- job runs / alert tables

### 3. Shared services
- intake / routing
- normalization
- validation
- summarization
- report generation
- portal publishing
- alerting
- health monitoring

### 4. Asset-specific analytical modules
- BESS first
- wind next
- retail next
- coal later

The architecture must allow the current BESS project to keep operating while gradually being absorbed into the broader framework.

---

## Data zone model

Preferred data pattern:

### Raw zone
- original incoming files
- raw source documents
- preserve source-of-truth files

### Standardized zone
- parsed / cleaned intermediate structures

### Canonical zone
- mapped into broader enterprise schema

Apply this without breaking current BESS workflows.

---

## Non-tabular document model

For non-tabular data such as:
- settlement invoices
- third-party market daily reports
- policy documents
- rules and notices

Preferred approach:
- raw originals in object storage
- metadata + extracted text + structured fields in Postgres
- connect to document registry and downstream summaries
- do not use NotebookLM or a knowledge workspace as system-of-record
- external research tools can be supplementary, not primary storage

---

## Agent model to preserve and support

Current target AI agent model:

### 1. Market Strategy & Investment Intelligence Agent
- monitors market data, policy, reports, and opportunity signals
- supports strategic summaries and investment intelligence
- should consume existing BESS and `marketdata` outputs where possible

### 2. Enterprise Portfolio, Risk & Capital Allocation Agent
- explains P&L, risk, exposures, and cross-asset portfolio implications
- should eventually use outputs from current BESS analytics

### 3. Trading, Dispatch & Execution Agent
- updates market and position data
- supports trading recommendations, dispatch recommendations, backtests, and settlement reconciliation
- should begin by wrapping current BESS workflows and marketdata ingestion

### 4. Platform Reliability, Data Quality & Control Agent
- monitors jobs, data freshness, app health, and anomalies
- should wrap and monitor current BESS jobs with minimal changes

### Additional practical capability
Build a **Data Intake & Routing** capability early:
- collect files/data from teams
- register them
- classify them
- route them into correct existing or future pipelines
- normalize them into compatible forms

---

## Current rollout priority

Expand from the current BESS platform in this order:

1. build common data intake / routing / registry / monitoring layers
2. integrate current BESS platform into enterprise shell with minimal changes
3. build BESS daily operations + trading intelligence cockpit
4. extend common framework to wind
5. extend to retail trading books
6. extend to coal later

So BESS remains the first production implementation pattern.

---

## Trading module expansion rules

Build a proper trading module under apps and services.

### Target app structure
- `apps/trading/bess/mengxi/market_monitor/app.py`
- `apps/trading/bess/mengxi/pnl_attribution/app.py`
- `apps/trading/bess/mengxi/pnl_attribution/calc.py`

### Target service structure
- `services/trading/bess/mengxi/run_tt_province_loader.py`
- `services/trading/bess/mengxi/run_tt_asset_loader.py`
- `services/trading/bess/mengxi/run_expected_dispatch.py`
- `services/trading/bess/mengxi/run_perfect_foresight_dispatch.py`
- `services/trading/bess/mengxi/run_pnl_refresh.py`

---

## TT loader rules

### Province loader
`services/loader/province_misc_to_db_v2.py` is the province-level TT ENOS loader.

Use it for daily scheduled refresh of:
- province spot prices
- province supply/demand fundamentals
- province market context
- in-house wind where enabled

### Asset loader
`services/common/focused_assets_data.py` is the asset-specific TT ENOS loader.

Use it for daily scheduled refresh of:
- nodal actual prices
- nodal forecast prices
- TT strategy where available
- asset-specific data needed for trading analytics

### Loader implementation rules
- make loaders ECS-friendly and env-driven
- no desktop-only assumptions
- no Spyder-only runtime assumptions
- no container-local state as system-of-record
- CSV exports are allowed as secondary outputs
- RDS / Postgres remains the primary persistent state
- avoid destructive changes to current tables

---

## Same-nature data from multiple sources

The platform has overlapping datasets from:
- TT ENOS loaders
- uploader app / exchange-based loaders
- Mengxi-specific ingestion
- manual Excel / operational workbooks

These may describe the same business concept but come from different source systems and may deviate.

Do **not** collapse them blindly.

Instead:
- preserve source-specific physical tables
- define dataset/source metadata
- define canonical views for downstream analytics and agents
- make authoritative-source choices explicit

### Default authoritative source policy
Unless explicitly changed by the user:
- province-level fundamentals / TT market context: TT province loader
- TT nodal prices / TT nodal forecast / TT strategy: TT asset loader
- Inner Mongolia cleared dispatch / cleared energy for BESS assets: existing Mengxi-specific ingestion is preferred over TT when overlap exists and TT is incomplete or stale

---

## Asset naming rules

Use stable internal `asset_code` values.

### Asset codes
- `suyou`
- `wulate`
- `wuhai`
- `wulanchabu`
- `hetao`
- `hangjinqi`
- `siziwangqi`
- `gushanliang`

### Alias mapping
- `suyou` = 景蓝乌尔图储能电站 = 苏右储能 = SuYou = Mengxi_SuYou
- `wulate` = 远景乌拉特储能电站 = 乌拉特中期储能 = WuLaTe = Mengxi_WuLaTe
- `wuhai` = 富景五虎山储能电站 = 乌海储能 = WuHai = Mengxi_WuHai
- `wulanchabu` = TT / node-style 乌兰察布 asset key where applicable
- `hetao` = 景怡查干哈达储能电站 = 河套储能
- `hangjinqi` = 悦杭独贵储能电站 = 杭锦旗储能
- `siziwangqi` = 景通四益堂储能电站 = 四子王旗储能
- `gushanliang` = 裕昭沙子坝储能电站 = 谷山梁储能

Never assume Chinese names, TT English names, workbook labels, and DB names are interchangeable without explicit alias mapping.

---

## P&L attribution rules

### Core objective
Build daily DB-backed P&L attribution, not weekly/monthly CSV-only analytics.

### Supported scenarios
- `perfect_foresight_unrestricted`
- `perfect_foresight_grid_feasible`
- `cleared_actual`
- `nominated_dispatch`
- `tt_forecast_optimal`
- `tt_strategy`

### Critical rule
Scenario coverage varies by asset.

Do not assume every asset has TT forecast or TT strategy.

#### Richer-coverage assets
- `suyou`
- `wulate`
- `wuhai`
- `wulanchabu` (depending on actual available inputs)

#### Partial-coverage assets
- `hetao`
- `hangjinqi`
- `siziwangqi`
- `gushanliang`

For partial-coverage assets, at minimum compare:
- perfect foresight unrestricted
- perfect foresight grid feasible
- cleared actual

### Attribution ladder
Where available, use:
- `grid_restriction_loss = PF_unrestricted - PF_grid_feasible`
- `forecast_error_loss = PF_grid_feasible - TT_forecast_optimal`
- `strategy_error_loss = TT_forecast_optimal - TT_strategy`
- `nomination_loss = TT_strategy - nominated_dispatch`
- `execution_clearing_loss = nominated_dispatch - cleared_actual`

For assets without TT forecast or TT strategy, leave those loss buckets null / unavailable rather than inventing values.

---

## Compensation rules

Compensation is **not fixed globally**.

It varies by asset and by month.

Use:
- `core.asset_monthly_compensation(asset_code, effective_month, compensation_yuan_per_mwh, ...)`

Use `350` only as a fallback default if asset/month data is missing.

Always include compensation in scenario P&L:
- `compensation_revenue = discharge_mwh * compensation_yuan_per_mwh`
- `total_pnl = market_revenue + compensation_revenue`

---

## Core metadata / reporting objects to add

These should be additive and sit around the current BESS platform.

### Core / metadata
- `core.asset_master`
- `core.portfolio_master`
- `core.book_master`
- `core.data_source_registry`
- `core.document_registry`
- `core.asset_alias_map`
- `core.asset_scenario_availability`
- `core.asset_monthly_compensation`

### Raw / ops
- `raw_data.file_registry`
- `raw_data.file_manifest`
- `ops.job_runs`
- `ops.alerts`
- `ops.data_freshness_status`

### Agent / reporting
- `agent.agent_summaries`
- `agent.findings`
- `reports.generated_reports`
- `reports.bess_asset_daily_scenario_pnl`
- `reports.bess_asset_daily_attribution`

### Canonical views
Prefer additive views such as:
- `canon.nodal_rt_price_15min`
- `canon.nodal_forecast_price_15min`
- `canon.cleared_dispatch_15min`
- `canon.nominated_dispatch_15min`
- `canon.grid_restriction_15min`
- `canon.province_fundamentals_15min`
- `canon.scenario_dispatch_15min`

Do not remove or rename existing `hist_*` tables as part of an initial task.

---

## App design rules

### Market monitor app
The market monitor app should:
- be read-first
- use RDS tables, not local script imports
- show loader freshness
- compare actual vs forecast clearly
- improve color contrast and layout versus legacy local app

### P&L attribution app
The P&L attribution app should:
- read reporting tables / canonical views from RDS
- support fleet summary and asset drill-down
- render only the scenarios actually available for each asset
- show compensation rate, compensation revenue, market revenue, and total P&L explicitly

---

## Terraform / infra rules

Use the existing ECS / EventBridge / RDS / CloudWatch pattern.

Do not create a parallel infra stack unless necessary.

Target scheduled tasks include:
- TT province loader
- TT asset loader
- Mengxi P&L refresh
- later: expected dispatch / perfect foresight / reconciliation jobs

Infrastructure changes must be incremental and consistent with current modules.

Terraform provisions infrastructure, not business-table DDL.

DB DDL belongs under `db/ddl/...`.

---

## Working style rules for Codex

1. Make minimal, high-confidence changes.
2. Preserve current working behavior unless the task explicitly requires change.
3. Prefer additive changes over rewrites.
4. Before editing, inspect nearby files and imports to follow repo conventions.
5. Keep business logic explicit; do not hide commercial assumptions.
6. Do not silently change source-of-truth policy.
7. When overlapping datasets exist, report the conflict instead of guessing.
8. When asset naming is ambiguous, route through alias mapping rather than ad hoc string matching.
9. Do not remove existing routes/apps/jobs unless explicitly told to.
10. Keep code production-oriented: env-driven, ECS-friendly, stateless app containers.
11. Put DB DDL under `db/ddl/...` even if temporary safety-net DDL also exists in job code.
12. Keep scope narrow. If you find adjacent issues, report them, but do not widen scope without instruction.
13. After each task:
    - summarize files changed
    - summarize assumptions made
    - list unresolved data-policy questions
    - run relevant checks if available

---

## What not to do

- do not redesign the whole platform
- do not propose replacing `marketdata` immediately
- do not create a second disconnected local workflow
- do not convert commercial logic into generic placeholders that lose business meaning
- do not hardcode monthly compensation in code if table-driven values are intended
- do not assume missing TT forecast / strategy exists for the additional Mengxi assets
- do not treat this as a demo app

---

## Preferred delivery style

When implementing a task:
1. inspect existing files first
2. state the smallest viable implementation plan
3. edit only the required files
4. keep diffs narrow
5. explain schema additions clearly
6. list follow-up tasks separately rather than performing uncontrolled extra refactors