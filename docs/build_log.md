# BESS Platform — Build Log

> Generated 2026-04-27. Covers work done in the `cost-optimisation` branch from 2026-04-17 to present.

---

## Summary

| Area | Status |
|---|---|
| Dashboard infrastructure (7 apps, dual-mode local/AWS) | Done |
| P&L attribution — full calculation engine | Done |
| Inner Mongolia ops Excel ingestion | Done |
| Canon ETL — nodal price backfill | Done |
| BESS dispatch strategy comparison workflow | Done |
| RT-only forecast models (no DA market) | Done |
| LP dispatch optimiser with rolling window | Done |
| Trading performance agent (Claude-powered, PDF, email) | Done |
| Daily ops strategy report (Streamlit + PDF) | Done |
| Decision model library scaffold | Done |
| Options Cockpit (vol surface, per-asset calibration) | Done |
| Realization & fragility monitoring services | Done |
| AWS infra cost reduction (5 services suspended) | Done |
| Terraform drift reconciliation | Done |
| File-drop watcher → auto-ingestion + report trigger | Done |

---

## 2026-04-17

### Dual-mode dashboard deployment
- Added `AUTH_MODE=dev` bypass across all Streamlit apps so they can run locally without Cognito/SSO
- Implemented `auth/rbac.py` pattern: ALB OIDC headers in AWS, no-op in dev
- Fixed Streamlit `set_page_config` ordering violation in uploader app (must precede `require_role`)
- All 7 dashboards launchable locally via `run_all_local_dashboards.ps1`

### Settlement & AWS migration docs
- Added settlement knowledge pool design doc
- Added AWS migration runbook and validation docs

---

## 2026-04-18

### P&L attribution — full calculation engine
- Restored `pnl-attribution` app from commit `b353403`
- Restored `spot-markets` dashboard from `feature/spot-markets-dashboard` branch
- Added `PNL_CALC_ENABLED` env-var gate (auto-on in dev, off in ECS unless explicitly set)
- Embedded full recalculation sidebar: lookback days, compensation rate, progress bar, cache clear + rerun

---

## 2026-04-19

### Terraform hardening
- Reconciled 4 infrastructure drifts so `terraform plan` is clean:
  - `aws_db_instance.pg`: `publicly_accessible = true`, `max_allocated_storage = 1000`
  - `aws_security_group.rds`: `lifecycle { ignore_changes = [ingress] }` (laptop CIDR rule)
  - `aws_ecs_cluster.this`: `lifecycle { ignore_changes = [configuration] }` (ECS Exec logging)
- Suspended 5 non-essential ECS services to zero (`desired_count = 0`) without deleting resources:
  `bess-map`, `uploader`, `model-catalogue`, `pnl-attribution`, `spot-markets`

### Decision model library
- Added `libs/decision_models/` scaffold: `model_spec`, `registry`, `contracts` primitives
- Added BESS dispatch strategy comparison workflow (`workflows/strategy_comparison.py`)
  - 6 scenarios: PF unrestricted, PF grid-feasible, cleared-actual, nominated, TT-forecast-optimal, TT-strategy
  - Per-scenario P&L, discharge/charge MWh, daily cycle count

### Ops runbook
- `docs/aws_dashboard_restore_runbook.md`: step-by-step guide to restore the 5 suspended dashboards
  - Exact `terraform.tfvars` changes, targeted plan commands, AWS CLI validation, ALB health checks, rollback steps

---

## 2026-04-20

### Inner Mongolia ops Excel ingestion pipeline
- `services/ops_ingestion/inner_mongolia/` — full CLI entrypoint
- Handles two naming conventions:
  - `【X月X日】内蒙储能电站运营统计.xlsx`
  - `谷山梁、杭锦旗、苏右、四子王旗储能日报YYYY-MM-DD.xlsx`
- Flags: `--dir`, `--file`, `--recursive`, `--dry-run`, `--force`, `--verify-prices`
- Transient-error retry, zero-row classification, dry-run mode
- 109 tests passing

### Decision model library — first build wave
- `dispatch_pnl_attribution` model
- `realization_monitor` service: tracks actual vs. expected P&L per asset per day
- `fragility_monitor` service: detects structural degradation in dispatch performance

### Platform design docs
- `docs/platform-design/`: analytics architecture, data contracts, implementation design, roadmap

---

## 2026-04-21

### BESS trading performance agent
- `libs/decision_models/agents/trading_performance_agent.py` — Claude-powered daily review
- `services/ops/run_trading_agent.py` — CLI runner with `--date`, `--asset-code`, `--send-email`, `--dry-run`
- Generates PDF report via reportlab, uploads to S3, sends email via SMTP
- `REPORT_EMAIL_TO` env var supports comma-separated addresses
- Terraform: `trading_performance_agent` ECS task definition + ECR repo + EventBridge schedule `cron(0 23 * * ? *)`

### Daily strategy performance agent
- Operator-grade daily strategy comparison: compares TT-strategy vs PF-optimal vs cleared
- Portfolio totals across all 4 Inner Mongolia assets
- Report history viewer in Streamlit dashboard (`apps/trading-performance-agent/app.py`)

### Monitoring hardening (B1-B6 sprint)
- `DATA_ABSENT` and `INDETERMINATE` result states
- Preflight identity checks
- Structured log output
- Idempotency tests

---

## 2026-04-22

### Options Cockpit (new app)
- `apps/options-cockpit/` — BESS energy optionality valuation tool
- Vol surface calibration from historical price data
- Solar-based peak/off-peak hour detection
- Per-asset vol calibration
- Fixed moneyness calculation, zero/negative price handling
- User guide: `docs/cockpit_user_guide.md`
- Terraform: ECS task definition, ECR repo, ALB path `/options-cockpit`, port 8507

### Docker build context fix
- `.dockerignore` updated to exclude `infra/`, `archived/`, `backup/`, `data/` from all image builds

---

## 2026-04-23

### File-drop watcher
- `services/ops/watch_dispatch_files.py`: monitors `data/operations/bess/inner-mongolia/2026/` for new Excel files
- On new file: runs ops ingestion → triggers trading agent → sends PDF report to configured email

---

## 2026-04-24

### Canon ETL — nodal price backfill
- ETL script populates `canon.nodal_rt_price_15min` from `md_id_cleared_energy` source table
- Covers all 4 Inner Mongolia assets: 谷山梁, 杭锦旗, 苏右旗, 四子王旗
- Backfilled 2026-03-01 → 2026-04-23

---

## 2026-04-25

### RT-only forecast models
- `libs/decision_models/models/rt_forecast.py`: price forecast models for markets with no day-ahead clearing
- Applied to all 4 Inner Mongolia assets
- `run_forecast_dispatch_suite.py`: unified single-pass call for forecast + dispatch

### P&L refresh fixes
- `run_pnl_refresh.py`: PGURL→DB_DSN normalisation, repo-root `sys.path`, TCP keepalives
- `shared/agents/db.py`: PGURL fallback when DB_DSN unset
- `--start-date` / `--end-date` CLI args for manual backfill runs

### Mengxi dashboard — dispatch & P&L tabs
- Added Dispatch and P&L Attribution tabs to the Mengxi dashboard
- Per-asset vol calibration wired into Options Cockpit

### Trading agent fix
- Fixed PF price map key mismatch that caused zero P&L on some days
- Report language switched to English; Chinese asset names retained in data

---

## 2026-04-26

### Options Cockpit infrastructure fixes
- Increased pip timeout and retries in Dockerfile
- Removed invalid `enableCORS`/`enableXsrfProtection` Streamlit flags
- Fixed deprecated `width="stretch"` → `use_container_width=True`
- ECS security group port range extended to 8507; task memory bumped

---

## 2026-04-27

### Dispatch optimiser — LP rolling window
- `libs/decision_models/dispatch/optimizer.py`: added `window_days` parameter for LP optimisation horizon
- Fixes issue where single-day solve ignored multi-day state-of-charge continuity

### Strategy comparison — 7 correctness fixes
- PF upper bound constraint
- PDF report formatting
- `id_cleared` units (MWh → MW conversion)
- `tt_forecast_optimal` P&L sign

### Daily ops hang fix
- Eliminated ~1-hour hang in "Run daily analysis" triggered by blocking subprocess call

---

## Infrastructure state (current)

| Service | ECS desired | Notes |
|---|---|---|
| `bess-platform-portal-svc` | 1 | Always on |
| `bess-platform-inner-mongolia-svc` | 1 | Always on |
| `bess-platform-bess-map-svc` | 0 | Suspended — see restore runbook |
| `bess-platform-uploader-svc` | 0 | Suspended — see restore runbook |
| `bess-platform-model-catalogue-svc` | 0 | Suspended — see restore runbook |
| `bess-platform-spot-markets-svc` | 0 | Suspended — see restore runbook |
| `bess-platform-pnl-attribution-svc` | 0 | Suspended — see restore runbook |
| `bess-platform-options-cockpit-svc` | 1 | New — deployed Apr 22 |
| `bess-platform-trading-performance-agent-svc` | 1 | New — deployed Apr 21, nightly at 23:00 UTC |

Restore runbook: `docs/aws_dashboard_restore_runbook.md`
