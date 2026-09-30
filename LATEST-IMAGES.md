# LATEST-IMAGES.md — Deployed Image Manifest

**Purpose:** multiple Claude sessions deploy to the same ECR repos and ECS services
concurrently. Before tagging an image or registering a task def, READ this file
(and verify against the live task def). After deploying, APPEND to the history
and update the service's row. This file is a coordination aid — **the live task
def is ground truth**; if the manifest disagrees, trust AWS and fix the row.

## Rules for every deployer

1. **Before building:** pick next tag = `max(tag seen below, tag on live td) + 1`.
   Verify live td image first:
   `aws ecs describe-services --cluster bess-platform-cluster --service <svc> --query 'services[0].taskDefinition'`.
2. **Tag by image ID, both names** (`docker tag <id> <repo>:vN` and `<ecr>/<repo>:vN`
   + `:latest`). Never rely on `docker build -t` alone — it only re-tags the short
   name (v25 incident, 2026-09-23).
3. **Register the new td from the service's CURRENT tdArn** (never from an older
   revision you registered yourself, never from bare family name).
4. **After deploying:** append one history line below and set the service's
   `current` row. Never edit or delete history lines; never tag below the max here.
5. **Never roll a service to an older td revision** — if a rollback is genuinely
   needed, diff the target revision's env list against the running one first.

## Current images (update on deploy)

| Service | ECR repo | Current image | TD | Deployed | By / notes |
|---|---|---|---|---|---|
| mengxi-dashboard | `bess-mengxi-dashboard` | **v28** (cc144bacdbcb) | **43** | 2026-09-27 | this session: add-candidate form in Asset Registry; built from current td:41 (parallel session's v27/nodal-trading), superset |
| deal-structurer | `bess-platform-deal-structurer` | **v29** (27e9f57385f6) | **33** | 2026-09-25 | this session: screening NaN-node + SQL date-type fixes |
| bess-map (Quant) | `bess-map` | **v70** (9daa6307c75e) | **105** | 2026-09-30 | this session: 调频收入 manual entry + draft review in Data Management |
| hermes | `bess-platform-hermes` | (latest hand-injected) | **185** | 2026-09-30 | this session: ancillary dedup patch (skip confirmed/superseded); 31 env vars |
| asset_risk | `bess-asset-risk` | v84 | 86 | 2026-09-23 | this session: merchant-exposure tile |
| data-ingestion (tt-api/enos) | `bess-data-ingestion` | v20260927 (575657c91df8) | :latest-driven | 2026-09-27 | this session: schema-variant frame guard (SheYang KeyError) |
| mengxi-reconcile (md_* ingestion) | `bess-mengxi-ingestion` | v20 | 21 | 2026-09-10 | this session: null-key row drop guard; launcher lambda pins td:21 |
| spot-market | `bess-spot-markets` | v46 | 145 | ~2026-09-26 | per CLAUDE.md snapshot |
| portal | `bess-platform-portal` | v13 | 70 | ~2026-09-26 | per CLAUDE.md snapshot |
| gb-market | `bess-gb-market` | v107 | 30 | 2026-09-20 | parallel session: modo question rotation |
| lingfeng-ingest | `lingfeng-ingest` | **v8** (968335748051) | **10** | 2026-09-29 | this session: auto-cols volume-trap fix (出清电量 was picked as rt_price for 河北南网) |

## Deploy history (append-only, newest at bottom)

- 2026-09-10 — `bess-mengxi-reconcile:21` ← `bess-mengxi-ingestion:v20`; null-key drop guard for md_id_cleared_energy (this session).
- 2026-09-19 — `bess-asset-risk:86` ← `bess-asset-risk:v84`; merchant-exposure tile in Settlement tab (this session). Note: parallel session had v83 live while tfvars said v56 — always check live td before tagging.
- 2026-09-20 — `bess-map:100` ← `bess-map:v66`; strategy promote loop + quantile bands (this session).
- 2026-09-21 — `bess-map:101` ← `bess-map:v67`; leaderboard anchor fix (this session). ECR token refresh + `git push` retries were needed (network flakiness).
- 2026-09-22 — EventBridge rule `bess-platform-bess-eval-daily` cron(30 23 * * ? *) created (CLI, not terraform) for nightly bands + evaluation (this session).
- 2026-09-23 — `mengxi-dashboard:36` ← `bess-mengxi-dashboard:v25` (eb7851648909); asset registry seeded (9 rows) (this session). Incident: ECR v25 tag initially pointed at pre-fix image because `docker build -t` only re-tags short name — pushed by image ID to fix.
- 2026-09-23 — `deal-structurer:32` ← `bess-platform-deal-structurer:v28`; 新资产筛选 tab (this session).
- 2026-09-25 — `deal-structurer:33` ← `bess-platform-deal-structurer:v29`; screening NaN-node + SQL date-type fixes (this session).
- 2026-09-27 — `mengxi-dashboard:43` ← `bess-mengxi-dashboard:v28` (cc144bacdbcb); add-candidate form (this session). Note: td:42 (from td:36) was registered but NOT deployed — parallel session's v27/td:41 was live; new td built from td:41 instead. Also: first push hit wrong local image (tmp context reaped) and was rebuilt + repushed over the bad tag.
- 2026-09-27 — `bess-data-ingestion:v20260927` + `:latest` (575657c91df8); schema-variant frame guard (this session).
- 2026-09-29 — `bess-platform-lingfeng-ingest:10` ← `lingfeng-ingest:v8` (968335748051); auto-cols 电量-exclusion fix — 2026-09 LingFeng layout's `…-实时出清电量` volume columns were picked as rt_price (河北南网 volume-as-price contamination; rebuild of 河北南网 series in progress same day). Built from td:9 (this session).
- 2026-09-29 — `bess-map:104` ← `bess-map:v69` (1957fa4ad2fd); ranking revenue stack (capacity payment from new `province_capacity_price` + 新疆 调频 from new `province_ancillary_revenue`), 甘肃河西 exclusion, σ annualised-vol labels on spread chart; 河北南网 series fully rebuilt + capture re-run same day. ECR push needed token re-login (this session).
- 2026-09-30 — `bess-platform-hermes:184` ← `bess-platform-hermes:v20260930` (c816df53d9e8); ancillary 调频收入 KB screener (`services/hermes/ancillary_screener.py`) + `/frrev` chat command + monthly cron day-5 13:00 UTC; env guard 31/31 (this session).
- 2026-09-30 — `bess-map:105` ← `bess-map:v70` (9daa6307c75e); Data Management 调频收入 section — manual entry form (confirmed) + draft review (this session).
- 2026-09-30 — `bess-platform-hermes:185` ← `bess-platform-hermes:v20260930b` (d34cb6596ac4); ancillary upsert dedup — skip confirmed/superseded (province, month, metric) so scans can't resurrect dismissed rows (this session).

## Known parallel-session coordination points

- **hermes td is hand-injected** (31 env vars; terraform `ignore_changes`) — jq-swap
  from current tdArn only; terraform apply on it would silently drop env vars.
- **deal-structurer, asset_risk, mengxi-dashboard tds** all have
  `ignore_changes=[container_definitions]` — terraform apply does nothing to
  images; use the jq-swap protocol.
- **The `terraform.tfvars` image tags lag reality** (e.g. said v56 while v83 was
  live). Manifest above is fresher; live td is freshest.
