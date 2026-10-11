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
| mengxi-dashboard | `bess-mengxi-dashboard` | **v33** (7a1fe8a1192a) | **48** | 2026-10-10 | this session: OpenInfraMap grid underlay + Chinese station-name TextLayer labels (toggle 站名标注) + psycopg[binary]; jq-swap td:47→48 |
| deal-structurer | `bess-platform-deal-structurer` | **v29** (27e9f57385f6) | **33** | 2026-09-25 | this session: screening NaN-node + SQL date-type fixes |
| bess-map (Quant) | `bess-map` | **v76** (cedb86400d29) | **114** | 2026-10-04 | this session: NaN-filter annual_real (arbitrage bars no longer vanish on warmup/forecast-hole days) |
| hermes | `bess-platform-hermes` | (latest hand-injected) | **185** | 2026-09-30 | this session: ancillary dedup patch (skip confirmed/superseded); 31 env vars |
| asset_risk | `bess-asset-risk` | v84 | 86 | 2026-09-23 | this session: merchant-exposure tile |
| data-ingestion (tt-api/enos) | `bess-data-ingestion` | v20260927 (575657c91df8) | :latest-driven | 2026-09-27 | this session: schema-variant frame guard (SheYang KeyError) |
| mengxi-reconcile (md_* ingestion) | `bess-mengxi-ingestion` | v20 | 21 | 2026-09-10 | this session: null-key row drop guard; launcher lambda pins td:21 |
| spot-market | `bess-spot-markets` | **v48** (6ceea6de90df) | **148** | 2026-10-08 | this session: intake hardening fixes from final review (EXISTS skip-guard, page_no guard, failed-pages warning) |
| portal | `bess-platform-portal` | v13 | 70 | ~2026-09-26 | per CLAUDE.md snapshot |
| gb-market | `bess-gb-market` | v107 | 30 | 2026-09-20 | parallel session: modo question rotation |
| lingfeng-ingest | `lingfeng-ingest` | **v10** (3f1d025fb50e) | **12** | 2026-10-06 | this session: residual-quantile forecast bands in capture pipeline + column_to_matrix KeyError guard |
| professor-topic-card | `bess-professor-topic-card` | **v4** | **4** | 2026-10-11 | this session: 「」/《》 inner-quote constraint fixes unparseable LLM JSON; one-shot Fargate task (no service), EventBridge Mondays 08:50 Beijing |

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
- 2026-10-01 — `bess-platform-lingfeng-ingest:11` ← `lingfeng-ingest:v9` (3686f1452375); folder-scan guard — `_resolve_province` (exact + longest-prefix) refuses non-province stems like 运行数据披露-_<dates>.xlsx; 8,880 re-created junk rows deleted same day (this session).
- 2026-10-01 — `bess-map:106` ← `bess-map:v71` (e5c062e1deeb); ranking exclusion 甘肃河西→both spellings (DB carries 甘肃西河) (this session).
- 2026-10-01 — `bess-map:108` ← `bess-map:v72` (d44283c1ee8c); ranking sorts by NET total (arb+cap+调频−系统运行费); sysopfee deduction (¥/kWh × charge energy at measured cycles); capacity re-sourced to curated province_cap_comp + fr pool-share fallback (30% / 20% Southern Grid); Dockerfile ancillary_revenue COPY fix (v70/v71 crashed on missing module); committed via temp-index pattern after OneDrive index.lock hang (this session).
- 2026-10-01 — `bess-map:109` ← `bess-map:v73` (81fd1ccaf6a2); 系统运行费 drawn as negative waterfall segment (px h-stack renders negatives positive — explicit base + go.Bar); sort fixed via categoryorder=array + autorange=reversed (px drops Categorical under facet_row); chart/KPI/table on 100MW/200MWh·2h + 100MW/400MWh·4h standard-station 万元 basis (this session).
- 2026-10-01 — `bess-map:111` ← `bess-map:v74` (23b2a524e2f3); 放电量补偿 capacity income stacked — 蒙西/蒙东 0.28 元/kWh, 山东 0.0705 元/kWh via `province_cap_comp` confirmed rows (rate<1 = discharge mode), 内蒙古（蒙东）fans out to both 蒙东/蒙西 per user; 河北南网 forecast table (volume-era) deleted + forecast/realized regeneration for 河北南网/河南/辽宁/黑龙江 launched same session (this session).
- 2026-10-02 — `bess-map:112` ← `bess-map:v75` (a7f1fcd3e05f); capture rate displayed as aggregate SUM(realized)/SUM(theoretical) — NaN-real days count 0, KPI shows overall aggregate (综合捕获率); 河南/辽宁/黑龙江 NaN-realized saga closed with --force + --force-theoretical regen (both flags needed: theo-skip + capture-freshness gates, see ERRORS.md 2026-10-01) (this session).
- 2026-10-04 — `bess-map:114` ← `bess-map:v76` (cedb86400d29); annual_real averages over evaluable days only (NULLIF NaN) — one warmup/forecast-hole NaN day poisoned AVG and erased every arbitrage bar under the realized basis (this session).

- 2026-10-04 — `bess-retail-risk:3` ← `bess-retail-risk:v3` (18d675fcab35); retail-risk P1 full build (8 tabs: trades/invoices/MTM ingestion live, recon + P&L bridge + per-book MtM; replaces Sep v2 shell). jq-swap from live tdArn rev 2 (td:3 registered), update-service force-new-deployment; tfvars set v3, NO terraform apply (parallel-session tf edits in tree + td has ignore_changes) (this session).
- 2026-10-06 — `bess-platform-lingfeng-ingest:12` ← `lingfeng-ingest:v10` (3f1d025fb50e); residual-quantile forecast bands in capture pipeline (`--with-quantiles` default-on, ols_rt_time_v1 backbone → new bands table, model `ols_rt_time_q_v1`) + column_to_matrix KeyError guard for schema-variant API responses (Jiangsu_SheYang no-`time`-column). First ECR push attempt EOF-failed silently despite exit 0 — verified absent via describe-images before re-push. Built 2026-10-05 (prior session), deployed this session.

- 2026-10-07 — `bess-retail-risk:4` ← `bess-retail-risk:v4` (a1e6b4e51c2c); hotfix — image ships `psycopg[binary]` (sqlalchemy>=2.0.38 makes psycopg v3 the default postgresql:// driver; v3 crashed ModuleNotFoundError on boot). Saga: first push silently failed (exit 0, v4 never landed — service circuit-broke on CannotPullContainerError for 7h serving v3's crash page); serial uploads (MaxConcurrentUploads=1 in Docker Desktop settings-store.json) fixed the concurrent-upload stalls; ALWAYS verify ECR pushes with describe-images (see ERRORS.md). jq-swap from live tdArn rev 3 (td:4), force-new-deployment, rollout COMPLETED, target healthy (this session).

## Known parallel-session coordination points

- **hermes td is hand-injected** (31 env vars; terraform `ignore_changes`) — jq-swap
  from current tdArn only; terraform apply on it would silently drop env vars.
- **deal-structurer, asset_risk, mengxi-dashboard tds** all have
  `ignore_changes=[container_definitions]` — terraform apply does nothing to
  images; use the jq-swap protocol.
- **The `terraform.tfvars` image tags lag reality** (e.g. said v56 while v83 was
  live). Manifest above is fresher; live td is freshest.
- 2026-10-07 — `bess-platform-spot-markets:147` ← `bess-spot-markets:v47` (92246d98d187); Market Intel Intake (spec/plan 2026-10-06): new 🧠 情报录入 tab in KB expander (batch upload → one vision call → review panel → commit), taxonomy +3 categories (market_intel/capacity_pipeline/ancillary_market), spot_knowledge_docs.province column, ingest_document_batch (order-independent dedup hash), intake_extract (max_tokens 16384 + metric vocabulary after fixture-surfaced defects), intake_routes (pipeline table marketdata.province_storage_pipeline + rate-draft writers with confirmed-row skip guards), get_storage_pipeline Strategist tool. jq-swap from live tdArn :145 (13 env preserved; a stale :146 existed, never deployed); NO terraform (parallel-session tf edits in tree). Live td was v45 — manifest row "v46" was a stale snapshot. 39/39 KB tests (this session).
- 2026-10-08 — `bess-platform-spot-markets:148` ← `bess-spot-markets:v48` (6ceea6de90df); intake hardening from final whole-branch review (commit a3a72e1): deterministic EXISTS-based rate skip-guard (confirmed/draft twin edge), page_no-null or-guard + contract test, failed-pages warning + all-failed extract skip. jq-swap from live td:147; NO terraform (parallel-session tf edits persist in tree) (this session).
- 2026-10-10 — `bess-platform-mengxi-dashboard:45` ← `bess-mengxi-dashboard:v30` (88180f470927); OpenInfraMap integration: Nodal Maps tab Grid-map section (pydeck underlay from staging.openinfra_*), services/openinfra extractor (gpkg→staging, PGURL+psycopg2-qualify fixes in v30), gpkg uploaded to s3://bess-uploader-data-chen-singp-2026/openinfra/CHN-2026-10.gpkg. jq-swap td:44→45 (ignore_changes service, terraform could not flip; tfvars broken by parallel session's line-160 paste at apply time) (this session).
- 2026-10-10 — `bess-platform-mengxi-dashboard:46` ← `bess-mengxi-dashboard:v31` (d23d1c47d868); substation PK (oim_fid, extent_tag, geom_type) — gpkg fid is unique per-table only, point/polygon collide (td:45 extract UniqueViolation at guangdong); extraction re-run with --recreate. td:45 superseded same hour (this session).
- 2026-10-10 — `bess-platform-mengxi-dashboard:47` ← `bess-mengxi-dashboard:v32` (390b1ec556b5); hotfix — image ships `psycopg[binary]` (today's rebuilds pulled sqlalchemy>=2.0.38 → bare postgresql:// resolves to psycopg v3 → ModuleNotFoundError app-wide; same as retail-risk v4). jq-swap td:46→47, rollout + bare-PGURL engine probe verified (this session).
- 2026-10-10 — `bess-platform-mengxi-dashboard:48` ← `bess-mengxi-dashboard:v33` (7a1fe8a1192a); Chinese station-name labels (TextLayer, 500kV+ stations) + 站名标注 toggle on the Grid-map section, per user feedback comparing with the offline preview map. jq-swap td:47→48 (this session).
- 2026-10-11 — `bess-platform-professor-topic-card:4` ← `bess-professor-topic-card:v4`; professor weekly Feishu topic card live — v1 psycopg-v3 ModuleNotFoundError → v2 dialect pin (`postgresql+psycopg2://`) → v3 llm.py raw-output logging → v4 PROPOSAL_SYSTEM 「」/《》 inner-quote constraint (model wrote raw `"` inside CJK JSON strings). First clean run: 4 proposals stored (marketdata.professor_topic_proposals, week 2026-10-05), card sent to owner, exit 0. terraform-managed (professor_topic_card.tf; 5 resources incl. EventBridge cron Mondays 08:50 Beijing). Incident: parallel-session stale-tree clobber staged deletions of services/professor/ + professor tf/variables blocks + xiyangjing column files; v3/v4 fixes existed only inside the Docker images — recovered via docker cp, restored column files from HEAD, committed 0a9da20 (this session).
