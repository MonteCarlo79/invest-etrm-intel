# Claude Code Session Handover
Repo: MonteCarlo79/invest-etrm-intel
Handover written: 2026-04-03
Outgoing session: local Claude Code (desktop) — 2026-04-02/03
Incoming session: next local Claude Code

---

## Repo state at close

| Field | Value |
|---|---|
| Active branch | `fix/pnl-data-path-manual` |
| HEAD | `b350619` — pushed to origin |
| Origin in sync | Yes — local HEAD == origin HEAD |
| Untracked files | `claude-local-wip.patch`, `claude-pnl.diff`, `claude-pnl.patch`, `pnl_py_only.patch`, `infra/terraform/trading-bess-mengxi/.terraform.lock.hcl`, `scripts/` |
| Unstaged deletions | 27 `.minimax/skills/minimax-docx/` files (tracked, deleted locally) — unrelated to workstreams, appears intentional |

---

## Branch map

```
main                                        →  fbc21ef  (origin)  ← base
feature/mengxi-strategy-diagnostics        →  f0f2452  (local + origin)  ← Workstream A
fix/pnl-data-path-manual                   →  b350619  (local + origin)  ← Workstream B  ← ACTIVE
feature/mengxi-strategy-dagnositics        →  972e290  (origin only, typo, obsolete)
feature/openclaw-mengxi-terraform-takeover →  3874447  (origin, OpenClaw-owned — do not touch)
```

`fix/pnl-data-path-manual` is strictly ahead of `feature/mengxi-strategy-diagnostics` by 3 commits:
- `96b38e4` fix(pnl): seed alias map, enable canon views, add Mengxi ingest schedule
- `6ad8913` fix(pnl): correct wulanchabu alias values in ASSET_ALIAS_MAP
- `b350619` docs: update session handover with accurate branch/commit state

All strategy diagnostics code is reachable from `fix/pnl-data-path-manual`.

---

## What was completed — code is done, all pushed

### Workstream A — Strategy Diagnostics (`feature/mengxi-strategy-diagnostics`)

**Status: code-complete, runtime-unverified**

All 6 files committed in `ddbc760`:
- `services/bess_inner_mongolia/__init__.py`
- `services/bess_inner_mongolia/queries.py`
- `services/bess_inner_mongolia/peer_benchmark.py`
- `services/bess_inner_mongolia/strategy_diagnostics.py`
- `apps/bess-inner-mongolia/im/pages/strategy_diagnostics.py`
- `apps/bess-inner-mongolia/im/Dockerfile`

Key facts verified this session:
- Dockerfile clean: `COPY services/bess_inner_mongolia` present, no pnl_attribution copy
- Page auto-discovered by Streamlit 1.43.2 `pages/` mechanism — no change to `app.py` needed
- `create_schema_and_table()` called at module load in `inner_pipeline.py` before any writes
- `queries.py`, `peer_benchmark.py`, `strategy_diagnostics.py` service reviewed — no bugs found
- Auth pattern (`require_role` before `st.set_page_config`) is consistent with `app.py`

### Workstream B — PnL Attribution (`fix/pnl-data-path-manual`)

**Status: code-complete, terraform not yet applied, runtime-unverified**

Three root-cause gaps fixed:

| ID | Gap | Fix | File |
|---|---|---|---|
| G1 | `core.asset_alias_map` never seeded | Idempotent `CREATE TABLE IF NOT EXISTS` + 32-row `INSERT ... ON CONFLICT DO NOTHING` in `ensure_report_tables()` | `run_pnl_refresh.py` |
| G2 | `PNL_ENABLE_CANON_COMPAT_VIEWS` not set in ECS env | Added env var to `mengxi_pnl_refresh` task definition | `schedules.tf` |
| G3 | No EventBridge schedule for Mengxi Excel ingest | New log group + task def + event rule (`30 0 * * ? *` = 08:30 CST) + event target | `schedules.tf` |

Also fixed (`6ad8913`): `apps/trading/bess/mengxi/pnl_attribution/calc.py`
- wulanchabu `dispatch_unit_name_cn`: `景通四益堂储能电站` → `景通红丰储能电站`
- wulanchabu `short_name_cn`: `四子王旗储能` → `乌兰察布储能`
- In-memory fallback only; does not affect DB-backed price/dispatch lookups

Seed count: **32 rows** (not 31 as stated in the original browser handover).
Validation query should expect 32.

---

## What the next session must do

### 1. Open PRs — `gh` CLI not installed, use browser

**PR 1 — Strategy Diagnostics** (can open now, runtime validation pending):
```
https://github.com/MonteCarlo79/invest-etrm-intel/compare/main...feature/mengxi-strategy-diagnostics?expand=1
```

**PR 2 — PnL data-path fix** (base on PR 1, shows only the 3 incremental commits):
```
https://github.com/MonteCarlo79/invest-etrm-intel/compare/feature/mengxi-strategy-diagnostics...fix/pnl-data-path-manual?expand=1
```

Or single combined PR to main:
```
https://github.com/MonteCarlo79/invest-etrm-intel/compare/main...fix/pnl-data-path-manual?expand=1
```

### 2. Terraform apply (activates G2 + G3)

```bash
cd infra/terraform/trading-bess-mengxi
terraform plan -var="image_mengxi_ingest=<ecr-uri>"
terraform apply
```

`image_mengxi_ingest` = ECR image built from `bess-marketdata-ingestion/providers/mengxi/`.
Check existing ECR repos or CI pipeline for the URI. The `.terraform.lock.hcl` in the
working tree suggests `terraform init` has already been run locally.

### 3. Runtime validation after next ECS run (or manual trigger)

```sql
-- Expected: 32
SELECT COUNT(*) FROM core.asset_alias_map WHERE active_flag = TRUE;

-- Expected: rows for suyou, wulate, wuhai, wulanchabu at minimum
SELECT asset_code, COUNT(*) FROM reports.bess_asset_daily_scenario_pnl
GROUP BY 1 ORDER BY 1;

SELECT asset_code, COUNT(*) FROM reports.bess_asset_daily_attribution
GROUP BY 1 ORDER BY 1;
```

```bash
# Check ECS logs for WARNING sentinel lines (zero rows written):
aws logs filter-log-events \
  --log-group-name /ecs/<stack-name>/mengxi-pnl-refresh \
  --filter-pattern "WARNING" \
  --start-time $(date -d '1 hour ago' +%s000)
```

### 4. Strategy diagnostics container validation

Build and run `apps/bess-inner-mongolia/im/` from `feature/mengxi-strategy-diagnostics`.
Confirm the strategy diagnostics page appears in Streamlit sidebar and queries return
non-empty results. Requires `inner_mongolia_bess_results` to have data (inner pipeline
must have run at least once).

---

## Known unknowns (all runtime, no code changes needed)

| Item | Status |
|---|---|
| `image_mengxi_ingest` ECR URI | Unknown — discover from ECR console or CI |
| `marketdata.md_id_cleared_energy` schema vs `run_pnl_refresh.py` expectations | Not confirmed |
| `canon.nodal_rt_price_15min` populated after compat views built | Depends on `hist_mengxi_*_clear_15min` tables having data |
| `inner_mongolia_bess_results` / `inner_mongolia_nodal_clusters` row count | Not confirmed |
| PnL attribution page showing non-None values end-to-end | Not confirmed |
| Strategy diagnostics page rendering with live data | Not confirmed |

---

## Key files

| Path | Purpose |
|---|---|
| `services/trading/bess/mengxi/run_pnl_refresh.py` | Main PnL refresh job — G1 alias seed + debug logging |
| `infra/terraform/trading-bess-mengxi/schedules.tf` | ECS schedules — G2 env var + G3 ingest task/rule |
| `apps/trading/bess/mengxi/pnl_attribution/calc.py` | PnL calc + ASSET_ALIAS_MAP — wulanchabu corrected |
| `apps/trading/bess/mengxi/pnl_attribution/app.py` | Streamlit PnL page — not modified |
| `db/ddl/core/asset_alias_map_seed.sql` | Canonical alias seed (32 rows) — source of truth |
| `db/ddl/core/canon_mengxi_compat_views.sql` | Canon compat views DDL — aligned with Python |
| `apps/bess-inner-mongolia/im/pages/strategy_diagnostics.py` | Strategy diagnostics Streamlit page |
| `services/bess_inner_mongolia/` | Strategy diagnostics service layer |
| `bess-marketdata-ingestion/providers/mengxi/run_pipeline.py` | Excel ingest pipeline — needs ECR image |

---

## Working tree cleanup (low priority, not blocking)

- Delete patch files when no longer needed: `claude-local-wip.patch`, `claude-pnl.diff`, `claude-pnl.patch`, `pnl_py_only.patch`
- The 27 `.minimax/skills/minimax-docx/` deletions: decide whether to `git restore` (keep the skill) or `git add -u && git commit` (remove it). Neither is urgent.

---

## Tool ownership reminder

| Branch | Owner |
|---|---|
| `fix/pnl-data-path-manual` | Claude Code |
| `feature/mengxi-strategy-diagnostics` | Claude Code |
| `feature/openclaw-mengxi-terraform-takeover` | OpenClaw — do not touch |
| `main` | Protected — no direct merge |

Do not let Codex write to either active Claude Code branch.
OpenClaw may inspect and run `terraform apply` but must not commit code.
