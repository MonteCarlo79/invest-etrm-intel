Claude Code Session Handover
Repo: MonteCarlo79/invest-etrm-intel
Date: 2026-04-02
Outgoing session: local Claude Code (desktop)
Incoming session: next local Claude Code

---

## Corrections to previous handover (2026-04-02 browser session)

The browser session handover contained three factual errors:

| Claim | Reality |
|---|---|
| PnL fix on `feature/mengxi-strategy-diagnostics` | Fix landed on `fix/pnl-data-path-manual` |
| Commit hash `36b32ea` | Actual hash `96b38e4` |
| Commit unpushed (403 on push) | Already pushed to `origin/fix/pnl-data-path-manual` |

---

## Current branch state

```
main                                   →  fbc21ef  (origin, base)
feature/mengxi-strategy-diagnostics   →  f0f2452  (local + origin)  ← strategy diag
fix/pnl-data-path-manual               →  6ad8913  (local + origin)  ← PnL fix  ← HEAD
feature/mengxi-strategy-dagnositics   →  972e290  (origin only, typo, obsolete)
feature/openclaw-mengxi-terraform-takeover  →  3874447  (origin, OpenClaw-owned)
```

`fix/pnl-data-path-manual` is strictly ahead of `feature/mengxi-strategy-diagnostics`
by 2 commits:
- `96b38e4` — fix(pnl): seed alias map, enable canon views, add Mengxi ingest schedule
- `6ad8913` — fix(pnl): correct wulanchabu alias values in ASSET_ALIAS_MAP

All strategy diagnostics work is reachable from `fix/pnl-data-path-manual`.

---

## What was completed in this session

### Workstream A — Strategy Diagnostics (feature/mengxi-strategy-diagnostics)

Status: **code-complete, runtime-unverified**

All 6 files committed (commit `ddbc760`):
- `services/bess_inner_mongolia/__init__.py`
- `services/bess_inner_mongolia/queries.py`
- `services/bess_inner_mongolia/peer_benchmark.py`
- `services/bess_inner_mongolia/strategy_diagnostics.py`
- `apps/bess-inner-mongolia/im/pages/strategy_diagnostics.py`
- `apps/bess-inner-mongolia/im/Dockerfile`

Dockerfile: clean — `COPY services/bess_inner_mongolia` present, no unrelated pnl_attribution copy.
Page registration: auto-discovered by Streamlit 1.43.2 `pages/` mechanism.
Table init: `create_schema_and_table()` called at module load in `inner_pipeline.py` before writes.

**Runtime-unverified:**
- Container has not been built or run
- `inner_mongolia_bess_results` / `inner_mongolia_nodal_clusters` data in DB not confirmed
- Queries runtime-checked: no

### Workstream B — PnL Attribution (fix/pnl-data-path-manual)

Status: **code-complete, terraform not yet applied, runtime-unverified**

Three root-cause fixes committed and pushed:

| Gap | Fix | Commit |
|---|---|---|
| G1: `core.asset_alias_map` never seeded | Idempotent CREATE + 32-row seed in `ensure_report_tables()` | 96b38e4 |
| G2: `PNL_ENABLE_CANON_COMPAT_VIEWS` not set | Added to `mengxi_pnl_refresh` ECS env in `schedules.tf` | 96b38e4 |
| G3: No Excel ingest schedule | New EventBridge rule + ECS task def in `schedules.tf` (cron 30 0 * * ? * = 08:30 CST) | 96b38e4 |

Note: seed has **32 rows** (not 31 as stated in the browser handover).
The previous handover's validation query `SELECT COUNT(*) = 31` should expect 32.

Also fixed (`6ad8913`): `apps/trading/bess/mengxi/pnl_attribution/calc.py`
- wulanchabu `dispatch_unit_name_cn`: `景通四益堂储能电站` → `景通红丰储能电站`
- wulanchabu `short_name_cn`: `四子王旗储能` → `乌兰察布储能`
(in-memory fallback only; does not affect DB-backed lookups)

---

## Immediate next steps for next session

### 1. Open PRs (gh CLI not installed — use browser)

**PR 1 — Strategy Diagnostics:**
```
https://github.com/MonteCarlo79/invest-etrm-intel/compare/main...feature/mengxi-strategy-diagnostics?expand=1
```

**PR 2 — PnL data path fix (base on PR 1 or directly on main):**
```
https://github.com/MonteCarlo79/invest-etrm-intel/compare/feature/mengxi-strategy-diagnostics...fix/pnl-data-path-manual?expand=1
```
This shows only the 2 PnL fix commits (not the full history from main).

### 2. Terraform apply (G2 + G3)

```bash
cd infra/terraform/trading-bess-mengxi
terraform plan -var="image_mengxi_ingest=<ecr-uri>"
terraform apply
```

`image_mengxi_ingest` = ECR image from `bess-marketdata-ingestion/providers/mengxi/`.
Check existing ECR repos or CI pipeline for the URI.

### 3. Runtime validation after next ECS run

```sql
-- Expected: 32
SELECT COUNT(*) FROM core.asset_alias_map WHERE active_flag = TRUE;

-- Expected: rows for suyou, wulate, wuhai, wulanchabu (at minimum)
SELECT asset_code, COUNT(*) FROM reports.bess_asset_daily_scenario_pnl
GROUP BY 1 ORDER BY 1;

SELECT asset_code, COUNT(*) FROM reports.bess_asset_daily_attribution
GROUP BY 1 ORDER BY 1;
```

```bash
# Check ECS logs for WARNING lines (zero-row sentinel):
aws logs filter-log-events \
  --log-group-name /ecs/<stack-name>/mengxi-pnl-refresh \
  --filter-pattern "WARNING" \
  --start-time $(date -d '1 hour ago' +%s000)
```

### 4. Container validation for strategy diagnostics

Build and run `apps/bess-inner-mongolia/im/` from `feature/mengxi-strategy-diagnostics`.
Confirm the strategy diagnostics page appears in the sidebar and queries return non-empty results
(requires `inner_mongolia_bess_results` to have data — pipeline must have run at least once).

---

## Known unknowns (still runtime-unverified)

| Item | Status |
|---|---|
| `image_mengxi_ingest` ECR URI | Unknown — must discover from ECR or CI |
| `marketdata.md_id_cleared_energy` schema compatibility | Not confirmed |
| `canon.nodal_rt_price_15min` populated after compat views built | Depends on `hist_mengxi_*_clear_15min` tables existing |
| `inner_mongolia_bess_results` / `inner_mongolia_nodal_clusters` have data | Not confirmed |
| End-to-end PnL page showing non-None values | Not confirmed |
| Strategy diagnostics page runtime | Not confirmed |

---

## Working tree state notes

- 27 tracked files under `.minimax/skills/minimax-docx/` are deleted (unstaged) — unrelated to both workstreams, appears intentional
- Untracked working artifacts: `claude-local-wip.patch`, `claude-pnl.diff`, `claude-pnl.patch`, `pnl_py_only.patch` — superseded by committed fixes, safe to delete

---

## Files of interest

| Path | Purpose |
|---|---|
| `services/trading/bess/mengxi/run_pnl_refresh.py` | Main P&L refresh job — G1/G2 fixes here |
| `infra/terraform/trading-bess-mengxi/schedules.tf` | ECS schedules — G2/G3 Terraform resources here |
| `apps/trading/bess/mengxi/pnl_attribution/calc.py` | P&L calc + ASSET_ALIAS_MAP — wulanchabu corrected |
| `apps/trading/bess/mengxi/pnl_attribution/app.py` | Streamlit PnL page — not modified |
| `db/ddl/core/asset_alias_map_seed.sql` | Canonical alias seed (32 rows) — source of truth |
| `db/ddl/core/canon_mengxi_compat_views.sql` | Canon compat views DDL — aligned with Python implementation |
| `apps/bess-inner-mongolia/im/pages/strategy_diagnostics.py` | Strategy diagnostics Streamlit page |
| `services/bess_inner_mongolia/` | Strategy diagnostics service layer |
| `bess-marketdata-ingestion/providers/mengxi/run_pipeline.py` | Excel ingest pipeline — needs ECR image |
