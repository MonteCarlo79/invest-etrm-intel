# BESS-Map + LingFeng Handoff — 2026-05-23

## Branch
`cost-optimisation` · HEAD: `2bb3fb9`

---

## What was fixed this session

### 1. bess-map app.py — 4 bugs fixed (commit `10de9b6`)
| Bug | Fix |
|-----|-----|
| `load_avg_cycles` queried non-existent table `bess_dispatch_hourly` with column `ts` | Fixed to `spot_dispatch_hourly_theoretical` / `datetime` |
| `load_avg_economics` averaged realized revenue across all 3 models | Added `model` parameter + CROSS JOIN pattern; updated IRR tab and agent tool call sites |
| `load_coverage` O(n²) join (Cartesian product before GROUP BY) | Replaced with 3 pre-aggregated subqueries |
| `forecast_engine.py:530` deprecated `fillna(method="ffill/bfill")` (prediction path) | Fixed to `.ffill().bfill()` for pandas 2.x |

### 2. LingFeng ingestion outage — fixed
- **Root cause**: LingFeng account password was changed on 2026-05-21, breaking all 29-province ingest. Error: `"Login did not redirect away from login page within 15 s."` (collector.py line 94).
- **Fix**: Credentials updated in `config/.env`.
- **Diagnostic added**: `collector.py` now saves `debug/lingfeng/login_fail_YYYYMMDD_HHMMSS.png` on any login failure + logs page body text.

### 3. Task Scheduler kill — fixed (commit `993d639`)
- **Root cause**: Task had `ExecutionTimeLimit = PT2H`. With 29 markets × ~27 min/market = ~13h ingest, the task was killed at 06:00 (2h after 04:00 start), leaving only the first 4-5 markets processed daily.
- **Fix**: `fix_task_action.ps1` updated to `PT16H`. Run the script to apply (requires UAC elevation).

---

## Pending actions for new session

### A — Apply the Task Scheduler fix (user runs manually, requires UAC)
```
! powershell -ExecutionPolicy Bypass -File services\lingfeng\fix_task_action.ps1
```

### B — Backfill May 21-22 data gap (all 29 provinces missed)
```bash
py services/lingfeng/run_daily.py --markets all --start-date 2026-05-21 --end-date 2026-05-22
```
⚠ Takes ~13 hours. Run as background task or overnight.

### C — Fix stale ops_log zombie for 湖北
Process was killed mid-run on 2026-05-20, leaving `status='running'` forever in the ops log dashboard. Clean it up:
```sql
UPDATE marketdata.data_ops_log
SET status='failed', finished_at=NOW(), message='Killed by Task Scheduler (timeout)'
WHERE op_name='lingfeng_ingest' AND status='running' AND started_at < NOW() - INTERVAL '6 hours';
```

### D — Investigate fundamentals ingest bug for 四川 / 浙江
Error: `ValueError: Cannot set a DataFrame with multiple columns to the single column _dt` in `run_fundamentals_ingest.py`. This means those two provinces' `spot_fundamentals_hourly` isn't being updated → affects `ols_fundamentals_v1` accuracy. Bug is in `services/bess_map/run_fundamentals_ingest.py`.

### E — Build and deploy bess-map:v41
`infra/terraform/terraform.tfvars` is pre-bumped to `bess-map:v41`. Image not yet built/pushed. Contains all fixes from item 1 above.
```bash
# Build (from repo root — build context includes services/bess_map/ and shared/)
docker build -t bess-map:v41 -f apps/bess-map/Dockerfile .
# Push to ECR and apply terraform
```

### F — Fix 蒙东 zeros + 福建 SSL gap (data in DB is bad)
Scripts are written and committed at `services/lingfeng/fix_data_gaps.py`. Not yet executed.
```bash
# Step 1 — dry run
python services/lingfeng/fix_data_gaps.py --dry-run

# Step 2 — apply (deletes audit.province_progress for 蒙东, clears derived tables)
python services/lingfeng/fix_data_gaps.py

# Step 3 — re-ingest 蒙东 (Feb–Apr 2026)
python services/lingfeng/run_daily.py --markets 蒙东 --start-date 2026-02-01 --end-date 2026-04-30 --models ols_rt_time_v1,naive_rt_ar17,ols_fundamentals_v1 --force-capture

# Step 4 — re-ingest 福建 (Nov 2025–Mar 2026, small chunks)
python services/lingfeng/run_daily.py --markets 福建 --start-date 2025-11-01 --end-date 2026-03-31 --models ols_rt_time_v1,naive_rt_ar17,ols_fundamentals_v1 --chunk-days 7 --force-capture
```

---

## Key file locations

| File | Purpose |
|------|---------|
| `services/lingfeng/collector.py` | Playwright login + download — saves `debug/lingfeng/login_fail_*.png` on failure |
| `services/lingfeng/run_daily.py` | Main pipeline orchestrator |
| `services/lingfeng/fix_task_action.ps1` | Re-register Task Scheduler task (UAC elevation) — now sets PT16H limit |
| `services/lingfeng/fix_data_gaps.py` | 蒙东 + 福建 DB cleanup script |
| `services/bess_map/forecast_engine.py` | `ols_fundamentals_v1` model — pandas `.ffill().bfill()` fix applied |
| `apps/bess-map/app.py` | Streamlit app — 4 bugs fixed in `load_avg_cycles`, `load_avg_economics`, `load_coverage` |
| `config/.env` | LingFeng credentials: `LINGFENG_USERNAME`, `LINGFENG_PASSWORD` |

---

## Pipeline architecture reminder

```
Daily at 04:00 SGT (Task Scheduler → run_daily.bat → run_daily.py):
  Phase 1: For each of 29 markets (sequential):
    → collector.py: login + download Excel (~30s download, ~19 min price ingest, ~8 min fundamentals)
    → run_all_provinces.py: price ingest → spot_prices_hourly (UPSERT)
    → run_fundamentals_ingest.py: fundamentals → spot_fundamentals_hourly
  Phase 2: Capture for all 3 models × 2 durations (2h, 4h):
    → run_capture_pipeline.py: theoretical LP → RT forecast → capture → bess_capture_daily

Models: ols_rt_time_v1, naive_rt_ar17, ols_fundamentals_v1
```

**Typical total runtime**: ~13h ingest + ~4h capture = ~17h. Task now allowed 16h (will capture stale data after that). Consider parallelising ingest as a future optimisation.
