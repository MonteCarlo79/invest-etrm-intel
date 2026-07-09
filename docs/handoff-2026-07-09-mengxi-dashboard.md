# Handoff — Mengxi Dashboard + Nodal PF Pre-compute (2026-07-09)

> **Branch:** `cost-optimisation` | **Last commit:** `8771dab`
> **Working directory:** `C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform`

---

## What was done this session

### 1. Section 8 CSV ingest — chunked reads (fixed overnight hang)

**File:** `apps/mengxi-dashboard/app.py` ~line 1116

Large CSVs (240 MB) caused the ingest to hang overnight because `read_csv` loaded the
entire file before upserting. Fixed by streaming in 50,000-row chunks:

```python
_CHUNK = 50_000
for _chunk_df in _pd_ingest.read_csv(_entry["path"], encoding="utf-8-sig",
                                      dtype={"time_order_96": "Int64"}, chunksize=_CHUNK):
    _n_upserted = _nodal_upsert(_chunk_df.to_dict("records"), _ingest_engine)
    _file_total += _n_upserted
    _ingest_append_log(f"  … {_label}: {_file_total:,} rows so far")
```

---

### 2. Section 9 — 南方区域 day-ahead nodal Excel ingest (new)

**File:** `apps/mengxi-dashboard/app.py` ~line 1148 onward

**Source path:**
```
data/market-fundamentals/【0.区域级】/南方区域/2-数据/日前节点电价数据/YYYYMM/YYYY-MM-DD日前节点电价查询.xlsx
```

**Target table:** `marketdata.md_shanxi_nodal_price_96` (same table as Inner Mongolia nodal data)

**Excel format:**
- Row 0: title (skipped)
- Row 1: headers — `地区`, `节点名称`, then 96 time columns `HH:MM` (00:00–23:45)
- Provinces covered: 广东, 广西, 云南, 贵州, 海南

**What the section does:**
1. Shows a coverage pivot table (Province × Month → days stored) from DB
2. Scans the OneDrive folder for `.xlsx` files matching the filename pattern
3. Let user select files → ingest (melt to long format, compute `time_order_96`, upsert)

---

### 3. Nodal PF daily pre-compute (fixes 27-min YTD timeout on /report mengxi)

**Root cause:** YTD MILP (190 days × 40 plants) was timing out at 25 minutes.

**Solution: pre-compute nightly and cache in DB.**

#### New table
```sql
CREATE TABLE IF NOT EXISTS reports.nodal_pf_daily (
    data_date   DATE  NOT NULL,
    plant_name  TEXT  NOT NULL,
    score_2h    FLOAT,
    score_4h    FLOAT,
    computed_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (data_date, plant_name)
);
```

#### New function
`services/hermes/mengxi_ranking_report.py` — `compute_and_store_nodal_pf_daily(pg_url, data_date, plant_names)`
- Runs single-day perfect-foresight MILP for all plants
- Upserts results into `reports.nodal_pf_daily`

#### Report now reads cache first
`_nodal_ranks_for()` inside `mengxi_ranking_report.py` — tries `_query_nodal_pf_daily_ranks()` 
(reads pre-computed cache); falls back to live MILP only if cache is empty.

#### Hermes scheduler (22:30 UTC daily)
`services/hermes/app.py` — new job added:
```python
scheduler.add_job(
    lambda: _compute_nodal_pf_daily_with_plants(_mengxi_pg_url),
    "cron", hour=22, minute=30,
)
```

#### `/backfill-daily` Feishu command
Triggers backfill from ECS (inside VPC — fast, reliable DNS). Usage:
```
/backfill-daily                         # 2025-01-01 → yesterday
/backfill-daily 2025-06-01              # from date → yesterday
/backfill-daily 2025-06-01 2025-12-31   # explicit range
```
Skips dates that already have ≥20 plants stored (idempotent).

#### Local backfill script
`scripts/backfill_nodal_pf_daily.py` — same logic as Feishu command, runs locally:
```bash
py scripts/backfill_nodal_pf_daily.py --dry-run        # show gaps
py scripts/backfill_nodal_pf_daily.py --start 2025-01-01
```
**Note:** Local machine hits RDS over public internet — DNS failures on ~50% of dates.
**Use `/backfill-daily 2025-01-01` in Feishu instead** for the initial backfill.

---

### 4. Local launcher fixes

**File:** `local_launcher.py` (gitignored — not in repo)

- `_get_running_services()`: switched from `subprocess.run(timeout=10)` to `Popen + communicate(timeout=5)` to fix blank page on Windows
- Mengxi Dashboard card: added `local_image` + `dockerfile` fields
- Added `Build` button → runs `docker build -t mengxi-dashboard-local:latest -f apps/mengxi-dashboard/Dockerfile .` in background
- Start button is disabled with "Start (image missing)" label when local image doesn't exist

---

## What still needs to be done

### A. Send `/backfill-daily 2025-01-01` in Feishu

**After the next Hermes deploy**, send this command in the Hermes Feishu bot to fill
`reports.nodal_pf_daily` from 2025-01-01 to yesterday. Takes ~minutes from ECS vs hours locally.

### B. Deploy Hermes to ECS

The changes in `services/hermes/app.py` (scheduler job + `/backfill-daily` command) are committed
but not yet deployed. Run:

```bash
docker build --no-cache -t hermes-service -f apps/hermes-service/Dockerfile .
docker tag hermes-service:latest 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-platform-hermes:latest
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-platform-hermes:latest
aws ecs update-service --cluster bess-platform-cluster --service bess-platform-hermes-svc --force-new-deployment --region ap-southeast-1
```

After deploy, send `/backfill-daily 2025-01-01` in Feishu.

### C. Deploy Mengxi Dashboard to ECS (when ready)

Section 9 is only in the local image. When ready to go to ECS:

```bash
docker build -t bess-mengxi-dashboard:v12 -f apps/mengxi-dashboard/Dockerfile .
docker tag bess-mengxi-dashboard:v12 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-mengxi-dashboard:v12
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-mengxi-dashboard:v12
# Then update ECS task definition to use :v12
```

### D. Ingest 南方区域 Excel files

Once mengxi-dashboard is running locally (or on ECS), go to Data Management → Section 9 and ingest the available Excel files.

### E. Monitor 22:30 UTC scheduler

Check CloudWatch logs (`/ecs/bess-platform`, hermes stream) tonight at 22:30 UTC to confirm:
- Daily pre-compute runs (`compute_and_store_nodal_pf_daily`)
- 23:00 report shows `"using pre-computed daily cache"` in logs

---

## Key files touched

| File | What changed |
|------|-------------|
| `apps/mengxi-dashboard/app.py` | Section 8 chunked CSV + Section 9 南方区域 Excel |
| `services/hermes/app.py` | 22:30 UTC scheduler job + `/backfill-daily` command |
| `services/hermes/mengxi_ranking_report.py` | `compute_and_store_nodal_pf_daily` + cache-first `_nodal_ranks_for` |
| `scripts/backfill_nodal_pf_daily.py` | NEW — local backfill CLI |
| `local_launcher.py` | Build button for local images (gitignored) |

---

## Architecture summary

```
22:30 UTC daily (ECS scheduler)
  └─ compute_and_store_nodal_pf_daily(yesterday, all 40 plants)
       └─ single-day MILP per plant → upsert reports.nodal_pf_daily

23:00 UTC daily (Hermes /report mengxi)
  └─ _nodal_ranks_for(start, end)
       ├─ try: _query_nodal_pf_daily_ranks() ← reads cache (fast, <1s)
       └─ fallback: live MILP (slow, 25min for YTD)
```
