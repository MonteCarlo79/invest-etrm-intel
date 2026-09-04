# BESS Map — Lingfeng Backfill Handoff (2026-06-28)

## What Was Fixed This Session

**Bug:** The lingfeng daily ECS run (`bess-platform-lingfeng-ingest-svc`) was crashing
with `RuntimeError: Market '...' not found in dropdown after scrolling` when it hit a
province whose name is no longer in the LingFeng platform's dropdown (specifically 青海
at position 28 of 29). Because there was no `except Exception` handler, the error
propagated and killed the entire run — all provinces after the failing one were never
processed.

**Fix applied:** `services/lingfeng/run_daily.py` — added `except Exception` after
`except CredentialError` in `run_pipeline()`. Now a market that can't be found in the
dropdown logs a warning, marks all its chunks as failed, and **continues to the next
province** instead of crashing.

**Deployed:** `lingfeng-ingest:v5` / ECS task def `bess-platform-lingfeng-ingest:6`
(pushed and service updated 2026-06-28 ~12:33 UTC).

**Commit:** `eb11947` on branch `cost-optimisation` — pushed to GitHub.

---

## Current Data Coverage (as of 2026-06-28)

Provinces with stale or limited data in `marketdata.spot_prices_hourly`:

| Province (decoded) | Last Date | Days | Notes |
|---|---|---|---|
| 江西 | 2025-09-23 | 176 | Not in LingFeng dropdown — skipped each run |
| 海南那悦 | 2025-10-29 | 121 | Sub-region, not in _ALL_MARKETS |
| 海南礼记 | 2025-10-29 | 121 | Sub-region, not in _ALL_MARKETS |
| 豫中东/豫西/豫北/豫南 | 2025-12-29 | 363 | Henan sub-regions, not in _ALL_MARKETS |
| 甘肃西河 | 2026-01-30 | 91 | Sub-region, not in _ALL_MARKETS |
| 运行数据披露 | 2026-05-31 | 117 | Operational disclosure data, not a province |
| 青海 | 2026-06-20 | 135 | In _ALL_MARKETS but not found in LingFeng dropdown — being skipped with warning since ~Jun 21 |
| 蒙东 | 2026-06-26 | 143 | In _ALL_MARKETS, started collecting from 2026-02-04 only |
| 四川 | 2026-06-27 | 144 | In _ALL_MARKETS, started collecting from 2026-02-04 only |

Most provinces (山东, 山西, 湖北, 浙江, 江苏, etc.) are current through Jun 26-27.

---

## What Still Needs Attention

### 1. Verify 青海 and 江西 on LingFeng Platform (IMPORTANT)
With the fix, both are now **skipped gracefully** (warning logged, run continues).
But it's unknown whether they are:
- Renamed in the dropdown (e.g., "青海省" instead of "青海")
- No longer available on the LingFeng subscription
- A temporary platform issue

**To investigate:**
- Check CloudWatch logs `ecs-lingfeng-ingest` after the next daily run (20:00 UTC)
  for the warning lines for 青海 and 江西
- Log into https://lingfeng-saas.tradingthink.cn manually and check the market dropdown

**To fix if the name changed:** Update `_ALL_MARKETS` in `services/lingfeng/run_daily.py`
with the correct name, rebuild, and redeploy.

### 2. Backfill 青海 Gap (Jun 21–28)
If 青海 becomes available again, backfill the gap via Feishu/Telegram:
```
/lf_run 2026-06-20:2026-06-28
```
Or via bess-map Data Management tab → Batch Backfill → select 青海, date range Jun 20–today.

### 3. Monitor Tomorrow's Daily Run
After deploying the fix, the next daily run at **04:00 CST (20:00 UTC)** on 2026-06-29
should run all 29 markets without crashing. Check logs:
```bash
aws logs tail ecs-lingfeng-ingest --region ap-southeast-1 --since 6h | grep -v check-trigger
```
Expected: all markets log `[OK] Price ingestion` and `[OK] Fundamentals ingestion`.
青海/江西 will log a warning like `skipped — Market '...' not found in dropdown`.

---

## Key File Locations

| File | Purpose |
|---|---|
| `services/lingfeng/run_daily.py` | Main pipeline — `_ALL_MARKETS` list, `run_pipeline()` with the fix at line ~482 |
| `services/lingfeng/collector.py` | Playwright scraper — `_goto_data_page()` raises RuntimeError if market not found |
| `services/lingfeng/entrypoint.py` | ECS scheduler loop (daily at 20:00 UTC + 15-min trigger check) |
| `apps/bess-map/app.py` | bess-map Streamlit app — Data Management tab has coverage table + Batch Backfill UI |

## Deployment Commands

```bash
# Rebuild and push new lingfeng image
aws ecr get-login-password --region ap-southeast-1 | docker login --username AWS --password-stdin 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com
docker build -t lingfeng-ingest:v6 -f services/lingfeng/Dockerfile .
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/lingfeng-ingest:v6
# Register new task def + update service (copy env from rev 6, change image to :v6)
```

## ECS Service Info
- **Cluster:** `bess-platform-cluster` (ap-southeast-1)
- **Service:** `bess-platform-lingfeng-ingest-svc`
- **Task def:** `bess-platform-lingfeng-ingest:6` (current as of 2026-06-28)
- **Image:** `319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/lingfeng-ingest:v5`
- **CloudWatch:** `ecs-lingfeng-ingest` (ap-southeast-1)
