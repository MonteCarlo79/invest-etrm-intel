# Handoff: bess-map fundamentals data quality + bidding_space backfill
_2026-07-13 — branch: cost-optimisation, commit 0f831c3_

---

## What was done this session

### 1. BESS installed capacity data quality fixes (deployed v57 → v58)

**Root causes found and fixed in DB:**
- `蒙西`: bess_mw was 1/10 of real size — unit detection failure (stored 万kW, needed ×10). Corrected Jun 2025+ rows.
- `江苏`: bess_mw was reading 储能机组 participant count, not capacity. NULLed out.
- `湖北`: Oct 2025+ near-zero values from wrong cells. NULLed out (Sep 2025 = 2561 MW preserved).
- `河南`: Jan–Apr 2025 and Apr–May 2026 NULLed (format changes in source Excel).

**Loader fix** (`services/market_fundamentals/loader.py`):
`load_latest_installed_monthly()` now uses a CTE that picks the latest non-NULL `bess_mw` row independently from the latest row for other fields (wind/solar use absolute latest row).

### 2. bess-map app.py changes (v58, deployed)

- **Default province list**: Hardcoded `_DEFAULT_DEMAND_PROVS` = 27 provinces with LingFeng fundamentals data (江西 included but shows 0 arbitrage — LingFeng times out for it).
- **灵活热电 checkbox**: Default changed to `value=False`.
- Docker image updated to v58 in `docker-compose.local.yml`.

### 3. Oct–Dec 2025 fundamentals gap filled

`scripts/ingest_oct_dec_2025_fundamentals.py` ingests from:
`data/lingfeng/2025年10-12月各省边界及现货价格.xlsx`

- 47,362 hourly rows upserted, 23 provinces, Oct 1 – Dec 31 2025
- Column mapping by positional index (Col 0=province, 2=date, 3=hour24, 6=renewable, 14=load, 18=wind, 19=solar, 21=net_export, 26=bidding_space)
- **Note: Col 26 (竞价空间) is ALL ZEROS in the source Excel** — the data simply wasn't collected in that file. So Oct–Dec 2025 `bidding_space_mw` remains 0.

### 4. Diagnosis: why 每日储能套利容量 chart looks empty

Root cause: `bidding_space_mw` is near-0 for Oct 2025 – Jun 2026.

| Period | bidding>0 rows | Root cause |
|--------|---------------|------------|
| Aug–Sep 2025 | 89% | Old static province Excel files with real data |
| Oct–Dec 2025 | 5% / 0% | Source Excel col 26 = all zeros |
| Jan–Jun 2026 | ~0% | Scraped from old LingFeng URL (old URL had bidding=0 for this period on new format) |
| Jul 7–12 2026 | ~80% | ECS daily scrape using new URL `/powerTrading/market` ✅ |

**Key finding:** The new LingFeng URL only retains ~2 weeks of history. The OLD URL (`/powerTrading/sass/data-consultation`) has full historical data back to at least May 2026 with 16-col format including real `bidding_space_mw`.

Verified: `山西` May 31–Jun 29 via old URL → 720 rows, 100% bidding_space > 0, avg 22,306 MW.

### 5. collector.py changes for old-URL support

`services/lingfeng/collector.py`:
- `_DATA_URL` is now configurable via env var `LINGFENG_DATA_URL` (default: new URL)
- Old-page detection: if `span.down-load-container` not present, it's the old page — use single-button flow (no separate 查询 step; `button.ant-btn-primary` IS the 导出 button)
- New-page flow: click 查询, then click `span.down-load-container:not(.mr-10)`

---

## Immediately pending: full backfill for May 1 – Jul 6 2026

The backfill for this range was attempted with the new URL but produced zero bidding data (old URL zero issue). Now that the old URL is confirmed to work, run:

```powershell
$env:LINGFENG_DATA_URL="https://lingfeng-saas.tradingthink.cn/#/powerTrading/sass/data-consultation"
py services/lingfeng/run_daily.py --markets all --start-date 2026-05-01 --end-date 2026-07-06 --skip-capture
```

**Expected time:** ~60–90 minutes (29 provinces × 3 chunks each).
**Expected outcome:** bidding_space_mw populated for ~22 provinces for May–Jun 2026.
**Do NOT run in parallel** — same LingFeng account, shared download dir.

After the backfill completes, verify:
```python
# Check bidding_space fill rate by month (should jump from ~0% to >50% for May-Jun)
SELECT DATE_TRUNC('month', datetime),
       COUNT(*) FILTER (WHERE bidding_space_mw > 0) AS bidding_positive,
       COUNT(*) AS total
FROM marketdata.spot_fundamentals_hourly
WHERE datetime >= '2026-05-01' AND datetime < '2026-07-07'
GROUP BY 1 ORDER BY 1;
```

Then rebuild and redeploy bess-map to reflect the updated chart data:
```bash
docker build --no-cache -t bess-map -f apps/bess-map/Dockerfile .
docker tag bess-map:latest 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-map:latest
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-map:latest
aws ecs update-service --cluster bess-platform-cluster --service bess-platform-bess-map-svc --force-new-deployment --region ap-southeast-1
```
(Increment image tag to v59 in docker-compose.local.yml after successful deploy.)

---

## Remaining known data gaps (not fixable without new source files)

| Gap | Reason | Fix |
|-----|--------|-----|
| Oct–Dec 2025 `bidding_space_mw` = 0 | Source Excel col 26 all zeros | Need Excel file with real 竞价空间 data for that period |
| 江苏/福建 Nov–Dec 2025 | Not in Oct–Dec source Excel | Need separate data file |
| Jan–Feb 2026 (all provinces) | Not available on either LingFeng URL | Need Excel file |
| Mar 31–Apr 29 2026 (all provinces) | LingFeng confirmed gap on both URLs | Need Excel file |
| 江西 (all time) | LingFeng export always times out | No fix; shows 0 arbitrage |

---

## DB state summary (as of 2026-07-13)

```
marketdata.spot_fundamentals_hourly:
  Aug-Sep 2025:   ~35k rows, 89% bidding_space > 0
  Oct-Dec 2025:   ~52k rows, 0-5% bidding_space (source Excel had zeros)
  Jan-Feb 2026:   ~37k rows, ~3% bidding_space
  Mar-Apr 2026:   ~27k rows, stubs (0% Mar, 3% Apr — only Apr 30 available)
  May-Jun 2026:   ~32k rows, ~0% bidding_space (OLD URL backfill PENDING)
  Jul 2026:       ~6k rows, 26% bidding_space (new URL ECS daily ✅)

marketdata.province_installed_monthly:
  bess_mw fixed for 蒙西/江苏/湖北/河南
  loader.py CTE fix deployed

ECS lingfeng-ingest: healthy as of 2026-07-12 13:51 UTC (recovered from Jul 9 failure)
bess-map: v58 deployed (27-province defaults, 灵活热电 unchecked)
```

---

## Key files touched this session

```
apps/bess-map/app.py                             # v58 changes
services/market_fundamentals/loader.py           # CTE bess_mw fix
services/lingfeng/collector.py                   # old-URL support
scripts/ingest_oct_dec_2025_fundamentals.py      # new: Oct-Dec 2025 ingest
scripts/scan_installed_capacity.py               # context_year from filename
docker-compose.local.yml                         # v58 image tag
```
