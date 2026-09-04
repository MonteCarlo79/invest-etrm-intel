# Handoff — Cap Comp Data Fixes + bess-map Notes Column
**Date:** 2026-07-09  
**Branch:** `cost-optimisation`  
**Last commit:** `d7b265f` — bess-map: add notes column to cap comp table; add 山东 dynamic scheme, fix conflicts

---

## What Was Done This Session

### 1. province_cap_comp — Data Corrections on RDS

All fixes applied directly to `marketdata.province_cap_comp` via scripts (PGURL from `config/.env`).

| Province | Fix |
|----------|-----|
| 辽宁 | effective_date 2027-01-01 → **2026-01-01** (was filtered out by year selector) |
| 吉林 | cap_comp 380 → **330 ¥/kW**, added **8h** peak_duration |
| 浙江 | cap_comp 170 → **180 ¥/kW**; KB/Claude row superseded, infographic row confirmed |
| 甘肃 | Correct row (330, id=37) → confirmed + **6h** peak_duration; wrong row (380, id=53) → superseded |
| 陕西 | Added **6h** peak_duration |
| 山东 | **Inserted** as dynamic model (null cap_comp, notes column) |
| 内蒙古（蒙东） | Added notes: "放电量补偿模式: 0.28元/kWh（单次放电，无固定底薪）" |

Source: infographic "各省独立储能容量补偿汇总（截至2026年6月）" uploaded to Feishu.

### 2. province_cap_comp — Schema Change

```sql
ALTER TABLE marketdata.province_cap_comp ADD COLUMN IF NOT EXISTS notes TEXT;
```

Added to `capcomp_etl.py` DDL as well (idempotent `ADD COLUMN IF NOT EXISTS` guard).

### 3. bess-map — Notes Column in Cap Comp Table

**Files changed:** `apps/bess-map/app.py`

- `load_cap_comp()` query: added `notes` to SELECT
- Display dataframe: added `notes` column between `effective_date` and `source`
- Translation keys added:
  - EN: `"aux_notes": "Scheme Notes"`
  - ZH: `"aux_notes": "补偿机制说明"`

### 4. bess-map Deployed

```
ECR: 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-map:latest
ECS: bess-platform-bess-map-svc force-redeployed 2026-07-09
```

---

## Current province_cap_comp State (confirmed rows, 2026)

| Province | Cap Comp (¥/kW) | Peak Duration (h) | Notes |
|----------|-----------------|-------------------|-------|
| 云南 | 165 | — | |
| 内蒙古（蒙东） | 0.28 | — | 放电量补偿模式: 0.28元/kWh（单次放电） |
| 天津 | 100 | — | |
| 宁夏 | 165 | 6 | |
| 山东 | — | — | 动态复合型: 0.0705元/kWh按月动态调整 |
| 山西 | 165 | — | |
| 广东 | 200 | — | |
| 广西 | 165 | — | |
| 新疆 | 165 | 6 | |
| 江苏 | 100 | — | |
| 河北 | 100 | — | |
| 浙江 | 180 | — | |
| 海南 | 165 | 6 | |
| 湖北 | 165 | 10 | |
| 甘肃 | 330 | 6 | |
| 辽宁 | 370 | — | 征求意见稿 |
| 陕西 | 165 | 6 | |
| 青海 | 185 | 8 | |
| 吉林 | 330 | 8 | |

---

## How to Run DB Correction Scripts

```powershell
# Always set PGURL first (points to RDS)
$env:PGURL = (Get-Content config/.env | Select-String "^PGURL=").Line.Substring(6)

# Or in bash:
export $(grep -E "^PGURL=" config/.env | head -1)

# Dry run
py scripts/upsert_capcomp_manual_2026.py --dry-run
py scripts/fix_capcomp_conflicts_2026.py --dry-run

# Apply
py scripts/upsert_capcomp_manual_2026.py
py scripts/fix_capcomp_conflicts_2026.py
```

---

## Outstanding TODOs

1. **内蒙古（蒙西）** — not in DB. User mentioned it also uses per-discharge-volume scheme. Need to confirm the rate before inserting.
2. **Hermes: /backfill-annual 2025** — send in Feishu chat. MILP for ~40 plants × 365 days (~30 min). Do NOT redeploy while running.
3. **bess-map cache TTL** — 1800s (30 min). After any DB change, either wait or restart Streamlit to see updates immediately.
4. **LingFeng backfill pending:**
   ```powershell
   py services/lingfeng/run_daily.py --markets all --start-date 2026-05-01 --end-date 2026-07-09 --skip-capture
   ```
5. **Capcomp /rescan** — run in Feishu to refresh source column with real KB doc filenames.

---

## Key Files

```
apps/bess-map/app.py                     # main app; load_cap_comp() ~line 1546; cap comp display ~line 2690
services/hermes/capcomp_etl.py           # DDL + upsert + conflict detection
services/hermes/capcomp_manual_etl.py    # extract_capcomp_from_image() — vision → upsert
services/hermes/agent.py                 # CAPCOMP_INGEST_NEXT_FILE action
scripts/upsert_capcomp_manual_2026.py    # manual INSERT missing rows + UPDATE wrong values
scripts/fix_capcomp_conflicts_2026.py    # resolve specific conflicts + date fixes
config/.env                              # PGURL for RDS (never commit this file)
```

---

## Deploy Commands (bess-map)

```bash
docker build --no-cache -t bess-map -f apps/bess-map/Dockerfile .
docker tag bess-map:latest 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-map:latest
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-map:latest
aws ecs update-service --cluster bess-platform-cluster --service bess-platform-bess-map-svc --force-new-deployment --region ap-southeast-1
```

> Note: ECR repo name is `bess-map` (not `bess-platform-bess-map`).
