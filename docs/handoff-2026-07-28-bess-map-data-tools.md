# Handoff: bess-map v59 + Agent Tools + Fundamentals Backfill
_2026-07-28 — branch: `feat/deal-structurer-bedrock-migration`_

## What was done this session

### 1. bess-map v59 built and deployed ✅
- **ECS**: td:89 / `bess-map:v59` — `COMPLETED`, 1/1 running
- Image `319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-map:v59`
- Contains Bedrock migration (`make_client` factory from commit `94b11cb`)
- Contains 3 new agent tools (see below)

### 2. Three new AI agent tools added to `apps/bess-map/app.py` ✅

The agent previously only had 3 tools covering spot price arbitrage. The UI had two more data-rich tabs (系统运行费, 容量补偿+辅助服务) whose DB tables were invisible to the agent. Fixed by adding:

| Tool | DB Table | Data type |
|---|---|---|
| `get_sysop_fee` | `province_sysopfee_monthly` | Grid system op fee ¥/kWh (cost) |
| `get_capacity_compensation` | `marketdata.province_cap_comp` | Capacity payment ¥/kW (revenue) |
| `get_freq_reg_market` | `marketdata.province_fr_market` | Freq reg price ¥/kW·h (revenue) |

Agent now uses these automatically in IRR analysis. Sysop fee → negative `subsidy_per_mwh`; capacity comp → positive `subsidy_per_mwh`.

Full tool list in `apps/bess-map/app.py` lines ~3898–3996 (6 tools total):
`get_bess_economics`, `get_dispatch_detail`, `get_irr_estimate`, `get_sysop_fee`, `get_capacity_compensation`, `get_freq_reg_market`

### 3. `spot_fundamentals_hourly` backfill — zero-protected batch ingest ✅

**New script**: `scripts/backfill_protective.py`

Ingests LingFeng Excel files with a per-column COALESCE guard:
- If >50% of values in a column are 0/NULL → uses `COALESCE(existing, incoming)` so existing non-zero values are preserved
- If <50% zeros → normal overwrite

**Usage**:
```bash
cd "C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform"
set -a && source config/.env && set +a
python scripts/backfill_protective.py --indir data/backfill --schema marketdata
```

Files must be named `<省份>.xlsx` (Chinese province name, no extra text). The script detects province from the filename stem.

**Ran against `data/backfill/` (10 files, 83,890 rows):**

| Province | Before | After | Notes |
|---|---|---|---|
| 河南 | 42% | **100%** | Full history covered |
| 甘肃 | 77% | **84%** | + June manual fix (see below) |
| 湖南 | 33% | 38% | Partial improvement |
| 辽宁 | 83% | 83% | Minimal |
| 蒙东 | 0% | 0% | All 8 key cols zero-protected — LingFeng has no fundamentals for this province |
| 浙江/福建 | 51-53% | 51-53% | File didn't cover missing date ranges |
| 新疆/江苏 | 67-76% | 67-76% | Same |

**甘肃 June manual fix**: User downloaded `debug/甘肃数据-2026年6月.xlsx` manually from LingFeng. Copied to `/tmp/gansujune/甘肃.xlsx` and ran `run_fundamentals_ingest.py` directly. Filled Jun 1–17 (was missing because LingFeng's automated download had a date picker issue). Result: 30/30 days for June.

---

## Current fundamentals coverage

```
Province    | Coverage | Notes
------------|----------|-------
安徽        | 100%     | ✅
河南        | 100%     | ✅ fixed this session
贵州        | 99%      | ✅
云南        | 99%      | ✅
黑龙江      | 99%      | ✅
广东        | 100%     | ✅
湖北        | 99%      | ✅
广西        | 99%      | ✅
陕西        | 97%      | ✅
吉林        | 95%      | ✅
海南        | 88%      | ✅
宁夏        | 85%      | ✅
蒙西        | 84%      | ✅
山西        | 84%      | ✅
甘肃        | 84%      | ✅
山东        | 84%      | ✅
辽宁        | 83%      | ✅
江苏        | 76%      | gap: scattered 2025
新疆        | 67%      | gap: scattered 2025
浙江        | 51%      | gap: Feb 2025 – Jul 2026 ongoing
福建        | 53%      | gap: Apr 2025 – Jul 2026 ongoing
湖南        | 38%      | gap: Jan 2025 – Jul 2026 (large)
蒙东        | 0%       | LingFeng has no fundamentals data
江西        | 0%       | LingFeng has no fundamentals data
豫中东/北/南/西 | 0%  | 河南 sub-regions, no separate fundamentals
```

---

## Pending — what needs more backfill Excel files

If the user can download these from LingFeng, drop in `data/backfill/` as `<省份>.xlsx` and re-run `backfill_protective.py`:

| Province | Target months | Gap size |
|---|---|---|
| 湖南 | Jan–Apr 2025, May–Jul 2026 | 384 days |
| 浙江 | Feb 2025 – Nov 2025 | 279 days |
| 福建 | Apr 2025 – Oct 2025 | 271 days |
| 江苏 | Jan–Feb 2025, recent 2026 | 136 days |
| 新疆 | Jan–Mar 2025 | 189 days |
| 青海 | Apr–Jul 2026 | 103 days |

Note: 蒙东 and 江西 are confirmed zero — LingFeng doesn't have fundamentals for these provinces regardless of date range.

---

## Current ECS state

| Service | Task Def | Image | Status |
|---|---|---|---|
| bess-platform-portal-svc | td:66 | portal:v10 | ✅ running |
| bess-platform-bess-map-svc | td:89 | bess-map:v59 | ✅ running |

## Key env vars in bess-map td:89
- `BEDROCK_REGION=ap-southeast-1` ✅
- `ANTHROPIC_API_KEY` — valid key ✅
- `DEEPSEEK_API_KEY` ✅
- `PGURL` / `PGHOST` / `PGDATABASE` etc. ✅
- **Missing**: `LINGFENG_USERNAME`, `LINGFENG_PASSWORD`, `OPENAI_API_KEY`
  → The batch backfill (批量补录) button in the app requires these to auto-download from LingFeng.
  → **Action needed**: Add these to the ECS task definition.

## Adding missing env vars to td:89

```bash
# Fetch td:89, add LINGFENG creds + OPENAI key, register td:90, deploy
aws ecs describe-task-definition --task-definition bess-platform-bess-map:89 \
  --region ap-southeast-1 --query 'taskDefinition' > /tmp/td89.json

# Edit /tmp/td89.json — add to environment array:
# {"name": "LINGFENG_USERNAME", "value": "<from config/.env>"}
# {"name": "LINGFENG_PASSWORD", "value": "<from config/.env>"}
# {"name": "OPENAI_API_KEY", "value": "<from config/.env>"}
# Strip read-only fields, save as /tmp/td90.json

aws ecs register-task-definition --cli-input-json file:///tmp/td90.json --region ap-southeast-1
aws ecs update-service --cluster bess-platform-cluster \
  --service bess-platform-bess-map-svc \
  --task-definition bess-platform-bess-map:90 \
  --force-new-deployment --region ap-southeast-1
```

---

## Key files changed this session

```
apps/bess-map/app.py              # 3 new agent tools (lines ~3948–4108)
docker-compose.local.yml          # bess-map image v58 → v59
scripts/backfill_protective.py    # NEW: zero-protected batch fundamentals ingest
docs/handoff-2026-07-26-bess-map-bedrock.md  # previous session handoff
```

## DB tables relevant to fundamentals

```
marketdata.spot_fundamentals_hourly     — main target table (province, datetime, bidding_space_mw, ...)
province_sysopfee_monthly               — grid system op fees (agent tool: get_sysop_fee)
marketdata.province_cap_comp            — capacity compensation (agent tool: get_capacity_compensation)
marketdata.province_fr_market           — freq reg market (agent tool: get_freq_reg_market)
marketdata.bess_capture_daily           — LP dispatch economics (existing agent tool: get_bess_economics)
marketdata.spot_prices_hourly           — spot prices
```

## DB connection

```
PGURL = see config/.env (RDS at bess-platform-pg.cjs000o4wn2w.ap-southeast-1.rds.amazonaws.com:5432/marketdata)
```

Useful diagnostic query — coverage per province:
```sql
WITH price_range AS (
    SELECT province, COUNT(DISTINCT datetime::date) AS price_days
    FROM marketdata.spot_prices_hourly GROUP BY province
),
fund_days AS (
    SELECT province, COUNT(DISTINCT datetime::date) AS fund_days
    FROM marketdata.spot_fundamentals_hourly
    WHERE bidding_space_mw IS NOT NULL AND bidding_space_mw > 0
    GROUP BY province
)
SELECT p.province,
       p.price_days, COALESCE(f.fund_days,0) AS fund_days,
       ROUND(100.0 * COALESCE(f.fund_days,0) / NULLIF(p.price_days,0), 1) AS pct
FROM price_range p LEFT JOIN fund_days f ON p.province = f.province
WHERE p.price_days > 50 AND length(p.province) <= 3
ORDER BY pct ASC;
```

## Branch
`feat/deal-structurer-bedrock-migration` — all changes committed and pushed.
Latest commit: `36ef89b`
