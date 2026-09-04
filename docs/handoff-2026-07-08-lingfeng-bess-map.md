# Handoff — 2026-07-08 LingFeng scraper fix + bess-map updates

## Branch & Deployment State

| Service | Commit | Deployed |
|---------|--------|---------|
| lingfeng-ingest ECS | `0484640` | 2026-07-08 (td:6, force-new-deployment done) |
| bess-map | `6d1bdbe` | previous session |
| hermes | td:148 | previous session |

Branch: `cost-optimisation` — all commits pushed to GitHub.

---

## All Changes This Session (2026-07-07/08)

### Fix 1 — LingFeng scraper: new page URL
**Commits:** `95dccf5`, `63083b0`, `814b228`, `c2f15f9`, `9d030a1`, `0484640`
**File:** `services/lingfeng/collector.py`

Old URL: `https://lingfeng-saas.tradingthink.cn/#/powerTrading/sass/data-consultation`
New URL: `https://lingfeng-saas.tradingthink.cn/#/powerTrading/market`

**Selector fixes on new page:**
- 查询 button: `button.ant-btn-primary` (old `has_text="查询"` broke due to "查 询" spacing)
- 导出 button: `span.down-load-container:not(.mr-10)` — NOT a `<button>` tag; the `.mr-10` sibling opens a data-comparison drawer and must be excluded
- Both `_collect_async` (line ~237) and `_collect_province_async` (line ~432) updated

**Important — comparison mode:** The page has a "数据对比" comparison mode. When comparison is active, the export gives 53-column "修正后" format with zero load/bidding values. Fresh browser sessions (ECS) always start clean and download the correct 57-column `运行数据披露` format with real data. This was confirmed: `load_mw=27557 MW, bidding_space_mw=23081 MW` for 山西 2026-07-05.

### Fix 2 — bess-map: province selection persistence
**Commit:** `6d1bdbe`
**File:** `apps/bess-map/app.py`

Province selections persist across page reloads via URL query params:
- BESS Demand tab: `?dp=省1,省2`
- System Op Fee tab: `?sp=...`
- Cap Comp tab: `?ap=...`

Uses `st.query_params` (Streamlit >= 1.32 dict API).

### Fix 3 — 甘肃 FR market data inserted
Direct DB insert (bypassed Claude API proxy key issue locally):
```python
upsert_fr_rows([{
    'province': '甘肃',
    'effective_date': '2026-04-01',
    'fr_price_yuan_kw_h': 10.0,
    'fr_pool_billion_yuan': 0.4,
}], pg_url, source='manual_text_2026')
```
Row confirmed upserted (1 row, 0 conflicts).

---

## What Remains To Do

### Priority 1 — LingFeng backfill (TRIGGER NOW)
In Feishu/Telegram:
```
/lf_run 2026-01-01:2026-06-30
```
Backfills all 29 provinces with complete data (prices + load_mw + bidding_space_mw). Takes a few hours.

To backfill specific provinces locally:
```bash
py services/lingfeng/run_daily.py --markets 山西 山东 --start 2026-01-01 --end 2026-06-30
```

### Priority 2 — Chart "Daily BESS Arbitrage Sizing" shows no 2026 data
**File:** `apps/bess-map/app.py` line ~1152
**Filter:** `AND bidding_space_mw > 0` — hides all rows where bidding_space is zero (which was ALL 2026 data before the scraper fix).
**Action needed:** After backfill completes and DB has real bidding_space values, verify the chart shows data. If it still doesn't, check whether the filter needs tuning (e.g., change to `IS NOT NULL`).

### Priority 3 — /capcomp rescan (from previous handoff — still pending)
In Feishu:
```
/capcomp
```
Takes ~35 min. Inserts real KB doc filenames as sources in `province_cap_comp`. After scan, optionally clean old generic rows:
```sql
DELETE FROM marketdata.province_cap_comp WHERE source LIKE 'KB/Claude search%';
DELETE FROM marketdata.province_fr_market WHERE source LIKE 'KB/Claude search%';
```

### Priority 4 — 内蒙古（蒙东）cap comp display
Current DB value: `id=40, cap_comp_yuan_kw=0.28, status=confirmed`
**Clarified:** 0.28 is yuan/kWh of power discharged (energy-based compensation), NOT yuan/kW capacity — so the value is correct. However the bess-map UI column header says "¥/kW" which is misleading for this province. Low priority.

### Priority 5 — bidding_space gap before 2026 (very low)
`bidding_space_mw = 0` for rows scraped before the URL fix. Will be resolved once backfill completes.

---

## Key Architecture Notes

**LingFeng export format (new page):**
- 57 columns: `日期, 时点, [省]全省现货价格-实时价格, 日前价格, 省内负荷-实时, ..., 竞价空间-实时, ...`
- Column keyword matching in `run_fundamentals_ingest.py` COLUMN_GROUPS works correctly with this format
- `省内负荷` → `load_mw` (index 4), `竞价空间` → `bidding_space_mw` (index 21)

**Province name mapping:**
| province_installed_monthly | spot_fundamentals_hourly |
|---------------------------|--------------------------|
| 冀南 | 河北南网 |
| 冀北 | 河北北网 |

**RDS DB connection:**
```
postgresql://postgres:!BESSmap2026@bess-platform-pg.cjs000o4wn2w.ap-southeast-1.rds.amazonaws.com:5432/marketdata?sslmode=require
```

**Local API key issue:** `ANTHROPIC_API_KEY` in `config/.env` is a LiteLLM proxy key, not a direct Anthropic key. Calling `extract_capcomp_from_text()` locally fails with 401. Use `upsert_fr_rows()` / `upsert_cap_comp_rows()` directly for local DB inserts.

---

## Key Files

```
services/lingfeng/collector.py              # Playwright scraper; new URL + selector fixes
services/lingfeng/run_daily.py             # Pipeline entry point; --markets --start --end
services/lingfeng/debug_export.py          # Debug: inspects 导出 button behavior
services/lingfeng/debug_compare_mode.py    # Debug: tests each download span, confirms 57-col format
apps/bess-map/app.py                       # Province persistence (query params) + chart filter line ~1152
services/hermes/capcomp_manual_etl.py      # extract_capcomp_from_text(); province alias map
services/hermes/capcomp_etl.py             # upsert_fr_rows(), upsert_cap_comp_rows()
services/bess_map/run_fundamentals_ingest.py  # COLUMN_GROUPS keyword mapping
```

## Deploy Commands

```bash
# lingfeng-ingest (already deployed 2026-07-08)
docker build -t lingfeng-ingest -f services/lingfeng/Dockerfile .
docker tag lingfeng-ingest:latest 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/lingfeng-ingest:latest
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/lingfeng-ingest:latest
aws ecs update-service --cluster bess-platform-cluster --service bess-platform-lingfeng-ingest-svc --force-new-deployment --region ap-southeast-1

# bess-map (if app.py changes needed)
docker build --no-cache -t bess-map -f apps/bess-map/Dockerfile .
docker tag bess-map:latest 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-map:latest
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-map:latest
aws ecs update-service --cluster bess-platform-cluster --service bess-platform-bess-map-svc --force-new-deployment --region ap-southeast-1
```
