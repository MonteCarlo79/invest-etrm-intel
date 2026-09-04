# Handoff — 2026-07-03 bess-map + Hermes fixes

## Current Deployment State

| Service | Image | ECS Task Def | Deployed |
|---------|-------|-------------|---------|
| bess-map | new image (2026-07-03) | td:87 | 2026-07-03 |
| hermes | td:148 | td:148 | 2026-07-03 |

Branch: `cost-optimisation` — all commits pushed to GitHub.

---

## What Was Fixed Today

### 1. `load_latest_installed_monthly()` — missing schema prefix (CRITICAL)
**File:** `services/market_fundamentals/loader.py:281`
**Symptom:** "Monthly (latest)" radio in BESS Demand tab silently fell back to Annual 2025 fundamentals — 山东 showed 6080 MW instead of manually-entered 9700 MW.
**Fix:** `FROM province_installed_monthly` → `FROM marketdata.province_installed_monthly`
**Commit:** `8ad01c6`

### 2. DB Coverage table — wrong filter column
**File:** `apps/bess-map/app.py` `load_scraping_progress()` line ~909
**Symptom:** Coverage table showed 2–3 days scraped per province (previously 200+). User's scraped data appeared "lost."
**Root cause:** `bidding_space_mw > 0` was applied to coverage counting. LingFeng stopped populating `bidding_space_mw` from Mar 2026 (writes 0.0, not NULL), so 99% of rows have `bidding_space_mw = 0`.
**Fix:** Changed to `load_mw IS NOT NULL` — this column is always populated on ingest.
**Note:** `bidding_space_mw > 0` filter is CORRECT and remains in demand charts/intraday profile queries. Only the coverage count was wrong.
**Commit:** `2f0427e`

### 3. `/capacity` DB write fixes (from morning session)
**File:** `services/hermes/capacity_manual_etl.py`
- Removed `connect_timeout=10` (caused 10s timeout in ECS when mixing URI DSN with kwargs)
- Added `marketdata.` prefix to all SQL referencing `province_installed_monthly`
- Date injection: current date passed to Claude prompt so undated entries default to current month
**Commits:** `004588d`, `0e45690`

### 4. Hermes — pulp in container (nodal ranking)
`pulp>=2.7` was already added to `apps/hermes-service/requirements.txt` (commit `bc87c43` yesterday) but the container hadn't been rebuilt. Today's td:148 deploy includes pulp → `2h节点排名` / `4h节点排名` columns now show `#N` ranks.

### 5. Exchange reports pipeline
**New service:** `services/exchange_reports/ingestor.py` (commit `e554596`)
- 9 provinces: 上海/冀南/安徽/山东/广东/江苏/浙江/福建/蒙西 + 广西 (commit `5f63d7c`)
- Auto-ingest via Feishu/Telegram file drop
- Spot-market UI: '交易所月报管理' section in Data Management tab
- Metrics extractor: multi-provider LLM (DeepSeek → Bedrock → Anthropic)
- Backfill: `py scripts/ingest_exchange_reports.py` from repo root
- Backfill metrics only: `py scripts/ingest_exchange_reports.py --extract-metrics-only`

---

## What Remains To Do (bess-map)

### Priority 1 — Conflict UI bad values
**Handoff note:** "Resolve bad data values in bess-map conflict UI (内蒙古蒙东 0.28, etc.)"
- Some province_cap_comp rows have implausibly low values (e.g. 0.28 yuan/kW instead of 280)
- These appear in the 容量补偿 conflict expander
- Fix: use `/capcomp-add` in Feishu to enter correct values, then resolve conflict via UI
- Or investigate capcomp_etl.py `resolve_conflict()` — may need unit normalisation logic

### Priority 2 — FR market data coverage
- Only 广西 has FR market data (1 row). Most provinces: no data.
- To improve: ingest province-specific FR market rule PDFs into KB (`staging.spot_knowledge_chunks`), then re-run `/capcomp` scan in Feishu
- Manually add known values: `/capcomp-add 甘肃 2026-04 调频价格10元/kW/h 调频资金池0.4亿元`

### Priority 3 — bidding_space data gap investigation (low priority)
- `bidding_space_mw = 0.0` for most provinces from Mar 2026 (LingFeng stopped populating)
- Check if newer LingFeng Excel files use a different column name
- Look at `COLUMN_GROUPS["bidding"]` in `services/bess_map/run_fundamentals_ingest.py`

---

## Key Files

```
apps/bess-map/app.py                          # main Streamlit app (~3500 lines)
services/market_fundamentals/loader.py         # load_latest_installed_monthly() — schema fix here
services/hermes/capcomp_etl.py                # upsert to province_cap_comp + province_fr_market
services/hermes/capcomp_manual_etl.py         # Feishu /capcomp-add manual entry
services/hermes/capacity_manual_etl.py        # Feishu /capacity-add manual entry
services/hermes/capcomp_screener.py           # monthly KB search scan (32 provinces)
services/exchange_reports/ingestor.py         # exchange monthly reports ETL
services/exchange_reports/metrics_extractor.py # LLM metrics extraction (multi-provider)
scripts/ingest_exchange_reports.py            # CLI backfill for exchange reports
```

## RDS Tables (production only — not in local DB at port 5433)

```sql
marketdata.spot_fundamentals_hourly    -- 296k rows, 27 provinces; load_mw always populated
marketdata.province_installed_monthly  -- monthly BESS/wind/solar; manual entries via /capacity-add
marketdata.province_cap_comp           -- capacity compensation per province/year
marketdata.province_fr_market          -- FR market settlement per province/month
marketdata.province_fundamentals       -- annual fundamentals 2024/2025
public.province_sysopfee_monthly       -- system op fee (still in public schema)
staging.exchange_monthly_reports       -- exchange report registry
staging.exchange_monthly_metrics       -- extracted numerical metrics per report
```

## Deploy Commands

```bash
# bess-map
docker build --no-cache -t bess-map -f apps/bess-map/Dockerfile .
docker tag bess-map:latest 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-map:latest
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-map:latest
aws ecs update-service --cluster bess-platform-cluster --service bess-platform-bess-map-svc --force-new-deployment --region ap-southeast-1

# hermes
docker build --no-cache -t hermes-service -f apps/hermes-service/Dockerfile .
docker tag hermes-service:latest 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-platform-hermes:latest
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-platform-hermes:latest
aws ecs update-service --cluster bess-platform-cluster --service bess-platform-hermes-svc --force-new-deployment --region ap-southeast-1
```

## Feishu Manual Entry Commands

```
/capcomp-add 甘肃 2026-04 调频价格10元/kW/h 调频资金池0.4亿元
/capacity-add 山东 2026-07 储能装机9700MW
/capcomp    # trigger full 32-province scan (~35 min)
/sendwecom  # send monthly ranking to WeCom
```
