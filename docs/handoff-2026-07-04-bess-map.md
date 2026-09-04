# Handoff — 2026-07-04 bess-map fixes

## Current Deployment State

| Service | Image | ECS Task Def | Deployed |
|---------|-------|-------------|---------|
| bess-map | new image (2026-07-04) | td:87 | 2026-07-04 |
| hermes | td:148 (redeployed) | td:148 | 2026-07-04 |

Branch: `cost-optimisation` — all commits pushed to GitHub.

---

## All Fixes Applied This Session (2026-07-03/04)

### Fix 1 — `load_latest_installed_monthly()` missing schema prefix
**File:** `services/market_fundamentals/loader.py:281`
**Symptom:** "Monthly (latest)" radio silently fell back to Annual 2025 — 山东 showed 6080 MW instead of manually-entered 9700 MW.
**Fix:** `FROM province_installed_monthly` → `FROM marketdata.province_installed_monthly`
**Commit:** `8ad01c6`

### Fix 2 — DB Coverage table wrong filter column
**File:** `apps/bess-map/app.py` `load_scraping_progress()` line ~909
**Symptom:** Coverage table showed 2–3 days scraped per province (was 200+). User's scraped data appeared "lost."
**Fix:** `bidding_space_mw > 0` → `load_mw IS NOT NULL` (bidding_space=0 for 99% rows from Mar 2026)
**Commit:** `2f0427e`

### Fix 3 — Province name alias 冀南 ↔ 河北南网
**File:** `services/market_fundamentals/loader.py` (end of `load_latest_installed_monthly`)
**Symptom:** 河北南网 showed Existing BESS = 0 in section③ chart despite 冀南 having 1354 MW in installed_monthly table.
**Root cause:** `province_installed_monthly` stores Excel-derived names (冀南/冀北); `spot_fundamentals_hourly` uses market names (河北南网/河北北网). Lookup `_monthly_all.get("河北南网")` returned None.
**Fix:** Added `_ALIASES = {"冀南": "河北南网", "冀北": "河北北网"}` post-processing in loader.
**Commit:** `cd97571`

### Fix 4 — Capacity Compensation Source column shows real KB doc filenames
**Files:** `services/hermes/capcomp_screener.py`, `apps/bess-map/app.py`
**Symptom:** Source column showed "KB/Claude search (2026-07-02 scan)" for all 18 provinces — generic tag from a previous bulk UPDATE.
**Root cause:** Screener let Claude's guessed `source_url` override `source_hint` (actual KB filenames). Even when KB results existed, Claude returned vague/wrong filenames.
**Fix 1 (screener):** Always set `data["source_url"] = source_hint` when KB results exist; Claude's guess only used as absolute fallback.
**Fix 2 (display):** Added `ingested_at DESC` to `load_cap_comp` ORDER BY so newest scan rows (with real sources) take priority in `drop_duplicates(keep="first")`.
**Commit:** `b1f75bc`

---

## What Remains To Do

### Priority 1 — Trigger `/capcomp` rescan (IMMEDIATE)
After Hermes/bess-map deploy, run in Feishu:
```
/capcomp
```
Takes ~35 min. This will insert new rows with real KB doc filenames as sources.
After scan completes, verify source column in bess-map shows real filenames.
Then optionally clean up old generic rows:
```sql
DELETE FROM marketdata.province_cap_comp WHERE source LIKE 'KB/Claude search%';
DELETE FROM marketdata.province_fr_market WHERE source LIKE 'KB/Claude search%';
```

### Priority 2 — 内蒙古蒙东 bad value (0.28 ¥/kW)
The cap comp conflict UI shows 内蒙古（蒙东） = 0.28 ¥/kW — clearly wrong (should be ~280 or in ¥/kW context).
Fix: use Feishu to add correct value, then resolve conflict in bess-map UI:
```
/capcomp-add 内蒙古蒙东 2026 容量补偿XXX元/kW
```
(Need to verify correct value first — check NEA or inner Mongolia grid documents)

### Priority 3 — 甘肃 FR data (not yet done from original handoff)
```
/capcomp-add 甘肃 2026-04 调频价格10元/kW/h 调频资金池0.4亿元
```

### Priority 4 — FR data coverage (low)
Only 广西 has FR market data. To improve: ingest province FR settlement PDFs into KB (`staging.spot_knowledge_chunks`), then re-run `/capcomp`.

### Priority 5 — bidding_space gap (low)
`bidding_space_mw = 0.0` for most provinces from Mar 2026. LingFeng may have changed column name. Check `COLUMN_GROUPS["bidding"]` in `services/bess_map/run_fundamentals_ingest.py`.

---

## Key Architecture Notes

**Province name mapping (important!):**
| province_installed_monthly | spot_fundamentals_hourly |
|---------------------------|--------------------------|
| 冀南 | 河北南网 |
| 冀北 | 河北北网 |
| 蒙东 | 蒙东 (same ✓) |
| 蒙西 | 蒙西 (same ✓) |

**RDS DB connection:**
```
postgresql://postgres:!BESSmap2026@bess-platform-pg.cjs000o4wn2w.ap-southeast-1.rds.amazonaws.com:5432/marketdata?sslmode=require
```

**Local dev DB:** `postgresql://postgres:root@127.0.0.1:5433/marketdata`
- Does NOT have `spot_fundamentals_hourly`, `province_cap_comp`, `province_fr_market`, etc.
- Only has Mengxi-specific tables (md_*, hist_*)

**Capcomp unique constraint:** `(province, effective_date, COALESCE(source, ''))` — source is part of the key, so a re-scan with different source string inserts a NEW row (doesn't update old one). `ingested_at DESC` sort ensures newest row wins in display.

---

## Key Files

```
apps/bess-map/app.py                        # main app; load_cap_comp ORDER BY ingested_at DESC
services/market_fundamentals/loader.py      # load_latest_installed_monthly() — schema fix + alias map
services/hermes/capcomp_screener.py         # always uses source_hint (KB filenames) as source
services/hermes/capcomp_etl.py             # upsert_cap_comp_rows; conflict >5% → status='conflict'
services/hermes/capcomp_manual_etl.py       # Feishu /capcomp-add
services/hermes/capacity_manual_etl.py      # Feishu /capacity-add; date injection
scripts/backfill_capcomp_sources.py         # one-shot: UPDATE source for generic-tagged rows (run manually if needed)
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

## Feishu Commands

```
/capcomp                          # trigger full 32-province scan (~35 min) — DO THIS FIRST
/capcomp-add 甘肃 2026-04 调频价格10元/kW/h 调频资金池0.4亿元
/capacity-add 山东 2026-07 储能装机9700MW
```
