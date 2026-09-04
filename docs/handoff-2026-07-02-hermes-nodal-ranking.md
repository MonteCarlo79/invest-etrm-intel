# Handoff: Hermes — Nodal BESS Ranking PDF Fix
**Date:** 2026-07-02  
**Branch:** `cost-optimisation`  
**Repo:** `C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform`

---

## What was just completed (do NOT redo)

Three commits pushed to `cost-optimisation` today:

1. **`8d4d712`** — `_latest_data_date()` now queries only `md_id_cleared_energy` (not `GREATEST(id,da)`).  
   - Root cause: `md_da_cleared_energy` had data for July 1 (next-day DA), making `GREATEST` return July 1 → empty "latest" and "month" sections.  
   - Fix: single `SELECT MAX(data_date) FROM marketdata.md_id_cleared_energy`.

2. **`bc87c43`** — Added `pulp>=2.7` to `apps/hermes-service/requirements.txt`.  
   - Root cause: `_compute_nodal_pf_ranks()` imports `services.bess_map.optimisation_engine` which does `import pulp`, but pulp was not installed in the Hermes container → silent exception → nodal ranks blank → "—" in PDF.

3. **`12c2001`** — Added `scripts/debug_nodal_pf.py` (local debug helper, not critical).

---

## Current state

- **Hermes ECS container**: partially fixed — the June 29 date and nodal columns ARE showing in the PDF (the user rebuilt once already), but nodal columns show "—" because pulp was missing at build time.
- **Still needed**: rebuild and redeploy Hermes Docker image to get the pulp fix into the container.

---

## The one remaining task: rebuild and redeploy Hermes

```bash
REGION="ap-southeast-1"
ACCOUNT="319383842493"
IMAGE="${ACCOUNT}.dkr.ecr.${REGION}.amazonaws.com/bess-platform-hermes:latest"

# 1. ECR login
aws ecr get-login-password --region $REGION | \
  docker login --username AWS --password-stdin "${ACCOUNT}.dkr.ecr.${REGION}.amazonaws.com"

# 2. Build (no DOCKER_BUILDKIT=0)
docker build -f apps/hermes-service/Dockerfile -t $IMAGE --platform linux/amd64 .

# 3. Push
docker push $IMAGE

# 4. Force redeploy
aws ecs update-service \
  --cluster bess-platform-cluster \
  --service bess-platform-hermes-svc \
  --force-new-deployment \
  --region $REGION \
  --output text --query 'service.deployments[0].status'
```

Or use the deploy script (note: correct Dockerfile path is `apps/hermes-service/Dockerfile`):
```bash
bash scripts/deploy_hermes.sh
```

After the new task is running (~2 min), trigger `/report` in Feishu. The PDF should show actual nodal rank numbers (e.g. `#3`, `#12`) instead of "—".

---

## What to verify after redeploy

1. Trigger `/report` in Feishu → PDF date should be **2026-06-29** (or newer if data arrives)
2. "Latest" and "Month" sections should have BESS data (not 0 plants)
3. Last two columns (`2h节点排名`, `4h节点排名`) should show `#N` ranks, not "—"
4. Green/red colour coding: green = actual rank < nodal rank (outperforms location); red = underperforms

---

## Key files

| File | What changed |
|---|---|
| `services/hermes/mengxi_ranking_report.py` | `_latest_data_date()` fix (line ~756); `_compute_nodal_pf_ranks()` (line ~228); `_enrich_and_rank()` NaN owner fix (line ~306) |
| `apps/hermes-service/requirements.txt` | Added `pulp>=2.7` |
| `apps/hermes-service/Dockerfile` | `PYTHONPATH=/app`; copies `services/` and `shared/`; CBC bundled with pulp |

---

## Architecture of nodal ranking (for context)

The nodal PF ranking runs inline during the daily report:

1. For each BESS plant in `plant_names`, fetch 15-min cleared prices from `md_id_cleared_energy` for "yesterday"
2. Run MILP (`services/bess_map/optimisation_engine.compute_dispatch_from_15min_prices`) for 2h and 4h BESS duration, `power_mw=1.0`, `roundtrip_eff=0.85`
3. Score = `profit.sum() / (duration_h * n_days)` — normalised CNY/MWh/day
4. Rank all plants by score descending → `rank_2h`, `rank_4h`
5. Parallelised with `ThreadPoolExecutor(max_workers=8)` — ~10s for ~100 plants
6. A monthly pre-computed version is stored in `reports.nodal_pf_monthly` (cron job: 5th of each month at 01:00 UTC)

**Known potential UTC/CST issue** (not confirmed as root cause but worth watching):  
The `datetime` column in `md_id_cleared_energy` may store UTC timestamps. `compute_dispatch_from_15min_prices` groups intervals by calendar date after stripping timezone. If timestamps are UTC, Beijing day N spans two UTC dates → no complete 96-interval day → LP returns zero profit → scores are 0.0 (not NaN) → ranks still computed (but meaningless).  
If after the pulp fix the ranks are all `#1` or show identical values, this UTC→CST conversion is needed in `_compute_plant`:
```python
# Add this BEFORE calling compute_dispatch_from_15min_prices:
if prices_s.index.tz is not None:
    prices_s.index = prices_s.index.tz_convert("Asia/Shanghai").tz_localize(None)
else:
    prices_s.index = prices_s.index + pd.Timedelta(hours=8)
```

---

## Next features (not started)

- **Mengxi Trader mark-to-market**: see `docs/handoff-2026-06-30-trader-mtm.md` — needs brainstorming with user before building
- **BESS ranking dashboard tab**: 33 stations shown (limited to `station_master`); user may want more plants or auto-discovery from `md_id_cleared_energy`; nodal PF columns not yet in dashboard (only in PDF)
