# Handoff — Hermes Nodal Annual PF Backfill (2026-07-04)

## Status

`/backfill-annual 2025` has been triggered in Feishu. It is **currently running** on ECS Fargate (td:148, redeployed 2026-07-04). Expected completion: ~4–5 hours from trigger. **Do not redeploy Hermes while it is running.**

---

## What Was Built

### Goal
Rank all 40 Inner Mongolia BESS plants by **2025 pre-BESS-era nodal merit** (4h perfect-foresight arbitrage spread) to:
1. Reveal true node quality before 2026 BESS builds suppressed spreads
2. Quantify the **BESS market impact** per node (2025 rank vs current 2026 YTD rank delta)

### Problem
18 of the 40 plants are **2026 new builds** with no 2025 price data in `marketdata.md_id_cleared_energy`.

### Solution — Nodal Proxy via Cluster Table
Use `marketdata.inner_mongolia_nodal_clusters` (already built for the Nodal Peer Explorer feature) to find peer plants at the same settlement node. Plants sharing a node have identical cleared prices → their 2025 prices are valid proxies.

**Logic in `_resolve_nodal_proxies()`:**
1. Load latest cluster run (highest `end_date`) from `inner_mongolia_nodal_clusters`
2. For each missing plant, look up its `cluster_id` → find all peer plants in the same cluster
3. Single batch query on `md_id_cleared_energy` to find which peers have 2025 data
4. Return `{missing_plant → proxy_plant}` map

### Schema Change
```sql
ALTER TABLE reports.nodal_pf_annual
  ADD COLUMN IF NOT EXISTS proxy_plant_name TEXT;
```
Handled via `ADD COLUMN IF NOT EXISTS` inside `compute_and_store_nodal_pf_annual`.

### PDF Column
- **Direct data** → `#N` (e.g. `#3`)
- **Proxy-inferred** → `~#N` (e.g. `~#7`)
- Column header: `2025年4h`

---

## Key Files

| File | What Changed |
|------|-------------|
| `services/hermes/mengxi_ranking_report.py` | `_resolve_nodal_proxies()` (new), `compute_and_store_nodal_pf_annual` (proxy logic), `_query_nodal_annual_ranks` (negative rank for proxy), `_nodal_cell` (~#N display) |
| `services/hermes/app.py` | `/backfill-annual [YEAR]` Feishu chat command |
| `apps/hermes-service/requirements.txt` | `pulp>=2.7` (added in prior session) |

Latest commit: `1dd5112` on `cost-optimisation`

---

## After Backfill Completes

1. Feishu shows: `✅ 2025 年节点PF排名已计算完成 (40/40 plants)`
2. Send `/report` in Feishu to regenerate the ranking PDF
3. Verify the PDF has `2025年4h` column with 40 rows — `#N` for 22 direct plants, `~#N` for 18 proxy plants

---

## Pending Strategic Feature (Not Yet Built)

**Node merit impact delta** — compare 2025 nodal rank vs 2026 YTD nodal rank per plant and display the delta (e.g. `↓5`) in a new PDF column or a separate table. This shows which nodes have been most degraded by new BESS builds.

To implement:
- In `_generate_pdf`, load both `_query_nodal_annual_ranks(pg_url, 2025)` and current YTD ranks
- Compute `delta = rank_2026_ytd - rank_2025` per plant
- Add `节点影响` column to the PDF table

---

## Infrastructure

- **ECS service**: `bess-platform-hermes-svc` on `bess-platform-cluster` (ap-southeast-1)
- **Task def**: td:148
- **ECR**: `319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-platform-hermes:latest`
- **Log group**: `/ecs/bess-platform`, stream prefix `hermes/hermes/<task-id>`
- **Branch**: `cost-optimisation`

Deploy command:
```bash
docker build --no-cache -f apps/hermes-service/Dockerfile -t 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-platform-hermes:latest --platform linux/amd64 .
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-platform-hermes:latest
aws ecs update-service --cluster bess-platform-cluster --service bess-platform-hermes-svc --force-new-deployment --region ap-southeast-1
```

**Do NOT use DOCKER_BUILDKIT=0** — breaks the build.  
**Always use `--no-cache`** on Windows/OneDrive (timestamps not updated, all layers otherwise show as CACHED).

---

## Corporate Network Note

POST requests to `https://pjh-etrm.ai/hermes/*` are blocked by the corporate proxy on LAN (returns 405 `Allow: GET, HEAD`). Hermes never receives them. Chat commands via Feishu WebSocket bypass this — always use Feishu chat for triggering backfills.
