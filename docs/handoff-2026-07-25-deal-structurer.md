# Deal Structurer — Handoff Note (2026-07-25)

## What this app is

`apps/deal_structurer/` — a Streamlit deal-pricing platform deployed on ECS Fargate at
`pjh-etrm.ai/deal-structurer/` (port 8522, baseUrlPath=`deal-structurer`).

Five tabs: Price Simulation → Dispatch Revenue → Project Cash Flow → Monte Carlo → Deal Pricing, plus a Strategist AI sidebar.

---

## Current state (as of 2026-07-25)

### ECS deployment
| Item | Value |
|------|-------|
| Cluster | `bess-platform-cluster` (ap-southeast-1) |
| Service | `bess-platform-deal-structurer-svc` |
| Task definition | `bess-platform-deal-structurer:14` |
| Image | `319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-platform-deal-structurer:v12` |
| Status | ACTIVE, 1/1 running |

### Git
Branch: `feat/deal-structurer-bedrock-migration`
Commit: `ff0f1d7` — all work up to this handoff is committed and pushed to
`github.com/MonteCarlo79/invest-etrm-intel`.

---

## What was done in this session (in order)

### 1. Bedrock migration (`strategist.py`)
- Replaced `anthropic.Anthropic()` with `make_client` / `is_llm_available` from
  `shared/anthropic_client.py`.
- Added `COPY shared/ ./shared/` to Dockerfile (was missing — caused import failure
  at container startup).
- `BEDROCK_REGION=ap-southeast-1` is set in the task definition env.

> **China IP block**: Anthropic blocks both direct API and Bedrock
> `global.anthropic.*` / `apac.anthropic.*` from Chinese source IPs. The ECS task
> (Singapore AWS IP) works fine. Local Docker on a Chinese machine cannot use
> Bedrock — use VPN or rely on ECS.

### 2. PCA price simulation — OpenBLAS segfault (recurring fix)

**Symptom**: "Connection failed with status 404" after clicking "▶ Run Simulation"
with PCA model selected. The Streamlit process crashes silently (SIGSEGV), ECS
restarts the task, the browser session becomes invalid → 404.

**Root cause**: `np.linalg.svd` / large matrix multiply in `simulate_pca` triggers
multi-threaded OpenBLAS on Fargate, causing SIGSEGV.

**Fixes applied (all in v12/td:14)**:

1. `libs/deal_models/price_simulator.py` — `fit_pca`: replaced `np.linalg.svd` with
   `np.linalg.eigh` on the 24×24 covariance matrix (stable, small).
   ```python
   cov = Xc.T @ Xc                          # (24, 24)
   eigenvalues, eigenvectors = np.linalg.eigh(cov)
   loadings = eigenvectors[:, ::-1].T[:n_components]
   scores = Xc @ loadings.T
   ```

2. `libs/deal_models/price_simulator.py` — `simulate_pca`: replaced large BLAS
   matrix multiply `(182500, 4) @ (4, 24)` with PC-by-PC `np.outer` accumulation
   (no BLAS dgemm at all):
   ```python
   daily_profiles = np.tile(mean_profile, (total_days, 1))
   for pc in params.pc_params:
       score_col = rng.normal(loc=pc.loc, scale=pc.scale, size=total_days)
       daily_profiles += np.outer(score_col, loadings[pc.pc_index])
   ```

3. `apps/deal_structurer/Dockerfile`:
   ```dockerfile
   ENV OPENBLAS_NUM_THREADS=1
   ENV OMP_NUM_THREADS=1
   ```

4. Task definition td:14 environment — ALL BLAS thread limits set explicitly:
   `OPENBLAS_NUM_THREADS=1`, `OMP_NUM_THREADS=1`, `MKL_NUM_THREADS=1`,
   `VECLIB_MAXIMUM_THREADS=1`, `NUMEXPR_NUM_THREADS=1`.

### 3. Price-Wind Correlation SQL fix (`services/deal_engine/price_data.py`)
山东 and 蒙西 use RT pricing (`da_price = 0`). Fixed `fetch_price_wind_correlation`
SQL to fall back to `rt_price` when `da_price = 0`:
```sql
AVG(CASE WHEN da_price IS NOT NULL AND da_price != 0
         THEN da_price ELSE rt_price END) AS avg_price
```

### 4. Dispatch Revenue tab enhancements (`apps/deal_structurer/dispatch_tab.py`)

- **P10/P50/P90/Mean** metric rows for both total revenue (¥M) and unit revenue
  (¥/MWh) — 4-column layout.
- **Dual histograms** side-by-side: revenue distribution (blue) + unit revenue
  distribution (orange-red).
- **Wind cannibalization model** — new "Cannibalization Model" section for
  wind/wind_bess asset types:
  - *Price-Wind Correlation ρ* slider (−1 to +1, step 0.05)
    — hint: 蒙西 ≈ −0.38 · 山东 ≈ −0.13 · 安徽 ≈ +0.26
  - *CF Annual Volatility σ* slider (0 to 0.30, step 0.01)
  - Province correlation table via "Load Correlation Data" button

### 5. Dispatch valuation model (`libs/deal_models/dispatch_valuation.py`)

`_dispatch_wind` now applies a **Gaussian copula** per-simulation CF scale factor
correlated with mean annual price:
```
z_price[i] = (mean_price[i] − μ) / σ_price
z_wind = ρ·z_price + √(1−ρ²)·noise
cf_scale = clip(1 + σ_cf · z_wind, 0.2, 2.0)
revenue[i] = base_revenue[i] × cf_scale[i]
```
When ρ < 0 (cannibalization), high-price years have lower wind output.
`contracts.py` — `DispatchRequest` has two new optional fields:
```python
price_wind_corr: float = Field(0.0, ge=-1.0, le=1.0)
cf_volatility:   float = Field(0.0, ge=0.0, le=0.5)
```

---

## Known issues / TODO

- **PCA 404 still occurring?** If the crash persists after v12:
  - The code fix (np.outer loop) should be definitive. If 404 still shows,
    it's likely a stale browser session from a previous crash — hard-refresh
    (Ctrl+Shift+R) and retry.
  - Check ECS logs: `aws logs get-log-events --log-group-identifier
    "arn:aws:logs:ap-southeast-1:319383842493:log-group:/ecs/bess-platform"
    --log-stream-name "deal-structurer/deal-structurer/<TASK_ID>" --region ap-southeast-1`

- **Strategist AI (local)**: `ANTHROPIC_API_KEY` in td:14 is a key that may be
  access-restricted. The Bedrock path (BEDROCK_REGION=ap-southeast-1) is the
  working path on ECS. Locally on a Chinese IP, neither works without VPN.

- **ib-platform Bedrock migration**: separate repo, local MacBook, not yet
  migrated — listed as "No" in `docs/BEDROCK_MIGRATION_GUIDE.md`.

---

## How to redeploy (after code changes)

```bash
# 1. ECR login
aws ecr get-login-password --region ap-southeast-1 \
  | docker login --username AWS --password-stdin \
    319383842493.dkr.ecr.ap-southeast-1.amazonaws.com

# 2. Build & push (increment tag: v13, v14, ...)
cd /path/to/bess-platform
docker build -f apps/deal_structurer/Dockerfile \
  -t 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-platform-deal-structurer:vNN .
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-platform-deal-structurer:vNN

# 3. Register new task definition (copy td:14 JSON, change image tag)
# 4. Update service
aws ecs update-service \
  --cluster bess-platform-cluster \
  --service bess-platform-deal-structurer-svc \
  --task-definition bess-platform-deal-structurer:NN \
  --force-new-deployment \
  --region ap-southeast-1
```

**Always include these env vars in the task definition** (they prevent BLAS crashes):
```json
{"name": "OPENBLAS_NUM_THREADS", "value": "1"},
{"name": "OMP_NUM_THREADS",      "value": "1"},
{"name": "MKL_NUM_THREADS",      "value": "1"},
{"name": "VECLIB_MAXIMUM_THREADS","value": "1"},
{"name": "NUMEXPR_NUM_THREADS",  "value": "1"}
```

---

## Key file map

| File | Purpose |
|------|---------|
| `apps/deal_structurer/app.py` | Streamlit entry point, tab router |
| `apps/deal_structurer/price_tab.py` | Tab 1 — price simulation UI |
| `apps/deal_structurer/dispatch_tab.py` | Tab 2 — dispatch revenue UI + cannibalization sliders |
| `apps/deal_structurer/cashflow_tab.py` | Tab 3 — project cash flow |
| `apps/deal_structurer/mc_tab.py` | Tab 4 — Monte Carlo |
| `apps/deal_structurer/deal_tab.py` | Tab 5 — deal pricing |
| `apps/deal_structurer/strategist.py` | AI strategist (Bedrock) |
| `apps/deal_structurer/Dockerfile` | Container build (port 8522) |
| `libs/deal_models/price_simulator.py` | OU + PCA simulators |
| `libs/deal_models/dispatch_valuation.py` | BESS/wind/wind_bess revenue models |
| `libs/deal_models/contracts.py` | Pydantic schemas + result dataclasses |
| `services/deal_engine/price_data.py` | DB fetch: hourly prices + wind correlation |
| `shared/anthropic_client.py` | Bedrock/direct Anthropic client factory |
