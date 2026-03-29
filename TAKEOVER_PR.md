# OpenClaw Takeover PR — Mengxi Terraform Stabilisation

**Branch**: `feature/openclaw-mengxi-terraform-takeover`
**Base**: `feature/mengxi-strategy-diagnostics` (commit `8e50248`)
**PR target**: `main`
**Operator**: OpenClaw (MiniMax Matrix Agent)
**Date**: 2026-03-30

---

## 1. Mission Summary

This branch represents a full controlled audit and verification of the Mengxi / Inner Mongolia
BESS platform work. The audit found the codebase in **better-than-expected condition** — all
prior sessions (Codex + MiniMax) had left the code in a functionally correct state. No
Terraform changes and no application code changes were required.

This PR documents, verifies, and formally closes the takeover operation.

---

## 2. Branch Lineage

```
main
└── feature/mengxi-strategy-diagnostics   ← all Mengxi commits (Codex + MiniMax)
    └── feature/openclaw-mengxi-terraform-takeover  ← THIS BRANCH (audit + PR only)
```

The takeover branch is a clean forward extension of the strategy diagnostics branch. No
cherry-picks, rebases, or force-pushes were performed.

### Commits included from prior work (newest → oldest)

| SHA       | Message                                                       |
|-----------|---------------------------------------------------------------|
| `8e50248` | Bring Codex pnl attribution cache and date guard fix          |
| `51b444a` | chore(im): remove unrelated pnl_attribution copy from Dockerfile |
| `ddbc760` | feat(im): add Strategy Diagnostics page (v1)                  |
| `fc83395` | Add dedicated pnl attribution image build path and ECR repo   |
| `1c1a1f7` | Run explicit Mengxi pnl attribution Streamlit entrypoint in ECS task |
| `2095351` | Freeze inner_mongolia task definition drift via lifecycle ignore_changes |
| `2145e57` | Stabilize inner_mongolia task tags to avoid null-vs-empty drift |
| `6b34fad` | Keep inner_mongolia task sizing unchanged to avoid service drift |
| `5ca976c` | Remove remaining Terraform drift for inner_mongolia and legacy SG descriptions |
| `5f78559` | Prevent unintended Terraform drift for existing ECS and RDS resources |
| `9ec9ad8` | Add validation for required PnL attribution image and PGURL   |
| `f656e5a` | Add optional ECS/Fargate Terraform service for Mengxi PnL attribution |

---

## 3. Audit Findings — What Was Verified

### 3.1 apps/bess-inner-mongolia/im/Dockerfile ✅ CLEAN
The previously-reported stray line (`COPY apps/trading/bess/mengxi/pnl_attribution ...`) was
already removed in commit `51b444a`. Dockerfile is correct:

```
PYTHONPATH=/apps
EXPOSE 8504
CMD streamlit run app.py --server.port=8504 --server.baseUrlPath=inner-mongolia
COPY services/bess_inner_mongolia /apps/services/bess_inner_mongolia   ← correct
```

**No action required.**

### 3.2 services/bess_inner_mongolia/ — Full import chain verified ✅

| Module              | Key functions                                                         | Evidence level  |
|---------------------|-----------------------------------------------------------------------|-----------------|
| `queries.py`        | `load_results_flat`, `load_clusters`, `load_available_date_ranges`    | observed/proxy  |
| `peer_benchmark.py` | `compute_leaderboard`, `compute_envision_vs_top`, `compute_gap_decomposition` | observed/proxy |
| `strategy_diagnostics.py` | `infer_strategy_style`, `build_strategy_table`              | heuristic       |

All functions present. All private helpers (`_get`, `_diff`, `_all_ok`, `_nansum`) confirmed.

### 3.3 apps/bess-inner-mongolia/im/ — All imports resolve under PYTHONPATH=/apps ✅

| Import in container (`/apps`)                            | Source file on disk                         | Status |
|----------------------------------------------------------|---------------------------------------------|--------|
| `auth.rbac.require_role`                                 | `auth/rbac.py`                              | ✅     |
| `shared.core.irr_from_cashflows`                         | `apps/bess-inner-mongolia/shared/core.py`   | ✅     |
| `shared.core.build_peer_detail_table`                    | `apps/bess-inner-mongolia/shared/core.py`   | ✅     |
| `shared.core.infer_asset_type`                           | `apps/bess-inner-mongolia/shared/core.py`   | ✅     |
| `services.bess_inner_mongolia.queries.*`                 | `services/bess_inner_mongolia/queries.py`   | ✅     |
| `services.bess_inner_mongolia.peer_benchmark.*`          | `services/bess_inner_mongolia/peer_benchmark.py` | ✅ |
| `services.bess_inner_mongolia.strategy_diagnostics.*`    | `services/bess_inner_mongolia/strategy_diagnostics.py` | ✅ |

### 3.4 apps/trading/bess/mengxi/pnl_attribution/ ✅ CLEAN
- `app.py`: read-only Streamlit UI; reads `reports.bess_asset_daily_scenario_pnl` and
  `reports.bess_asset_daily_attribution` via `DB_DSN` / `PGURL` env var; no dependency on
  `calc.py` in production
- `calc.py`: batch job library; used only by `services/trading/bess/mengxi/run_pnl_refresh.py`
- `Dockerfile`: FROM python:3.11-slim, port 8502, baseUrlPath=pnl-attribution, no stray COPYs
- `requirements.txt`: `streamlit`, `pandas`, `sqlalchemy`, `psycopg2-binary` — sufficient

### 3.5 infra/terraform/main.tf — pnl-attribution FULLY WIRED ✅

All resources are count-gated on `var.enable_pnl_attribution_service`:

| Resource                                       | Gate                                 | Notes                       |
|------------------------------------------------|--------------------------------------|-----------------------------|
| `aws_ecr_repository.pnl_attribution`           | always created (no count gate)       | repo name: `bess-pnl-attribution` |
| `aws_lb_target_group.pnl_attribution`          | `enable_pnl_attribution_service`     | port 8502, health: `/_stcore/health` |
| `aws_lb_listener_rule.pnl_attribution_path`    | `enable_pnl_attribution_service`     | priority 25, path `/pnl-attribution/*` |
| `aws_ecs_task_definition.pnl_attribution`      | `enable_pnl_attribution_service`     | 512 CPU / 1024 MB, env: PGURL+DB_DSN |
| `aws_ecs_service.pnl_attribution`              | `enable_pnl_attribution_service`     | desired_count=1             |

ECS security group allows ingress 8500–8504; port 8502 is within range. ✅
Cognito auth applied to all ALB listener rules. ✅

### 3.6 infra/terraform/variables.tf — All variables declared ✅

```hcl
variable "enable_pnl_attribution_service"   { type = bool;   default = false }
variable "pnl_attribution_image"            { type = string; default = "";    validation: required when enabled }
variable "pnl_attribution_container_port"   { type = number; default = 8502  }
variable "pnl_attribution_path"             { type = string; default = "/pnl-attribution" }
variable "pnl_attribution_cpu"              { type = number; default = 512   }
variable "pnl_attribution_memory"           { type = number; default = 1024  }
variable "pnl_attribution_desired_count"    { type = number; default = 1     }
variable "pnl_attribution_pgurl"            { type = string; sensitive = true; validation: required when enabled }
```

---

## 4. Files Changed in This PR

**Terraform files changed**: None. All wiring was already complete.

**Application files changed**: None. All code was already correct.

**Files added by this PR**:
- `TAKEOVER_PR.md` (this document) — audit record and operator handoff guide

---

## 5. Unresolved Runtime Dependencies

These are not blockers for merging but must be resolved before `enable_pnl_attribution_service=true`:

| Item | Description | Owner |
|------|-------------|-------|
| `pnl_attribution_image` | ECR image URI for `bess-pnl-attribution` — must be built and pushed before enabling the service | Platform Reliability Agent / CI |
| `pnl_attribution_pgurl` | PostgreSQL DSN with read access to `reports.bess_asset_daily_scenario_pnl` and `reports.bess_asset_daily_attribution` | Data / DB team |
| `reports.*` tables | Tables must be populated by `run_pnl_refresh.py` batch job before UI is useful | Trading/Dispatch Agent |
| `marketdata.inner_mongolia_bess_results` | Must be populated for IM app Strategy Diagnostics page to render data | Market Strategy Agent |
| `marketdata.inner_mongolia_nodal_clusters` | Must be populated for gap decomposition and nodal context | Market Strategy Agent |

---

## 6. Known Gaps (Not Blocking)

| Gap | Details | Recommended action |
|-----|---------|-------------------|
| `infra/terraform/trading-bess-mengxi/schedules.tf` is standalone | Defines 3 scheduled ECS batch jobs (`tt-province-loader`, `tt-asset-loader`, `mengxi-pnl-refresh`) but is NOT wired as a `module {}` block in `main.tf` | Wire as a module block in main.tf or apply independently via `terraform -chdir=infra/terraform/trading-bess-mengxi apply` |
| `services/bess-inner-mongolia/` (hyphen) coexists with `services/bess_inner_mongolia/` (underscore) | Hyphen directory contains legacy pipeline scripts; not importable as Python package; not used by any Streamlit app | Archive or move legacy scripts; name collision risk is low but confusing |
| `pnl_attribution_pgurl` validation logic | Validation fires if `enable_pnl_attribution_service=true` and `pnl_attribution_pgurl` is empty; will block `terraform plan` until DSN is supplied | Supply DSN via `-var` or `terraform.tfvars` at apply time |

---

## 7. Local Verification Commands

### 7a. Verify IM app imports (dry run, no DB needed)
```bash
cd C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform

docker build \
  -f apps/bess-inner-mongolia/im/Dockerfile \
  -t bess-im:verify \
  .

docker run --rm bess-im:verify \
  python -c "
from auth.rbac import require_role
from shared.core import irr_from_cashflows, build_peer_detail_table, infer_asset_type
from services.bess_inner_mongolia.queries import load_results_flat, load_clusters, load_available_date_ranges
from services.bess_inner_mongolia.peer_benchmark import compute_leaderboard, compute_envision_vs_top, compute_gap_decomposition
from services.bess_inner_mongolia.strategy_diagnostics import DISCLAIMER, EVIDENCE_OBSERVED, EVIDENCE_PROXY, EVIDENCE_HEURISTIC, infer_strategy_style, build_strategy_table
print('All imports OK')
"
```

### 7b. Verify pnl-attribution app imports (dry run, no DB needed)
```bash
docker build \
  -f apps/trading/bess/mengxi/pnl_attribution/Dockerfile \
  -t bess-pnl:verify \
  .

docker run --rm bess-pnl:verify \
  python -c "import streamlit; import pandas; import sqlalchemy; import psycopg2; print('All imports OK')"
```

### 7c. Run IM app locally with DB
```bash
docker run --rm -p 8504:8504 \
  -e PGURL="postgresql://user:pass@host:5432/dbname" \
  -e DB_DSN="postgresql://user:pass@host:5432/dbname" \
  bess-im:verify
# Visit: http://localhost:8504/inner-mongolia
```

### 7d. Run pnl-attribution app locally with DB
```bash
docker run --rm -p 8502:8502 \
  -e PGURL="postgresql://user:pass@host:5432/dbname" \
  -e DB_DSN="postgresql://user:pass@host:5432/dbname" \
  bess-pnl:verify
# Visit: http://localhost:8502/pnl-attribution
```

---

## 8. Terraform Commands — Enable pnl-attribution Service

### Step 1: Build and push the image
```bash
cd C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform

AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
AWS_REGION=ap-southeast-1   # adjust to your region

aws ecr get-login-password --region $AWS_REGION \
  | docker login --username AWS --password-stdin \
    $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com

docker build \
  -f apps/trading/bess/mengxi/pnl_attribution/Dockerfile \
  -t bess-pnl-attribution:latest \
  .

docker tag bess-pnl-attribution:latest \
  $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/bess-pnl-attribution:latest

docker push \
  $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/bess-pnl-attribution:latest
```

### Step 2: Plan (verify no unintended drift)
```bash
cd infra/terraform

terraform plan \
  -var="enable_pnl_attribution_service=true" \
  -var="pnl_attribution_image=$AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/bess-pnl-attribution:latest" \
  -var="pnl_attribution_pgurl=postgresql://user:pass@host:5432/dbname" \
  -out=pnl_attribution_enable.plan
```

Expected plan output:
```
Plan: 4 to add, 0 to change, 0 to destroy.
# aws_lb_target_group.pnl_attribution[0]           will be created
# aws_lb_listener_rule.pnl_attribution_path[0]     will be created
# aws_ecs_task_definition.pnl_attribution[0]        will be created
# aws_ecs_service.pnl_attribution[0]               will be created
# (aws_ecr_repository.pnl_attribution already exists)
```

### Step 3: Apply
```bash
terraform apply pnl_attribution_enable.plan
```

### Step 4: Verify
```bash
# Check ECS service is RUNNING
aws ecs describe-services \
  --cluster <cluster-name> \
  --services <name>-pnl-attribution \
  --query "services[0].{status:status,running:runningCount,desired:desiredCount}"

# Check ALB health
aws elbv2 describe-target-health \
  --target-group-arn $(terraform output -raw pnl_attribution_target_group_arn)
```

---

## 9. Operator Handoff Checklist

- [x] `feature/openclaw-mengxi-terraform-takeover` branch created from `feature/mengxi-strategy-diagnostics` HEAD
- [x] Branch pushed to origin (tracking set)
- [x] IM app Dockerfile verified clean (bad COPY removed in `51b444a`)
- [x] All Python imports for IM app verified (auth, shared, services.bess_inner_mongolia)
- [x] Strategy Diagnostics page (4 tabs) verified structurally complete
- [x] pnl-attribution app verified clean (no stray imports, correct port 8502)
- [x] Terraform resources verified: ECR, TG, listener rule, task def, ECS service all wired
- [x] All Terraform variables declared with correct types, defaults, and validations
- [x] Evidence levels declared on all analytical outputs (observed/proxy/heuristic)
- [x] DISCLAIMER present on all heuristic inferences
- [ ] Docker image built and pushed to ECR (operator action required)
- [ ] `enable_pnl_attribution_service=true` applied with image + pgurl (operator action required)
- [ ] `reports.*` tables populated by batch job (data pipeline action required)
- [ ] `infra/terraform/trading-bess-mengxi/schedules.tf` wired into main.tf (future sprint)

---

## 10. PR Merge Instructions

1. Open PR: `feature/openclaw-mengxi-terraform-takeover` → `main`
2. Title: `feat(mengxi): stabilise Mengxi IM + pnl-attribution; Terraform-ready`
3. Reviewers: GPT-5.4 (architecture sign-off), platform lead
4. Merge strategy: **squash merge** — this branch contains prior Codex/MiniMax commit noise;
   squash keeps main clean
5. After merge: delete both `feature/mengxi-strategy-diagnostics` and
   `feature/openclaw-mengxi-terraform-takeover` (superseded)

---

*Generated by OpenClaw (MiniMax Matrix Agent) — operations orchestrator*
*Audit scope: additive verification only; no destructive changes made*
