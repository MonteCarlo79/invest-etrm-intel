# Deal Structurer — Handoff 2026-07-16

## One-line status

**All 9 implementation tasks complete, 45 tests pass. App code is in git. Portal card is live in code but not yet deployed (Docker push pending).**

---

## What was built

A probabilistic deal-structuring & pricing platform (`apps/deal_structurer/`) that replaces Excel `.xlsm` valuation models. It is a **5-tab Streamlit app** with a Claude Strategist agent.

### Architecture (two-layer)

```
libs/deal_models/          ← pure computation, zero I/O
  price_simulator.py       ← OU + PCA price path models
  dispatch_valuation.py    ← BESS / wind / wind+BESS spread-call strip
  project_cashflow.py      ← IRR, ROACE, DSCR, NPV
  monte_carlo.py           ← 1000-10000 path orchestrator, VaR/CVaR/tornado
  deal_structures.py       ← 6 payoff functions + universal pricer
  registry.py              ← dict-based DealSpec registry
  contracts.py             ← all dataclasses (PriceSimRequest, DispatchRequest, etc.)
  adapters/
    agent_tools.py         ← AGENT_TOOLS list + dispatch_tool() for Claude API

services/deal_engine/      ← DB access + persistence
  price_data.py            ← fetch_price_history() from marketdata.spot_prices_hourly
  scenario_store.py        ← JSON save/load/list/delete in scenarios/<id>.json
  batch_runner.py          ← run_batch() with st.progress() callback

apps/deal_structurer/      ← Streamlit app (note: underscore for Python importability)
  app.py                   ← sidebar nav + portal back-link
  price_tab.py             ← Tab 1: price simulation
  dispatch_tab.py          ← Tab 2: dispatch revenue
  cashflow_tab.py          ← Tab 3: project cash flow
  mc_tab.py                ← Tab 4: Monte Carlo
  deal_tab.py              ← Tab 5: deal pricing
  strategist.py            ← Claude Strategist agent (tool-use, streaming)
```

### Key commits on branch `cost-optimisation`

```
5a2c331  refactor(portal): clean layout — Deal Structurer live, GB moved to International
315801c  feat(portal): enable Deal Structurer card + add portal back-link
2c3f86a  feat(deal-structurer): full Streamlit app — 5 tabs + Claude Strategist
3a1a3a9  feat(deal_engine): price_data, scenario_store, batch_runner
6a51639  feat(deal_models): agent tools adapter
ef61b6a  feat(deal_models): deal structures — 6 payoff functions + universal pricer
3575338  fix: remove fallback revenue logic from monte_carlo (spec compliance)
6058469  feat(deal_models): monte carlo orchestrator
d216630  feat(deal_models): project cashflow — IRR, ROACE, DSCR, NPV
6b82ec9  feat(deal_models): dispatch valuation
af68a71  feat(deal_models): price simulator — OU and PCA models
```

---

## What still needs to be done

### 1. Deploy portal v8 (highest priority — unblocks Deal Structurer card)

The portal `app.py` already has the Deal Structurer card set to `available=True` (commit `5a2c331`), but the live ECS service is still running the old **v7** Docker image. `terraform.tfvars` has been updated to `v8`.

**Steps:**
```bash
# From repo root, on branch cost-optimisation
cd C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform

# 1. Ensure Docker Desktop is running
docker version   # should show both Client and Server

# 2. ECR login
aws ecr get-login-password --region ap-southeast-1 | \
  docker login --username AWS --password-stdin 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com

# 3. Build (already done in previous session — image is in local Docker cache as v8)
#    Only re-run if cache was cleared:
docker build --platform linux/amd64 -f apps/portal/Dockerfile \
  -t 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-platform-portal:v8 .

# 4. Push
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-platform-portal:v8

# 5. Deploy
cd infra/terraform
terraform apply -target=aws_ecs_task_definition.portal -target=aws_ecs_service.portal -auto-approve
```

**Verify:** Visit `https://pjh-etrm.ai/portal/` → Deal Structurer card should show **"Open App →"** button.

**If button still shows "Coming Soon":** Check your Cognito role in the portal header. The button requires role `Admin`, `Trader`, `Quant`, or `Analyst`. Role `Viewer` is excluded (`CAN_OPEN_APPS` at `apps/portal/app.py:257`).

---

### 2. Deploy deal-structurer app itself

The `apps/deal_structurer/` Streamlit app has **never been deployed** — there is no Docker image, no ECS service, no ALB rule for `/deal-structurer/`. It needs:

1. **ECR repository** — `bess-platform-deal-structurer`
2. **Dockerfile** at `apps/deal-structurer/Dockerfile` (note: the app dir in git uses underscore `apps/deal_structurer/` but the URL slug uses hyphen `deal-structurer`)
3. **ECS task definition + service** in terraform
4. **ALB listener rule** for path `/deal-structurer/*`
5. **Cognito auth** (reuse same Cognito user pool / client as other services)

The service is already defined in `shared/service_control.py` SERVICES dict (key `"deal-structurer"`).

Reference: look at how `apps/spot-market/` or `apps/gb-market/` are deployed — their Dockerfiles, terraform modules, and ALB rules are the pattern to follow.

---

### 3. Pending git commit (minor)

The implementation plan file `docs/superpowers/plans/2026-07-13-deal-structurer.md` is staged but not committed — `git commit` was hanging (likely OneDrive `.git` lock). Run manually:

```bash
git commit -m "docs: add deal-structurer implementation plan"
git push origin cost-optimisation
```

---

## Key file locations

| What | Path |
|------|------|
| App entry point | `apps/deal_structurer/app.py` |
| Core models | `libs/deal_models/` |
| Tests | `libs/deal_models/tests/` (45 tests, all pass) |
| Services | `services/deal_engine/` |
| Portal card | `apps/portal/app.py` ~line 778 |
| Service registry | `shared/service_control.py` key `"deal-structurer"` |
| Terraform image tag | `infra/terraform/terraform.tfvars` → `image_portal = "...portal:v8"` |
| Implementation spec | `docs/superpowers/specs/2026-07-09-deal-structurer-design.md` |
| Implementation plan | `docs/superpowers/plans/2026-07-13-deal-structurer.md` |

---

## Run tests locally

```bash
cd C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform
pytest libs/deal_models/tests/ -v
# Expected: 45 passed
```

---

## Design decisions to remember

- **BESS dispatch uses sigma=60 OU**: intraday spread ~15 yuan/MWh is below the 1/0.85 = 1.176× roundtrip-eff breakeven, so BESS revenue ≈ 0 with typical params. Use `wind` asset type in tests.
- **Deal pricing**: `price_structure()` charges `expected_cost + 0.30 × max(CVaR_95 - expected_cost, 0)` as `suggested_premium`.
- **Agent tools**: `dispatch_tool()` in `libs/deal_models/adapters/agent_tools.py` uses module-level globals `_last_mc_result` and `_last_price_paths` to share state between tool calls within a Strategist turn.
- **DB prices**: `marketdata.spot_prices_hourly` (columns: `province`, `datetime`, `da_price`, `rt_price`). Auto-converts kWh→MWh if median < 5.0.
- **Scenario store**: writes JSON to `scenarios/<id>.json` relative to CWD. In production this will need an S3-backed store or EFS mount.

---

## Branch

`cost-optimisation` — pushed to `https://github.com/MonteCarlo79/invest-etrm-intel.git`
