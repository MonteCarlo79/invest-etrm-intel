# Handoff — bess-map IRR tab fixes & deploy hardening (2026-08-08)

**For the next Claude session working on `apps/bess-map`.** Read CLAUDE.md and MEMORY.md first, then this.

---

## 1. Where things stand

- **Live: bess-map v63 on ECS** (`bess-platform-bess-map-svc`, terraform-managed task def family `bess-platform-bess-map`). Healthy, logs clean.
- **All tests green: 27 passed** (`apps/bess-map/tests/` — irr_helpers + def-order guards).
- Deploys v61→v62→v63 happened 2026-08-07/08 to fix a production outage and two follow-ups.

## 2. What was broken and what changed

### v60 production outage (root causes — both from the same loader hoist)
1. `NameError: load_sysopfee` — v60 hoisted `_sof_df = load_sysopfee(_ENG_KEY)` to module level (line ~1733) but left `def load_sysopfee` at line ~2702. Streamlit runs top-to-bottom → whole app crashed on load. **Fix:** def relocated next to the other two loaders (`load_cap_comp`, `load_fr_market`, ~line 1604).
2. Latent `KeyError: 'province'` — the FR-demand section reused `_fr_df` for a localized display frame, clobbering the shared frame the aux tab reads. **Fix:** display frame renamed `_fr_demand_df`.

**Regression guards:** `apps/bess-map/tests/test_app_def_order.py` (AST-based, no streamlit needed):
- no module-level call may precede its def
- shared frames `_sof_df`/`_cc_df`/`_fr_df` may only be assigned once at module level

**Why v60 shipped broken:** its 18 tests only covered `irr_helpers.py`; nobody executed app.py before deploy. **New pre-deploy standard:** run the headless smoke test below.

### Headless smoke test (use before EVERY Streamlit deploy)
```bash
~/.venvs/bess-platform/bin/python - <<'EOF'
import os
for line in open("config/.env"):
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
os.environ.setdefault("AUTH_MODE", "dev")
from streamlit.testing.v1 import AppTest
at = AppTest.from_file("apps/bess-map/app.py", default_timeout=240)
at.run()
print("exception:", bool(at.exception))
EOF
```
AppTest executes the full script (all tabs) against the real DB — catches module-level crashes unit tests can't.

### 系统运行费 default rule (user-specified, current)
In `_irr_defaults_for_province` (irr_helpers.py): **past 12 months of `province_sysopfee_monthly`, floored at 2026-01-01; NO pre-2026 fallback** (2025-only provinces get 0 + label "无2026数据", user keys it manually). Label: `2026至今均值 (N个月)`. The geo-map overlay (`_build_extra_rev_map`) shares this helper, so both tabs stay consistent by construction.

### Payback fix (v63)
IRR tab simple payback previously never counted the initial equity outlay (`cum = 0.0` + operating years) → almost always showed "1年". Now `_compute_payback(cashflows)` in irr_helpers.py starts from `cfs[0]`. Numbers are now materially longer (甘肃 4h: 1年 → 13年) — correct, not a regression.

### Terraform reconciliation (do not skip this again)
Live td:91 had 5 env vars terraform didn't know (HERMES_URL, DEEPSEEK_API_KEY, OPENAI_API_KEY, LINGFENG_USERNAME, LINGFENG_PASSWORD) and BEDROCK_REGION drifted (tf said us-east-1, live ap-southeast-1). A blind `terraform apply` would have stripped them. Reconciled into `infra/terraform/main.tf` + `variables.tf` (new vars: `deepseek_api_key`, `lingfeng_username`, `lingfeng_password`; `openai_api_key` already existed). Values live in `terraform.tfvars` — **never stage tfvars in git** (repo rule). Note: td:91's LingFeng password was stale; tfvars carries the working one (verified equal to the daily ingest service's, sourced from `config/.env`).

## 3. Deploy procedure that works from this Mac (learned the hard way)

```bash
docker build --platform linux/amd64 -f apps/bess-map/Dockerfile -t bess-map:vN .   # repo-root context, NOT apps/bess-map
docker tag bess-map:vN 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-map:vN
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-map:vN
docker buildx imagetools inspect 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-map:vN | grep Platform   # MUST say linux/amd64
# tfvars: image_bess_map = ".../bess-map:vN"  (local only, never commit)
cd infra/terraform
terraform plan  -target=aws_ecs_task_definition.bess_map -target=aws_ecs_service.bess_map -out=/tmp/tfplan
terraform apply /tmp/tfplan
```

- **`--platform linux/amd64` is mandatory on this arm64 Mac** — plain build makes arm64-only images; Fargate fails with `CannotPullContainerError ... platform 'linux/amd64'`.
- **Always use `-target`** for these two resources and review the plan: the wider terraform state is DRIFTED (tfvars has portal v9 vs live v24, etc.). A blanket `terraform apply` would roll back ~11 services and destroy 10 resources. Never run untargeted apply.
- Verify after rollout: new target `healthy` in TG `tgmap-*`, container log shows "You can now view your Streamlit app", zero Traceback lines.
- ECR login expires ~12h: `aws ecr get-login-password --region ap-southeast-1 | docker login --username AWS --password-stdin 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com`

## 4. Known data observations

- 甘肃 `province_sysopfee_monthly` has **12 monthly rows for 2026** (only ~7 months elapsed) — possibly Hermes-projected values. The 2026-average rule consumes them as-is; sanity-check if numbers look off.
- Remaining LingFeng price gaps (from the 2026-07-28 handoff, still blocked on manual Excel downloads → `data/backfill/` → `python scripts/backfill_protective.py --indir data/backfill --schema marketdata`): 湖南 384d, 浙江 279d, 福建 271d, 新疆 189d, 江苏 136d, 青海 103d. In-app 批量补录 should now work on ECS (LingFeng creds live since td:92).

## 5. OneDrive + git hazards on this Mac (critical ops context)

- OneDrive offloads files (incl. `.git` objects) aggressively; **a full disk makes it worse** (hit ENOSPC at 536 MB free on 2026-08-08; freed via local-snapshot thinning + Docker.raw reset + OneDrive Free-Up-Space on data folders).
- If git dies with `fatal: mmap failed: Operation timed out` / `bad object`: 1) ensure OneDrive client is running (`pgrep -f "OneDrive.app/Contents/MacOS"`; `open -a OneDrive` if not), 2) hydrate: `find .git -type f -print0 | xargs -0 -P 8 -n 50 cat | wc -c`, 3) if specific loose objects still time out, OneDrive is mid-sync — wait for menubar "Up to date", then retry.
- `git show --stat` may report text files as "Bin" locally (mmap over FileProvider quirk) — cosmetic; blobs are clean (verify with `git cat-file blob <sha> | head`).
- A stale `index.lock` from a killed git command blocks commits — check `ps` for git processes, then `rm .git/index.lock`.
- Long batch jobs should stage files in /tmp, not OneDrive paths.

## 6. Git state at handoff

- Branch: `feat/deal-structurer-bedrock-migration`. Local commits incl. bess-map fixes (5834e90 crash fixes, 6826e5b terraform reconcile, 132dfd6 ERRORS.md, 9a5222d 2026-avg, efe4727 12mo-floor rule, 1524ee1 payback) plus the vault session's commits (9dbd253, 1471869, 4328105, f52e64a, 140693f, f6c7b76…).
- Remote has `b90712f` (vault hooks, pushed from the OTHER OneDrive clone at `OneDrive-Personal/etrm/` — note lowercase) that local lacks → push needs a merge of b90712f first. b90712f touches `scripts/ingest_nodal_csvs.py`, which the ingest session may still have dirty — check `git status --short -- scripts/` before merging.
- **Two clones sync via OneDrive** (`ETRM/` and `etrm/`) — coordinate before running git in both at once; `.git/index.lock` collisions observed.

## 7. Useful commands

```bash
# tests
~/.venvs/bess-platform/bin/python -m pytest apps/bess-map/tests/ -q     # 27 pass
# local run
set -a; source config/.env; set +a
streamlit run apps/bess-map/app.py --server.port 8503
# service state
aws ecs describe-services --cluster bess-platform-cluster --services bess-platform-bess-map-svc \
  --region ap-southeast-1 --query "services[0].{td:taskDefinition,running:runningCount}"
```
