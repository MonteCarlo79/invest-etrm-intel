# Handoff — bess-map: catch-up deploy + realistic-dispatch feature (in progress)

**For the next Claude session picking up `apps/bess-map`.** Read `CLAUDE.md` and `MEMORY.md` first, then this. Session was invoked as `/resume bess-map` / `/resume quant` — neither found a prior session by that name, so this doc is the continuity thread instead.

---

## 1. Where things stand

- **Live: bess-map v63 / td:94** on ECS (`bess-platform-bess-map-svc`), image built 2026-08-08. Matches CLAUDE.md's service table.
- Confirmed via `aws ecr describe-images` that no `v64`+ image has ever been built — v63 is genuinely the newest.
- Since the [2026-08-08 handoff](handoff-2026-08-08-bess-map-irr-fixes.md), 6 commits landed on `main` touching the `apps/bess-map`/`services/bess_map` path, **but only one touches `apps/bess-map/app.py` itself**:
  - `d859134` (2026-08-18) — Ollama local-LLM shadow-mode memory-extraction wiring, 5 lines in `app.py`, gated behind `LOCAL_LLM_SHADOW` env var (no-op unless set to `"1"`). This is the **only undeployed bess-map app change** — tests pass, safe, trivial.
  - The other 5 commits (`70b7f65`, `078b39d`, `d74aeaf`, `74ff47c`, `4c9fe87`) touch `apps/asset_risk`, `apps/mengxi-dashboard`, and shared `services/bess_map/optimisation_engine.py` / `nodal_pf_daily.py` — **not** `apps/bess-map/app.py`, which doesn't import either module. These are Trader/asset_risk-pillar work, not bess-map's.
- Full test suite (`apps/bess-map/tests/` + `tests/services/test_optimisation_engine.py`): **32/32 pass.**
- `git status` for bess-map paths is clean — no uncommitted app.py work.

**User decision (confirmed this session):** deploy the pending Ollama-shadow change as **v64** on its own, before starting new feature work.

## 2. BLOCKER — local DB connectivity via Astrill VPN

The pre-deploy standard (from the Aug 8 handoff) requires a headless `AppTest` smoke run against the real DB before building/pushing an image. It failed twice, both times with:

```
psycopg2.OperationalError: connection to server at "bess-platform-pg...rds.amazonaws.com" ... timeout expired
```

**Root cause identified — not an RDS or code problem:**
- RDS instance status is `available`, ~141 steady connections (not near limits), no pending maintenance.
- Raw TCP to port 5432 connects instantly (0.24s).
- Full Postgres protocol handshake (with or without SSL, via psycopg2 directly, bypassing Streamlit) hangs and times out every time.
- **Astrill VPN is active on this Mac** (`utun` interface, IP `198.18.198.198` — a CGNAT-style range Astrill uses). This is a classic VPN split-tunnel/MTU black-hole pattern: TCP handshake completes (small packets), but the larger Postgres startup packet never gets a response.
- User's chosen resolution: **pause/disconnect Astrill VPN**, then retry. This had not yet been confirmed done when the session moved to this handoff — **first step for the next session is to verify DB connectivity works** (quick check below) before doing anything else.

**Quick connectivity check:**
```bash
cd /path/to/bess-platform
set -a; source config/.env; set +a
~/.venvs/bess-platform/bin/python -c "
import sqlalchemy as sa, os
eng = sa.create_engine(os.environ['PGURL'], connect_args={'connect_timeout': 8})
with eng.connect() as c:
    print(c.execute(sa.text('select 1')).scalar())
"
```
If this hangs/times out again, check `ifconfig | grep -A2 utun` for VPN interfaces before assuming it's an RDS issue.

## 3. Once DB connectivity is confirmed — deploy v64

```bash
# 1. Smoke test (from the Aug 8 handoff's pre-deploy standard)
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

# 2. Build + push (platform flag is mandatory on this arm64 Mac)
docker build --platform linux/amd64 -f apps/bess-map/Dockerfile -t bess-map:v64 .
docker tag bess-map:v64 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-map:v64
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-map:v64
docker buildx imagetools inspect 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-map:v64 | grep Platform   # must say linux/amd64

# 3. tfvars: image_bess_map = ".../bess-map:v64"  (local only, never commit)
cd infra/terraform
terraform plan  -target=aws_ecs_task_definition.bess_map -target=aws_ecs_service.bess_map -out=/tmp/tfplan
terraform apply /tmp/tfplan   # ALWAYS -target these two — wider state is drifted, blanket apply rolls back ~11 services
```

**Confirm explicitly with the user before running the actual deploy/apply steps** — CLAUDE.md requires in-session "yes" for every deployment; the "deploy v64 now" answer from this session's AskUserQuestion covers the *decision*, but re-confirm before executing if meaningful time has passed or anything changed.

## 4. New feature — brainstorming in progress, NOT yet designed or built

**User's ask:** continue developing new functions for bess-map (Pillar 2 / Quant). After using the `superpowers:brainstorming` skill (this is an **architectural**-classified feature — it touches a shared calculation and DB semantics), three scoping questions were asked and answered:

| Question | Answer |
|---|---|
| Replace existing "theoretical" (ideal LP) numbers, or add alongside? | **Add alongside** — existing `theoretical_profit` / capture-rate definition in `bess_capture_daily` stays untouched. New numbers are a separate, comparable series. |
| Which tabs get the realistic-dispatch numbers? | **All three**: IRR tab, Province Ranking, Dispatch & Economics. |
| Should ramp-rate / cycles-per-day be fixed or user-adjustable? | **User-adjustable in the UI** (not just fixed at Mengxi's 3.3%/min + 1.5 cycles/day defaults). |

**Context that motivated this direction:** `services/bess_map/optimisation_engine.py::optimise_window()` already supports `ramp_rate_pct_per_min` and `max_cycles_per_day` as optional params (built Aug–Sep for `apps/asset_risk` and `apps/mengxi-dashboard`), but bess-map's own Province Ranking / Dispatch & Economics / IRR tabs still assume unconstrained ideal LP dispatch when computing "theoretical" revenue — likely overstating the investment case vs. what mengxi's ops-grade model with real operator constraints shows.

**Key finding from investigation (before the session paused for git push):** `spot_dispatch_hourly_theoretical`'s primary key is `(province, datetime, duration_h, power_mw, roundtrip_eff)` — **no constraint-profile dimension**. Combined with the "user-adjustable" answer, this means the DB-backfill approach (precomputing a `bess_capture_daily` row per constraint combo) doesn't fit — arbitrary user-chosen ramp/cycle values can't be pre-backfilled.

**Benchmarked instead: live computation is fast enough to skip DB schema changes entirely.**
```
30-day window solve (constrained, ramp=3.3%/min, cycles=1.5/day): 0.72s
365-day window solve (constrained):                                3.58s  (vs 1.59s unconstrained)
```
At ~3.6s/province/year, a full 29-province Province Ranking recompute is roughly **~100s** — slow for a live slider but fine behind an explicit "Recompute" button with `st.cache_data` keyed on `(province, year, power_mw, duration_h, rte, ramp_rate, max_cycles)`. IRR tab and Dispatch & Economics only need one province at a time — trivially fast.

**Leaning recommendation (not yet presented to user as a formal design):** no DB schema changes; live-compute via `optimise_window()` directly in the Streamlit app, cached per exact parameter combination; new UI inputs for ramp-rate %/min and cycles/day (default 3.3 / 1.5, matching Mengxi) in IRR tab, an "Apply" button + toggle in Province Ranking, and a second dispatch curve overlay in Dispatch & Economics.

**NOT yet done — pick up here:**
1. One more open question worth raising with the user before designing further: where does the hourly RT price series for a live full-year solve come from? `apps/bess-map/app.py` currently only reads *precomputed* dispatch from `spot_dispatch_hourly_theoretical` (charge/discharge/SOC columns) — need to check whether that table also carries the hourly price itself (it likely does, matching `optimise_window`'s output shape) or whether a separate raw RT price loader is needed. This wasn't checked before the session paused.
2. Present the 2-3 approach options + full design in chat (per `superpowers:brainstorming` architectural path) — not yet done.
3. Write the design to `docs/superpowers/specs/YYYY-MM-DD-bess-map-realistic-dispatch-design.md`, get user approval, then hand off to `superpowers:writing-plans`.
4. No code has been written for this feature yet — investigation and scoping only.

## 5. Everything else pushed this session

All previously-uncommitted repo changes (unrelated to bess-map, found sitting in the working tree at session start) were reviewed for secrets, grouped logically, and committed + pushed to `main`:
- Hermes `render_fetch.py` (headless Chromium for Aliyun-WAF-gated sources) + WeChat proxy routing + 环境异常 challenge-page guard in `news_screener.py`
- `thinking_agent.py` ETL freshness now grounded in actual `MAX(date)`, not just collector bookkeeping
- Shared `_shift15_pivot_hour` hour-column dtype bug fix (3 duplicated copies)
- `tt_api_collector.py` per-market failure isolation + `SKIP_PROVINCE_MISC` backfill flag
- `register_url` WeChat proxy test coverage
- Hermes vault briefings/inbox knowledge files (2026-08-07 → 2026-09-13)
- BESS SOP/SOE reference data, 蒙西 复盘 (review) decks, superseded 乌拉特 dispatch xls removed

None of this touched bess-map code — noted here only so the next session doesn't re-investigate it.

## 6. Useful commands

```bash
# tests
~/.venvs/bess-platform/bin/python -m pytest apps/bess-map/tests/ tests/services/test_optimisation_engine.py -q   # 32 pass

# local run
set -a; source config/.env; set +a
streamlit run apps/bess-map/app.py --server.port 8503

# service state
aws ecs describe-services --cluster bess-platform-cluster --services bess-platform-bess-map-svc \
  --region ap-southeast-1 --query "services[0].{td:taskDefinition,running:runningCount}"

# check for VPN interference before assuming a DB/RDS problem
ifconfig | grep -A2 "^utun"
```
