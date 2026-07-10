# Handoff — Nodal PF Daily Backfill + /backfill-daily Fix (2026-07-10)

> **Branch:** `cost-optimisation` | **Last commits:** `8771dab`, `8d8ce37`, `dc9eda7`
> **Working directory:** `C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform`

---

## What was completed this session

### Deployments (all live on ECS)

| Service | Image | Task Def | Status |
|---------|-------|----------|--------|
| Hermes | `bess-platform-hermes:latest` (dc9eda7) | — | Running 1/1 |
| Mengxi Dashboard | `bess-mengxi-dashboard:v12` | rev 20 | Running 1/1 |

---

### /backfill-daily command — two-step fix

**Problem:** Sending `/backfill-daily 2025-01-01` in Feishu returned a spot market
data report instead of triggering the nodal PF pre-compute. CloudWatch logs showed
`Action=MARKET_AGENT` — the LLM router was intercepting the message before the
explicit handler at line ~2692 could fire.

**Fix (commit dc9eda7):** Moved the `/backfill-daily` handler to the **pre-LLM intercept**
block in `services/hermes/app.py` (~line 2194), alongside the LingFeng and source-file
intercepts. Also relaxed the trailing `$` regex anchor to tolerate any invisible
padding characters Feishu may append.

**Key pattern to remember:** Any new Hermes slash command that contains a date (e.g.
`/command 2025-01-01`) will be caught by MARKET_AGENT unless it is added to the
pre-LLM intercept section at the TOP of `_handle_message()`, not lower down.

---

### Architecture summary (nodal PF pre-compute)

```
22:30 UTC daily (Hermes ECS scheduler)
  └─ compute_and_store_nodal_pf_daily(yesterday, all plants from station_master)
       └─ single-day MILP per plant → upsert reports.nodal_pf_daily

23:00 UTC daily (/report mengxi)
  └─ _nodal_ranks_for(start, end)
       ├─ try: _query_nodal_pf_daily_ranks()  ← reads cache (fast, <1s)
       └─ fallback: live MILP (slow, 25min for YTD)

/backfill-daily Feishu command (pre-LLM intercept, ~line 2194 in app.py)
  └─ iterates dates, skips dates with ≥20 plants in reports.nodal_pf_daily
  └─ sends "⏳ 开始预计算..." ack, then completion message when done
```

---

## What still needs to be done

### B — Send `/backfill-daily 2025-01-01` in Feishu ← CRITICAL, do this first

The new Hermes container (dc9eda7) is deployed. Send this in Feishu now:

```
/backfill-daily 2025-01-01
```

**Expected response (within a few seconds):**
```
⏳ 开始预计算节点PF日度数据 2025-01-01 → 2026-07-09（555 天），稍候…
每天约1秒，共约 555 秒。完成后会通知。
```

**Expected completion (~10 min later):**
```
✅ 节点PF日度预计算完成：XXX 天已写入 / YYY 天无数据或已存在 / ZZZ 天失败
数据范围：2025-01-01 → 2026-07-09
```

Once this runs, `reports.nodal_pf_daily` is populated and the 23:00 UTC daily report
will use the pre-computed cache (fixing the 27-min YTD timeout).

### D — Ingest 南方区域 Excel files

Open Mengxi Dashboard (ECS: `http://pjh-etrm.ai/mengxi-dashboard/` or local on port 8505)
→ Data Management → scroll to **Section 9 · Ingest 南方区域 day-ahead nodal Excel → RDS**

- Coverage pivot shows what's already in DB
- Source: `data/market-fundamentals/【0.区域级】/南方区域/2-数据/日前节点电价数据/YYYYMM/`
- Target table: `marketdata.md_shanxi_nodal_price_96`
- Provinces: 广东, 广西, 云南, 贵州, 海南

### Monitor tonight at 22:30 UTC

Check CloudWatch logs (`/ecs/bess-platform`, stream prefix `hermes`):
- Should see `compute_and_store_nodal_pf_daily` running for yesterday's date
- At 23:00: report log should say `"using pre-computed daily cache"` not `"live MILP"`

---

## Key files changed this session

| File | Commits | What changed |
|------|---------|-------------|
| `services/hermes/app.py` | 8771dab, dc9eda7 | 22:30 UTC scheduler + `/backfill-daily` command (moved to pre-LLM intercept in dc9eda7) |
| `services/hermes/mengxi_ranking_report.py` | 8771dab | `compute_and_store_nodal_pf_daily` + cache-first `_nodal_ranks_for` |
| `apps/mengxi-dashboard/app.py` | 8771dab | Section 8 chunked CSV + Section 9 南方区域 Excel ingest |
| `scripts/backfill_nodal_pf_daily.py` | 8771dab | NEW — local backfill CLI |
| `local_launcher.py` | (gitignored) | Build + Start buttons for local images |

---

## ECR token note

ECR login expires every ~12 hours. If a push fails with `403 Forbidden`, run:
```bash
aws ecr get-login-password --region ap-southeast-1 | docker login --username AWS --password-stdin 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com
```
Then retry the push.
