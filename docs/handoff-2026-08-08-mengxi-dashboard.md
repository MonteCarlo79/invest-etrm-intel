# Mengxi Dashboard — Session Handover (2026-08-08)

**Branch:** `feat/deal-structurer-bedrock-migration`
**Machine:** MacBook — `/Users/chenzhuqi/Library/CloudStorage/OneDrive-Personal/ETRM/bess-platform` (Windows paths in older handoffs are stale)
**Predecessor:** `docs/handoff-mengxi-dashboard-2026-08-04.md`
**Session theme:** completed the 2026-08-04 handoff's three tasks (terraform memory fix, nodal CSV backfill, LINGFENG key) — with an RDS OOM crash and a hostile home network along the way.

---

## 1. Deployed state (verified 2026-08-07)

| Service | Task def | Notes |
|---------|----------|-------|
| mengxi-dashboard | **td:23** — **512 CPU / 2048 MB** (was 256/512), image v13 | OOM 503s fixed |
| hermes | td:163 (:latest @ sha256:64f47a88…) | has 河北南网 spot-ingest fix |
| spot-markets | td:124 (v91) | |
| RDS `bess-platform-pg` | **db.t4g.medium (4 GB)** — was t4g.micro | OOM-crashed 2026-08-06 under backfill load; upsized 2026-08-07 00:06–00:10 UTC (~3 min downtime). `apply_immediately = true` now set in main.tf |

## 2. What was done this session

### 2.1 Terraform memory fix (handoff task 1) — DONE
- Targeted apply ONLY: `-target=aws_ecs_task_definition.mengxi_dashboard -target=aws_ecs_service.mengxi_dashboard`.
- **⚠️ Terraform state is fleet-stale** (thinks spot-markets is td:41; live td:124; tfvars image tags are old: spot v31 vs live v91). A bare `terraform apply` would re-register 11 task defs + redeploy 12 services with downgraded images. **Targeted applies only until someone reconciles state** (refresh + align tfvars to live tags).
- tfvars bumped v11→v13 first (live was already v13); tfvars is never committed.

### 2.2 Nodal CSV backfill (handoff task 2) — ~DONE (verify)
`marketdata.md_shanxi_nodal_price_96` backfilled from `data/nodal/<province>/` CSVs. **306/306 file-equivalents loaded (~34M rows)** pending final verification pass. The journey matters for whoever runs this next:

**Script hardening in `scripts/ingest_nodal_csvs.py` (all in working tree; commit status uncertain — other session rebased repeatedly):**
1. Per-chunk (100k rows) fresh connections + `execute_values` + `ON CONFLICT` — a dropped connection costs one chunk, not a file (TEMP-staging COPY abandoned).
2. `tcp_user_timeout=120000` + keepalives in `create_engine(connect_args=...)` — dead sockets error in ~2 min instead of hanging 15+ min.
3. Timer-thread watchdog (`threading.Timer` + `socket.shutdown` on the psycopg2 fd) at 300 s — catches "looks-alive but never responds" stalls. **SIGALRM does NOT work** (libpq retries EINTR internally; the Python handler never runs).
4. `init_table()` DDL skipped when the table exists (`to_regclass` check) — orphaned `idle in transaction` backends from killed runs hold `RowExclusiveLock` for hours and block `CREATE INDEX IF NOT EXISTS`. Orphans were terminated manually twice (`pg_terminate_backend`) — **needs user approval per permission classifier**.
5. `statement_timeout='600s'` per connection (was 180s — big upserts on the grown table exceed 180s).
6. **metric_time reconstruction** — CST midnight + 15 min×(slot−1), tz-aware, matching `services/fengxing/nodal_price.py`'s API-path convention. 陕西 2026 CSVs mix date-only and `+08:00` formats with every row duplicated — dedupe keep-last handles it (values verified identical).
7. **Part-file split**: 12 big files (>25 MB) were split into 200k-row parts (`<name>.partNNN.csv`, headers preserved) in-place; originals moved to `data/nodal/<prov>/_split_src/`. Marker keys are `province/YYYY-MM.partNNN`. Resume via `scripts/.ingest_nodal_done` (297→375 keys as parts complete).

**RDS OOM crash (2026-08-06 22:37 UTC):** instance ran "critically low on memory" (RDS event) under the backfill + a heavy parallel analytics query → auto-restart, ~4 min outage. Upsized to t4g.medium the same night. **Follow-up: set `idle_in_transaction_session_timeout` (~15 min) via a Terraform parameter group** so orphaned backends self-reap instead of parking locks for hours.

**Home-network hazard (2026-08-07/08):** the path from the user's network to RDS:5432 flaps wildly — handshakes complete but data flows die (`SSL SYSCALL error: EOF detected`, silent stalls). AWS API/HTTPS (443) works throughout. The script's resilience suite exists because of this. If it recurs, the alternate finisher is: `aws s3 sync` the staged CSVs (`/tmp/nodal_s3_stage` — may be gone by now; re-stage from `_split_src`) → one-off ECS task with `/tmp/nodal_loader.py` logic (written; downloads presigned URLs, upserts at VPC speed). Loader content is in this repo's git history conversation — recreate from `ingest_nodal_csvs.py` if needed.

**OneDrive hazard:** the Mac client had exited → newly-created part files were offloaded (`dataless`) → reads failed instantly with `[Errno 60]`. Fixed by `open -a OneDrive` + hydration loop. Rule added to memory: verify client up + hydrate before batch reads.

### 2.3 LINGFENG_API_KEY (handoff task 3) — NOT DONE (blocked)
- `config/.env` still has the placeholder. `FENGXING_API_KEY` (real, 26 chars) exists and the LingFeng ODS client (`services/lingfeng/api_client.py`) uses the same vendor host + `X-API-KEY-SECRET` header + same `/api/open/v1/ods/data/query` endpoint as fengxing — likely the same credential, but **never tested** (outbound-call command kept hitting the permission classifier; user was given a `!` one-liner to test but hasn't run it).
- The dashboard Section 7 "Download → DB" actually reads **`FENGXING_API_KEY`** (app.py:834), not LINGFENG — so the button may work already once connectivity is sane. Test one province/month.
- Handoff P1 still open: no cron/ECS schedule exists for `services/fengxing/nodal_price.py` daily nodal ingestion.

## 3. Immediate next steps (in order)

1. **Verify the backfill completed**: latest run's tail should show `Done in … 0 error(s)`. If parts failed again, hydrate (`open -a OneDrive` first) + re-run `~/.venvs/bess-platform/bin/python3 scripts/ingest_nodal_csvs.py` (resume default). Spot-check: `SELECT market_name, count(*), max(metric_time) FROM marketdata.md_shanxi_nodal_price_96 GROUP BY 1 ORDER BY 1` for the 辽宁/黑龙江 recent months.
2. **LINGFENG key**: run the empirical test (ask me for the one-liner or check session history) → if FENGXING key passes, copy it to `LINGFENG_API_KEY` in config/.env → test Section 7 Download→DB with one province/month → then wire the daily schedule.
3. **Commit + push** the `scripts/ingest_nodal_csvs.py` resilience suite + any other uncommitted work. The other Claude session (vault work) rebases frequently — expect `git pull --rebase` friction. Two sessions sharing one OneDrive-synced `.git` already caused a HEAD ref race once.
4. **Terraform state reconciliation** before any bare `terraform apply` (see 2.1 warning).
5. **RDS parameter group**: add `idle_in_transaction_session_timeout` (15 min) to kill orphaned backends automatically.

## 4. Key files

```
apps/mengxi-dashboard/app.py      — Section 7 = Download→DB (reads FENGXING_API_KEY), Section 8 = CSV ingest
scripts/ingest_nodal_csvs.py      — hardened bulk loader (resume via scripts/.ingest_nodal_done)
services/fengxing/nodal_price.py  — download_and_upsert(), init_table(), API-path metric_time convention
services/lingfeng/api_client.py   — LingFeng ODS client (LINGFENG_API_KEY)
infra/terraform/main.tf           — mengxi task def 512/2048; aws_db_instance.pg has apply_immediately=true
data/nodal/<prov>/                — part files in place; originals in _split_src/
```

## 5. Environment notes (Mac)

- `aws`, `crane`, `terraform`, `uv` in `~/.local/bin`; venv `~/.venvs/bess-platform`; Docker Desktop 29.6.2 (build amd64 only; **ECR pushes need the crane workaround in ERRORS.md** — Docker Desktop's proxy kills large uploads)
- Load env before DB scripts: `set -a; source config/.env; set +a`
- RDS reachable from this machine (IP 103.130.145.210 is whitelisted in the rds-sg; it rotates — if connects time out with no handshake, re-whitelist the new IP)
- Memory files to read: `project_fengxing_nodal.md`, `project_mac_transfer.md`, `project_hermes_service.md`, `project_mengxi_dashboard_v11.md` + repo `ERRORS.md`
