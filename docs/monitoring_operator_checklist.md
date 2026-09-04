# BESS Monitoring Stack — Operator Execution Checklist

**Deployment date:** ____________  
**Operator:** ____________  
**Target DB:** ____________  
**Reviewer:** ____________

Reference: `docs/monitoring_validation_runbook.md` for full SQL and failure detail.

---

## STEP 1 — Environment

| # | Check | Command | Result |
|---|---|---|---|
| 1.1 | `PGURL` is set | `echo $PGURL` | `[ ] PASS  [ ] FAIL` |
| 1.2 | DB connection succeeds | `python -c "from services.common.db_utils import get_engine; get_engine()"` | `[ ] PASS  [ ] FAIL` |
| 1.3 | Both monitoring tables exist | `psql $PGURL -c "\dt monitoring.*"` → expect `asset_realization_status`, `asset_fragility_status` | `[ ] PASS  [ ] FAIL` |

> **Failure 1.1–1.3:** Stop. Notify DBA. Do not proceed until DB access is confirmed.

---

## STEP 2 — Unit Tests (no DB required)

| # | Check | Command | Result |
|---|---|---|---|
| 2.1 | All unit tests pass | `python -m pytest services/monitoring/tests/ -k "not integration" -q` → expect **35 passed, 0 failed** | `[ ] PASS  [ ] FAIL` |
| 2.2 | Attribution model tests pass | `python -m pytest libs/decision_models/tests/test_dispatch_pnl_attribution.py -q` → expect **10 passed** | `[ ] PASS  [ ] FAIL` |

> **Failure 2.1:** Record which test class failed. Likely causes: B1 not applied (DATA_ABSENT), B2 not applied (INDETERMINATE), B5 logger mismatch. Escalate to developer before proceeding.  
> **Failure 2.2:** Attribution kernel broken. Stop. Escalate to developer.

---

## STEP 3 — Schema Migration

| # | Check | Command | Result |
|---|---|---|---|
| 3.1 | Migration applies without error | `psql $PGURL -f db/ddl/monitoring/migrations/001_add_status_check_constraints.sql` → expect `DO DO DO` | `[ ] PASS  [ ] FAIL` |
| 3.2 | 3 CHECK constraints present | Run §2.2 query from runbook → expect 3 rows: `chk_realization_status_level`, `chk_fragility_level`, `chk_fragility_realization_status_level` | `[ ] PASS  [ ] FAIL` |
| 3.3 | Constraint rejects invalid value | Run §2.3 INSERT test from runbook → expect constraint violation error | `[ ] PASS  [ ] FAIL` |

> **Failure 3.1:** Check for syntax error or missing schema — run `psql $PGURL -c "\dn"` to confirm `monitoring` schema exists.  
> **Failure 3.2:** Migration ran but constraints were not created. Check for silent errors in DO blocks. Re-run migration.  
> **Failure 3.3:** Constraint is present but misconfigured — values list may be wrong. Escalate to developer.

---

## STEP 4 — Realization Monitor Run

| # | Check | Command | Result |
|---|---|---|---|
| 4.1 | Job runs without exception | `python -m services.monitoring.run_realization_monitor --date YYYY-MM-DD --lookback 30 2>&1 \| tee /tmp/real_run.log` → no `ERROR` or traceback in log | `[ ] PASS  [ ] FAIL` |
| 4.2 | MONITORING_RUN line present | `grep "MONITORING_RUN" /tmp/real_run.log` → expect `assets=8` | `[ ] PASS  [ ] FAIL` |
| 4.3 | Exactly 8 rows written | `psql $PGURL -c "SELECT COUNT(*) FROM monitoring.asset_realization_status WHERE snapshot_date='YYYY-MM-DD' AND lookback_days=30;"` → expect `8` | `[ ] PASS  [ ] FAIL` |
| 4.4 | No bad DATA_ABSENT rows | Run §5.1 queries from runbook → expect `0` from both queries | `[ ] PASS  [ ] FAIL` |
| 4.5 | No bad INDETERMINATE rows | Run §5.2 query from runbook → expect `0` | `[ ] PASS  [ ] FAIL` |

> **Failure 4.2 `assets=0`:** Attribution table empty for that date. Run `run_pnl_refresh.py` first.  
> **Failure 4.3 row count ≠ 8:** Check log for `ERROR Failed computing` lines — one or more assets threw an exception.  
> **Failure 4.4/4.5:** B1/B2 patch not applied correctly. Stop. Escalate to developer — do not run fragility monitor until resolved.

---

## STEP 5 — Fragility Monitor Run

| # | Check | Command | Result |
|---|---|---|---|
| 5.1 | Pre-flight passes | `python -m services.monitoring.run_fragility_monitor --date YYYY-MM-DD --lookback 30 2>&1 \| tee /tmp/frag_run.log` → first INFO line: `Pre-flight OK: 8 realization rows found` | `[ ] PASS  [ ] FAIL` |
| 5.2 | MONITORING_RUN line present | `grep "MONITORING_RUN" /tmp/frag_run.log` → expect `assets=8` | `[ ] PASS  [ ] FAIL` |
| 5.3 | Exactly 8 rows written | `psql $PGURL -c "SELECT COUNT(*) FROM monitoring.asset_fragility_status WHERE snapshot_date='YYYY-MM-DD';"` → expect `8` | `[ ] PASS  [ ] FAIL` |
| 5.4 | Composite score arithmetic correct | Run §3.4 HAVING query from runbook → expect `0` rows | `[ ] PASS  [ ] FAIL` |
| 5.5 | DATA_ABSENT/INDETERMINATE assets score neutrally | Run §5.3 query from runbook → expect `0` rows | `[ ] PASS  [ ] FAIL` |

> **Failure 5.1 pre-flight abort:** Step 4 did not complete successfully. Rerun Step 4 before proceeding.  
> **Failure 5.4:** Composite score arithmetic drift — likely a floating-point issue introduced by a code change. Escalate.  
> **Failure 5.5:** `_STATUS_TO_SCORE` missing `DATA_ABSENT`/`INDETERMINATE` keys. Escalate to developer.

---

## STEP 6 — Attribution Identity Check

| # | Check | Command | Result |
|---|---|---|---|
| 6.1 | No identity failures in log | `grep "Identity check failed\|POST_WRITE_IDENTITY_FAIL" /tmp/attr_run.log` → expect `0` matches | `[ ] PASS  [ ] FAIL` |
| 6.2 | SQL identity check clean | Run §4.2 identity query from runbook → expect `0` rows | `[ ] PASS  [ ] FAIL` |

> **Failure 6.1 / 6.2 discrepancy < 100 ¥:** Acceptable floating-point accumulation. Log and proceed. Record asset and date for reviewer.  
> **Failure 6.2 discrepancy > 1000 ¥:** Scenario PnL mapping error. Stop. Do not use attribution data for monitoring until resolved. Escalate to developer.

---

## STEP 7 — Idempotency

| # | Check | Command | Result |
|---|---|---|---|
| 7.1 | Double-run realization: count stays 8 | Re-run Step 4.1 for the same date; re-run Step 4.3 → still `8` | `[ ] PASS  [ ] FAIL` |
| 7.2 | Double-run fragility: count stays 8 | `SKIP_PREFLIGHT=1` re-run Step 5.1 for same date; re-run Step 5.3 → still `8` | `[ ] PASS  [ ] FAIL` |

> **Failure 7.1 or 7.2 count doubles:** `ON CONFLICT` broken. Stop immediately. Roll back duplicate rows with `DELETE ... WHERE ctid NOT IN (SELECT MIN(ctid) ...)`. Do not proceed to production backfill until fixed. Escalate to developer.

---

## STEP 8 — Production Backfill (60 days)

| # | Check | Result |
|---|---|---|
| 8.1 | Backfill loop runs to completion without error | `[ ] PASS  [ ] FAIL` |
| 8.2 | Row count: `SELECT COUNT(*) FROM monitoring.asset_realization_status WHERE lookback_days=30` ≥ `8 × 60` = 480 | `[ ] PASS  [ ] FAIL` |
| 8.3 | Row count: `SELECT COUNT(*) FROM monitoring.asset_fragility_status` matches realization count | `[ ] PASS  [ ] FAIL` |

> **Failure 8.1:** Note the date where the loop stopped. Check for missing attribution data on that date. Resume the loop from the failed date after data is confirmed.

---

## STEP 9 — End-to-End Smoke Test

| # | Check | Command | Result |
|---|---|---|---|
| 9.1 | Agent query returns data | `python -c "from services.monitoring.realization_monitor import query_realization_status; r=query_realization_status(); print(len(r), r[0]['status_level'])"` → expect `8` rows | `[ ] PASS  [ ] FAIL` |
| 9.2 | Streamlit page loads | Launch app → monitoring page shows 8 assets in both tables, no empty-state placeholders | `[ ] PASS  [ ] FAIL` |

> **Failure 9.1:** Check `PGURL` in the agent runtime environment. DB credentials may differ from the job environment.

---

## Sign-off

**All steps passed?**  `[ ] YES  [ ] NO`

| Role | Name | Signature | Date |
|---|---|---|---|
| Operator | | | |
| Reviewer | | | |

**Notes / exceptions recorded during run:**

```
(free text)
```
