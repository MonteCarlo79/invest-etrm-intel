# BESS Monitoring Stack — Reviewer Decision Memo

**Date:** ____________  
**Reviewer:** ____________  
**Deployment target:** `[ ] Staging  [ ] Production`  
**Operator checklist completed by:** ____________  
**Data window reviewed:** ____________ to ____________

Reference: `docs/monitoring_validation_runbook.md` (technical detail), `docs/monitoring_operator_checklist.md` (execution record).

---

## 1. Test Results Summary

### Unit Tests

| Suite | Tests run | Passed | Failed | Notes |
|---|---|---|---|---|
| `test_realization_monitor.py` | 20 | | | |
| `test_run_daily_attribution.py` | 10 | | | |
| `test_idempotency.py` (unit) | 5 | | | |
| `test_dispatch_pnl_attribution.py` | 10 | | | |
| **Total** | **45** | | | |

**Overall unit test verdict:** `[ ] All passed  [ ] Failures present`

If failures present, describe:

```
(which tests failed, root cause, resolution status)
```

### Integration Tests (if PGURL available)

| Suite | Tests run | Passed | Failed | Notes |
|---|---|---|---|---|
| `test_idempotency.py` (integration) | 4 | | | |

**Overall integration test verdict:** `[ ] All passed  [ ] Failures present  [ ] Skipped (no PGURL)`

### Schema Migration

| Check | Result | Notes |
|---|---|---|
| Migration applied without error | `[ ] YES  [ ] NO` | |
| 3 CHECK constraints present | `[ ] YES  [ ] NO` | |
| Invalid value rejected by constraint | `[ ] YES  [ ] NO` | |

---

## 2. Batch Job Execution Summary

### Realization Monitor

| Check | Value | Within expected range? |
|---|---|---|
| Assets processed | / 8 | `[ ] YES  [ ] NO` |
| NORMAL count | | |
| WARN count | | |
| ALERT count | | |
| CRITICAL count | | |
| DATA_ABSENT count | | `[ ] Acceptable  [ ] Elevated` |
| INDETERMINATE count | | `[ ] Acceptable  [ ] Elevated` |
| MONITORING_RUN line present in log | `[ ] YES  [ ] NO` | |
| `elapsed_ms` | ms | `[ ] < 30s  [ ] > 30s` |

**Notable anomalies:**

```
(any unexpected MONITORING_ALERT lines, elevated DATA_ABSENT, etc.)
```

### Fragility Monitor

| Check | Value | Within expected range? |
|---|---|---|
| Assets processed | / 8 | `[ ] YES  [ ] NO` |
| LOW count | | |
| MEDIUM count | | |
| HIGH count | | |
| CRITICAL count | | |
| Pre-flight passed | `[ ] YES  [ ] NO` | |
| MONITORING_RUN line present | `[ ] YES  [ ] NO` | |
| Composite score arithmetic correct (0 discrepancy rows) | `[ ] YES  [ ] NO` | |

**Notable anomalies:**

```
(MONITORING_ALERT lines for HIGH/CRITICAL assets, unexpected fragility levels, etc.)
```

### Attribution Identity Check

| Metric | Value | Acceptable? |
|---|---|---|
| Pre-write identity failures (discrepancy > 1 ¥) | | `[ ] 0 — clean  [ ] < 100 ¥ — acceptable  [ ] > 1000 ¥ — stop` |
| Post-write SQL check (0 rows expected) | | `[ ] 0 rows  [ ] Rows present` |

**If identity failures present, list affected assets and dates:**

```
(asset_code, trade_date, discrepancy)
```

---

## 3. Threshold Calibration Findings

Based on the 90-day historical distribution query (runbook §7.2 Steps 1–2).

### Status Distribution (realization)

| Status | Observed % | Target range | Assessment |
|---|---|---|---|
| NORMAL | % | 60–80% | `[ ] OK  [ ] Too low  [ ] Too high` |
| WARN | % | 10–25% | `[ ] OK  [ ] Too low  [ ] Too high` |
| ALERT | % | 5–15% | `[ ] OK  [ ] Too low  [ ] Too high` |
| CRITICAL | % | < 5% | `[ ] OK  [ ] Too high` |
| DATA_ABSENT | % | < 10% | `[ ] OK  [ ] Elevated — pipeline issue` |
| INDETERMINATE | % | < 5% | `[ ] OK  [ ] Elevated — market condition` |

### Per-Asset Ratio Statistics

Record any assets where the median ratio falls significantly below the NORMAL threshold (0.70), or where volatility (stddev) is high enough to suggest asset-specific thresholds are needed.

| Asset | Median ratio | Stddev | P10 | Assessment |
|---|---|---|---|---|
| suyou | | | | |
| wulate | | | | |
| wuhai | | | | |
| wulanchabu | | | | |
| hetao | | | | |
| hangjinqi | | | | |
| siziwangqi | | | | |
| gushanliang | | | | |

### Threshold Adjustment Recommendation

`[ ] No adjustment needed — current thresholds are well-placed`  
`[ ] Minor adjustment recommended — document below`  
`[ ] Significant recalibration required — block rollout`

If adjustment recommended:

```
Proposed change:
Justification (which assets, which percentile distribution):
Owner:
Target date for recalibration:
```

---

## 4. False Positive / False Negative Observations

### False Positives (ALERT/CRITICAL fired, no identifiable loss bucket)

Query: runbook §7.2 Step 3. Expected: 0 rows.

| Count | Affected assets | Dates | Assessment |
|---|---|---|---|
| | | | `[ ] 0 — clean  [ ] Present — describe` |

**If false positives present:**

```
(asset, date, ratio, dominant_loss_bucket=NULL — what should status have been?)
```

### False Negatives (known operational events not reflected in status)

Cross-reference against any known incidents, grid outages, or underperformance events in the review window.

| Known incident | Date | Asset | Expected status triggered? | Actual status |
|---|---|---|---|---|
| | | | `[ ] YES  [ ] NO` | |
| | | | `[ ] YES  [ ] NO` | |

**If false negatives present:**

```
(what threshold would have caught this event? is this a data issue or a threshold issue?)
```

### Alert Fatigue Assessment

`[ ] ALERT/CRITICAL together < 20% of asset-days — alert volume is manageable`  
`[ ] ALERT/CRITICAL together 20–35% — borderline, monitor closely`  
`[ ] ALERT/CRITICAL together > 35% — thresholds require recalibration before rollout`

---

## 5. Rollout Recommendation

### Blocking Issues

List any items from sections 1–4 that must be resolved before go-live. A blocking issue is anything that would cause the monitoring stack to: produce incorrect statuses, write duplicate rows, silently fail without logging, or generate so many false alarms that ops ignores them.

| # | Issue | Severity | Owner | Resolution required by |
|---|---|---|---|---|
| | | `[ ] Critical  [ ] Major  [ ] Minor` | | |
| | | `[ ] Critical  [ ] Major  [ ] Minor` | | |

**If no blocking issues:** `[ ] Confirmed — none`

### Non-Blocking Observations (log for next sprint)

```
(threshold recalibration candidates, noisy assets, pipeline reliability concerns, etc.)
```

### Deployment Scope

`[ ] Deploy to staging only — further validation required`  
`[ ] Deploy to production with full backfill (60 days)`  
`[ ] Deploy to production with limited backfill (30 days) — note reason:`  
`[ ] Do not deploy — reopen sprint`

---

## 6. Go / No-Go Decision

### Pre-conditions (all must be YES for GO)

| Pre-condition | Status |
|---|---|
| All unit tests passed (45/45) | `[ ] YES  [ ] NO` |
| Schema migration clean, 3 constraints present | `[ ] YES  [ ] NO` |
| Realization monitor wrote 8 rows for today's date | `[ ] YES  [ ] NO` |
| No rows where CRITICAL has days_in_window < 5 | `[ ] YES  [ ] NO` |
| No rows where INDETERMINATE has positive benchmark | `[ ] YES  [ ] NO` |
| Fragility composite score arithmetic correct (0 drift rows) | `[ ] YES  [ ] NO` |
| DATA_ABSENT/INDETERMINATE fragility scores in neutral range [0.35, 0.65] | `[ ] YES  [ ] NO` |
| Attribution identity check: 0 discrepancies > 1 ¥ (or all < 100 ¥ and documented) | `[ ] YES  [ ] NO` |
| Idempotency confirmed: double-run does not produce duplicate rows | `[ ] YES  [ ] NO` |
| No blocking issues listed in Section 5 | `[ ] YES  [ ] NO` |

### Decision

```
┌─────────────────────────────────────────────────────────────────┐
│                                                                 │
│   [ ] GO      All pre-conditions met. Approved for rollout.    │
│                                                                 │
│   [ ] NO-GO   One or more pre-conditions failed.               │
│               Blocking issues must be resolved first.          │
│                                                                 │
│   [ ] CONDITIONAL GO   All critical pre-conditions met.        │
│               Non-critical gaps documented. Approved with       │
│               named owner and resolution date for each gap.    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Conditions (if Conditional GO):**

```
(list each condition, owner, and resolution date)
```

**Reviewer signature:** ____________  
**Date:** ____________

---

*This memo covers the hardening sprint (B1–B6) only. Scope: `realization_monitor`, `fragility_monitor`, `dispatch_pnl_attribution` identity checks. It does not constitute sign-off for any other components of the BESS monitoring platform.*
