# IB Platform Phase 9B — Operations Handoff

> **For a new Claude session:** This document covers Phase 9B specifically (Portfolio Greeks aggregation).
> Read the main handoff first: `ib-platform-handoff.md` (Mac) or `ib-platform-handoff-windows.md` (Windows).
> **Latest commit:** `7e2541c` — pushed 2026-06-29
> **Tests:** 521 passing

---

## What Phase 9B Added

| Component | File | Purpose |
|---|---|---|
| Greeks calculator | `services/broker_service/greeks_calculator.py` | `compute_portfolio_greeks(conn) -> int` |
| Scheduler job | `services/broker_service/algo_scheduler.py` | `compute_greeks` job at 09:30 ET |

---

## How the Data Flows

```
trading.paper_fills
  └─ JOIN trading.signals (for strike/expiry/right on options fills)
       └─ trading.options_chain (IV lookup for options positions)
       └─ trading.bars_1d (spot price S for bs_greeks)
            └─ compute_portfolio_greeks (09:30 ET)
                 └─ trading.portfolio_risk (paper / live / total rows)
                      └─ Portfolio app Risk tab (already reads this table)

trading.positions (live IB positions)
  └─ trading.options_chain (IV lookup for options positions)
  └─ trading.bars_1d (spot price S for bs_greeks)
       └─ compute_portfolio_greeks (same job)
```

---

## Running the Service

No new standalone process. The `compute_greeks` job runs inside the existing `broker_service`:

```bash
# Existing (now with 6 APScheduler jobs incl. compute_greeks)
python -m services.broker_service.main
```

On Windows (dipeng.chen account):
```bash
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m services.broker_service.main
```

---

## Scheduler Timeline (Mon–Fri ET)

| Time | Job | Service process |
|---|---|---|
| 09:00 | `paper_promotions` — auto-promote paper→shadow | broker_service |
| **09:30** | **`compute_greeks`** — upsert paper/live/total Greeks | **broker_service** |
| 09:25 | `ingest_chains_open` — fetch options chains pre-open | options_chain |
| 09:35 | `run_all_strategies` — strategies now find chain data | broker_service |
| 16:05 | `ingest_chains_close` — refresh chains with closing IVs | options_chain |
| 16:10 | `eod_bars` — bar data | market_data |
| 16:15 | `compute_pnl` — upsert realized + unrealized P&L | broker_service |
| 16:30 | `daily_report` — Feishu card | broker_service |
| 17:00 | `weekly_report` (Fridays) — Feishu table | broker_service |
| 18:00 | `extract_trade_insights` — trade monitor → KB | trade_monitor |

Note: `compute_greeks` runs at 09:30, which is after `ingest_chains_open` (09:25) so today's IVs are available.

---

## DB Tables Written by Phase 9B

### `trading.portfolio_risk`
Three rows per run (account_id: 'paper', 'live', 'total'). Written at 09:30 ET by `compute_greeks` job.

```sql
SELECT ts, account_id, total_delta, total_gamma, total_theta, total_vega
FROM trading.portfolio_risk
ORDER BY ts DESC;
```

---

## Verifying Phase 9B Is Working

### Check Greeks are being computed

```sql
SELECT ts, account_id, total_delta, total_gamma, total_theta, total_vega
FROM trading.portfolio_risk
ORDER BY ts DESC LIMIT 6;
```
Expected: 3 rows per trading day (paper, live, total) appearing after 09:30 ET.

### Check the Risk tab is showing data

Open Portfolio app → Risk tab. The existing `get_portfolio_risk(conn, account_id)` query reads `trading.portfolio_risk`. It should now show real numbers instead of "No risk data available".

---

## Key Design Decisions

- **Paper options greeks**: via `paper_fills LEFT JOIN signals` (for strike/expiry/right) → `options_chain` (IV) → `bs_greeks`
- **Live options greeks**: via `trading.positions` (has expiry/strike/right columns) → `options_chain` (IV) → `bs_greeks`
- **Fallback on missing IV**: logs warning, leaves greeks = 0.0 (does not crash job)
- **Fallback on IV=0**: silently leaves greeks = 0.0
- **Fallback on missing spot price**: uses strike as proxy
- **`var_1d_95` and `nav`**: left NULL (Phase 9C scope)
- **`dv01` and `fx_delta_usd`**: hardcoded 0 (fixed-income/FX not in paper fills yet)

---

## Known Gaps (Phase 9C — not yet designed)

1. **`var_1d_95`** — requires returns history; not computed
2. **`nav`** — requires account value from broker; not computed
3. **Intraday Greeks refresh** — currently once daily at 09:30 ET; no intraday updates
4. **`dv01` / `fx_delta_usd`** — fixed-income and FX positions not yet in `trading.paper_fills`
5. **Expiry-day T=0 edge case** — when an option expires today, `T=0` is handled gracefully by `bs_greeks` returning zeros, but the position still has zero greeks on expiry day

To design Phase 9C: use `superpowers:brainstorming`.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `portfolio_risk` has no rows | `compute_greeks` job not running | Check broker_service is running; check logs for `[greeks_calculator]` |
| All greeks = 0 for options positions | No options_chain data for today | Verify options_chain service ran at 09:25; check `ts_snapshot::date = CURRENT_DATE` |
| Risk tab still shows "No risk data" | `account_id` filter mismatch | Tab queries by account_id; check what it passes and whether 'paper'/'live'/'total' rows exist |
| Warning: "no chain row for SPY..." | Option position has no matching chain row | Normal if chain is stale or strategy traded an illiquid strike |
