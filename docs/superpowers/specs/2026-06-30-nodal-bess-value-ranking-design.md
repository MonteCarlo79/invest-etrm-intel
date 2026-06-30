# Nodal BESS Value Ranking — Design Spec

**Date:** 2026-06-30  
**Status:** Approved  
**Surface:** Daily BESS Ranking PDF (`services/hermes/mengxi_ranking_report.py`)

---

## 1. Purpose

Add two columns to every ranking table in the daily Mengxi BESS ranking PDF:

- **2h节点排名** — rank of the asset's node by perfect-foresight BESS value assuming 2h duration (0.5C)
- **4h节点排名** — rank of the asset's node by perfect-foresight BESS value assuming 4h duration (0.25C)

This surfaces two distinct signals per asset:
1. **Location quality** — how valuable is this node for BESS arbitrage, independent of who operates it
2. **Trading merit** — does the asset's actual performance rank above or below its nodal rank? Above = outperforming the location; below = underperforming

---

## 2. Scope

- **Phase 1 (this spec):** Mengxi only — nodes from `marketdata.md_id_cleared_energy`
- **Phase 2 (future):** Extend to other provinces as nodal price data becomes available

---

## 3. Perfect-Foresight BESS Value Metric

### 3.1 Model

For each asset's node and each trading day, solve a MILP arbitrage problem over all 96 × 15-min intervals using the existing engine in `services/bess_map/optimisation_engine.py`:

```
compute_dispatch_from_15min_prices(
    prices_s,
    power_mw=1.0,        # normalised per MW installed
    duration_h=2.0,      # or 4.0 for the 4h variant
    roundtrip_eff=0.85,
)
```

The engine applies symmetric efficiency: `η_c = η_d = √0.85 ≈ 0.922`. Binary variable `y_t` prevents simultaneous charge and discharge. SOC resets to 0 each day (`window_days=1`, default).

### 3.2 Normalisation

```
pf_score = Σ_days(daily_profit_yuan) / (duration_h × days)   [CNY / MWh_installed / day]
```

Same unit as the existing `score` column (`收益/MWh/天`) — directly comparable.

### 3.3 Ranking

All plants present in the current ranking window are ranked by `pf_score` descending. `nodal_rank_2h` and `nodal_rank_4h` are integer ranks (1 = highest value node).

---

## 4. Data

### 4.1 New query — `_query_nodal_prices`

```sql
SELECT plant_name, datetime, cleared_price
FROM marketdata.md_id_cleared_energy
WHERE data_date >= %(start)s
  AND data_date <  %(end_excl)s
  AND plant_name = ANY(%(plant_names)s)
ORDER BY plant_name, datetime
```

Same plant list and DB connection already used by `_query()`. Timeout: 600s (same as existing queries). Called once per time window (yesterday / month / YTD) — or once for YTD and subsetted for narrower windows.

### 4.2 Parallelisation

Plants are independent — solved in parallel with `concurrent.futures.ThreadPoolExecutor`. Each plant requires 2 calls to `compute_dispatch_from_15min_prices` (one per duration). PuLP/CBC is not thread-safe for shared state, but each call creates its own `LpProblem` instance, so parallel execution is safe.

---

## 5. PDF Changes

### 5.1 Table columns

Before (7 columns):
```
排名 | 项目名称 | 业主 | MW | 总收益(万元) | 收益/MWh/天 | 天数
```

After (9 columns):
```
排名 | 项目名称 | 业主 | MW | 总收益(万元) | 收益/MWh/天 | 天数 | 2h节点排名 | 4h节点排名
```

Column widths (A4, 12mm margins): project name narrowed from 58mm → 46mm; owner from 34mm → 28mm; two new columns at 15mm each.

### 5.2 Colour coding for nodal rank columns

- **Green** if nodal rank > actual rank (asset outperforms its location)
- **Red** if nodal rank < actual rank (asset underperforms its location)
- **Neutral** if equal

Colour applied to the nodal rank cell only, not the entire row.

### 5.3 Footer note addition

```
节点价值排名：基于完美预见MILP套利模型，假设2h（0.5C）和4h（0.25C）电池时长，
往返效率85%，排名越高表示节点价值越大。绿色 = 实际排名优于节点排名；红色 = 低于节点排名。
```

---

## 6. Code Changes

All changes are confined to `services/hermes/mengxi_ranking_report.py`:

| Function | Change |
|---|---|
| `_query_nodal_prices(pg_url, plant_names, start, end_excl)` | New — fetches 15-min cleared prices |
| `_compute_nodal_pf_ranks(prices_df, duration_h, rte)` | New — runs PF MILP per plant, returns `{plant_name: rank}` |
| `send_daily_ranking(...)` | Call both functions for each time window; pass rank dicts to enrich/rank |
| `_enrich_and_rank(raw_df, plant_list, nodal_ranks_2h, nodal_ranks_4h)` | Add two rank columns |
| `_build_table(df)` | Add two columns; apply cell-level colour coding |
| `_generate_pdf(...)` | Update footer note |

No new tables, no new services, no schema changes.

---

## 7. Error Handling

- If `_query_nodal_prices` fails or returns empty: nodal rank columns show `—` (dash); rest of report unaffected
- If MILP solver returns non-Optimal for a plant-day: that day is excluded from the sum (same as existing NaN handling)
- Plants with fewer than 1 complete day of prices: nodal rank = `—`

---

## 8. Performance Estimate

| Window | Plants | Days | LP problems | Est. time (8 workers) |
|---|---|---|---|---|
| Yesterday | ~100 | 1 | 200 | ~5s |
| Month | ~100 | 30 | 6,000 | ~60s |
| YTD | ~100 | 180 | 36,000 | ~5 min |

Total added latency to PDF generation: ~6 min worst case (YTD). The existing PDF job runs at 23:00 UTC with no hard deadline — this is acceptable. If latency becomes an issue, `window_days=7` (weekly LP windows) can reduce problem count 7×.
