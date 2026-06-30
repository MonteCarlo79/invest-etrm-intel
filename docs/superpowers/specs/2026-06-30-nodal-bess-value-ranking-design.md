# Nodal BESS Value Ranking — Design Spec

**Date:** 2026-06-30  
**Status:** Approved  
**Surface:** Daily BESS Ranking PDF (`services/hermes/mengxi_ranking_report.py`) + Hermes scheduler

---

## 1. Purpose

Two additions to the daily Mengxi BESS ranking PDF:

1. **Nodal rank columns** — add `2h节点排名` and `4h节点排名` to every existing BESS performance ranking table, showing where each asset's node ranks by perfect-foresight BESS value
2. **Monthly nodal ranking page** — a new standalone page showing all Mengxi nodes ranked by PF BESS value, computed once on the 5th of each month using the previous full calendar month's data

This surfaces two distinct signals per asset:
- **Location quality** — how valuable is this node for BESS arbitrage, independent of who operates it
- **Trading merit** — does the asset's actual performance rank above or below its nodal rank? Above = outperforming the location; below = underperforming

---

## 2. Scope

- **Phase 1 (this spec):** Mengxi only — nodes from `marketdata.md_id_cleared_energy`
- **Phase 2 (future):** Extend to other provinces as nodal price data becomes available

---

## 3. Perfect-Foresight BESS Value Metric

### 3.1 Model

For each node and each trading day, solve a MILP arbitrage problem over all 96 × 15-min intervals using the existing engine in `services/bess_map/optimisation_engine.py`:

```python
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

All nodes with at least 1 complete trading day of prices are ranked by `pf_score` descending. `rank_2h` and `rank_4h` are integer ranks (1 = highest value node).

---

## 4. Pre-Computed Monthly Table

### 4.1 New DB table — `reports.nodal_pf_monthly`

```sql
CREATE TABLE reports.nodal_pf_monthly (
    month          DATE        NOT NULL,   -- first day of the calendar month (e.g. 2026-06-01)
    plant_name     TEXT        NOT NULL,
    pf_score_2h    FLOAT,                  -- CNY / MWh_installed / day, 2h duration
    pf_score_4h    FLOAT,                  -- CNY / MWh_installed / day, 4h duration
    rank_2h        INTEGER,
    rank_4h        INTEGER,
    n_days         INTEGER,                -- trading days used in computation
    computed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (month, plant_name)
);
```

### 4.2 Monthly computation job — `compute_nodal_pf_monthly`

**Trigger:** Hermes APScheduler cron, 5th of each month at 01:00 UTC (09:00 Beijing).

**Logic:**
1. Determine previous full calendar month (e.g. if today is July 5, compute June 1–June 30)
2. Query all plants with data in that month from `md_id_cleared_energy`
3. For each plant, fetch 15-min prices and run `compute_dispatch_from_15min_prices` for both durations — parallelised with `ThreadPoolExecutor`
4. Compute `pf_score`, rank all plants
5. Upsert into `reports.nodal_pf_monthly` (replace existing rows for that month if re-run)

**Location:** New function `compute_and_store_nodal_pf_monthly(pg_url)` in `services/hermes/mengxi_ranking_report.py`.

### 4.3 Parallelisation

Plants are independent. Each plant requires 2 calls to `compute_dispatch_from_15min_prices`. PuLP/CBC creates its own `LpProblem` per call — parallel execution with `ThreadPoolExecutor` is safe.

**Performance estimate (one month, ~100 plants):**
- 100 plants × 30 days × 2 durations = 6,000 MILP problems
- With 8 workers: ~60–90 seconds total
- Runs once a month, not time-critical

---

## 5. Daily PDF: Nodal Rank Columns

### 5.1 Data source

The daily PDF reads nodal ranks from `reports.nodal_pf_monthly` for the most recent available month. No MILP runs during daily PDF generation — ranks are pre-computed.

```sql
SELECT plant_name, rank_2h, rank_4h
FROM reports.nodal_pf_monthly
WHERE month = (SELECT MAX(month) FROM reports.nodal_pf_monthly)
```

If the table is empty (not yet computed), nodal rank columns show `—`.

### 5.2 Table columns

Before (7 columns):
```
排名 | 项目名称 | 业主 | MW | 总收益(万元) | 收益/MWh/天 | 天数
```

After (9 columns):
```
排名 | 项目名称 | 业主 | MW | 总收益(万元) | 收益/MWh/天 | 天数 | 2h节点排名 | 4h节点排名
```

Column widths (A4, 12mm margins): project name narrowed 58mm → 46mm; owner 34mm → 28mm; two new columns at 15mm each.

### 5.3 Colour coding for nodal rank columns

- **Green** if nodal rank > actual rank (asset outperforms its location)
- **Red** if nodal rank < actual rank (asset underperforms its location)
- **Neutral** if equal or nodal rank unavailable

Colour applied to the nodal rank cell only, not the entire row.

---

## 6. Monthly Nodal Ranking Page in PDF

### 6.1 When it appears

The monthly nodal ranking page is appended to the daily PDF **only on the 5th of each month** (when the monthly job runs) or whenever `reports.nodal_pf_monthly` has been updated for the current month. On other days, the page is omitted.

Actually — simpler: always append the page using the latest available month from `reports.nodal_pf_monthly`. It stays static until recomputed on the next 5th.

### 6.2 Table structure

```
节点排名(2h) | 节点排名(4h) | 节点名称 | 2h收益/MWh/天 | 4h收益/MWh/天 | 交易天数
```

All nodes in `reports.nodal_pf_monthly` for the latest month, sorted by `rank_2h` ascending.

### 6.3 Header

```
蒙西BESS节点价值月度排名（{month_str}）
基于完美预见MILP套利模型 | 往返效率85% | 每5日更新
```

### 6.4 Footer note

```
节点价值：完美预见LP套利收益 ÷ (装机容量MWh × 天数)，单位CNY/MWh/天。
2h = 0.5C电池（2小时时长）；4h = 0.25C电池（4小时时长）。数据来源：蒙西集中式现货市场出清数据。
```

---

## 7. Code Changes

### `services/hermes/mengxi_ranking_report.py`

| Function | Change |
|---|---|
| `_query_nodal_ranks(pg_url)` | New — reads latest month's ranks from `reports.nodal_pf_monthly` |
| `_query_nodal_prices_for_month(pg_url, start, end_excl)` | New — fetches all plant 15-min prices for a calendar month |
| `_compute_nodal_pf_for_plants(prices_df, rte)` | New — runs MILP for all plants (parallelised), returns score dicts |
| `compute_and_store_nodal_pf_monthly(pg_url)` | New — orchestrates monthly computation and upserts to DB |
| `_enrich_and_rank(raw_df, plant_list, nodal_ranks)` | Updated — accepts `nodal_ranks` dict, adds two rank columns |
| `_build_table(df)` | Updated — renders two extra columns with cell-level colour coding |
| `_generate_pdf(...)` | Updated — accepts `nodal_monthly_df`; appends monthly nodal ranking page; updates footer |
| `send_daily_ranking(...)` | Updated — reads nodal ranks from DB; passes to PDF generator |

### `services/hermes/app.py`

| Change | Detail |
|---|---|
| Import `compute_and_store_nodal_pf_monthly` | From `mengxi_ranking_report` |
| Add monthly cron job | 5th of each month, 01:00 UTC, calls `compute_and_store_nodal_pf_monthly` |

### DB

| Change | Detail |
|---|---|
| New table `reports.nodal_pf_monthly` | Created by `compute_and_store_nodal_pf_monthly` on first run (CREATE TABLE IF NOT EXISTS) |

---

## 8. Error Handling

- If `_query_nodal_ranks` fails or table is empty: nodal rank columns show `—`; monthly page omitted
- If MILP solver returns non-Optimal for a plant-day: that day excluded from the sum
- If monthly job fails: error logged, Feishu alert sent; next month's run retries from scratch
- Plants with fewer than 1 complete day: excluded from ranking
