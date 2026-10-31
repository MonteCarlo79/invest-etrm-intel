# Trading Book Object Model — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the AssetBook object model for the 6 蒙西 BESS stations — forecast → strategy → cleared → actual → settled chain with a named-delta attribution waterfall — per the approved spec.

**Architecture:** Thin orchestration layer (`libs/trading_book/`) over existing tables. No new ingestion pipelines. The existing `reports.bess_asset_daily_attribution` supplies most waterfall deltas; new pieces are the station registry, the settlement bridge (bill daily breakdown → per-book daily rows), the waterfall assembler, a Trader book-view tab, the D-1 forecast artifact, and the fleet-response estimator.

**Tech Stack:** Python 3.11, SQLAlchemy 2.x, pandas, Streamlit (mengxi-dashboard), pytest.

**Spec:** `docs/superpowers/specs/2026-10-31-trading-book-model-design.md` (read first — locked decisions and hard invariants live there)

## Global Constraints

- **Frame invariant:** all wall-clock rendering uses named zone `Asia/Shanghai` — NEVER the interval literal `'+08:00'` (POSIX-inverted). `md_mengxi_nodal_price_96_filled.metric_time`/`time_order_96` are **CST-naive** (v5 frame); window with naive CST literals, no `+08` suffix. `ops_bess_dispatch_15min.interval_start` and `md_id_cleared_energy.datetime` are also CST-naive. `rm_settlements` is date/month-keyed.
- **Bills are authoritative for the settled layer** (spec decision 3): use the bill's daily breakdown where carried; monthly lumps accrue pro-rata with `accrual_grade='accrual'`.
- **Strategy objects carry `scope`** (`'nodal_agent'`, `'dp_optimal'`); attribution names the primary scope. DP-vs-nominated spread is its own delta line.
- **No RetailBook code** — interface stays family-neutral, nothing retail is built.
- Every task's tests must run with `pytest tests/` from repo root and pass before commit.

---

### Task 1: Core models + frame constants

**Files:**
- Create: `libs/trading_book/__init__.py`
- Create: `libs/trading_book/models.py`
- Test: `tests/trading_book/test_models.py`

**Interfaces:**
- Produces: `CST_TZ`, `TradingBook` (abstract interface), `ForecastSet`, `Strategy`, `ClearedDispatch`, `ActualDispatch`, `Settlement`, `WaterfallLine`, `Waterfall` dataclasses; `Day = datetime.date`.

- [ ] **Step 1: Write the failing test**

```python
# tests/trading_book/test_models.py
from datetime import date
from libs.trading_book.models import CST_TZ, Strategy, WaterfallLine

def test_cst_tz_is_named_zone():
    assert str(CST_TZ) == "Asia/Shanghai"  # never an interval literal (POSIX inversion lesson)

def test_strategy_scope_required():
    s = Strategy(scope="nodal_agent", day=date(2026, 11, 1), series=[0.0] * 96, primary=True)
    assert s.scope == "nodal_agent" and s.primary is True and len(s.series) == 96

def test_waterfall_line_named():
    line = WaterfallLine(name="forecast_error", delta_cny=-1234.5, note="forecast vs actual")
    assert line.name == "forecast_error"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/trading_book/test_models.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'libs.trading_book'`

- [ ] **Step 3: Implement the models**

```python
# libs/trading_book/models.py
"""Trading book object model — 蒙西 AssetBook v1 (spec 2026-10-31)."""
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import date
from zoneinfo import ZoneInfo

# Hard invariant: named zone only. The interval literal '+08:00' is
# POSIX-inverted (renders UTC−8 walls) — never use it anywhere.
CST_TZ = ZoneInfo("Asia/Shanghai")

Day = date

@dataclass(frozen=True)
class ForecastSet:
    day: Day
    nodal_price_fc: list[float]        # 96 values, station price node
    net_load_fc_mw: list[float] | None # 96 values, provincial
    fleet_response: dict | None        # aggregate stack curves (Task 8)

@dataclass(frozen=True)
class Strategy:
    scope: str                         # 'nodal_agent' | 'dp_optimal' | …
    day: Day
    series: list[float]                # 96 values, signed MW (discharge +)
    primary: bool = False

@dataclass(frozen=True)
class ClearedDispatch:
    day: Day
    cleared_mw: list[float]            # 96
    cleared_price: list[float]         # 96

@dataclass(frozen=True)
class ActualDispatch:
    day: Day
    nominated_mw: list[float]          # 96
    actual_mw: list[float]             # 96

@dataclass(frozen=True)
class Settlement:
    period: str                        # 'YYYY-MM'
    settled_cny: float
    daily: list[tuple[Day, float, str]]  # (day, amount_cny, accrual_grade)

@dataclass(frozen=True)
class WaterfallLine:
    name: str                          # forecast_error|strategy_selection|clearing|
                                       # curtailment_so|equipment_site|settlement_residual
    delta_cny: float
    note: str = ""

@dataclass(frozen=True)
class Waterfall:
    strategy_pnl_cny: float
    lines: tuple[WaterfallLine, ...]
    settled_pnl_cny: float

class TradingBook:
    """Interface. AssetBook is the only family built in v1 (spec decision 1)."""
    def forecast(self, day: Day) -> ForecastSet: raise NotImplementedError
    def strategies(self, day: Day) -> list[Strategy]: raise NotImplementedError
    def cleared(self, day: Day) -> ClearedDispatch: raise NotImplementedError
    def actual(self, day: Day) -> ActualDispatch: raise NotImplementedError
    def settlement(self, period: str) -> Settlement: raise NotImplementedError
    def attribution(self, start: Day, end: Day) -> Waterfall: raise NotImplementedError
```

```python
# libs/trading_book/__init__.py
from .models import (CST_TZ, TradingBook, ForecastSet, Strategy, ClearedDispatch,
                     ActualDispatch, Settlement, WaterfallLine, Waterfall)
__all__ = ["CST_TZ", "TradingBook", "ForecastSet", "Strategy", "ClearedDispatch",
           "ActualDispatch", "Settlement", "WaterfallLine", "Waterfall"]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/trading_book/test_models.py -v`
Expected: 3 PASS

- [ ] **Step 5: Commit**

```bash
git add libs/trading_book/ tests/trading_book/test_models.py
git commit -m "Add trading book core models (蒙西 AssetBook v1)"
```

---

### Task 2: Station registry

**Files:**
- Create: `libs/trading_book/registry.py`
- Test: `tests/trading_book/test_registry.py`

**Interfaces:**
- Consumes: `knowledge/mengxi/bess_node_registry.md` (machine-readable `|` tables, 6 current assets)
- Produces: `StationRef` dataclass (`asset_code, plant_name, capacity_mw, substation, conn_kv, price_nodes: list[str]`); `get_stations() -> list[StationRef]`; `get_station(asset_code) -> StationRef`

- [ ] **Step 1: Write the failing test**

```python
# tests/trading_book/test_registry.py
from libs.trading_book.registry import get_stations, get_station

CODES = {"suyou", "hangjinqi", "siziwangqi", "gushanliang", "bameng", "wulate"}

def test_six_current_stations():
    stations = get_stations()
    assert {s.asset_code for s in stations} == CODES

def test_station_price_nodes():
    s = get_station("suyou")
    assert s.plant_name == "景蓝乌尔图储能电站"
    assert s.capacity_mw == 100
    assert any("苏尼特" in n for n in s.price_nodes)

def test_unknown_code_raises():
    import pytest
    with pytest.raises(KeyError):
        get_station("nope")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/trading_book/test_registry.py -v`
Expected: FAIL — module missing

- [ ] **Step 3: Implement the registry**

`libs/trading_book/registry.py` — module-level constant `STATIONS` (the 6 rows transcribed from `knowledge/mengxi/bess_node_registry.md` "Current portfolio" table: asset_code, plant_name, capacity_mw, substation, conn_kv, price_nodes = own node + parent nodes split on `;`), `get_stations()` returning the list, `get_station(code)` returning one or raising `KeyError`. Upcoming assets (alashan/wuchuan/xixier) are NOT included.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/trading_book/test_registry.py -v`
Expected: 3 PASS

- [ ] **Step 5: Commit**

```bash
git add libs/trading_book/registry.py tests/trading_book/test_registry.py
git commit -m "Add 6-station 蒙西 registry to trading_book"
```

---

### Task 3: AssetBook data readers

**Files:**
- Create: `libs/trading_book/asset_book.py`
- Test: `tests/trading_book/test_asset_book.py`

**Interfaces:**
- Consumes: Task 1 models, Task 2 registry
- Produces: `AssetBook(engine, station: StationRef)` implementing `.cleared(day)`, `.actual(day)`, `.strategies(day)`, `.settlement(period)`; `.forecast(day)` raises `NotImplementedError` until Task 7

**Data facts (verified):**
- `marketdata.md_id_cleared_energy`: columns `data_date`, `datetime` (CST-naive), `cleared_price`, `cleared_energy_mwh`, `plant_name`. Match station by `plant_name` substring of registry plant_name (already the join used in `services/mengxi_nodal/data.py::get_asset_price_vectors`).
- `marketdata.ops_bess_dispatch_15min`: `(asset_code, interval_start (CST-naive), nominated_dispatch_mw, actual_dispatch_mw, nodal_price_excel)`, PK `(asset_code, interval_start)`.
- `nodal_strategy_daily`: strategy rows per station/day (nodal writer output).
- Windows: naive CST literals `f"{day} 00:00:00"` — NO `+08` suffix (Global Constraints).

- [ ] **Step 1: Write the failing test** (DB-free: assert SQL text + window builder only; no RDS in unit tests)

```python
# tests/trading_book/test_asset_book.py
from datetime import date
from libs.trading_book.asset_book import _cst_naive_window, _CLEARED_SQL, _ACTUAL_SQL

def test_window_is_naive_cst():
    s, e = _cst_naive_window(date(2026, 11, 1))
    assert s == "2026-11-01 00:00:00" and e == "2026-11-02 00:00:00"
    assert "+08" not in s and "+08" not in e  # POSIX-inversion guard

def test_sql_targets_right_tables():
    assert "md_id_cleared_energy" in _CLEARED_SQL
    assert "ops_bess_dispatch_15min" in _ACTUAL_SQL
    assert "Asia/Shanghai" not in _CLEARED_SQL  # columns already CST-naive; no rendering needed
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/trading_book/test_asset_book.py -v`
Expected: FAIL — module missing

- [ ] **Step 3: Implement AssetBook**

`libs/trading_book/asset_book.py`:
- `_cst_naive_window(day) -> (str, str)` — naive CST bounds
- `_CLEARED_SQL`: `SELECT datetime, cleared_price, cleared_energy_mwh FROM marketdata.md_id_cleared_energy WHERE plant_name LIKE :pn AND datetime >= CAST(:s AS timestamp) AND datetime < CAST(:e AS timestamp) ORDER BY datetime`
- `_ACTUAL_SQL`: `SELECT interval_start, nominated_dispatch_mw, actual_dispatch_mw FROM marketdata.ops_bess_dispatch_15min WHERE asset_code = :ac AND interval_start >= CAST(:s AS timestamp) AND interval_start < CAST(:e AS timestamp) ORDER BY interval_start`
- `_STRATEGY_SQL`: read `nodal_strategy_daily` for the station's node/day (scope='nodal_agent')
- `_to_96(df, ts_col, val_col) -> list[float]` — align to 96-slot series by quarter-hour
- `AssetBook(engine, station)` with `.cleared()/.actual()/.strategies()/.settlement()` (settlement delegates to the Task 4 bridge), `.forecast()` raising NotImplementedError

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/trading_book/test_asset_book.py -v`
Expected: 2 PASS

- [ ] **Step 5: Commit**

```bash
git add libs/trading_book/asset_book.py tests/trading_book/test_asset_book.py
git commit -m "Add AssetBook data readers (cleared/actual/strategies)"
```

---

### Task 4: Settlement bridge — bill daily breakdown → per-book daily rows

**Files:**
- Create: `services/mengxi_nodal/settlement_bridge.py`
- Test: `tests/mengxi_nodal/test_settlement_bridge.py`

**Interfaces:**
- Consumes: `marketdata.rm_settlements` (book_id, settlement_month, file_type, raw_data), existing bill parsers in asset-risk (charge hierarchies, 定远 precedent)
- Produces: table `reports.book_settled_daily(asset_code text, data_date date, amount_cny numeric, source text, accrual_grade text, PK(asset_code, data_date, source))`; `refresh_settled_daily(engine, asset_code, month) -> int`

- [ ] **Step 1: Write the failing test**

```python
# tests/mengxi_nodal/test_settlement_bridge.py
from datetime import date
from services.mengxi_nodal.settlement_bridge import _spread_lump, _DDL

def test_ddl_has_accrual_grade():
    assert "accrual_grade" in _DDL and "book_settled_daily" in _DDL

def test_spread_lump_prorata():
    rows = _spread_lump(3100.0, date(2026, 10, 1), date(2026, 10, 31))
    assert len(rows) == 31
    assert abs(sum(r[1] for r in rows) - 3100.0) < 0.01
    assert all(r[2] == "accrual" for r in rows)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/mengxi_nodal/test_settlement_bridge.py -v`
Expected: FAIL — module missing

- [ ] **Step 3: Implement the bridge**

`services/mengxi_nodal/settlement_bridge.py`:
- `_DDL` — CREATE TABLE IF NOT EXISTS `reports.book_settled_daily` (columns per Interfaces)
- `_spread_lump(amount, month_start, month_end)` — pro-rata daily rows tagged `accrual`
- `refresh_settled_daily(engine, asset_code, month)` — read the month's settlement items for the book: lines carrying a daily breakdown (energy charges with dates) insert as `accrual_grade='bill'`; monthly lumps go through `_spread_lump`; upsert on PK; return rowcount. Reuse the existing charge-hierarchy parser from asset-risk (`parser_*` per book) — do NOT reparse PDFs; read `rm_settlement_items` if present, else `raw_data` via the registered parser.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/mengxi_nodal/test_settlement_bridge.py -v`
Expected: 2 PASS

- [ ] **Step 5: Commit**

```bash
git add services/mengxi_nodal/settlement_bridge.py tests/mengxi_nodal/test_settlement_bridge.py
git commit -m "Add settlement bridge for book daily settled rows"
```

---

### Task 5: Waterfall assembler

**Files:**
- Create: `libs/trading_book/waterfall.py`
- Test: `tests/trading_book/test_waterfall.py`

**Interfaces:**
- Consumes: `reports.bess_asset_daily_attribution` (existing per-day deltas: forecast_error_loss, strategy_error_loss, nomination_loss, execution_clearing_loss, realisation gaps), `reports.book_settled_daily` (Task 4), strategies (Task 3)
- Produces: `build_waterfall(engine, book: AssetBook, start, end) -> Waterfall` — strategy_pnl → 5 named lines → settled

**Delta mapping (existing attribution → waterfall lines):**
| Waterfall line | Source |
|---|---|
| forecast_error | `forecast_error_loss` |
| strategy_selection | `nominated_pnl − tt_strategy_pnl` (nominated vs DP-optimal spread) |
| clearing | `execution_clearing_loss` |
| curtailment_so + equipment_site | `realisation_gap_vs_pf_grid` split: cleared-vs-actual share vs actual-vs-capability share (compute from actual vs cleared curves; if indistinguishable, lump as one line `curtailment_so`) |
| settlement_residual | `settled − (strategy_pnl − Σ above)` |

- [ ] **Step 1: Write the failing test**

```python
# tests/trading_book/test_waterfall.py
from libs.trading_book.waterfall import _map_lines

def test_lines_sum_to_settled():
    w = _map_lines(strategy_pnl=10000.0, forecast_err=-500.0, strat_sel=-200.0,
                   clearing=-300.0, curtail=-400.0, settled=8500.0)
    names = [l.name for l in w.lines]
    assert names == ["forecast_error", "strategy_selection", "clearing",
                     "curtailment_so", "settlement_residual"]
    total = w.strategy_pnl_cny + sum(l.delta_cny for l in w.lines)
    assert abs(total - w.settled_pnl_cny) < 1e-6

def test_residual_is_balancing():
    w = _map_lines(strategy_pnl=10000.0, forecast_err=-500.0, strat_sel=0.0,
                   clearing=0.0, curtail=0.0, settled=9000.0)
    resid = [l for l in w.lines if l.name == "settlement_residual"][0]
    assert resid.delta_cny == -500.0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/trading_book/test_waterfall.py -v`
Expected: FAIL — module missing

- [ ] **Step 3: Implement the assembler**

`libs/trading_book/waterfall.py`:
- `_map_lines(strategy_pnl, forecast_err, strat_sel, clearing, curtail, settled) -> Waterfall` — pure function, residual = settled − strategy_pnl − Σ(others)
- `build_waterfall(engine, book, start, end) -> Waterfall` — aggregate the attribution rows and settled rows over the range, call `_map_lines`

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/trading_book/test_waterfall.py -v`
Expected: 2 PASS

- [ ] **Step 5: Commit**

```bash
git add libs/trading_book/waterfall.py tests/trading_book/test_waterfall.py
git commit -m "Add attribution waterfall assembler"
```

---

### Task 6: Trader book view tab

**Files:**
- Create: `apps/mengxi-dashboard/book_tab.py`
- Modify: `apps/mengxi-dashboard/app.py` (register tab)
- Test: `tests/mengxi_dashboard/test_book_tab.py`

**Interfaces:**
- Consumes: Tasks 2, 3, 5
- Produces: `render_book_tab(engine)` — station selector, date-range, chain chart (strategy vs cleared vs actual vs price), waterfall chart (5 named deltas + settled), settled-vs-bill reconciliation line

- [ ] **Step 1: Write the failing test**

```python
# tests/mengxi_dashboard/test_book_tab.py
from apps.mengxi_dashboard.book_tab import _waterfall_fig_data

def test_waterfall_fig_has_all_lines():
    from libs.trading_book.models import Waterfall, WaterfallLine
    w = Waterfall(strategy_pnl_cny=10000.0,
                  lines=(WaterfallLine("forecast_error", -500.0),
                         WaterfallLine("strategy_selection", -200.0),
                         WaterfallLine("clearing", -300.0),
                         WaterfallLine("curtailment_so", -400.0),
                         WaterfallLine("settlement_residual", -100.0)),
                  settled_pnl_cny=8500.0)
    labels, values = _waterfall_fig_data(w)
    assert labels[0] == "Strategy P&L" and labels[-1] == "Settled P&L"
    assert len(labels) == 7
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/mengxi_dashboard/test_book_tab.py -v`
Expected: FAIL — module missing

- [ ] **Step 3: Implement the tab**

`apps/mengxi-dashboard/book_tab.py`:
- `_waterfall_fig_data(w) -> (labels, values)` — plotly waterfall input
- `render_book_tab(engine)` — `st.selectbox` over `get_stations()`, date range, sections: (a) dispatch chain chart (4 series on one day), (b) waterfall (range), (c) settled rows table from `reports.book_settled_daily`
- Register in `app.py` alongside existing tabs ("Book" / "资产账本")

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/mengxi_dashboard/test_book_tab.py -v`
Expected: 1 PASS; full app smoke locally (`streamlit run apps/mengxi-dashboard/app.py --server.port 8511`, Book tab renders with live data)

- [ ] **Step 5: Commit**

```bash
git add apps/mengxi-dashboard/book_tab.py apps/mengxi-dashboard/app.py tests/mengxi_dashboard/test_book_tab.py
git commit -m "Add Trader book view tab with attribution waterfall"
```

---

### Task 7: D-1 forecast artifact

**Files:**
- Create: `services/mengxi_nodal/book_forecast.py`
- Test: `tests/mengxi_nodal/test_book_forecast.py`

**Interfaces:**
- Consumes: `md_mengxi_nodal_price_96_filled` (history), `spot_fundamentals_hourly`, station registry, fleet-response estimator (Task 8)
- Produces: table `reports.book_forecast_daily(asset_code, forecast_date, slot smallint, price_fc numeric, net_load_fc_mw numeric, created_at)`; `run_day(engine, asset_code, day)`; `AssetBook.forecast()` now returns ForecastSet

- [ ] **Step 1: Write the failing test**

```python
# tests/mengxi_nodal/test_book_forecast.py
import numpy as np
from services.mengxi_nodal.book_forecast import _ols_time_forecast

def test_forecast_returns_96_finite():
    rng = np.random.default_rng(7)
    hist = [np.full(96, 300.0) + rng.normal(0, 20, 96) for _ in range(30)]
    fc = _ols_time_forecast(hist)
    assert len(fc) == 96 and np.all(np.isfinite(fc))

def test_forecast_handles_short_history():
    fc = _ols_time_forecast([np.full(96, 250.0)] * 3)
    assert len(fc) == 96 and np.allclose(fc, 250.0)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/mengxi_nodal/test_book_forecast.py -v`
Expected: FAIL — module missing

- [ ] **Step 3: Implement the forecast job**

`services/mengxi_nodal/book_forecast.py`:
- `_ols_time_forecast(history: list[np.ndarray]) -> np.ndarray` — rolling OLS + time-of-day on the station price node (same family as `ols_rt_time_v1`, per spec phasing; fundamentals terms optional in v1)
- `run_day(engine, asset_code, day)` — fetch the station node's last 60 days from the filled table (naive CST window), compute forecast for `day`, upsert into `reports.book_forecast_daily`
- Wire `AssetBook.forecast(day)` to read that table

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/mengxi_nodal/test_book_forecast.py -v`
Expected: 2 PASS

- [ ] **Step 5: Commit**

```bash
git add services/mengxi_nodal/book_forecast.py libs/trading_book/asset_book.py tests/mengxi_nodal/test_book_forecast.py
git commit -m "Add D-1 nodal forecast artifact per station"
```

---

### Task 8: Fleet-response estimator

**Files:**
- Create: `services/mengxi_nodal/fleet_response.py`
- Test: `tests/mengxi_nodal/test_fleet_response.py`

**Interfaces:**
- Consumes: historical nodal prices + provincial net load (`spot_fundamentals_hourly`)
- Produces: `estimate_stack(engine, province, lookback_days) -> dict` — `{"bands": [(p_lo, p_hi, mean_response_mw)], "floors": {"coal_var_cost_cny_mwh": X, "bess_spread_cny_mwh": Y}}`; persisted into ForecastSet.fleet_response

- [ ] **Step 1: Write the failing test**

```python
# tests/mengxi_nodal/test_fleet_response.py
import numpy as np
from services.mengxi_nodal.fleet_response import _fit_bands

def test_bands_monotone_response():
    rng = np.random.default_rng(1)
    prices = rng.uniform(0, 800, 500)
    response = 200 + 0.5 * prices + rng.normal(0, 30, 500)
    bands = _fit_bands(prices, response, n_bands=4)
    means = [b[2] for b in bands]
    assert all(m1 <= m2 for m1, m2 in zip(means, means[1:]))  # higher price ⇒ more response

def test_bands_cover_range():
    bands = _fit_bands(np.linspace(0, 800, 200), np.linspace(100, 500, 200), n_bands=4)
    assert bands[0][0] <= 0 and bands[-1][1] >= 800
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/mengxi_nodal/test_fleet_response.py -v`
Expected: FAIL — module missing

- [ ] **Step 3: Implement the estimator**

`services/mengxi_nodal/fleet_response.py`:
- `_fit_bands(prices, response, n_bands) -> list[(lo, hi, mean_response)]` — quantile bands, mean response per band
- `estimate_stack(engine, province='蒙西', lookback_days=60)` — pull nodal prices (filled table) + cleared dispatch aggregate (`md_id_cleared_energy` summed) or net load; fit bands; attach optimization floors (coal ≥ variable cost constant, BESS spread ≥ 0 constant — v1 constants in code, not a coal registry)

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/mengxi_nodal/test_fleet_response.py -v`
Expected: 2 PASS

- [ ] **Step 5: Commit**

```bash
git add services/mengxi_nodal/fleet_response.py tests/mengxi_nodal/test_fleet_response.py
git commit -m "Add aggregate fleet-response estimator"
```

---

### Task 9: Wire-up + full verification

**Files:**
- Modify: `libs/trading_book/asset_book.py` (forecast via Task 7, fleet_response via Task 8)
- Test: `tests/trading_book/test_book_e2e.py`

- [ ] **Step 1: Write the failing test** — assemble an AssetBook and verify the interface end-to-end with fakes

```python
# tests/trading_book/test_book_e2e.py
def test_book_interface_complete():
    from libs.trading_book.registry import get_station
    from libs.trading_book.asset_book import AssetBook
    book = AssetBook(engine=None, station=get_station("suyou"))
    for meth in ("cleared", "actual", "strategies", "settlement", "forecast", "attribution"):
        assert callable(getattr(book, meth))
```

- [ ] **Step 2: Run test → implement wiring → verify pass**

- [ ] **Step 3: Full suite green + local app smoke**

Run: `pytest tests/ -q` — all new + existing tests pass. Local app run shows the Book tab with live data.

- [ ] **Step 4: Commit**

```bash
git add libs/trading_book/ tests/
git commit -m "Wire AssetBook end-to-end (forecast + fleet response + waterfall)"
```

---

### Task 10: Deploy Trader with Book tab

- [ ] **Step 1:** `/deploy` checklist for mengxi-dashboard (next image after v40; git-archive build, `--platform linux/amd64`, both psycopg drivers already in requirements)
- [ ] **Step 2:** Local run verification (Book tab renders with real data, no errors)
- [ ] **Step 3:** Get explicit deploy confirmation, then build → push → jq-swap td from current tdArn → update-service → append LATEST-IMAGES.md
- [ ] **Step 4:** Verify live: Book tab on https://www.pjh-etrm.ai/mengxi-dashboard/
