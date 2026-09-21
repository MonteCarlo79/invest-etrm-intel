# Nodal Trading Agents Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Per-asset (~40 Mengxi BESS) trading agents producing next-day nominated dispatch strategies from nodal price forecasts, with recursive BESS-feedback convergence, promote-loop evaluation, and comparison vs traders' realized strategies.

**Architecture:** L2 nodal price formation (`services/nodal_forecast/`) on top of L1's grid forecast (parallel session, via `marketdata.nodal_fc_grid_daily` contract; LingFeng RT fallback), L3 per-asset agents (`services/nodal_agents/`) reusing the prod dispatch engine, plus a "Nodal Trading" tab in apps/mengxi-dashboard. New node map (full 网架图 extraction, user-reviewed) + dynamic asset registry.

**Tech Stack:** pandas, numpy, scipy (ridge), pdfplumber + vision (PDF extraction), psycopg2/SQLAlchemy, Streamlit, plotly, pytest.

**Spec:** `docs/superpowers/specs/2026-09-22-nodal-trading-agents-design.md`

## Global Constraints

- Work on branch `feat/nodal-trading`; merge to `main` after Task 8 verification (CLAUDE.md: incomplete features don't go to main).
- **Parallel-session safety:** another session is actively working on main/feat/price-forecasting in this repo. Never `git checkout` when the tree has their staged files (commit with explicit pathspecs or plumbing; never `git add .`; never reset/rebase shared branches; if a commit lands on the wrong branch, cherry-pick to main and update-ref back).
- RDS is unreachable from the Mac (SG half-open): all DB tests use fixtures/fakes; integration verification happens on ECS (post-deploy) or via run-task probes.
- All new tables `CREATE TABLE IF NOT EXISTS` (idempotent). In-app upload only, no S3.
- pandas NULL→NaN discipline: `pd.notna` guards in aggregations; `astype(object).where()` before `to_dict` (pandas 3.x semantics).
- Vision extraction outputs go to `knowledge/mengxi/` and REQUIRE user review before seeding DB (like channel registry).
- Recursive loop: converged strategy only (max 3 iterations, <2% MWh delta) — never evaluate first-pass strategies.
- Every forecast/strategy table row carries `model_version` (git short hash or semantic tag) for auditability.

---

### Task 1: Contract table + grid forecast fallback loader

**Files:**
- Create: `services/nodal_forecast/__init__.py` (empty), `services/nodal_forecast/db.py`
- Test: `tests/nodal_trading/test_db.py`

**Interfaces:**
- Produces:
  - `db.DDL: list[str]` + `db.ensure_tables(conn) -> None`
  - `db.get_grid_forecast(conn, target_date: date) -> pd.DataFrame` — columns `[province, target_date, price_hat, model_version]`; returns `nodal_fc_grid_daily` rows when present, else **LingFeng RT fallback** (latest 30 days of `marketdata.spot_prices_hourly` RT for 蒙西 averaged per day-part as a naive forecast with `model_version='lingfeng_rt_fallback'`), so L2 works from day one
  - `db.upsert_grid_forecast(conn, rows: list[dict]) -> int`

- [ ] **Step 1: Write the failing test**

```python
# tests/nodal_trading/test_db.py
from datetime import date
from services.nodal_forecast import db

class _Cur:
    def __init__(self, rows=()): self._rows = list(rows); self.calls = []
    def execute(self, sql, params=None): self.calls.append((sql, params))
    def executemany(self, sql, seq): self.calls.append((sql, list(seq)))
    def fetchall(self): return self._rows
class _Conn:
    def __init__(self, rows=()): self.cur = _Cur(rows); self.committed = 0
    def cursor(self): return self.cur
    def commit(self): self.committed += 1

def test_ddl_has_three_new_tables():
    ddl = " ".join(db.DDL)
    assert "nodal_fc_grid_daily" in ddl
    assert "nodal_node_registry" in ddl
    assert "nodal_asset_registry" in ddl
    assert "nodal_strategy_daily" in ddl

def test_upsert_grid_forecast_roundtrip_sql():
    conn = _Conn()
    n = db.upsert_grid_forecast(conn, [dict(province="蒙西", target_date=date(2026,9,23),
        price_hat=321.5, model_version="v1")])
    assert n == 1
    assert "ON CONFLICT (province, target_date, model_version) DO UPDATE" in conn.cur.calls[0][0]

def test_fallback_flagged_when_table_empty(monkeypatch):
    conn = _Conn(rows=[])
    monkeypatch.setattr(db, "_lingfeng_rt_daily", lambda conn, prov: [
        (date(2026,9,22), 300.0), (date(2026,9,23), 310.0)])
    df = db.get_grid_forecast(conn, date(2026, 9, 24), province="蒙西")
    assert not df.empty
    assert set(df["model_version"]) == {"lingfeng_rt_fallback"}
```

- [ ] **Step 2: Run test to verify it fails** — FAIL (module missing)

- [ ] **Step 3: Implement db.py**

```python
# services/nodal_forecast/db.py
"""DB contract for the nodal forecast layer. L1 (bess-map, parallel session)
writes nodal_fc_grid_daily; L2/L3 read it. LingFeng RT fallback when empty."""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

DDL = [
    """CREATE TABLE IF NOT EXISTS marketdata.nodal_fc_grid_daily (
        id BIGSERIAL PRIMARY KEY,
        province TEXT NOT NULL, fc_date DATE NOT NULL, target_date DATE NOT NULL,
        price_hat NUMERIC, model_version TEXT NOT NULL,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (province, target_date, model_version))""",
    """CREATE TABLE IF NOT EXISTS marketdata.nodal_node_registry (
        name TEXT PRIMARY KEY, voltage_kv INT, substation TEXT,
        transformers INT, rated_mva NUMERIC, n1_firm_mva NUMERIC,
        zone TEXT, fengxing_node_name TEXT, connected_plants TEXT,
        source TEXT, updated_at TIMESTAMPTZ DEFAULT NOW())""",
    """CREATE TABLE IF NOT EXISTS marketdata.nodal_asset_registry (
        plant_name TEXT PRIMARY KEY, node TEXT, substation TEXT,
        capacity_mw NUMERIC, duration_h NUMERIC, rte_pct NUMERIC,
        zone TEXT, settle_node TEXT, active BOOLEAN DEFAULT TRUE,
        updated_at TIMESTAMPTZ DEFAULT NOW())""",
    """CREATE TABLE IF NOT EXISTS marketdata.nodal_strategy_daily (
        id BIGSERIAL PRIMARY KEY,
        plant_name TEXT NOT NULL, target_date DATE NOT NULL,
        curve_json TEXT NOT NULL, assumptions_json TEXT,
        iterations INT DEFAULT 1, convergence_delta_mwh NUMERIC,
        model_version TEXT NOT NULL,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (plant_name, target_date, model_version))""",
]

def ensure_tables(conn) -> None:
    with conn.cursor() as cur:
        for stmt in DDL:
            cur.execute(stmt)
    conn.commit()

_FC_UPSERT = """INSERT INTO marketdata.nodal_fc_grid_daily
    (province, fc_date, target_date, price_hat, model_version)
    VALUES (%(province)s, %(fc_date)s, %(target_date)s, %(price_hat)s, %(model_version)s)
    ON CONFLICT (province, target_date, model_version) DO UPDATE SET
        price_hat = EXCLUDED.price_hat, created_at = NOW()"""

def upsert_grid_forecast(conn, rows: list[dict]) -> int:
    with conn.cursor() as cur:
        cur.executemany(_FC_UPSERT, rows)
    conn.commit()
    return len(rows)

def _lingfeng_rt_daily(conn, province: str) -> list[tuple[date, float]]:
    """Naive fallback: latest 30 days of RT prices, averaged — used only when
    the L1 forecast table is empty for the province."""
    df = pd.read_sql(
        """SELECT datetime::date AS d, AVG(rt_price) AS p
           FROM marketdata.spot_prices_hourly
           WHERE province = %s AND datetime >= CURRENT_DATE - 30
           GROUP BY 1 ORDER BY 1""", conn, params=(province,))
    return [(r["d"], float(r["p"])) for _, r in df.iterrows() if pd.notna(r["p"])]

def get_grid_forecast(conn, target_date: date, province: str = "蒙西") -> pd.DataFrame:
    df = pd.read_sql(
        """SELECT province, target_date, price_hat, model_version
           FROM marketdata.nodal_fc_grid_daily
           WHERE province = %s AND target_date = %s""",
        conn, params=(province, target_date))
    if not df.empty:
        return df
    # fallback: repeat the most recent RT average as the forecast, flagged
    rt = _lingfeng_rt_daily(conn, province)
    if not rt:
        return pd.DataFrame(columns=["province", "target_date", "price_hat", "model_version"])
    _, p = rt[-1]
    return pd.DataFrame([{"province": province, "target_date": target_date,
                          "price_hat": p, "model_version": "lingfeng_rt_fallback"}])
```

- [ ] **Step 4: Run tests** — pass
- [ ] **Step 5: Commit** `git add services/nodal_forecast/ tests/nodal_trading/test_db.py && git commit -m "Add nodal forecast DB contract (4 tables + LingFeng RT fallback)"`

---

### Task 2: Asset registry extraction (~40 assets)

**Files:**
- Create: `services/nodal_forecast/registry_extract.py`
- Test: `tests/nodal_trading/test_registry_extract.py`

**Interfaces:**
- Consumes: `marketdata.md_id_cleared_energy` (plant_name, data_date, cleared_energy_mwh) — source of the BESS plant list
- Produces:
  - `registry_extract.extract_bess_plants(rows: list[dict]) -> list[dict]` — per plant: `{plant_name, capacity_mw_est, duration_h_est, zone_guess, first_seen, last_seen}`; capacity estimated from max |cleared_energy_mwh|×4 (15-min → MW), duration from sign-persistence analysis
  - `registry_extract.write_registry_md(plants: list[dict], path) -> None` — writes `knowledge/mengxi/bess_asset_registry_draft.md` (user review gate; the reviewed version feeds Task 3's seed)

- [ ] **Step 1: Write the failing test**

```python
# tests/nodal_trading/test_registry_extract.py
from datetime import date
from services.nodal_forecast import registry_extract as rx

def _rows(plant, series):
    return [dict(plant_name=plant, data_date=date(2026, 1, 1), datetime=f"2026-01-01 {h:02d}:00",
                 cleared_energy_mwh=v) for h, v in series]

def test_capacity_estimated_from_max_abs_interval():
    rows = _rows("谷山梁", [(0, -25.0), (1, -25.0), (2, 25.0)])
    plants = rx.extract_bess_plants(rows)
    assert plants[0]["capacity_mw_est"] == 100.0   # 25 MWh per 15min → 100 MW

def test_duration_estimated_from_sign_persistence():
    rows = (_rows("谷山梁", [(h, -25.0) for h in range(8)]) +
            _rows("谷山梁", [(8 + h, 25.0) for h in range(4)]))
    plants = rx.extract_bess_plants(rows)
    assert plants[0]["duration_h_est"] == 2.0

def test_write_registry_md_contains_review_marker(tmp_path):
    plants = [dict(plant_name="谷山梁", capacity_mw_est=100.0, duration_h_est=2.0,
                   zone_guess="乌兰察布", first_seen=date(2026,1,1), last_seen=date(2026,9,1))]
    p = tmp_path / "draft.md"
    rx.write_registry_md(plants, p)
    txt = p.read_text()
    assert "REVIEW REQUIRED" in txt and "谷山梁" in txt and "100.0" in txt
```

- [ ] **Step 2: Run test to verify it fails**

- [ ] **Step 3: Implement registry_extract.py**

```python
# services/nodal_forecast/registry_extract.py
"""Extract the BESS asset registry draft from md_* dispatch data.
Capacity ≈ max |cleared_energy_mwh| × 4 (15-min → MW); duration ≈ longest
run of same-sign intervals ÷ 4. Zone guessed from plant-name prefix."""
from __future__ import annotations

from collections import defaultdict
from datetime import date

_ZONE_PREFIX = {"远景": "—", "谷山梁": "乌兰察布", "苏尼特": "锡林郭勒",
                "四子王": "乌兰察布", "杭锦旗": "鄂尔多斯", "乌尔图": "锡林郭勒",
                "巴盟": "巴彦淖尔", "乌梁素海": "巴彦淖尔"}

def extract_bess_plants(rows: list[dict]) -> list[dict]:
    by_plant: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        if r["cleared_energy_mwh"] is not None:
            by_plant[r["plant_name"]].append(r)
    out = []
    for plant, rs in sorted(by_plant.items()):
        cap = max(abs(r["cleared_energy_mwh"]) for r in rs) * 4
        series = sorted(rs, key=lambda r: (r["data_date"], r["datetime"]))
        best = cur = 0
        prev_sign = 0
        for r in series:
            v = r["cleared_energy_mwh"]
            s = 1 if v > 0 else (-1 if v < 0 else prev_sign)
            cur = cur + 1 if s == prev_sign and s != 0 else (1 if s != 0 else 0)
            best = max(best, cur)
            prev_sign = s
        dur = round(best / 4, 1) if best else None
        zone = next((z for p, z in _ZONE_PREFIX.items() if plant.startswith(p)), None)
        dates = [r["data_date"] for r in rs]
        out.append(dict(plant_name=plant, capacity_mw_est=round(cap, 1),
                        duration_h_est=dur, zone_guess=zone,
                        first_seen=min(dates), last_seen=max(dates)))
    return out

def write_registry_md(plants: list[dict], path) -> None:
    lines = ["# BESS Asset Registry — DRAFT (REVIEW REQUIRED before DB seed)",
             "",
             "Each row needs: node (Fengxing node_name or 母线), substation,",
             "substation_cap_mw, zone, settle_node. Edit inline; do not rename plant_name.",
             "",
             "| plant_name | capacity_mw_est | duration_h_est | zone_guess | node (FILL) | substation (FILL) | cap_mw (FILL) |",
             "|---|---:|---:|---|---|---|---|"]
    for p in plants:
        lines.append(f"| {p['plant_name']} | {p['capacity_mw_est']} | {p['duration_h_est'] or '—'} "
                     f"| {p['zone_guess'] or '—'} |  |  |  |")
    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")
```

- [ ] **Step 4: Run tests** — pass
- [ ] **Step 5: Run the extraction against RDS via run-task probe (ECS, since RDS is blocked from Mac)**

```bash
# run-task: dump md_id_cleared_energy plant list + energies to JSON, then
# registry_extract locally → knowledge/mengxi/bess_asset_registry_draft.md
```
Expected: draft file with ~40 plants. **STOP: present the draft to the user for review (fill node/substation/cap columns).**

- [ ] **Step 6: Commit** `git add services/nodal_forecast/registry_extract.py tests/nodal_trading/test_registry_extract.py && git commit -m "Add BESS asset registry extraction from md_* dispatch data"`

---

### Task 3: 网架图 full node map extraction + node registry seed

**Files:**
- Create: `services/nodal_forecast/gridmap_extract.py`, `knowledge/mengxi/substation_capacity.md` (generated draft)
- Test: `tests/nodal_trading/test_gridmap_extract.py`

**Interfaces:**
- Produces:
  - `gridmap_extract.parse_substation_rows(rows: list[dict]) -> list[dict]` — validates/normalizes vision-extracted rows: `{name, voltage_kv, substation, transformers, rated_mva, n1_firm_mva, connected_plants, source}`; dedupes by (substation, voltage_kv); MVA/MW conversions (MVA≈MW for cap purposes, note in docstring)
  - `gridmap_extract.seed_node_registry(conn, nodes: list[dict]) -> int` — upsert into `nodal_node_registry` (Task 1 DDL)
  - `gridmap_extract.link_fengxing_names(conn) -> dict[str, str]` — maps registry nodes to Fengxing `node_name` values from `md_mengxi_nodal_price_96` (exact + normalized-substring match), returns unresolved list

- [ ] **Step 1: Write the failing test**

```python
# tests/nodal_trading/test_gridmap_extract.py
from services.nodal_forecast import gridmap_extract as gx

def test_parse_dedupes_by_substation_and_voltage():
    rows = [
        dict(name="汗海变电站", voltage_kv=500, substation="汗海", transformers=2,
             rated_mva=1500.0, n1_firm_mva=None, connected_plants=""),
        dict(name="汗海变电站", voltage_kv=500, substation="汗海", transformers=3,
             rated_mva=1500.0, n1_firm_mva=None, connected_plants=""),
    ]
    out = gx.parse_substation_rows(rows)
    assert len(out) == 1 and out[0]["transformers"] == 3   # later row wins on dedupe

def test_seed_upserts_and_link_matches_fengxing():
    class Cur:
        def __init__(self): self.calls = []
        def execute(self, s, p=None): self.calls.append((s, p))
        def executemany(self, s, seq): self.calls.append((s, list(seq)))
        def fetchall(self): return [("内蒙.汗海站/500kV.1M",)]
    class Conn:
        def __init__(self): self.cur = Cur()
        def cursor(self): return self.cur
        def commit(self): pass
    conn = Conn()
    nodes = [dict(name="汗海变电站", voltage_kv=500, substation="汗海", transformers=2,
                  rated_mva=1500.0, n1_firm_mva=None, zone=None,
                  fengxing_node_name=None, connected_plants="", source="网架图2025-12-22")]
    assert gx.seed_node_registry(conn, nodes) == 1
    links = gx.link_fengxing_names(conn)
    assert links.get("汗海") == "内蒙.汗海站/500kV.1M"
```

- [ ] **Step 2: Run test to verify it fails**

- [ ] **Step 3: Implement gridmap_extract.py (parse + seed + link)**

- [ ] **Step 4: Vision-extract the 网架图 (pymupdf render → Read, page by page)** — produce `knowledge/mengxi/substation_capacity.md` draft with ALL nodes (columns: name/voltage/transformers/rated_mva/n1_firm/connected_plants). **STOP: user review gate before seeding.**

- [ ] **Step 5: Commit** `git add services/nodal_forecast/gridmap_extract.py tests/nodal_trading/test_gridmap_extract.py && git commit -m "Add grid node map extraction and Fengxing name linking"`

---

### Task 4: L2 nodal price formation + backtest

**Files:**
- Create: `services/nodal_forecast/price_formation.py`, `services/nodal_forecast/backtest.py`
- Test: `tests/nodal_trading/test_price_formation.py`

**Interfaces:**
- Consumes: `db.get_grid_forecast` (Task 1), `marketdata.spot_fundamentals_hourly` (load/renewable/bidding_space), `md_mengxi_nodal_price_96` (node_name, metric_time, time_order_96, avg_node_price), Task 3's node registry
- Produces:
  - `price_formation.fit_cluster_model(hist: pd.DataFrame) -> dict` — ridge coefficients per regime
  - `price_formation.nodal_curve(grid_price_hat: float, features: dict, model: dict) -> np.ndarray` (96,) — nodal 15-min curve for one node for one day
  - `backtest.nodal_mae(model_fn, days: list[date]) -> dict[str, float]` — nodal MAE per cluster vs actual; **gate: must not exceed grid-only baseline MAE**

- [ ] **Step 1: Write the failing test**

```python
# tests/nodal_trading/test_price_formation.py
import numpy as np
import pandas as pd
import pytest
from services.nodal_forecast import price_formation as pf

def _hist(days=60, congested_from=30):
    rows = []
    for d in range(days):
        congested = d >= congested_from
        grid = 300.0
        nodal = grid + (50 if congested else 5)
        rows.append(dict(d=d, grid_price=grid, headroom=-10.0 if congested else 200.0,
                         substation_renewable=900.0 if congested else 100.0,
                         congested=congested, nodal_price=nodal))
    return pd.DataFrame(rows)

def test_fit_recovers_regime_coefficients():
    model = pf.fit_cluster_model(_hist())
    free = model["regimes"][False]
    cong = model["regimes"][True]
    assert free["delta_intercept"] == pytest.approx(5, abs=3)
    assert cong["delta_intercept"] == pytest.approx(50, abs=8)

def test_nodal_curve_applies_regime_delta_to_shape():
    model = pf.fit_cluster_model(_hist())
    curve = pf.nodal_curve(310.0, dict(headroom=-10.0, substation_renewable=900.0,
                                       congested=True), model)
    assert curve.shape == (96,)
    assert curve.mean() == pytest.approx(310 + 50, abs=8)

def test_fallback_to_cluster_when_node_thin():
    model = pf.fit_cluster_model(_hist())
    assert pf.resolve_model(model, node="nonexistent_node") is model["cluster_default"]
```

- [ ] **Step 2: Run test to verify it fails**

- [ ] **Step 3: Implement price_formation.py + backtest.py**

Core shape: `nodal = grid_price_hat + delta`, `delta = intercept_regime + a·headroom_norm + b·renewable_norm`; ridge (λ=1.0) per regime per cluster; `cluster_default` = cluster-mean model for thin nodes. Backtest compares nodal-model MAE vs grid-only MAE per cluster.

- [ ] **Step 4: Run tests** — pass
- [ ] **Step 5: Commit** `git add services/nodal_forecast/price_formation.py services/nodal_forecast/backtest.py tests/nodal_trading/test_price_formation.py && git commit -m "Add L2 nodal price formation with congestion regimes"`

---

### Task 5: L3 agent engine (behavior forecast + LP + recursion + writer)

**Files:**
- Create: `services/nodal_agents/__init__.py`, `services/nodal_agents/behavior.py`, `services/nodal_agents/agent.py`, `services/nodal_agents/recursion.py`, `services/nodal_agents/writer.py`
- Test: `tests/nodal_trading/test_agents.py`

**Interfaces:**
- Consumes: `services/bess_map/optimisation_engine.compute_dispatch_from_15min_prices(prices_15min, power_mw, duration_h, roundtrip_eff, ...)` (prod engine — verified signature), `md_id_cleared_energy` (per-asset 15-min cleared energy), `nodal_asset_registry`, `marketdata.province_fuel_fleet`
- Produces:
  - `behavior.forecast_zone_bess(zone: str, target_date: date, hist: pd.DataFrame) -> np.ndarray` (96,) — day-type-matched mean dispatch of zone's other BESS
  - `behavior.coal_stack_response(residual: np.ndarray, fuel: dict) -> np.ndarray` (96,) — aggregate coal output from fuel-fleet stack at forecast price
  - `agent.optimize_asset(asset: dict, curve: np.ndarray, others: np.ndarray, cap_mw: float | None, zones: np.ndarray) -> dict` — LP via compute_dispatch_from_15min_prices with substation cap (`max_throughput_mwh`) and zone masks (restricted intervals zeroed)
  - `recursion.converge(assets, curves_fn, max_iter=3, tol_mwh_pct=2.0) -> dict` — recursive loop per spec §2; returns converged strategies + iteration count + convergence delta
  - `writer.run_day(conn, target_date: date) -> dict` — the daily orchestrator: forecast curves → per-asset optimize → converge → upsert `nodal_strategy_daily` (Task 1 DDL)

- [ ] **Step 1: Write the failing test**

```python
# tests/nodal_trading/test_agents.py
import numpy as np
import pytest
from services.nodal_agents import agent, behavior, recursion

def test_zone_bess_forecast_is_daytype_matched():
    hist = np.vstack([np.full(96, -20.0), np.full(96, -40.0), np.full(96, -20.0)])
    # two "weekday-like" days and one "weekend-like" → weekend forecast uses the -40 day
    out = behavior._daytype_mean(hist, mask=[True, False, True])
    assert out.mean() == pytest.approx(-40.0)

def test_optimize_respects_substation_cap():
    asset = dict(plant_name="谷山梁", capacity_mw=100.0, duration_h=2.0, rte_pct=85.0)
    curve = np.array([200.0]*48 + [500.0]*48)
    others = np.full(96, 50.0)   # other BESS discharging 50 MW at peak
    cap = 100.0                  # substation allows only 100 MW total discharge
    out = agent.optimize_asset(asset, curve, others, cap_mw=cap,
                               zones=np.zeros(96, dtype=int))
    assert out["curve"].shape == (96,)
    assert out["curve"].max() <= cap - others.max() + 1e-6

def test_recursion_converges_and_stops_on_small_delta():
    calls = {"n": 0}
    def curves_fn(iter_no, dispatch):
        calls["n"] += 1
        return np.array([200.0]*48 + [500.0 - 5*iter_no]*48)
    asset = dict(plant_name="谷山梁", capacity_mw=100.0, duration_h=2.0, rte_pct=85.0)
    out = recursion.converge([asset], curves_fn, max_iter=3, tol_mwh_pct=2.0)
    assert out["iterations"] <= 3 and calls["n"] >= 2
    assert out["strategies"]["谷山梁"]["curve"].shape == (96,)
```

- [ ] **Step 2: Run test to verify it fails**

- [ ] **Step 3: Implement behavior.py, agent.py, recursion.py, writer.py**

Key: `agent.optimize_asset` wraps `compute_dispatch_from_15min_prices` with prices = forecast curve; substation cap enforced by scaling `power_mw` available per interval = cap − max(others discharge at that interval, 0); zone mask: restricted intervals have prices set to −∞ (charge) / +∞ (discharge) per zone type; recursion: after each iteration, aggregate BESS dispatch = Σ agent curves + zone forecast of others → bidding-space adjustment → curves_fn re-generates curves → compare MWh delta.

- [ ] **Step 4: Run tests** — pass
- [ ] **Step 5: Commit** `git add services/nodal_agents/ tests/nodal_trading/test_agents.py && git commit -m "Add L3 asset agent engine with substation caps, zones, recursion"`

---

### Task 6: Promote loop registration

**Files:**
- Modify: `services/nodal_agents/writer.py` (append registration)
- Test: `tests/nodal_trading/test_agents.py` (append)

**Interfaces:**
- Consumes: `services.bess_map.strategy_experiments` — `evaluate(eng, schema, window_days)` pattern with `scope='nodal_agent'`; strategy_experiments table (verified DDL: scope/model/province/duration_h/power_mw/roundtrip_eff/window_days/window_end/days/mean_capture_rate/mean_realized_per_mwh/mean_theoretical_per_mwh/delta_vs_champion/status/note)
- Produces: `writer.register_strategies(conn, target_date) -> int` — evaluates converged strategies vs actual RT (when actuals exist) and upserts rows with `scope='nodal_agent'`, `model='nodal_agent_vN'`, per plant

- [ ] **Step 1: Write the failing test** — fake conn captures SQL; assert scope + model + per-plant rows with capture-rate computed as realized/theoretical (theoretical from `reports.nodal_pf_node_daily` for the node)

- [ ] **Step 2: Run test → fail; Step 3: Implement; Step 4: Run → pass**
- [ ] **Step 5: Commit** `git add services/nodal_agents/writer.py tests/nodal_trading/test_agents.py && git commit -m "Register converged strategies into promote loop (scope nodal_agent)"`

---

### Task 7: Nodal Trading tab (mengxi-dashboard)

**Files:**
- Create: `apps/mengxi-dashboard/nodal_trading_tab.py`
- Modify: `apps/mengxi-dashboard/app.py` (tabs row + `with` block)

**Interfaces:**
- Consumes: everything from Tasks 1–6
- Produces: `nodal_trading_tab.render(get_engine)` with 4 sections:
  - **S1 Forecast & stack:** L2 nodal curves per cluster vs actuals (plotly), L1 grid forecast value + source tag (L1 table vs fallback)
  - **S2 Asset agents:** per-asset cards — nodal forecast curve, nominated strategy curve, substation cap usage, zone-restricted intervals shaded, promote status badge (champion/candidate)
  - **S3 Comparison:** trader realized vs recursive-optimal — uses `services/monitoring/run_daily_attribution.py` ladder (grid_restriction_loss/forecast_error_loss/strategy_error_loss) for trader strategies and the same ladder computed for recursive-optimal; side-by-side per asset per month
  - **S4 Registry & data:** node map table, asset registry editor (st.data_editor → upsert), backtest metrics (nodal MAE vs baseline per cluster)
- app.py wiring: insert `tab_nodal_trading` into the st.tabs list (after the Nodal Analysis tab) + `with tab_nodal_trading: import nodal_trading_tab; nodal_trading_tab.render(_get_sqlalchemy_engine)`

- [ ] **Step 1: Write the tab module (sections as above, loaders cached with _conn underscore params)**
- [ ] **Step 2: Wire into app.py; syntax check + local smoke with mocked loaders (RDS blocked)**
- [ ] **Step 3: Commit** `git add apps/mengxi-dashboard/nodal_trading_tab.py apps/mengxi-dashboard/app.py && git commit -m "Add Nodal Trading tab to mengxi-dashboard"`

---

### Task 8: Full verification + merge gate

**Files:** none new.

- [ ] **Step 1: Full suites green:** `pytest tests/nodal_trading/ tests/interconnector/ tests/mengxi_nodal/ -q`
- [ ] **Step 2: Backtest gate on real data (run-task on ECS):** nodal MAE ≤ grid-only baseline for every cluster with ≥30 days history; print table
- [ ] **Step 3: Dry-run writer for one target date (run-task):** `writer.run_day(conn, D)` produces nodal_strategy_daily rows for all active assets; spot-check 谷山梁 curve vs its nodal price history
- [ ] **Step 4: Merge `feat/nodal-trading` → main** (after user confirms; run full broad suite)
- [ ] **Step 5: Deploy** — separate confirmation; mengxi-dashboard image bump (v22 → v23)

---

## Self-Review Notes

- **Spec coverage:** §2 architecture → Tasks 1/4/5; §3 data tables → Task 1 DDL; §4 L2 → Task 4; §5 L3 + recursion → Tasks 5/6; §6 node map + registry → Tasks 2/3; §7 tab → Task 7; comparison → Tasks 6/7-S3; §9 testing → Task 8.
- **Type consistency:** `nodal_curve` is np.ndarray (96,) everywhere; asset dict keys (plant_name/capacity_mw/duration_h/rte_pct/node/substation/zone/settle_node) consistent across Tasks 2/5/6/7; strategy row carries model_version everywhere (global constraint).
- **Open points carried:** L1 writer lives in the parallel session's repo area (bess-map) — Task 1's fallback makes L2 independent until it lands; 谷山梁's actual substation cap comes from the Task 3 extraction (test values are illustrative).
