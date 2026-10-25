# Asset-Backed Trading — Deal Structurer App Surface (Plan 2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild the deal-structurer app surface around the option-value engine: new 期权价值 (Option Value) and 价值筛选 (Value Screen) tabs, Project Finance absorbing the finance-distribution layer, removal of the four superseded pipeline tabs, and 3 new Structurer agent tools. Spec build step 8 only — the scout layer (step 9) is plan 3; deployment is NOT in this plan (explicit confirmation required separately).

**Architecture:** New tabs read `marketdata.option_value_snapshot` (fast, written by `services/option_engine/batch.py` — built in plan 1, on main) and recompute on demand via `batch.run_one`. The 期权价值 tab becomes the pipeline head: its computed cell (value stack + per-path revenue + Greeks) lives in session state and feeds Project Finance (base revenue default) and Deal Pricing (revenue distribution). Removed tabs: `price_tab.py`, `dispatch_tab.py`, `mc_tab.py`, `screening_tab.py`.

**Tech Stack:** Streamlit 1.58, plotly, numpy/pandas, SQLAlchemy; the plan-1 engine (`libs/option_value/`, `services/option_engine/`).

**Spec:** `docs/superpowers/specs/2026-10-24-asset-backed-trading-design.md` (§6 consolidation map is the authority for this plan)

## Global Constraints

- **Branch:** `feat/abt-app-surface` from main. Commits pathspec-scoped (`git add <paths> && git commit -m "..." -- <paths>`), trailer `Co-Authored-By: Claude Code <noreply@anthropic.com>`. Push at plan end.
- **The plan-1 engine is consumed, not modified.** If a task believes the engine needs a change, STOP and report BLOCKED with the reason — the controller rules.
- **Honesty rules (spec §11.17, hard requirements on UI):** extrinsic always displayed with its P10/P90 band; scenario-sourced value labeled "情景假设/判断" not data; wind/solar rows carry a "筛选级" (screen-grade) badge, BESS rows "bid-grade" where a calibration row exists; the masking counts from `mc_params` visible in an expander on any value display.
- **Units:** ¥/kW/yr for value metrics; ¥/MWh prices; capex ¥/kWh. The session-state revenue distribution for Deal Pricing is absolute ¥/yr at a user-entered power_mw.
- **Reads only from production tables**; writes to `option_value_snapshot` ONLY via `batch.run_one` (the engine's own writer). The app never hand-writes rows.
- **The app must degrade gracefully without DB** (dual-environment rule): tabs show a warning banner, not a traceback, when PGURL/DB_DSN is absent or RDS is unreachable.
- Existing engine entry point for an engine: `services/common/db_utils.py` `get_engine()` (check exact name at implementation; plan-1 readers take an `eng` argument).
- Test runner: `~/.venvs/bess-platform/bin/python -m pytest tests/deal_structurer/ -v`. Streamlit rendering is verified by (a) module-level import smoke tests, (b) pure-helper unit tests, (c) a manual local run step — not by mocking `st`.

## Review Focus

1. **Deleted-tab survivors' broken inputs**: deal_tab read `mc_result`, cashflow_tab read dispatch state — both from deleted tabs. Any leftover `st.session_state` reference to removed keys renders a blank/broken tab silently. Pinned by Task 5's grep step + import smoke.
2. **Snapshot-absent first run**: a fresh DB has no rows; tabs must render the empty state with guidance ("run the batch"), never raise. Pinned in Task 1/3 helper tests (None row → empty-state dict).
3. **capture_adj is None for wind/solar** — margin/IRR columns must not propagate NaN into sorting crashes; sort with NaN last. Pinned in Task 3 tests.
4. **The scenario editor's probabilities** can sum > 1 on the same effective date — `value_permit` raises ValueError; the tab must catch and show the message in Chinese, not crash. Pinned in Task 2 tests.
5. **On-demand recompute takes 10–50 s** — a double-click or a rerun-loop must not fire two batch runs (button disabled while running / `st.spinner` + session guard). Pinned in Task 1 implementation note (no test — Streamlit runtime behavior; reviewer verifies the guard exists in code).

---

### Task 1: 期权价值 tab — value display + on-demand recompute

**Files:**
- Create: `apps/deal_structurer/option_tab.py`
- Test: `tests/deal_structurer/test_option_tab.py` (pure helpers only)

**Interfaces:**
- Consumes: `services/option_engine/snapshot.read_latest(eng, province, asset_type, duration_h) -> dict | None`; `batch.run_one(eng, province, asset_type, duration_h, n_paths, seed=42) -> dict`; `services/option_engine/readers` (`read_hist_pf_per_kw_yr`, `read_capture_rate`, `list_provinces`); `libs/option_value/contracts.ValueStack` fields appear inside the snapshot row's numeric columns.
- Produces (session contract consumed by Tasks 4–5 and the agent tools):
  - `st.session_state["ov_result"]: dict | None` with keys `{province, asset_type, duration_h, asof_date, intrinsic_yuan_kw, extrinsic_yuan_kw, policy_net_yuan_kw, total_yuan_kw, capture_adj_yuan_kw, extrinsic_p10, extrinsic_p90, greeks: dict, policy_json: dict, mc_params: dict, revenue_paths_yuan: np.ndarray | None, power_mw: float}` — `revenue_paths_yuan` is None when the row came from the snapshot (paths aren't persisted); populated after an on-demand recompute (from `batch.run_one`'s per-path revenues scaled × power_mw × 1000 — see note in step 3).

- [ ] **Step 1: Write failing tests for the pure helpers**

`apps/deal_structurer/option_tab.py` keeps ALL data assembly in pure functions (no `st` calls) so they're testable:

```python
# tests/deal_structurer/test_option_tab.py
import numpy as np
from apps.deal_structurer.option_tab import waterfall_data, calibration_line, cell_label

ROW = dict(province="山东", asset_type="bess", duration_h=2.0, asof_date="2026-10-25",
           intrinsic_yuan_kw=168.34, extrinsic_yuan_kw=83.81, policy_net_yuan_kw=62.59,
           total_yuan_kw=314.75, capture_adj_yuan_kw=192.34,
           extrinsic_p10=10.0, extrinsic_p90=160.0, greeks={"delta": -2.6, "vega": 13.6, "theta": -0.02},
           policy_json={"capacity_price": 0.0, "discharge_comp": 150.2, "sysopfee": -87.6},
           mc_params={"n_paths": 1000, "seed": 42, "fallback": {"negatives_kept": 756}})

def test_waterfall_data_components_order_and_sum():
    wf = waterfall_data(ROW)
    assert [w[0] for w in wf] == ["intrinsic", "extrinsic", "policy", "total", "capture_adj"]
    assert wf[3][1] == 168.34 + 83.81 + 62.59          # total = sum of components
    assert wf[4][1] == 192.34                          # capture_adj displayed absolute, not cumulative
    assert wf[1][2] == (10.0, 160.0)                   # extrinsic carries its (p10, p90) band tuple

def test_waterfall_none_capture_adj_wind():
    row = dict(ROW, asset_type="wind", capture_adj_yuan_kw=None)
    wf = waterfall_data(row)
    assert wf[4][1] is None                            # no NaN fabrication

def test_calibration_line():
    cal = calibration_line(ROW, v_hist_yuan_kw=235.95)
    assert abs(cal["ratio_energy"] - (168.34+83.81)/235.95) < 1e-3
    assert cal["in_band"] is True                      # [0.7, 1.4]

def test_cell_label_grade_badges():
    assert "bid-grade" in cell_label(ROW, has_calibration=True)
    assert "筛选级" in cell_label(dict(ROW, asset_type="wind"), has_calibration=False)
```

- [ ] **Step 2: Run tests, verify FAIL** (module missing).

- [ ] **Step 3: Implement `option_tab.py`**

Structure: pure helpers `waterfall_data(row: dict) -> list[tuple[str, float | None, tuple | None]]`, `calibration_line(row, v_hist) -> dict`, `cell_label(row, has_calibration) -> str`, then `render()` (the only function that imports streamlit).

`render()` layout: (a) selectors — province (from `readers.list_provinces`, fallback `["山东","蒙西","广东","山西","江苏"]` on DB failure), asset type radio (BESS 2h / BESS 4h / wind / solar); (b) latest snapshot read → value waterfall (plotly `go.Figure.Waterfall` from `waterfall_data`; extrinsic bar carries error-bar band from the p10/p90 tuple), Greeks metric row, policy components table from `policy_json`, calibration line vs `read_hist_pf_per_kw_yr` (BESS only), and **for wind/solar rows the empirical price–volume correlation from `readers.read_price_volume_corr(eng, province, kind)`** (the spec's mapping of the old dispatch_tab correlation table); `mc_params` expander showing seed/paths/masking counts (data-transparency rule); (c) on-demand recompute: paths selector (200/500/1000), power_mw input (default 100), button with a `st.session_state["_ov_running"]` re-entrancy guard + `st.spinner`; result refreshes `ov_result` including `revenue_paths_yuan` — NOTE for the implementer: `batch.run_one` returns the snapshot row dict; if it does not expose per-path revenues, call the engine pieces directly in the tab (`readers` → `build_expected_curve` → `fit_pca_v2` → `simulate_pca_v2` → `ASSET_MODELS[...].value_paths`) under the same seed, and use `compute_value_stack` for the display row — do NOT modify the engine; (d) empty state when no snapshot row: `st.info` guidance to run the batch, no exception; (e) DB-absent: warning banner, everything else skipped.

- [ ] **Step 4: Run tests, verify PASS.**

- [ ] **Step 5: Commit** — `git add apps/deal_structurer/option_tab.py tests/deal_structurer/ && git commit -m "Add option-value tab: waterfall, Greeks, calibration, recompute" -- apps/deal_structurer/option_tab.py tests/deal_structurer/`

---

### Task 2: Acquisition panel (permit + M&A modes)

**Files:**
- Modify: `apps/deal_structurer/option_tab.py` (append panel section + pure helpers)
- Test: `tests/deal_structurer/test_acquisition_panel.py`

**Interfaces:**
- Consumes: Task 1's `ov_result`; `libs/option_value/build_option.value_permit(scenarios, base_levers, energy_value_yuan_kw, asset_type, rte, inputs, volume_mwh_per_kw, asof=None) -> PermitResult` and `value_acquisition(ask_yuan, energy_value_yuan_kw, levers, power_mw, duration_h, asset_type, rte, uplift_rows, actuals_anchor_yuan_kw, hurdle, life_years) -> AcquisitionResult` (read the exact signatures from `libs/option_value/build_option.py`); `libs/option_value/contracts.PolicyScenario, PermitInputs`; `services/option_engine/readers.read_policy_levers(eng, province, asset_type, duration_h) -> PolicyLevers`.
- Produces: `scenario_rows_to_scenarios(rows: list[dict]) -> list[PolicyScenario]`; `st.session_state["acq_result"]: dict | None` = `{mode: "permit"|"mna", result: PermitResult | AcquisitionResult, inputs_echo: dict}`.

- [ ] **Step 1: Write failing tests**

```python
# tests/deal_structurer/test_acquisition_panel.py
from datetime import date
from apps.deal_structurer.option_tab import scenario_rows_to_scenarios, permit_verdict_text
import pytest

def test_scenario_rows_conversion():
    rows = [{"name": "容量电价x1.5", "lever": "capacity_price_yuan_kw_yr", "mode": "multiplier",
             "value": 1.5, "effective_date": date(2027, 7, 1), "probability": 0.6}]
    sc = scenario_rows_to_scenarios(rows, base_levers_value=100.0)
    assert sc[0].lever_overrides == {"capacity_price_yuan_kw_yr": 150.0}
    assert sc[0].probability == 0.6

def test_same_date_probability_sum_guard():
    rows = [
        {"name": "a", "lever": "capacity_price_yuan_kw_yr", "mode": "absolute", "value": 100.0, "effective_date": date(2027,1,1), "probability": 0.7},
        {"name": "b", "lever": "fr_yuan_kw_yr", "mode": "absolute", "value": 50.0, "effective_date": date(2027,1,1), "probability": 0.7},
    ]
    with pytest.raises(ValueError):
        scenario_rows_to_scenarios(rows, base_levers_value=100.0)

def test_permit_verdict_text_shows_decomposition():
    class R:  # minimal PermitResult shape
        v_permit_yuan = 5_000_000.0; breakeven_permit_yuan = 8_000_000.0; go = True
        decomposition = {"容量电价x1.5": 3_000_000.0}
    t = permit_verdict_text(R())
    assert "GO" in t and "容量电价x1.5" in t and "60%" in t   # which policy bet carries the value (3M/5M)
```

- [ ] **Step 2: Run tests, verify FAIL.**

- [ ] **Step 3: Implement the panel** (in `option_tab.py`, below the value section)

Mode toggle `许可指标 (permit)` / `并购 (M&A)`. Permit mode inputs: permit cost (¥), validity years (default 2.0), capex ¥/kWh (default 600) + annual decline % (default 5%), power/duration from the cell, and a scenario editor (up to 4 rows: name, lever dropdown filtered to the asset's `applicable_levers`, multiplier|absolute, value, effective date, probability; presets `current_policy` (empty), `capacity_x1.5`, `fr_pool_halved` fill the rows). Base levers from `read_policy_levers`; `energy_value_yuan_kw` = the cell's `capture_adj` energy part (`capture_adj − policy_net`, falling back to `intrinsic + extrinsic` when capture_adj is None); volume = the snapshot's MC-mean volume if present in `mc_params`, else recompute note. Every value sourced from a scenario gets the "情景假设" label. `value_permit` ValueError → `st.error` with the Chinese message, no crash. M&A mode inputs: ask price ¥, turnaround uplift rows (name + ¥/kW/yr delta), actuals anchor input (¥/kW/yr, optional) → `value_acquisition` → margin, GO/NO-GO, per-uplift decomposition. Both write `acq_result`.

- [ ] **Step 4: Run tests, verify PASS.**

- [ ] **Step 5: Commit** — `git add apps/deal_structurer/option_tab.py tests/deal_structurer/test_acquisition_panel.py && git commit -m "Add permit/M&A acquisition panel to option-value tab" -- apps/deal_structurer/option_tab.py tests/deal_structurer/test_acquisition_panel.py`

---

### Task 3: 价值筛选 (Value Screen) tab

**Files:**
- Create: `apps/deal_structurer/screen_tab.py`
- Test: `tests/deal_structurer/test_screen_tab.py`

**Interfaces:**
- Consumes: snapshot table via a new pure query function (implement in `screen_tab.py` using the passed engine): `SELECT DISTINCT ON (province, asset_type, duration_h) * FROM marketdata.option_value_snapshot ORDER BY province, asset_type, duration_h, asof_date DESC`; `libs/option_value/value_stack.annuitize`; portfolio-fit engine `libs/deal_models/workflows/portfolio_contribution.py` (read its public functions before use).
- Produces:
  - `screen_rows_to_frame(rows: list[dict], capex_yuan_kwh_bess: float, capex_yuan_kw_renewable: float, hurdle: float, life_years: int) -> pd.DataFrame` — **unit-correct margins (pinned)**: BESS per-kWh-energy margin `= annuitize(v) / duration_h − capex_yuan_kwh_bess` (1 kW = duration_h kWh; no ×1000 anywhere — the ¥/kW→¥/kWh conversion is ÷duration_h); wind/solar (duration_h=0) per-kW margin `= annuitize(v) − capex_yuan_kw_renewable` where `v = capture_adj if not None else total`. `irr_uplift = implied_irr(v) − implied_irr(intrinsic + policy_net)` (capex basis matched per class). **Sort by `implied_irr(v)` descending** (unit-free, comparable across classes); NaN IRR last.
  - `implied_irr(annual_yuan_kw: float, capex_yuan_kw: float, life_years: int = 15) -> float | None` — bisection on `annuitize(annual, r, life) − capex = 0`, r ∈ (−0.5, 1.0), tol 1e-4; **None when `annual × life_years < capex` (no undiscounted payback → no meaningful IRR — ratified ruling G1, 2026-10-26) or no sign change in the bracket**; None when unsolvable.

- [ ] **Step 1: Write failing tests**

```python
# tests/deal_structurer/test_screen_tab.py
import math
from apps.deal_structurer.screen_tab import implied_irr, screen_rows_to_frame

def test_implied_irr_annuity_inversion():
    # annual 100 ¥/kW, capex 855.95 ¥/kW (= 100 × annuity factor at 8%/15y) → IRR ≈ 8%
    r = implied_irr(100.0, 855.95)
    assert abs(r - 0.08) < 0.005
    assert implied_irr(10.0, 100000.0) is None          # unsolvable → None, not crash

def test_screen_frame_margin_and_nan_safe_sort():
    rows = [
        dict(province="蒙西", asset_type="bess", duration_h=4.0, asof_date="2026-10-25",
             intrinsic_yuan_kw=300.0, extrinsic_yuan_kw=200.0, policy_net_yuan_kw=300.0,
             total_yuan_kw=800.0, capture_adj_yuan_kw=560.0, extrinsic_p10=100.0, extrinsic_p90=300.0,
             policy_json={}, mc_params={}),
        dict(province="江苏", asset_type="wind", duration_h=0.0, asof_date="2026-09-01",
             intrinsic_yuan_kw=1200.0, extrinsic_yuan_kw=-50.0, policy_net_yuan_kw=100.0,
             total_yuan_kw=1250.0, capture_adj_yuan_kw=None, extrinsic_p10=-120.0, extrinsic_p90=10.0,
             policy_json={}, mc_params={}),
    ]
    df = screen_rows_to_frame(rows, capex_yuan_kwh_bess=600.0, capex_yuan_kw_renewable=5500.0,
                              hurdle=0.08, life_years=15)
    # 蒙西 BESS 4h: capex 2400 ¥/kW, capture-adj 560 → IRR ≈ 22.2%; 江苏 wind: 1250 vs 5500 → ≈ 21.5%
    assert df.iloc[0]["province"] == "蒙西"                     # sorted by implied IRR desc, unit-free
    mx = df[df.province=="蒙西"].iloc[0]
    assert abs(mx["npv_margin_yuan_kwh"] - (560.0*8.5595/4.0 - 600.0)) < 1.0   # ÷duration, no ×1000
    js = df[df.province=="江苏"].iloc[0]
    assert abs(js["npv_margin_yuan_kw"] - (1250.0*8.5595 - 5500.0)) < 1.0      # renewable per-kW basis
    assert "stale" in js["freshness"]                            # old asof flagged
    assert js["extrinsic_yuan_kw"] == -50.0                      # negative extrinsic intact
```

- [ ] **Step 2: Run tests, verify FAIL.**

- [ ] **Step 3: Implement `screen_tab.py`**

`render()`: margin-ranked table (extrinsic shown red when negative; badges; freshness), margin bar chart, capex/hurdle/life inputs in a sidebar form, portfolio-fit section (select a row → run `portfolio_contribution` vs the fleet, show correlation + diversification benefit; DB-absent → section hidden with a note). Empty table → guidance to run the batch. All assembly in the pinned pure functions.

- [ ] **Step 4: Run tests, verify PASS.**

- [ ] **Step 5: Commit** — `git add apps/deal_structurer/screen_tab.py tests/deal_structurer/test_screen_tab.py && git commit -m "Add value screen tab with margin ranking and portfolio fit" -- apps/deal_structurer/screen_tab.py tests/deal_structurer/test_screen_tab.py`

---

### Task 4: Project Finance rework (absorb finance-distribution layer)

**Files:**
- Modify: `apps/deal_structurer/cashflow_tab.py`
- Test: `tests/deal_structurer/test_cashflow_tab.py`

**Interfaces:**
- Consumes: `ov_result` from Task 1 (base revenue default = capture_adj × power × 1000 when set); `libs/deal_models/project_cashflow.compute_cashflow` (unchanged); revenue paths for the distribution section from `ov_result["revenue_paths_yuan"]`.
- Produces: `cashflow_tab.render()` with two sections: (1) the existing deterministic cashflow form (unchanged behavior, new default source); (2) NEW "分布 / Distribution" section — per-path `compute_cashflow` over `revenue_paths_yuan` → equity IRR P10/P50/P90 histogram, P(IRR < hurdle), revenue VaR/CVaR 5%, and a ±10% one-at-a-time NPV tornado on {capex, interest, O&M, revenue scale} computed on the P50 cashflow (pure recomputes, no MC re-runs — replaces the old mc_tab tornado).
- Pure helper for tests: `distribution_metrics(irr_paths: np.ndarray, revenue_paths: np.ndarray, hurdle: float) -> dict` → `{irr_p10, irr_p50, irr_p90, prob_below_hurdle, rev_var5, rev_cvar5}`.

- [ ] **Step 1: Write failing test**

```python
# tests/deal_structurer/test_cashflow_tab.py
import numpy as np
from apps.deal_structurer.cashflow_tab import distribution_metrics

def test_distribution_metrics():
    irr = np.array([0.04, 0.08, 0.12, 0.16, 0.20])
    rev = np.array([80.0, 90.0, 100.0, 110.0, 120.0])
    m = distribution_metrics(irr, rev, hurdle=0.10)
    assert m["irr_p50"] == 0.12
    assert m["prob_below_hurdle"] == 2/5
    assert m["rev_var5"] <= m["rev_cvar5"] <= rev.mean()
```

- [ ] **Step 2: Run, verify FAIL.**

- [ ] **Step 3: Implement** — keep the existing form code; add the section; default base revenue from `ov_result` when present (session key guard: works when None). If `revenue_paths_yuan` is None (snapshot-sourced cell), the Distribution section shows "run an on-demand recompute on the Option Value tab first" guidance.

- [ ] **Step 4: Run, verify PASS.**

- [ ] **Step 5: Commit** — `git add apps/deal_structurer/cashflow_tab.py tests/deal_structurer/test_cashflow_tab.py && git commit -m "Rework project finance tab with distribution section" -- apps/deal_structurer/cashflow_tab.py tests/deal_structurer/test_cashflow_tab.py`

---

### Task 5: Consolidation — remove 4 tabs, rewire Deal Pricing, rebuild session cache

**Files:**
- Modify: `apps/deal_structurer/app.py` (nav list + routing)
- Modify: `apps/deal_structurer/session_cache.py` (key list)
- Modify: `apps/deal_structurer/deal_tab.py` (input rewiring)
- Delete: `apps/deal_structurer/price_tab.py`, `dispatch_tab.py`, `mc_tab.py`, `screening_tab.py`
- Test: `tests/deal_structurer/test_shell.py`

**Interfaces:**
- Consumes: Tasks 1–4.
- Produces: nav list `["0 · Deal Intake", "期权价值", "项目财务", "Deal Pricing", "价值筛选", "投委会", "💬 Structurer"]` (spec §6 order); session `_KEYS` = `["ov_result", "acq_result", "last_financials", "last_cf_result", "dp_result", "agent_messages", "agent_display"]`; deal_tab reads the revenue distribution from `ov_result["revenue_paths_yuan"]` (scaled by its power_mw input) instead of `mc_result`.

- [ ] **Step 1: Write failing tests**

```python
# tests/deal_structurer/test_shell.py
import importlib, pathlib

def test_removed_tabs_gone():
    base = pathlib.Path("apps/deal_structurer")
    for gone in ("price_tab.py", "dispatch_tab.py", "mc_tab.py", "screening_tab.py"):
        assert not (base / gone).exists()

def test_all_nav_targets_import():
    for mod in ("intake_tab", "option_tab", "cashflow_tab", "deal_tab", "screen_tab", "committee_tab", "strategist", "session_cache"):
        importlib.import_module(f"apps.deal_structurer.{mod}")

def test_session_cache_keys_match_nav():
    from apps.deal_structurer import session_cache
    assert "mc_result" not in session_cache._KEYS and "price_paths" not in session_cache._KEYS
    assert "ov_result" in session_cache._KEYS

def test_no_dead_session_refs():
    import re
    src = pathlib.Path("apps/deal_structurer/deal_tab.py").read_text()
    assert "mc_result" not in src                          # rewired to ov_result
```

- [ ] **Step 2: Run, verify FAIL** (files still exist).

- [ ] **Step 3: Implement** — rewrite nav/routing in `app.py` (keep the sidebar portal link and clear-session button; update the flow caption to "Intake → 期权价值 → 项目财务 → Deal Pricing → 投委会"); rewire `deal_tab.py` (read its current code first — replace the `mc_result` dependency with `ov_result`, adding a power_mw input and an empty-state guidance when `revenue_paths_yuan` is None); rebuild `session_cache._KEYS`; delete the four tab files with `git rm`. **Grep gate:** `grep -rn "mc_result\|price_paths\|dispatch_result\|price_sim_req\|last_dispatch_req" apps/deal_structurer/ --include="*.py"` must return hits only in `session_cache.py` history comments (none allowed) — fix every survivor reference.

- [ ] **Step 4: Run, verify PASS** (incl. Task 1–4 suites still green).

- [ ] **Step 5: Commit** — `git rm apps/deal_structurer/price_tab.py apps/deal_structurer/dispatch_tab.py apps/deal_structurer/mc_tab.py apps/deal_structurer/screening_tab.py && git add apps/deal_structurer/ tests/deal_structurer/test_shell.py && git commit -m "Consolidate deal-structurer tabs onto option-value engine" -- apps/deal_structurer/ tests/deal_structurer/test_shell.py`

---

### Task 6: Agent tools (+3) + Dockerfile + portal copy

**Files:**
- Modify: `libs/deal_models/adapters/agent_tools.py` (append schemas + dispatch routes + handlers)
- Modify: `apps/deal_structurer/Dockerfile` (COPY libs/option_value + services/option_engine)
- Modify: `apps/portal/app.py` (Deal Structurer card copy, ~lines 740-762 — text only)
- Test: `tests/deal_models/test_agent_option_tools.py`

**Interfaces:**
- Consumes: `services/option_engine/{readers, snapshot, batch}`; engine via `services/common/db_utils.get_engine()` (verify the exact helper name at implementation; lazily created inside handlers, never at import).
- Produces (schemas on `AGENT_TOOLS`, routes in `dispatch_tool`):
  - `get_option_value(province, asset_type, duration_h)` → latest snapshot row as JSON (intrinsic/extrinsic/policy/total/capture_adj, greeks, asof, calibration note), or `{"error": "no snapshot — run batch"}`.
  - `screen_option_values(sort_by: "margin"|"extrinsic_share"|"total", min_freshness_days: int = 14)` → top-15 rows of the latest-per-cell screen (same SQL as screen_tab) with grade badges.
  - `get_policy_levers(province, asset_type, duration_h = 2.0)` → `readers.read_policy_levers` output + notes.
- Grounding: handlers return only DB/engine numbers; the tool descriptions state values are ¥/kW/yr as of the snapshot's asof_date.

- [ ] **Step 1: Write failing tests** — dispatch routing + handler behavior with a stubbed engine (monkeypatch `snapshot.read_latest` / `readers.read_policy_levers`):

```python
# tests/deal_models/test_agent_option_tools.py
from libs.deal_models.adapters import agent_tools

def test_dispatch_get_option_value(monkeypatch):
    monkeypatch.setattr(agent_tools._snapshot, "read_latest",
                        lambda eng, p, a, d: {"province": p, "total_yuan_kw": 314.75, "asof_date": "2026-10-25"})
    out = agent_tools.dispatch_tool("get_option_value", {"province": "山东", "asset_type": "bess", "duration_h": 2.0})
    assert "314.75" in out and "山东" in out

def test_dispatch_get_option_value_no_row(monkeypatch):
    monkeypatch.setattr(agent_tools._snapshot, "read_latest", lambda *a: None)
    out = agent_tools.dispatch_tool("get_option_value", {"province": "福建", "asset_type": "bess", "duration_h": 2.0})
    assert "no snapshot" in out

def test_schemas_registered():
    names = {t["name"] for t in agent_tools.AGENT_TOOLS}
    assert {"get_option_value", "screen_option_values", "get_policy_levers"} <= names
```

- [ ] **Step 2: Run, verify FAIL.**

- [ ] **Step 3: Implement** — schemas + handlers in `agent_tools.py` (import `services.option_engine.snapshot as _snapshot` etc. at module top; engine via lazy `get_engine()` inside handlers; DB errors → JSON error string, never raise through `dispatch_tool`); Dockerfile: add `COPY libs/option_value /app/libs/option_value` and `COPY services/option_engine /app/services/option_engine` adjacent to the existing COPY lines (deployment is NOT part of this plan — the Dockerfile change is staged for the next confirmed deploy); portal card copy: update the capabilities sentence to mention the option-value engine (text edit only).

- [ ] **Step 4: Run, verify PASS.**

- [ ] **Step 5: Commit** — `git add libs/deal_models/adapters/agent_tools.py apps/deal_structurer/Dockerfile apps/portal/app.py tests/deal_models/test_agent_option_tools.py && git commit -m "Add option-value agent tools, Dockerfile copies, portal copy" -- libs/deal_models/adapters/agent_tools.py apps/deal_structurer/Dockerfile apps/portal/app.py tests/deal_models/test_agent_option_tools.py`

---

### Task 7: Local run verification

**Files:** none new (verification task).

- [ ] **Step 1: Import smoke** — `~/.venvs/bess-platform/bin/python -c "import apps.deal_structurer.app"` passes (catches dead imports left by consolidation; note app.py's `sys.path.insert(0, "/app")` is a container path — harmless locally when run from repo root).
- [ ] **Step 2: App boots** — `set -a; source config/.env; set +a; ~/.venvs/bess-platform/bin/python -m streamlit run apps/deal_structurer/app.py --server.port 8522 --server.headless true` in background; `curl -sf http://localhost:8522/deal-structurer/healthz` or the root returns 200 within 30 s; then kill. (baseUrlPath `deal-structurer` is set in the app's config — check `.streamlit`/Dockerfile env; curl the root if the health path 404s.)
- [ ] **Step 3: DB-backed render probe** — with PGURL loaded, run a headless script that calls `snapshot.read_latest(eng, "山东", "bess", 2.0)` and `screen_tab`'s snapshot query — both return rows (the plan-1 batch wrote them).
- [ ] **Step 4: Full suite** — `~/.venvs/bess-platform/bin/python -m pytest tests/deal_structurer/ tests/deal_models/ libs/option_value/ tests/option_engine/ -q` green.
- [ ] **Step 5: Commit** any verification fixes (pathspec-scoped), then push: `git fetch origin && git rebase origin/main && git push origin feat/abt-app-surface` (if the shared tree blocks rebase, use the temp-index plumbing pattern from MEMORY).

---

## Self-Review Notes (filled at plan write time)

- **Spec coverage:** §6 consolidation map → Tasks 1–5; agent tools → Task 6 (3 of the 4 — `list_opportunity_cards` deferred to plan 3 with the cards table; controller ruling, spec §6 remains the end state); verification → Task 7. Portfolio-fit preserved (Task 3 section) per the map; price-wind correlation table NOT separately ported — its content lives in the engine's calibration path (cf profiles + corr readers); noted as a deliberate drop, final review can object.
- **Type consistency:** `ov_result`/`acq_result` session contracts are the plan-1 dataclasses' dict forms; `PermitResult`/`AcquisitionResult` consumed as objects from build_option (field names from the plan-1 contracts).
- **Proportion:** 7 tasks; tests pin the money math (margins, IRR inversion, waterfall sums, scenario guards); rendering verified by smoke + local run, not st-mocking.
