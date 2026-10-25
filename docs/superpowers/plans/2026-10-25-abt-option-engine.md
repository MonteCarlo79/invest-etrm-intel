# Asset-Backed Trading — Option-Value Engine (Core) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the option-value engine core (intrinsic/extrinsic/policy stack per province × asset type), its DB service layer, and the calibration report — spec build steps 1–7. Tabs/consolidation (step 8) and the scout (step 9) are separate follow-on plans, written after the calibration numbers are reviewed.

**Architecture:** New pure-computation `libs/option_value/` (zero I/O: no DB, no files, no streamlit) + `services/option_engine/` (DB readers, snapshot persistence, batch CLI, calibration). LP dispatch (`services/bess_map/optimisation_engine.py`) is the validation oracle for a new vectorized DP engine. PCA simulator v2 extends the `libs/deal_models/price_simulator.py` pattern (seasonal curve removed, AR(1) scores, Student-t innovations). Policy levers reuse `services/bess_map/capacity_stack.py` loaders/converters.

**Tech Stack:** Python 3.11, numpy, pandas, PuLP/CBC (oracle only), pydantic-free plain dataclasses, pytest, SQLAlchemy engine for readers.

**Spec:** `docs/superpowers/specs/2026-10-24-asset-backed-trading-design.md` (read it first; section references below point there)

## Global Constraints

- **Units (hard rules, repo has been bitten):** prices ¥/MWh internally everywhere; value metrics ¥/kW/yr (per kW of rated power); readers normalize on ingest (`_normalize_units`: per price column, if `nanmedian < 5` → ×1000). Conversion `¥/kW/yr = profit_per_mwh_installed_day × 365 × duration_h`.
- **No SVD, no large BLAS matmuls** in new code — Fargate OpenBLAS segfault (`docs/handoff-2026-07-25-deal-structurer.md`). Use `np.linalg.eigh` on the 24×24 covariance and per-PC `np.outer` accumulation, exactly as `libs/deal_models/price_simulator.py:55-96`.
- **Determinism:** every simulation takes an explicit `seed` (default 42); same seed → identical output. Greeks use common random numbers (same seed on bumped re-values).
- **Negative extrinsic is reported, never floored** (wind/solar cannibalization; spec §11.10).
- **BESS numbers are bid-grade; wind/solar are screen-grade in v1** (spec §11.17).
- `libs/option_value/` contains **no I/O** — no sqlalchemy, no file reads, no streamlit. Services layer does all I/O.
- **Commits on this shared tree:** `git add <explicit paths> && git commit -m "..." -- <explicit paths>` (pathspec-scoped; never a bare `git commit` — ERRORS.md 2026-09-20). Push at end of session, not per task.
- **Test runner:** `~/.venvs/bess-platform/bin/python -m pytest <path> -v` (repo venv; if missing, `python -m pytest` with repo deps installed).
- Existing files are **not modified** in this plan except where a task says so explicitly (none modify `libs/deal_models/` or `services/bess_map/`).

## Review Focus

1. **Unit heuristic boundary:** a province whose price median sits near ¥5/MWh could mis-normalize (×1000 or not) — silently 1000× wrong. Pinned in Task 7 (`test_normalize_units_boundary`): RT and DA columns normalized *independently*, and the function returns per-column counts of scaled values so batch logs them.
2. **Thin-history provinces:** window with < 11.5 months of complete days must produce `insufficient_history=True` and the batch must skip writing a screen row without crashing (spec §5.1; 福建 LingFeng feed currently broken past 2026-09-15). Pinned in Task 1 and Task 8.
3. **OpenBLAS segfault cannot be caught by a Mac test** — it fires on Fargate. Reviewer step in Task 2/3: grep the new lib for `np.linalg.svd`, `np.linalg.svdvals`, and `@` on (days×24)@(24×24)-scale operands is fine but no (T×8760)-scale matmuls; only `eigh` + `np.outer` patterns allowed.
4. **Degenerate PC innovation fit:** a near-zero-variance PC gives a garbage Student-t df. Must fall back to Normal. Pinned in Task 2 (`test_degenerate_pc_falls_back_to_normal`).
5. **Missing capture rate or missing policy rows for a province** → `capture_adj=None` / zero levers with a status note in the snapshot params, never an exception. Pinned in Task 5 (`test_missing_capture_gives_none`) and Task 7.

---

### Task 1: contracts + expected-curve construction

**Files:**
- Create: `libs/option_value/__init__.py` (empty)
- Create: `libs/option_value/contracts.py`
- Create: `libs/option_value/curves.py`
- Test: `libs/option_value/tests/__init__.py` (empty), `libs/option_value/tests/test_curves.py`

**Interfaces:**
- Consumes: nothing (first task).
- Produces (all later tasks rely on these exact names):
  - `@dataclass ExpectedCurve`: `province: str, window_start: date, window_end: date, month_hour_mean: np.ndarray  # (12, 24) ¥/MWh — ROW INDEX = month % 12 (row 0 = December, row 2 = February); every consumer must use month % 12`, `hourly: np.ndarray  # (n_complete_days*24,) ¥/MWh tiled curve`, `floor: float, cap: float, n_days: int, coverage_months: float, insufficient_history: bool`
  - Window semantics (pinned by T1 tests): `build_expected_curve` applies a LOWER time bound only (window_start = first day of month (window_end − months)); days after window_end are not excluded by this function — upstream readers bound the query at window_end. `build_expected_curve` does not take a province argument; callers set `curve.province` downstream (e.g. via `dataclasses.replace`).
  - `@dataclass PCASimConfig`: `n_components: int = 4, n_paths: int = 500, seed: int = 42, innovations: str = "student_t"`
  - `@dataclass PCAv2Params`: `loadings: np.ndarray  # (k, 24)`, `phi: np.ndarray  # (k,) AR(1) coef`, `inno_scale: np.ndarray  # (k,)`, `inno_df: np.ndarray  # (k,) — NaN means Normal`
  - `@dataclass BESSParams`: `power_mw: float = 1.0, duration_h: float = 2.0, rte: float = 0.85, soc_grid_step_frac: float = 0.1`
  - `@dataclass RenewableParams`: `capacity_mw: float = 1.0, cf_profile: np.ndarray = None  # (12, 24)`, `price_volume_corr: float = 0.0, cf_sigma: float = 0.15, curtailment_floor: bool = True, asset_type: str = "wind"`
  - `@dataclass DispatchResult`: `revenue_per_path: np.ndarray  # (n_paths,) ¥ for power_mw unit`, `discharge_mwh_per_path: np.ndarray  # (n_paths,) grid-delivered`
  - `@dataclass PolicyLevers`: `capacity_price_yuan_kw_yr: float = 0.0, cap_comp_yuan_kw_yr: float = 0.0, fr_yuan_kw_yr: float = 0.0, discharge_comp_yuan_kwh: float = 0.0, sysopfee_yuan_kwh: float = 0.0, mechanism_premium_yuan_mwh: float = 0.0, green_cert_yuan_mwh: float = 0.0, notes: str = ""`
  - `@dataclass ValueStack`: `intrinsic_yuan_kw: float, extrinsic_yuan_kw: float, policy_net_yuan_kw: float, total_yuan_kw: float, capture_adj_yuan_kw: float | None, extrinsic_p10: float, extrinsic_p90: float`
  - `@dataclass PolicyScenario`: `name: str, lever_overrides: dict, effective_date: date, probability: float`
  - `@dataclass PermitInputs`: `permit_cost_yuan: float, validity_years: float, capex_yuan_kwh_today: float, capex_decline_rate: float, power_mw: float, duration_h: float, hurdle: float = 0.08, life_years: int = 15`
  - `@dataclass PermitResult`: `v_permit_yuan: float, breakeven_permit_yuan: float, go: bool, decomposition: dict, dp_check_value: float`
  - `@dataclass AcquisitionResult`: `fair_npv_yuan: float, margin_yuan: float, go: bool, rows: list`
  - `curves.build_expected_curve(prices: pd.Series, window_end: date, months: int = 12) -> ExpectedCurve` — `prices` is an hourly Series (naive datetime index, ¥/MWh, already normalized/fallback-filled by readers); keeps only complete days (24 non-NaN hours); `month_hour_mean[m, h]` = mean over window; `hourly` = each complete day's 24 hours replaced by its month-hour row, in chronological order; floor/cap = observed min/max of the complete-day series; `coverage_months = n_days / 30.44`; `insufficient_history = coverage_months < 11.5`.

- [ ] **Step 1: Write the failing tests**

```python
# test_curves.py
import numpy as np, pandas as pd
from datetime import date
from libs.option_value.curves import build_expected_curve

def _series(n_days, fn):
    idx = pd.date_range("2025-11-01", periods=n_days*24, freq="h")
    return pd.Series([fn(ts) for ts in idx], index=idx)

def test_month_hour_means_exact():
    # price = month_number*1000 + hour → mean curve is exact by construction
    s = _series(400, lambda ts: ts.month*1000.0 + ts.hour)
    c = build_expected_curve(s, date(2026, 11, 30))
    assert c.month_hour_mean.shape == (12, 24)
    assert c.month_hour_mean[2, 7] == 2007.0
    assert not c.insufficient_history
    assert c.hourly.shape == (c.n_days*24,)

def test_incomplete_day_dropped():
    s = _series(400, lambda ts: 100.0 + ts.hour)
    s = s.drop(s.index[37])            # remove one hour of day 2
    c = build_expected_curve(s, date(2026, 11, 30))
    assert c.n_days == 399

def test_insufficient_history_flag():
    s = _series(300, lambda ts: 100.0)  # 300/30.44 ≈ 9.9 months
    c = build_expected_curve(s, date(2026, 8, 26))
    assert c.insufficient_history
```

- [ ] **Step 2: Run tests, verify they fail** — `~/.venvs/bess-platform/bin/python -m pytest libs/option_value/tests/test_curves.py -v` → FAIL (module not found).

- [ ] **Step 3: Implement `contracts.py` (dataclasses exactly as in Interfaces) and `curves.py`**

`build_expected_curve` approach: group by date; keep days with 24 non-NaN; build (n,24) matrix; month/hour from index; `np.nanmean` over a (12,24) accumulator; tile rows chronologically for `hourly`.

- [ ] **Step 4: Run tests, verify PASS**

- [ ] **Step 5: Commit** — `git add libs/option_value/ && git commit -m "Add option_value contracts and expected-curve builder" -- libs/option_value/`

---

### Task 2: PCA simulator v2

**Files:**
- Create: `libs/option_value/pca_simulator.py`
- Test: `libs/option_value/tests/test_pca_simulator.py`

**Interfaces:**
- Consumes: `ExpectedCurve, PCASimConfig, PCAv2Params` (Task 1).
- Produces:
  - `fit_pca_v2(prices: pd.Series, curve: ExpectedCurve, cfg: PCASimConfig) -> PCAv2Params`
  - `simulate_pca_v2(params: PCAv2Params, curve: ExpectedCurve, n_days: int, cfg: PCASimConfig) -> np.ndarray  # (n_paths, n_days*24) ¥/MWh`

Method (from spec §5.2 — pinned values): residuals = daily complete-day profiles − their month-hour mean row; covariance (24×24) → `np.linalg.eigh`, descending, keep `n_components`; daily score series per PC → AR(1) `phi = Σ(s_t·s_{t−1}) / Σ(s_{t−1}²)`, innovations `e = s_t − phi·s_{t−1}`; per-PC Student-t df by moment estimator `df = 6/kurt_excess + 4` valid only when `kurt_excess > 0`, clipped to [3, 30], else NaN (= Normal fallback; also fallback when `e.std() < 1e-9`); t-innovation scale = `e.std() / sqrt(df/(df−2))` (unit-variance standard_t). Simulation per PC: AR(1) recursion over days, `s_0 ~ N(0, e.std()²/(1−phi²))`, vectorized across paths; reconstruct `profile[day] = curve.month_hour_mean[month(day)] + Σ_p score_p[day]·loadings[p]` via per-PC `np.outer` accumulation (NO big matmul); clip to `[curve.floor, curve.cap]`; simulated calendar = `n_days` consecutive days starting `curve.window_end` (month labels roll forward).

- [ ] **Step 1: Write the failing tests**

```python
# test_pca_simulator.py
import numpy as np, pandas as pd
from datetime import date
from libs.option_value.curves import build_expected_curve
from libs.option_value.contracts import PCASimConfig
from libs.option_value.pca_simulator import fit_pca_v2, simulate_pca_v2

def _synthetic(n_days=400, seed=7):
    # two known shape factors; PC1 scores AR(1) phi=0.6, PC2 iid
    rng = np.random.default_rng(seed)
    L1 = np.sin(np.linspace(0, np.pi, 24)); L2 = np.cos(np.linspace(0, np.pi, 24))
    s1 = np.zeros(n_days); e1 = rng.normal(0, 3.0, n_days)
    for d in range(1, n_days): s1[d] = 0.6*s1[d-1] + e1[d]
    s2 = rng.normal(0, 2.0, n_days)
    resid = np.outer(s1, L1) + np.outer(s2, L2)
    idx = pd.date_range("2025-11-01", periods=n_days*24, freq="h")
    base = np.array([300.0 + ts.month*5 + 20*np.sin(2*np.pi*ts.hour/24) for ts in idx])
    return pd.Series(base + resid.reshape(-1), index=idx)

def test_ar1_phi_recovered():
    s = _synthetic(); c = build_expected_curve(s, date(2026, 11, 30))
    p = fit_pca_v2(s, c, PCASimConfig(n_components=2))
    assert abs(p.phi[0] - 0.6) < 0.1

def test_simulated_score_autocorr_and_moments():
    s = _synthetic(); c = build_expected_curve(s, date(2026, 11, 30))
    cfg = PCASimConfig(n_components=2, n_paths=400, seed=42)
    p = fit_pca_v2(s, c, cfg)
    sim = simulate_pca_v2(p, c, 365, cfg)
    assert sim.shape == (400, 365*24)
    assert sim.min() >= c.floor and sim.max() <= c.cap
    hist_spread = s.values.reshape(-1, 24).ptp(axis=1).mean()
    sim_spread = sim.reshape(400, 365, 24).ptp(axis=2).mean()
    assert abs(sim_spread - hist_spread) / hist_spread < 0.15

def test_seed_reproducible():
    s = _synthetic(); c = build_expected_curve(s, date(2026, 11, 30))
    cfg = PCASimConfig(n_components=2, n_paths=8, seed=42)
    p = fit_pca_v2(s, c, cfg)
    assert np.array_equal(simulate_pca_v2(p, c, 10, cfg), simulate_pca_v2(p, c, 10, cfg))

def test_degenerate_pc_falls_back_to_normal():
    s = _synthetic(); c = build_expected_curve(s, date(2026, 11, 30))
    p = fit_pca_v2(s, c, PCASimConfig(n_components=4))  # PCs 3-4 are pure noise
    assert np.isnan(p.inno_df[2:]).all() or (p.inno_df[2:] >= 3).all()  # no garbage df
```

- [ ] **Step 2: Run, verify FAIL** (module not found).

- [ ] **Step 3: Implement `pca_simulator.py`** per the pinned method above. Mirror the eigh + outer-accumulation structure of `libs/deal_models/price_simulator.py:55-96`.

- [ ] **Step 4: Run, verify PASS.**

- [ ] **Step 5: Reviewer grep (OpenBLAS guard)** — `grep -rn "svd\|pinv\|lstsq" libs/option_value/` must return nothing; matmuls only on (days×24)@(24×k) scale.

- [ ] **Step 6: Commit** — `git add libs/option_value/pca_simulator.py libs/option_value/tests/test_pca_simulator.py && git commit -m "Add PCA price simulator v2 (seasonal, AR(1) scores, t innovations)" -- libs/option_value/pca_simulator.py libs/option_value/tests/test_pca_simulator.py`

---

### Task 3: Vectorized DP dispatch (BESS) + LP gate

**Files:**
- Create: `libs/option_value/dp_dispatch.py`
- Test: `libs/option_value/tests/test_dp_dispatch.py`

**Interfaces:**
- Consumes: `BESSParams, DispatchResult` (Task 1).
- Produces:
  - `dp_dispatch_paths(paths: np.ndarray, params: BESSParams) -> DispatchResult` — `paths` (n_paths, T) ¥/MWh; boundary conditions SOC[0]=0, terminal free; per path returns total revenue ¥ and grid-delivered discharge MWh (for the `power_mw` unit).
  - `dp_dispatch_day(prices_24: np.ndarray, params: BESSParams) -> tuple[float, float]` — (revenue ¥, discharge MWh) convenience wrapper used by the gate test.

Method (pinned): SOC grid `0..power*duration` step `soc_grid_step_frac × E`; action set {idle, charge P/2, charge P, discharge P/2, discharge P}; physics identical to `optimise_window`: `soc' = soc + ch·η_c·dt − dis·dt/η_d`, `η_c=η_d=√rte`, dt=1.0; backward value iteration vectorized over (n_paths, S) per timestep with linear interpolation of V_next at `soc'`; store argmax action tensor (T, S, n_paths) as int8 (NOT the full V tensor — memory), then forward roll-out from soc=0 for revenue and discharge. **Ratified 2026-10-25 (T3 review): taper-consistent execution** — each nominal action executes PARTIAL power when clipped at the SOC floor/ceiling (`ch_exec = min(ch_nominal, (E−soc)/(η·dt))`, `dis_exec = min(dis_nominal, soc·η/dt)`), reward priced on executed energy in BOTH passes. The literal discrete-action reading was proven structurally incapable of passing the gate (~14% median gap, invariant to grid refinement — the action set, not the state grid, binds); taper-consistency guarantees DP ≤ LP by construction and measured 1.091% median. If a future task needs tighter than ~1.1%: the proven lever is adding ±P/4 actions, not finer SOC grid.

- [ ] **Step 1: Write the failing tests**

```python
# test_dp_dispatch.py
import numpy as np
from libs.option_value.contracts import BESSParams
from libs.option_value.dp_dispatch import dp_dispatch_paths, dp_dispatch_day
from services.bess_map.optimisation_engine import optimise_window

P = BESSParams(power_mw=1.0, duration_h=2.0, rte=0.85)

def test_crafted_day_arbitrages():
    day = np.array([10.0]*12 + [500.0]*12)
    rev, dis = dp_dispatch_day(day, P)
    assert rev > 0 and dis > 0
    # charge must happen in the cheap half: revenue bounded by 2 MWh grid delivery
    assert dis <= 2.0 + 1e-9

def test_dp_lp_gate_synthetic():
    rng = np.random.default_rng(3)
    gaps = []
    for _ in range(100):
        shape = 200 + 150*np.sin(2*np.pi*(np.arange(24)-6)/24)
        day = shape + rng.normal(0, 40, 24)
        lp = optimise_window(day, power_mw=1.0, duration_h=2.0, roundtrip_eff=0.85)
        dp_rev, _ = dp_dispatch_day(day, P)
        assert dp_rev <= lp.profit * 1.005 + 1e-6      # DP discretization cannot over-earn
        gaps.append((lp.profit - dp_rev) / max(lp.profit, 1e-9))
    assert float(np.median(gaps)) < 0.02                # THE GATE (spec §5.3)

def test_vectorized_matches_single():
    rng = np.random.default_rng(5)
    days = 200 + 100*np.sin(2*np.pi*np.arange(24)/24) + rng.normal(0, 30, (7, 24))
    batch = dp_dispatch_paths(days, P)
    singles = [dp_dispatch_day(d, P)[0] for d in days]
    assert np.allclose(batch.revenue_per_path, singles, rtol=1e-6)
```

- [ ] **Step 2: Run, verify FAIL** (module not found). Note: `optimise_window` import pulls PuLP — repo venv has it (capture pipeline dependency).

- [ ] **Step 3: Implement `dp_dispatch.py`** per pinned method. If the gate fails at `soc_grid_step_frac=0.1`, re-run at 0.05 — document the chosen grid in the module docstring with the measured median gap.

- [ ] **Step 4: Run, verify PASS** (gate included).

- [ ] **Step 5: Commit** — `git add libs/option_value/dp_dispatch.py libs/option_value/tests/test_dp_dispatch.py && git commit -m "Add vectorized DP BESS dispatch with LP validation gate" -- libs/option_value/dp_dispatch.py libs/option_value/tests/test_dp_dispatch.py`

---

### Task 4: Wind/solar revenue models (copula + curtailment floor)

**Files:**
- Create: `libs/option_value/renewable_models.py`
- Test: `libs/option_value/tests/test_renewable_models.py`

**Interfaces:**
- Consumes: `ExpectedCurve, RenewableParams, PCASimConfig` (Task 1).
- Produces:
  - `simulate_cf_paths(price_paths: np.ndarray, curve: ExpectedCurve, params: RenewableParams, seed: int) -> np.ndarray  # (n_paths, T) CF in [0,1]`
  - `renewable_revenue_paths(price_paths: np.ndarray, cf_paths: np.ndarray, params: RenewableParams) -> tuple[np.ndarray, np.ndarray]  # (revenue_per_path ¥, gen_mwh_per_path)`
  - `renewable_intrinsic(curve: ExpectedCurve, params: RenewableParams) -> tuple[float, float]  # (¥ for capacity_mw unit, gen MWh)`

Method (pinned): `z_price` = standardized residual of each simulated hour vs. its month-hour mean (std floored at 1e-6); `z_cf = ρ·z_price + √(1−ρ²)·z'`, `z'` iid Normal; `cf = clip(cf_profile[month, hour] + cf_sigma·z_cf, 0, 1)`; revenue = `Σ_t max(price_t, 0)·cf_t·capacity_mw·1h` when `curtailment_floor` else `Σ price_t·cf_t·capacity`. Intrinsic uses `curve.hourly` with the deterministic `cf_profile` rows (no noise).

- [ ] **Step 1: Write the failing tests**

```python
# test_renewable_models.py
import numpy as np, pandas as pd
from datetime import date
from libs.option_value.curves import build_expected_curve
from libs.option_value.contracts import RenewableParams
from libs.option_value.renewable_models import (simulate_cf_paths,
    renewable_revenue_paths, renewable_intrinsic)

def _curve_and_paths():
    rng = np.random.default_rng(11)
    idx = pd.date_range("2025-11-01", periods=400*24, freq="h")
    s = pd.Series(300 + 80*np.sin(2*np.pi*(idx.hour-14)/24) + rng.normal(0, 25, len(idx)), index=idx)
    c = build_expected_curve(s, date(2026, 11, 30))
    paths = np.tile(c.hourly[:24*365], (300, 1)) + rng.normal(0, 25, (300, 24*365))
    return c, paths

def _params(rho):
    cf = np.tile(np.clip(np.sin(2*np.pi*(np.arange(24)-6)/24), 0, None), (12, 1)) * 0.4
    return RenewableParams(capacity_mw=1.0, cf_profile=cf, price_volume_corr=rho)

def _cf_mean(p: RenewableParams, c, n_days):
    idx = pd.date_range(c.window_end, periods=n_days*24, freq="h")
    return np.array([p.cf_profile[ts.month % 12, ts.hour] for ts in idx])

def test_correlation_recovered():
    c, paths = _curve_and_paths()
    cf = simulate_cf_paths(paths, c, _params(-0.4), seed=42)
    z_p = (paths[:, :24*365] - c.hourly[:24*365]) / 25.0
    cf_mean = _cf_mean(_params(-0.4), c, 365)
    z_c = (cf - cf_mean[:24*365]) / 0.15
    assert abs(np.corrcoef(z_p.ravel(), z_c.ravel())[0,1] - (-0.4)) < 0.05

def test_curtailment_floor_blocks_negative_revenue():
    c, _ = _curve_and_paths()
    paths = np.full((4, 24*365), -50.0)   # all-negative price year
    cf = simulate_cf_paths(paths, c, _params(0.0), seed=1)
    rev_floor, _ = renewable_revenue_paths(paths, cf, _params(0.0))
    p2 = _params(0.0); p2.curtailment_floor = False
    rev_nofloor, _ = renewable_revenue_paths(paths, cf, p2)
    assert (rev_floor == 0).all() and (rev_nofloor < 0).all()

def test_negative_extrinsic_construction():
    # strong negative corr + CF concentrated in low-price hours → E[p·cf] < E[p]·E[cf]
    c, paths = _curve_and_paths()
    p = _params(-0.9); p.cf_sigma = 0.5
    cf = simulate_cf_paths(paths, c, p, seed=42)
    rev, _ = renewable_revenue_paths(paths, cf, p)
    int_rev, _ = renewable_intrinsic(c, p)
    assert rev.mean() - int_rev < 0      # extrinsic negative, reported with sign
```

- [ ] **Step 2: Run, verify FAIL.**

- [ ] **Step 3: Implement `renewable_models.py`** per pinned method.

- [ ] **Step 4: Run, verify PASS.**

- [ ] **Step 5: Commit** — `git add libs/option_value/renewable_models.py libs/option_value/tests/test_renewable_models.py && git commit -m "Add wind/solar copula revenue models with curtailment floor" -- libs/option_value/renewable_models.py libs/option_value/tests/test_renewable_models.py`

---

### Task 5: Asset models + value stack + Greeks

**Files:**
- Create: `libs/option_value/asset_models.py`
- Create: `libs/option_value/value_stack.py`
- Test: `libs/option_value/tests/test_value_stack.py`

**Interfaces:**
- Consumes: Tasks 1–4 (contracts, curves, `simulate_pca_v2`, `dp_dispatch_paths`, `renewable_*`).
- Produces:
  - `asset_models.ASSET_MODELS: dict[str, type]` — keys `"bess"`, `"wind"`, `"solar"`, `"coal"` (stub `CoalDarkSpread` whose methods raise `NotImplementedError("coal dark-spread DP: phase 2, spec §5 asset plugin")`)
  - `BESSDispatchOption(params: BESSParams)` / `WindRevenueOption(params: RenewableParams)` / `SolarRevenueOption(params: RenewableParams)` (wind/solar share a base; only `asset_type` and defaults differ), each with:
    - `applicable_levers: list[str]` — bess: `["capacity_price","cap_comp","fr","discharge_comp","sysopfee"]`; wind/solar: `["mechanism_premium","green_cert","sysopfee"]`
    - `intrinsic(curve: ExpectedCurve) -> tuple[float, float]` — (¥ for unit, volume MWh: discharge for BESS, generation for wind/solar)
    - `value_paths(paths: np.ndarray) -> DispatchResult-like tuple[np.ndarray, np.ndarray]` — (revenue_per_path, volume_per_path)
  - `value_stack.apply_policy(levers: PolicyLevers, asset_type: str, volume_mwh_per_kw: float, rte: float) -> tuple[float, dict]` — **all quantities per kW of rated power**. Pinned formulas (per year): `capacity_price`/`cap_comp`/`fr` pass through directly (already ¥/kW/yr); `discharge_comp = rate_¥kWh × volume_mwh_per_kw × 1000`; `sysopfee = −fee_¥kWh × (volume/rte) × 1000` for bess, `−fee × volume × 1000` for wind/solar (volume = generation per kW); `mechanism_premium = premium_¥MWh × volume`; `green_cert = rate_¥MWh × volume`. Levers not in `applicable_levers` are zeroed and named in `comp["not_applicable"]: list[str]`.
  - `value_stack.compute_value_stack(model, curve, paths, levers, capture_rate: float | None) -> ValueStack` — intrinsic; `total_energy = mean(path_revenue)`; `extrinsic = total_energy − intrinsic` (assert ≥ −1e-6 for bess only); policy via `apply_policy` with **mean path volume** (spec §11.8); `total = intrinsic + extrinsic + policy_net`; `capture_adj = capture_rate × (intrinsic+extrinsic) + policy_net` or `None` when `capture_rate is None`; `extrinsic_p10/p90 = np.percentile(path_revenue, [10,90]) − intrinsic`. The models run at `power_mw`/`capacity_mw` MW scale: divide revenue and volume by `unit_mw × 1000` to get the per-kW basis before policy.
  - `value_stack.annuitize(annual_yuan_kw: float, hurdle: float = 0.08, life_years: int = 15) -> float` — `annual × (1 − (1+hurdle)**−life) / hurdle`; `hurdle == 0` → `annual × life_years`.
  - `value_stack.compute_greeks(model, curve, sim_params, cfg, levers, capture_rate) -> dict` — keys `delta` (recompute with `curve.hourly`/`month_hour_mean` +10 ¥/MWh, same seed, minus base), `vega` (`inno_scale ×1.1`, same seed, minus base), `theta` (refit with window_end − 30 days, minus base). Values ¥/kW/yr.

- [ ] **Step 1: Write the failing tests**

```python
# test_value_stack.py
import numpy as np, pandas as pd, pytest
from datetime import date
from libs.option_value.curves import build_expected_curve
from libs.option_value.contracts import BESSParams, PolicyLevers, PCASimConfig
from libs.option_value.asset_models import ASSET_MODELS, BESSDispatchOption
from libs.option_value.value_stack import apply_policy, compute_value_stack, annuitize

def test_apply_policy_signs_and_magnitude():
    lv = PolicyLevers(capacity_price_yuan_kw_yr=100.0, discharge_comp_yuan_kwh=0.28,
                      sysopfee_yuan_kwh=0.03)
    # BESS 2h at ~1 cycle/day → 0.73 MWh discharged per kW per year
    net, comp = apply_policy(lv, "bess", volume_mwh_per_kw=0.73, rte=0.85)
    assert comp["capacity_price"] == 100.0
    assert comp["discharge_comp"] == pytest.approx(0.28 * 0.73 * 1000)        # 204.4 ¥/kW/yr
    assert comp["sysopfee"] == pytest.approx(-0.03 * (0.73/0.85) * 1000)      # −25.76 ¥/kW/yr
    assert net == pytest.approx(sum(v for k, v in comp.items() if isinstance(v, (int, float))))

def test_lever_not_applicable_zeroed():
    lv = PolicyLevers(capacity_price_yuan_kw_yr=100.0)        # a BESS-only lever
    net, comp = apply_policy(lv, "wind", volume_mwh_per_kw=3.0, rte=1.0)
    assert comp["capacity_price"] == 0.0
    assert "capacity_price" in comp["not_applicable"]

def test_annuitize_value():
    assert abs(annuitize(1.0, 0.08, 15) - 8.5595) < 1e-3
    assert annuitize(2.0, 0.0, 10) == 20.0                    # zero-hurdle rule

def test_jensen_extrinsic_nonnegative_bess():
    idx = pd.date_range("2025-11-01", periods=400*24, freq="h")
    s = pd.Series(300 + 150*np.sin(2*np.pi*(idx.hour-6)/24), index=idx)
    c = build_expected_curve(s, date(2026, 11, 30))
    model = BESSDispatchOption(BESSParams())
    int_rev, _ = model.intrinsic(c)    rng = np.random.default_rng(9)
    paths = c.hourly[:24*365] + rng.normal(0, 30, (50, 24*365))  # mean-zero noise
    vs = compute_value_stack(model, c, paths, PolicyLevers(), capture_rate=None)
    assert vs.extrinsic_yuan_kw >= -1e-6
    assert vs.capture_adj_yuan_kw is None                       # missing capture → None, no crash

def test_coal_stub_raises():
    import pytest
    with pytest.raises(NotImplementedError):
        ASSET_MODELS["coal"]().intrinsic(None)
```

- [ ] **Step 2: Run, verify FAIL.**

- [ ] **Step 3: Implement `asset_models.py` + `value_stack.py`.** Unit basis care: BESS DP runs at `power_mw` (default 1.0 MW) → DP revenue is ¥/MW/yr → divide by 1000 for ¥/kW/yr; volumes likewise per unit.

- [ ] **Step 4: Run, verify PASS.**

- [ ] **Step 5: Commit** — `git add libs/option_value/asset_models.py libs/option_value/value_stack.py libs/option_value/tests/test_value_stack.py && git commit -m "Add asset model registry, value stack decomposition, Greeks" -- libs/option_value/`

---

### Task 6: Permit & M&A acquisition option

**Files:**
- Create: `libs/option_value/build_option.py`
- Test: `libs/option_value/tests/test_build_option.py`

**Interfaces:**
- Consumes: `PolicyLevers, PolicyScenario, PermitInputs, PermitResult, AcquisitionResult` (Task 1); `apply_policy, annuitize` (Task 5).
- Produces:
  - `dp_perpetual_build(v: float, cost: float, sigma: float, r: float, rn_drift: float) -> float` — Dixit-Pindyck closed form, formulas exactly as `power-academy/labs/real_options_framework_for_generation_assets/compute.py:17-36` (reimplemented, source cited in comment; power-academy is not an importable dependency of libs).
  - `value_permit(scenarios: list[PolicyScenario], base_levers: PolicyLevers, energy_value_yuan_kw: float, asset_type: str, rte: float, inputs: PermitInputs) -> PermitResult` — backward tree over sorted unique `effective_date`s within `validity_years`; states = subsets of activated scenarios (independent Bernoulli; n ≤ 4 scenarios → ≤ 16 states); at each node, state levers = base with overrides applied, `NPV(τ, state) = annuitize((energy_value + policy_net(state)) × power_mw × 1000 … − capex(τ))` where `capex(τ) = capex_yuan_kwh_today × (1−decline)^τ × power_mw × duration_h × 1000`; exercise iff NPV > 0; root value − permit_cost = `v_permit_yuan`; `breakeven_permit_yuan` = root value before cost; `decomposition` = per-scenario marginal value contribution; `dp_check_value = dp_perpetual_build(v=base NPV, cost=capex(0), sigma=state-spread stdev of NPV, r=hurdle, rn_drift=0.0)`. Volume for policy held at the engine's base MC-mean (documented simplification, spec §5.6).
  - `value_acquisition(ask_yuan: float, energy_value_yuan_kw: float, levers: PolicyLevers, power_mw: float, duration_h: float, asset_type: str, rte: float, uplift_rows: list[tuple[str, float]], actuals_anchor_yuan_kw: float | None, hurdle: float = 0.08, life_years: int = 15) -> AcquisitionResult` — when `actuals_anchor_yuan_kw` is not None it **replaces** `energy_value_yuan_kw`; `fair_npv = annuitize((energy + policy + Σuplift) × power × 1000)`; `margin = fair_npv − ask`; `go = margin > 0`; `rows` = per-uplift decomposition.

- [ ] **Step 1: Write the failing tests**

```python
# test_build_option.py
from datetime import date
import pytest
from libs.option_value.build_option import dp_perpetual_build, value_permit, value_acquisition
from libs.option_value.contracts import PolicyLevers, PolicyScenario, PermitInputs

def test_dixit_pindyck_matches_academy_lab():
    # lab defaults: sigma=0.25, r=0.05, rn_drift=-0.04, cost=100
    # beta1 = 2.8428, wait multiplier = 1.5427, trigger = 154.27, value(100) = 15.82
    v = dp_perpetual_build(100.0, 100.0, 0.25, 0.05, -0.04)
    assert abs(v - 15.82) < 0.01
    assert dp_perpetual_build(200.0, 100.0, 0.25, 0.05, -0.04) == 100.0  # above trigger → exercise

def _inputs(cost):
    return PermitInputs(permit_cost_yuan=cost, validity_years=2.0,
                        capex_yuan_kwh_today=600.0, capex_decline_rate=0.0,
                        power_mw=0.1, duration_h=2.0, hurdle=0.0, life_years=1)

def test_permit_no_scenarios_immediate_or_never():
    # energy 50 ¥/kW/yr, no levers → annual ¥ = 50×100 kW = 5 000; capex = 600×200 kWh = 120 000
    r = value_permit([], PolicyLevers(), 50.0, "bess", 0.85, _inputs(10.0))
    assert r.v_permit_yuan == -10.0 and not r.go          # NPV negative → never build → worth 0 − cost

def test_permit_two_state_hand_computed():
    # scenario doubles capacity_price to 5 000 000 ¥/kW/yr at p=1.0 effective immediately:
    # NPV(state) = (50 + 5e6)×100 − 120 000 > 0 → exercise → root = that NPV
    sc = PolicyScenario("cap_x", {"capacity_price_yuan_kw_yr": 5_000_000.0}, date(2026,1,1), 1.0)
    r = value_permit([sc], PolicyLevers(), 50.0, "bess", 0.85, _inputs(0.0))
    assert r.go and r.breakeven_permit_yuan == pytest.approx((50+5_000_000)*100 - 120_000)

def test_probability_sum_rejected():
    sc1 = PolicyScenario("a", {}, date(2026,1,1), 0.7)
    sc2 = PolicyScenario("b", {}, date(2026,1,1), 0.7)   # same date, overlapping → sum 1.4
    with pytest.raises(ValueError):
        value_permit([sc1, sc2], PolicyLevers(), 50.0, "bess", 0.85, _inputs(0.0))

def test_acquisition_actuals_anchor_replaces_model():
    r = value_acquisition(ask_yuan=1.0, energy_value_yuan_kw=50.0, levers=PolicyLevers(),
                          power_mw=1.0, duration_h=2.0, asset_type="bess", rte=0.85,
                          uplift_rows=[], actuals_anchor_yuan_kw=80.0, hurdle=0.0, life_years=1)
    assert r.fair_npv_yuan == 80.0 * 1000                  # anchor wins over model 50
    assert r.go
```

- [ ] **Step 2: Run, verify FAIL.**

- [ ] **Step 3: Implement `build_option.py`.** Tree approach: nodes = sorted unique effective dates + expiry; backward induction over (date, activated-subset) lattice; transition probabilities from per-scenario Bernoulli; probability-overlap validation raises `ValueError` when same-date probabilities sum > 1.

- [ ] **Step 4: Run, verify PASS.**

- [ ] **Step 5: Commit** — `git add libs/option_value/build_option.py libs/option_value/tests/test_build_option.py && git commit -m "Add permit/M&A acquisition option valuation with Dixit-Pindyck cross-check" -- libs/option_value/`

---

### Task 7: Service layer — readers + snapshot persistence

**Files:**
- Create: `services/option_engine/__init__.py` (empty)
- Create: `services/option_engine/readers.py`
- Create: `services/option_engine/snapshot.py`
- Create: `db/ddl/marketdata/option_value_snapshot.sql`
- Test: `tests/option_engine/__init__.py` (empty), `tests/option_engine/test_readers.py`, `tests/option_engine/test_snapshot.py`

**Interfaces:**
- Consumes: contracts/PolicyLevers (Task 1); `services/bess_map/capacity_stack` loaders `load_capacity_rows(eng)`, `load_cap_comp_latest(eng)`, `load_fr_pool(eng)`, `load_sysopfee_latest(eng)`, `load_ancillary_annual(eng)` and its converter functions (`capacity_stack.py:39-44, 113-121`); `services/deal_engine/price_data.fetch_price_wind_correlation` (line 69); `apps/bess-map` exclusion list pattern `_RANKING_EXCLUDE` (re-declare the 7 sub-provincial names locally with a comment pointing at `apps/bess-map/app.py:994-997` — do not import from the app).
- Produces:
  - `readers.read_price_series(eng, province: str, window_end: date, months: int = 12) -> pd.Series` — hourly ¥/MWh RT series. SQL: `SELECT datetime, rt_price, da_price FROM marketdata.spot_prices_hourly WHERE province=%s AND datetime >= %s AND datetime <= %s ORDER BY datetime`. Then `_normalize_units(df) -> tuple[pd.DataFrame, dict]` (per column rt/da independently: `nanmedian < 5 → ×1000`; returns counts) then `_rt_da_fallback(df) -> tuple[pd.DataFrame, dict]` (rt ≤ 0 or NaN → da same hour; count touches — this is the per-hour fallback `fetch_price_history` lacks, spec §5.1). Returns the rt column as a Series (NaN where both missing — curves drops those days).
  - `readers.read_policy_levers(eng, province: str, asset_type: str) -> PolicyLevers` — maps loaders into `PolicyLevers`: cap_comp entries with value < 1.0 route to `discharge_comp_yuan_kwh` (per-kWh rule, `capacity_stack.py:189`); FR via `ancillary_per_mwh_yr` when mileage actuals exist else pool-share (precedence per `fr_component`, `capacity_stack.py:150-163`); all converted to the contract's per-kW/per-kWh units. Missing rows → 0.0 with `notes` entry.
  - `readers.read_capture_rate(eng, province: str, duration_h: float, model: str = "ols_rt_time_v1", days: int = 90) -> float | None` — `SELECT SUM(realized_profit)/NULLIF(SUM(theoretical_profit),0) FROM marketdata.bess_capture_daily WHERE province=%s AND model=%s AND duration_h=%s AND date >= CURRENT_DATE - %s`; None when no rows (theoretical rows duplicate across models — always filter one model).
  - `readers.read_hist_pf_per_kw_yr(eng, province: str, duration_h: float, window_start: date, window_end: date) -> float | None` — `AVG(theoretical_profit_per_mwh_day) × 365 × duration_h` over the window (champion model rows only).
  - `readers.read_cf_profile(eng, province: str, kind: str) -> np.ndarray | None` — (12,24) CF from `marketdata.spot_fundamentals_hourly` `wind_mw`/`solar_mw` (month-hour means normalized by column max); None when coverage < 6 months.
  - `readers.read_price_volume_corr(eng, province: str, kind: str) -> float | None` — wraps `fetch_price_wind_correlation`; None for solar when unavailable (default 0.0 at call site with note).
  - `readers.list_provinces(eng) -> list[str]` — distinct provinces in `spot_prices_hourly` minus the sub-provincial exclusion set.
  - `snapshot.ensure_table(eng) -> None` (executes the DDL file, idempotent), `snapshot.write_snapshot(eng, row: dict) -> None`, `snapshot.read_latest(eng, province: str, asset_type: str, duration_h: float) -> dict | None`.

DDL (`db/ddl/marketdata/option_value_snapshot.sql`) — columns exactly per spec §5.7 (`duration_h DOUBLE` = 0 for wind/solar; PK `(province, asset_type, duration_h, asof_date)`), `CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS … ON … (asof_date)`.

- [ ] **Step 1: Write the failing tests**

```python
# test_readers.py — pure transforms only (no DB)
import numpy as np, pandas as pd
from services.option_engine.readers import _normalize_units, _rt_da_fallback

def test_normalize_units_boundary():
    df = pd.DataFrame({"rt_price": [0.35, 0.42, 0.38], "da_price": [320.0, 350.0, 310.0]})
    out, meta = _normalize_units(df)
    assert out["rt_price"].iloc[0] == 350.0      # kWh legacy scaled
    assert out["da_price"].iloc[0] == 320.0      # already MWh, untouched
    assert meta["rt_price"]["scaled"] is True and meta["da_price"]["scaled"] is False

def test_rt_da_fallback_per_hour():
    df = pd.DataFrame({"rt_price": [0.0, np.nan, 400.0], "da_price": [300.0, 310.0, 350.0]})
    out, meta = _rt_da_fallback(df)
    assert out["rt_price"].tolist() == [300.0, 310.0, 400.0] and meta["filled"] == 2
```

```python
# test_snapshot.py
import os, pytest
from sqlalchemy import create_engine

@pytest.mark.skipif(not os.environ.get("DB_DSN"), reason="integration: needs DB_DSN")
def test_snapshot_roundtrip():
    from services.option_engine.snapshot import ensure_table, write_snapshot, read_latest
    eng = create_engine(os.environ["DB_DSN"])
    ensure_table(eng)
    row = dict(province="测试省", asset_type="bess", duration_h=2.0, asof_date="2026-10-25",
               window_start="2025-10-26", window_end="2026-10-25",
               intrinsic_yuan_kw=1.0, extrinsic_yuan_kw=2.0, total_yuan_kw=3.0,
               capture_adj_yuan_kw=2.9, policy_json={}, greeks_json={}, mc_params={"seed": 42})
    write_snapshot(eng, row)
    got = read_latest(eng, "测试省", "bess", 2.0)
    assert got["total_yuan_kw"] == 3.0
```

- [ ] **Step 2: Run, verify FAIL** (module not found; snapshot test skips without DB_DSN).

- [ ] **Step 3: Implement `readers.py`, `snapshot.py`, and the DDL file.**

- [ ] **Step 4: Run, verify PASS** (unit; integration skips cleanly).

- [ ] **Step 5: Commit** — `git add services/option_engine/ db/ddl/marketdata/option_value_snapshot.sql tests/option_engine/ && git commit -m "Add option_engine readers and snapshot persistence" -- services/option_engine/ db/ddl/marketdata/option_value_snapshot.sql tests/option_engine/`

---

### Task 8: Batch CLI + calibration report + real-data integration gate

**Files:**
- Create: `services/option_engine/batch.py`
- Create: `services/option_engine/calibration.py`
- Create: `services/option_engine/run_option_batch.py` (thin CLI: argparse `--province --asset-type --duration-h --paths --seed --screen`)
- Create: `services/option_engine/run_calibration.py` (thin CLI: `--provinces 山东,蒙西,…`)

**Interfaces:**
- Consumes: all of Tasks 1–7.
- Produces:
  - `batch.run_one(eng, province: str, asset_type: str, duration_h: float, n_paths: int = 1000, seed: int = 42) -> dict` — full chain: readers → `build_expected_curve` → `fit_pca_v2` → `simulate_pca_v2` (n_days = curve days) → asset model from `ASSET_MODELS` (RenewableParams built from `read_cf_profile` + `read_price_volume_corr`, parametric fallback with note when None) → `compute_value_stack` + `compute_greeks` → snapshot row dict (`mc_params` includes seed, n_paths, n_components, clip bounds, data coverage, lever notes, and **`git_sha` via `subprocess.check_output(["git","rev-parse","--short","HEAD"])`** — spec §5.7). `insufficient_history` → returns `{"skipped": province, "reason": "insufficient_history"}` and writes nothing (Review Focus #2).
  - `batch.run_screen(eng, provinces: list[str] | None = None, n_paths: int = 200, seed: int = 42) -> list[dict]` — provinces × [("bess",2.0), ("bess",4.0), ("wind",0.0), ("solar",0.0)].
  - `calibration.calibration_report(eng, provinces: list[str]) -> pd.DataFrame` — columns `[province, asset_type, duration_h, v_tot_yuan_kw, v_hist_yuan_kw, ratio, flag_ok]`; `flag_ok = ratio between 0.7 and 1.4` (spec §8); reads latest snapshots + `read_hist_pf_per_kw_yr`.

- [ ] **Step 1: Wire test — batch end-to-end with stubbed readers**

```python
# tests/option_engine/test_batch.py
import numpy as np, pandas as pd
from datetime import date
import services.option_engine.batch as batch
from libs.option_value.contracts import PolicyLevers

class _FakeEng: pass

def test_run_one_chain(monkeypatch):
    idx = pd.date_range("2025-11-01", periods=390*24, freq="h")
    s = pd.Series(300 + 120*np.sin(2*np.pi*(idx.hour-6)/24) + np.random.default_rng(1).normal(0, 25, len(idx)), index=idx)
    monkeypatch.setattr(batch.readers, "read_price_series", lambda *a, **k: s)
    monkeypatch.setattr(batch.readers, "read_policy_levers", lambda *a, **k: PolicyLevers(capacity_price_yuan_kw_yr=50.0))
    monkeypatch.setattr(batch.readers, "read_capture_rate", lambda *a, **k: 0.75)
    written = {}
    monkeypatch.setattr(batch.snapshot, "write_snapshot", lambda eng, row: written.update(row))
    row = batch.run_one(_FakeEng(), "测试省", "bess", 2.0, n_paths=50, seed=42)
    assert row["total_yuan_kw"] > 0 and row["extrinsic_yuan_kw"] >= -1e-6
    assert row["capture_adj_yuan_kw"] < row["total_yuan_kw"]      # 0.75 haircut applied
    assert written["mc_params"]["seed"] == 42

def test_run_one_skips_insufficient_history():
    idx = pd.date_range("2026-06-01", periods=100*24, freq="h")
    s = pd.Series(300.0, index=idx)
    import services.option_engine.batch as b
    from unittest.mock import patch
    with patch.object(b.readers, "read_price_series", return_value=s):
        out = b.run_one(_FakeEng(), "福建", "bess", 2.0, n_paths=10)
    assert out["skipped"] == "福建"
```

- [ ] **Step 2: Run, verify FAIL** (module not found).

- [ ] **Step 3: Implement `batch.py`, `calibration.py`, both CLIs.**

- [ ] **Step 4: Run, verify PASS.**

- [ ] **Step 5: Real-data integration gate (manual, needs `DB_DSN` from `config/.env`; if the Mac↔RDS route is blocked today, run from an ECS one-shot task per the nodal-writer NAT pattern and record that in the commit message)**

Run and paste outputs into the commit message / session notes:
1. **DP/LP gate on real days:** 30 random complete days each from 山东， 蒙西， 广东 (via `readers.read_price_series`), LP `optimise_window` vs `dp_dispatch_day` per day — report median gap; must be < 2% (spec §5.3 hard gate; if not, refine `soc_grid_step_frac` to 0.05 and re-run before proceeding).
2. `python services/option_engine/run_option_batch.py --province 山东 --asset-type bess --duration-h 2 --paths 1000` → row visible via `read_latest`; intrinsic/extrinsic numbers sane vs `read_hist_pf_per_kw_yr`.
3. `python services/option_engine/run_calibration.py --provinces 山东,蒙西,广东,山西,江苏` → print the report; provinces outside [0.7, 1.4] get a one-line cause hypothesis in the commit message (data gap vs model miss).

- [ ] **Step 6: Commit** — `git add services/option_engine/ tests/option_engine/test_batch.py && git commit -m "Add option batch runner and calibration report; real-data DP/LP gate: <median gap>% (山东/蒙西/广东)" -- services/option_engine/ tests/option_engine/test_batch.py`

- [ ] **Step 7: Push** — `git fetch origin && git rebase origin/main && git push origin main` (if the shared tree blocks rebase, rebuild via the temp-index plumbing pattern: `GIT_INDEX_FILE=$(mktemp) git read-tree origin/main && git add <paths> && write-tree → commit-tree -p origin/main → update-ref`).

---

## Self-Review Notes (filled at plan write time)

- **Spec coverage:** spec build steps 1–7 map to Tasks 1–8 (curves T1, simulator T2, DP T3, wind/solar T4, value stack T5, build option T6, service T7, batch+calibration T8). Steps 8 (tabs) and 9 (scout) intentionally deferred to follow-on plans after calibration review — stated in Goal.
- **Type consistency:** `DispatchResult` in `libs/option_value/contracts.py` is NOT the `DispatchResult` of `optimisation_engine.py` — different shape; import discipline noted in T3 step 3 (import engine inside test only).
- **Proportion:** spec ~300 lines, plan ~2× — bodies omitted except pinned formulas and test assertions.
