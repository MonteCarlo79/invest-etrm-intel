# IRR Full Revenue Stack + Geo Map Overlays — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add 调频, 容量补偿, and 系统运行费 to the IRR calculator (auto-populated, editable, broken out in cashflow chart), and add optional overlays of those three items on the geo map payback choropleth.

**Architecture:** Extract pure-math IRR functions into a testable `irr_helpers.py` module; all Streamlit UI changes stay in `app.py`. The three new data items reuse `_sof_df`/`_cc_df`/`_fr_df` already loaded by earlier tabs — no new DB queries. The geo map receives an optional `extra_rev_map` dict (province → annual ¥/MWh adjustment) to adjust payback colours and labels.

**Tech Stack:** Python 3.12, pandas, plotly, streamlit, pytest

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `apps/bess-map/irr_helpers.py` | **Create** | `_compute_irr`, `_compute_npv`, `build_cashflows`, `_irr_defaults_for_province`, `_build_extra_rev_map` |
| `apps/bess-map/tests/__init__.py` | **Create** | empty — marks test package |
| `apps/bess-map/tests/test_irr_helpers.py` | **Create** | pytest unit tests for all five functions |
| `apps/bess-map/app.py` | **Modify** | Remove moved functions; add import; add i18n strings; update geo tab; update IRR tab |

---

### Task 1: Create `irr_helpers.py` with moved + new functions

**Files:**
- Create: `apps/bess-map/irr_helpers.py`

- [ ] **Step 1: Create the file**

```python
# apps/bess-map/irr_helpers.py
"""Pure-math IRR helpers — no streamlit imports, fully testable."""
from __future__ import annotations
from typing import Optional
import pandas as pd


def _compute_irr(cashflows: list) -> Optional[float]:
    """Newton-Raphson IRR. Returns None if no solution found."""
    if not cashflows or cashflows[0] >= 0:
        return None
    r = 0.1
    for _ in range(300):
        npv  = sum(cf / (1 + r) ** t for t, cf in enumerate(cashflows))
        dnpv = sum(-t * cf / (1 + r) ** (t + 1) for t, cf in enumerate(cashflows))
        if abs(dnpv) < 1e-12:
            break
        r -= npv / dnpv
        if r <= -1:
            return None
    return r if -1 < r < 10 else None


def _compute_npv(cashflows: list, rate: float = 0.08) -> float:
    return sum(cf / (1 + rate) ** t for t, cf in enumerate(cashflows))


def build_cashflows(
    theo_per_mwh_day: float,
    capture_rate: float,
    duration_h: float,
    capex_per_kwh: float,
    rte: float,
    om_per_kw_yr: float,
    degradation: float,
    equity_pct: float,
    loan_rate: float,
    loan_tenure: int,
    project_life: int,
    power_mw: float = 1.0,
    sysopfee_per_mwh_day: float = 0.0,
    cap_comp_per_mwh_day: float = 0.0,
    fr_per_mwh_day: float = 0.0,
    subsidy_per_mwh: float = 0.0,
) -> tuple[list, dict]:
    """Returns (cashflows_list, annual_breakdown_dict) — normalised to 1 MW / N-hour plant.

    All *_per_mwh_day args are in ¥/MWh of installed energy capacity per day.
    sysopfee_per_mwh_day is negative (cost). cap_comp and fr are positive (revenue).
    subsidy_per_mwh is a legacy ¥/MWh-of-discharge catch-all, kept for back-compat.
    """
    e_cap = power_mw * duration_h          # MWh energy capacity
    capex = capex_per_kwh * e_cap * 1000   # ¥ total
    equity_capex = capex * equity_pct
    debt = capex * (1 - equity_pct)
    ann_debt = (
        debt * loan_rate / (1 - (1 + loan_rate) ** (-loan_tenure))
        if debt > 0 and loan_rate > 0
        else (debt / loan_tenure if loan_tenure > 0 else 0)
    )
    om_annual = om_per_kw_yr * power_mw

    # ~1 full cycle/day; discharge MWh ≈ e_cap × RTE
    daily_discharge = e_cap * rte

    base_spot_daily    = theo_per_mwh_day * capture_rate * e_cap
    base_cap_comp_daily = cap_comp_per_mwh_day * e_cap
    base_fr_daily      = fr_per_mwh_day * e_cap
    base_sysopfee_daily = sysopfee_per_mwh_day * e_cap   # negative
    base_subsidy_daily = subsidy_per_mwh * daily_discharge

    cfs = [-equity_capex]
    breakdown = {}
    for yr in range(1, project_life + 1):
        deg = (1 - degradation) ** (yr - 1)
        spot     = base_spot_daily     * 365 * deg
        cap_comp = base_cap_comp_daily * 365 * deg
        fr       = base_fr_daily       * 365 * deg
        sysopfee = base_sysopfee_daily * 365 * deg
        subsidy  = base_subsidy_daily  * 365 * deg
        ds       = ann_debt if yr <= loan_tenure else 0.0
        net      = spot + cap_comp + fr + sysopfee + subsidy - om_annual - ds
        cfs.append(net)
        breakdown[yr] = {
            "spot":     spot,
            "cap_comp": cap_comp,
            "fr":       fr,
            "sysopfee": sysopfee,
            "subsidy":  subsidy,
            "om":       om_annual,
            "debt_svc": ds,
            "net":      net,
        }

    # Scale to per-MWh installed capacity for readability
    scale = 1.0 / e_cap if e_cap > 0 else 1.0
    bd_scaled = {
        yr: {k: v * scale for k, v in row.items()}
        for yr, row in breakdown.items()
    }
    return cfs, bd_scaled


def _irr_defaults_for_province(
    province: str,
    duration_h: float,
    sof_df: pd.DataFrame,
    cc_df: pd.DataFrame,
    fr_df: pd.DataFrame,
    rte: float = 0.85,
    fr_util_pct: float = 0.30,
) -> dict:
    """Compute per-day ¥/MWh revenue/cost defaults from province market data.

    Returns dict with keys:
      sysopfee_day   float  ¥/MWh/day (negative — cost)
      cap_comp_day   float  ¥/MWh/day (positive — revenue)
      fr_day         float  ¥/MWh/day (positive — revenue)
      sysopfee_src   str    human-readable source label
      cap_comp_src   str
      fr_src         str
    """
    result: dict = {
        "sysopfee_day": 0.0,
        "cap_comp_day": 0.0,
        "fr_day":       0.0,
        "sysopfee_src": "无数据",
        "cap_comp_src": "无数据",
        "fr_src":       "无数据",
    }

    # ── 系统运行费: average of last 12 available months ───────────────────────
    if not sof_df.empty and province in sof_df["province"].values:
        prov_sof = (
            sof_df[sof_df["province"] == province]
            .sort_values("year_month", ascending=False)
            .head(12)
        )
        if not prov_sof.empty:
            avg_fee = float(prov_sof["fee_yuan_kwh"].mean())
            # fee(¥/kWh) × 1000(kWh/MWh capacity) / RTE / 365 days
            result["sysopfee_day"] = -(avg_fee * 1000.0 / rte / 365.0)
            result["sysopfee_src"] = f"市场均值 ({len(prov_sof)}月)"

    # ── 容量补偿: most recent row with standard ¥/kW value ───────────────────
    if not cc_df.empty and province in cc_df["province"].values:
        prov_cc = cc_df[cc_df["province"] == province].copy()
        # Skip rows where notes indicate a ¥/kWh volume-based model (e.g. 蒙东, 山东)
        if "notes" in prov_cc.columns:
            prov_cc = prov_cc[
                ~prov_cc["notes"].fillna("").str.contains("kWh", case=False)
            ]
        prov_cc = prov_cc.dropna(subset=["cap_comp_yuan_kw"]).sort_values(
            "effective_date", ascending=False
        )
        if not prov_cc.empty:
            row = prov_cc.iloc[0]
            cap_val = float(row["cap_comp_yuan_kw"])
            # ¥/kW/year ÷ duration_h ÷ 365 = ¥/MWh installed/day
            result["cap_comp_day"] = cap_val / duration_h / 365.0
            result["cap_comp_src"] = f"最新数据 {str(row['effective_date'])[:7]}"
        else:
            # Province has cap_comp rows but all are non-standard
            result["cap_comp_src"] = "无标准数据（非¥/kW模式）"

    # ── 调频: most recent price × utilisation × 8760 / duration_h / 365 ──────
    if not fr_df.empty and province in fr_df["province"].values:
        prov_fr = (
            fr_df[fr_df["province"] == province]
            .dropna(subset=["fr_price_yuan_kw_h"])
            .sort_values("effective_date", ascending=False)
        )
        if not prov_fr.empty:
            row = prov_fr.iloc[0]
            fr_val = float(row["fr_price_yuan_kw_h"])
            # ¥/kW·h × util_pct × 8760 h/yr / duration_h / 365 = ¥/MWh installed/day
            result["fr_day"] = fr_val * fr_util_pct * 8760.0 / duration_h / 365.0
            result["fr_src"] = (
                f"最新 {str(row['effective_date'])[:7]} × {fr_util_pct*100:.0f}%"
            )

    return result


def _build_extra_rev_map(
    sof_df: pd.DataFrame,
    cc_df: pd.DataFrame,
    fr_df: pd.DataFrame,
    duration_h: float,
    selected_items: list,
    fr_util_pct: float = 0.30,
    rte: float = 0.85,
) -> dict:
    """Return {province: net annual ¥/MWh adjustment} for geo map payback overlay.

    selected_items is a subset of ["sysopfee", "cap_comp", "fr"].
    Result units match rank_df annual_theo/annual_real (¥/MWh installed/year).
    """
    if not selected_items:
        return {}

    all_provinces: set = set()
    for df in (sof_df, cc_df, fr_df):
        if not df.empty and "province" in df.columns:
            all_provinces.update(df["province"].unique())

    result = {}
    for prov in all_provinces:
        defaults = _irr_defaults_for_province(
            prov, duration_h, sof_df, cc_df, fr_df, rte, fr_util_pct
        )
        adj = 0.0
        if "sysopfee" in selected_items:
            adj += defaults["sysopfee_day"] * 365.0
        if "cap_comp" in selected_items:
            adj += defaults["cap_comp_day"] * 365.0
        if "fr" in selected_items:
            adj += defaults["fr_day"] * 365.0
        if adj != 0.0:
            result[prov] = adj
    return result
```

- [ ] **Step 2: Confirm file saved** — no test yet, just verify syntax

```bash
cd "C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform/apps/bess-map"
py -c "import irr_helpers; print('OK')"
```

Expected output: `OK`

---

### Task 2: Write and run tests for `irr_helpers.py`

**Files:**
- Create: `apps/bess-map/tests/__init__.py`
- Create: `apps/bess-map/tests/test_irr_helpers.py`

- [ ] **Step 1: Create `__init__.py`**

```python
# apps/bess-map/tests/__init__.py
```
(empty file)

- [ ] **Step 2: Write test file**

```python
# apps/bess-map/tests/test_irr_helpers.py
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
import pytest
from irr_helpers import (
    _compute_irr,
    _compute_npv,
    build_cashflows,
    _irr_defaults_for_province,
    _build_extra_rev_map,
)


# ── _compute_irr ──────────────────────────────────────────────────────────────

def test_compute_irr_simple():
    # -100 now, +30/yr for 5 years → IRR ≈ 15.2%
    cfs = [-100, 30, 30, 30, 30, 30]
    irr = _compute_irr(cfs)
    assert irr is not None
    assert abs(irr - 0.1524) < 0.001

def test_compute_irr_no_positive_cashflows():
    assert _compute_irr([-100, -10, -10]) is None

def test_compute_irr_initial_positive_returns_none():
    assert _compute_irr([100, -10]) is None


# ── _compute_npv ─────────────────────────────────────────────────────────────

def test_compute_npv_zero_rate():
    cfs = [-100, 50, 50, 50]
    assert abs(_compute_npv(cfs, rate=0.0) - 50.0) < 0.01

def test_compute_npv_positive():
    cfs = [-100, 200]
    npv = _compute_npv(cfs, rate=0.08)
    assert abs(npv - (200 / 1.08 - 100)) < 0.01


# ── build_cashflows ───────────────────────────────────────────────────────────

def test_build_cashflows_baseline_spot_only():
    cfs, bd = build_cashflows(
        theo_per_mwh_day=10.0,
        capture_rate=1.0,
        duration_h=4.0,
        capex_per_kwh=600,
        rte=0.85,
        om_per_kw_yr=24000,
        degradation=0.0,
        equity_pct=1.0,  # all equity → no debt service
        loan_rate=0.05,
        loan_tenure=10,
        project_life=3,
    )
    # equity_capex = 600 × 4 MWh × 1000 = 2,400,000 ¥
    assert cfs[0] == pytest.approx(-2_400_000, rel=1e-4)
    # year 1 spot revenue (for 1 MW × 4h plant) = 10 ¥/MWh/day × 4 MWh × 365 days = 14,600 ¥
    # per-MWh scaled: 14,600 / 4 = 3,650
    assert bd[1]["spot"] == pytest.approx(3_650.0, rel=1e-4)
    assert bd[1]["cap_comp"] == pytest.approx(0.0)
    assert bd[1]["fr"] == pytest.approx(0.0)
    assert bd[1]["sysopfee"] == pytest.approx(0.0)
    assert bd[1]["debt_svc"] == pytest.approx(0.0)

def test_build_cashflows_sysopfee_is_negative():
    _, bd = build_cashflows(
        theo_per_mwh_day=10.0, capture_rate=1.0, duration_h=4.0,
        capex_per_kwh=600, rte=0.85, om_per_kw_yr=0, degradation=0.0,
        equity_pct=1.0, loan_rate=0.05, loan_tenure=10, project_life=1,
        sysopfee_per_mwh_day=-1.0,
    )
    assert bd[1]["sysopfee"] == pytest.approx(-365.0, rel=1e-4)

def test_build_cashflows_cap_comp_adds_to_revenue():
    _, bd_base = build_cashflows(
        theo_per_mwh_day=10.0, capture_rate=1.0, duration_h=4.0,
        capex_per_kwh=600, rte=0.85, om_per_kw_yr=0, degradation=0.0,
        equity_pct=1.0, loan_rate=0.05, loan_tenure=10, project_life=1,
    )
    _, bd_with = build_cashflows(
        theo_per_mwh_day=10.0, capture_rate=1.0, duration_h=4.0,
        capex_per_kwh=600, rte=0.85, om_per_kw_yr=0, degradation=0.0,
        equity_pct=1.0, loan_rate=0.05, loan_tenure=10, project_life=1,
        cap_comp_per_mwh_day=2.0,
    )
    # cap_comp adds 2 × 365 = 730 ¥/MWh/yr
    assert bd_with[1]["cap_comp"] == pytest.approx(730.0, rel=1e-4)
    assert bd_with[1]["net"] == pytest.approx(bd_base[1]["net"] + 730.0, rel=1e-4)

def test_build_cashflows_degradation_applies_to_all_components():
    _, bd = build_cashflows(
        theo_per_mwh_day=10.0, capture_rate=1.0, duration_h=4.0,
        capex_per_kwh=600, rte=0.85, om_per_kw_yr=0, degradation=0.10,
        equity_pct=1.0, loan_rate=0.05, loan_tenure=10, project_life=2,
        cap_comp_per_mwh_day=2.0, fr_per_mwh_day=1.0,
    )
    # Year 2: all revenue components × (1-0.10)^1 = 0.9
    assert bd[2]["spot"]     == pytest.approx(bd[1]["spot"]     * 0.9, rel=1e-4)
    assert bd[2]["cap_comp"] == pytest.approx(bd[1]["cap_comp"] * 0.9, rel=1e-4)
    assert bd[2]["fr"]       == pytest.approx(bd[1]["fr"]       * 0.9, rel=1e-4)

def test_build_cashflows_subsidy_legacy_still_works():
    _, bd = build_cashflows(
        theo_per_mwh_day=0.0, capture_rate=1.0, duration_h=4.0,
        capex_per_kwh=600, rte=0.85, om_per_kw_yr=0, degradation=0.0,
        equity_pct=1.0, loan_rate=0.05, loan_tenure=10, project_life=1,
        subsidy_per_mwh=100.0,
    )
    # discharge = 1MW × 4h × 0.85 RTE = 3.4 MWh/day
    # subsidy annual = 100 × 3.4 × 365 = 124,100 ¥ → /4 MWh = 31,025 ¥/MWh/yr
    assert bd[1]["subsidy"] == pytest.approx(31_025.0, rel=1e-3)


# ── _irr_defaults_for_province ────────────────────────────────────────────────

def _make_sof_df(province, fee, months=12):
    import datetime
    rows = []
    for i in range(months):
        rows.append({
            "province": province,
            "year_month": pd.Timestamp("2025-01-01") + pd.DateOffset(months=i),
            "fee_yuan_kwh": fee,
        })
    return pd.DataFrame(rows)

def _make_cc_df(province, val, notes=""):
    return pd.DataFrame([{
        "province": province, "cap_comp_yuan_kw": val,
        "effective_date": pd.Timestamp("2026-01-01"),
        "notes": notes, "status": "confirmed",
    }])

def _make_fr_df(province, price):
    return pd.DataFrame([{
        "province": province, "fr_price_yuan_kw_h": price,
        "effective_date": pd.Timestamp("2026-01-01"),
        "status": "confirmed",
    }])

def test_irr_defaults_sysopfee_conversion():
    sof = _make_sof_df("广东", fee=0.05, months=12)
    d = _irr_defaults_for_province("广东", 4.0, sof, pd.DataFrame(), pd.DataFrame())
    # expected: -(0.05 × 1000 / 0.85 / 365) ≈ -0.1612
    assert d["sysopfee_day"] == pytest.approx(-(0.05 * 1000 / 0.85 / 365), rel=1e-4)
    assert d["sysopfee_day"] < 0
    assert "12月" in d["sysopfee_src"]

def test_irr_defaults_cap_comp_conversion():
    cc = _make_cc_df("广东", 200.0)
    d = _irr_defaults_for_province("广东", 4.0, pd.DataFrame(), cc, pd.DataFrame())
    # expected: 200 / 4 / 365 ≈ 0.1370
    assert d["cap_comp_day"] == pytest.approx(200.0 / 4.0 / 365.0, rel=1e-4)
    assert d["cap_comp_day"] > 0

def test_irr_defaults_cap_comp_skips_kwh_notes():
    cc = _make_cc_df("蒙东", 0.28, notes="放电量补偿模式: 0.28¥/kWh（单次放电）")
    d = _irr_defaults_for_province("蒙东", 4.0, pd.DataFrame(), cc, pd.DataFrame())
    assert d["cap_comp_day"] == 0.0
    assert "非¥/kW模式" in d["cap_comp_src"]

def test_irr_defaults_fr_conversion():
    fr = _make_fr_df("甘肃", 10.0)
    d = _irr_defaults_for_province("甘肃", 4.0, pd.DataFrame(), pd.DataFrame(), fr,
                                    fr_util_pct=0.30)
    # expected: 10 × 0.30 × 8760 / 4 / 365 ≈ 18.0
    assert d["fr_day"] == pytest.approx(10.0 * 0.30 * 8760 / 4 / 365, rel=1e-4)

def test_irr_defaults_missing_province_returns_zeros():
    d = _irr_defaults_for_province(
        "不存在省", 4.0, pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    )
    assert d["sysopfee_day"] == 0.0
    assert d["cap_comp_day"] == 0.0
    assert d["fr_day"] == 0.0


# ── _build_extra_rev_map ──────────────────────────────────────────────────────

def test_build_extra_rev_map_empty_selection():
    result = _build_extra_rev_map(
        pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
        4.0, selected_items=[],
    )
    assert result == {}

def test_build_extra_rev_map_sysopfee_only():
    sof = _make_sof_df("广东", 0.05)
    result = _build_extra_rev_map(
        sof, pd.DataFrame(), pd.DataFrame(),
        4.0, selected_items=["sysopfee"],
    )
    assert "广东" in result
    expected = -(0.05 * 1000 / 0.85 / 365) * 365
    assert result["广东"] == pytest.approx(expected, rel=1e-4)
    assert result["广东"] < 0

def test_build_extra_rev_map_combined():
    sof = _make_sof_df("广东", 0.05)
    cc  = _make_cc_df("广东", 200.0)
    fr  = _make_fr_df("广东", 10.0)
    result = _build_extra_rev_map(
        sof, cc, fr, 4.0,
        selected_items=["sysopfee", "cap_comp", "fr"],
        fr_util_pct=0.30,
    )
    sof_ann = -(0.05 * 1000 / 0.85 / 365) * 365
    cc_ann  = (200.0 / 4.0 / 365.0) * 365
    fr_ann  = (10.0 * 0.30 * 8760 / 4 / 365) * 365
    assert result["广东"] == pytest.approx(sof_ann + cc_ann + fr_ann, rel=1e-4)
```

- [ ] **Step 3: Run all tests — expect PASS**

```bash
cd "C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform/apps/bess-map"
py -m pytest tests/test_irr_helpers.py -v
```

Expected: all 22 tests PASS, 0 failures.

- [ ] **Step 4: Commit**

```bash
cd "C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform"
git add apps/bess-map/irr_helpers.py apps/bess-map/tests/__init__.py apps/bess-map/tests/test_irr_helpers.py
git commit -m "feat(bess-map): extract IRR helpers + add market revenue helpers with tests"
```

---

### Task 3: Update `app.py` — replace inline functions with import

**Files:**
- Modify: `apps/bess-map/app.py:1` (add import near top)
- Modify: `apps/bess-map/app.py:1400–1467` (remove old function bodies)

- [ ] **Step 1: Add import at top of app.py**

Find the existing import block (around line 1–30). Add after the last local import:

```python
from irr_helpers import (
    _compute_irr, _compute_npv, build_cashflows,
    _irr_defaults_for_province, _build_extra_rev_map,
)
```

Use the Edit tool. Find the line:
```python
from typing import Optional
```
Replace with:
```python
from typing import Optional
from irr_helpers import (
    _compute_irr, _compute_npv, build_cashflows,
    _irr_defaults_for_province, _build_extra_rev_map,
)
```

- [ ] **Step 2: Remove old `_compute_irr` definition from app.py**

Find and delete lines 1400–1414 (the `_compute_irr` function body). Replace:

```python
# ── IRR computation ────────────────────────────────────────────────────────────
def _compute_irr(cashflows: list) -> Optional[float]:
    """Newton-Raphson IRR. Returns None if no solution found."""
    if not cashflows or cashflows[0] >= 0:
        return None
    r = 0.1
    for _ in range(300):
        npv  = sum(cf / (1 + r) ** t for t, cf in enumerate(cashflows))
        dnpv = sum(-t * cf / (1 + r) ** (t + 1) for t, cf in enumerate(cashflows))
        if abs(dnpv) < 1e-12:
            break
        r -= npv / dnpv
        if r <= -1:
            return None
    return r if -1 < r < 10 else None

def _compute_npv(cashflows: list, rate: float = 0.08) -> float:
    return sum(cf / (1 + rate) ** t for t, cf in enumerate(cashflows))

def build_cashflows(
    theo_per_mwh_day: float,
    capture_rate: float,
    duration_h: float,
    capex_per_kwh: float,
    rte: float,
    om_per_kw_yr: float,
    subsidy_per_mwh: float,
    degradation: float,
    equity_pct: float,
    loan_rate: float,
    loan_tenure: int,
    project_life: int,
    power_mw: float = 1.0,
) -> tuple[list, dict]:
    """Returns (cashflows_list, annual_breakdown_dict) — normalised to 1 MW / N-hour plant."""
    e_cap = power_mw * duration_h          # MWh capacity
    capex = capex_per_kwh * e_cap * 1000   # yuan (1 MW = 1000 kW)
    equity_capex = capex * equity_pct
    debt = capex * (1 - equity_pct)
    ann_debt = (
        debt * loan_rate / (1 - (1 + loan_rate) ** (-loan_tenure))
        if debt > 0 and loan_rate > 0 else
        (debt / loan_tenure if loan_tenure > 0 else 0)
    )
    om_annual = om_per_kw_yr * power_mw          # om_per_kw_yr is actually ¥/MW/yr (param renamed for back-compat)
    # Approx: ~1 effective full cycle per day; discharge MWh ≈ e_cap × RTE
    daily_discharge = e_cap * rte
    base_rev_daily = (
        theo_per_mwh_day * capture_rate * e_cap
        + subsidy_per_mwh * daily_discharge
    )

    cfs = [-equity_capex]
    breakdown = {}
    for yr in range(1, project_life + 1):
        rev  = base_rev_daily * 365 * (1 - degradation) ** (yr - 1)
        ds   = ann_debt if yr <= loan_tenure else 0.0
        net  = rev - om_annual - ds
        cfs.append(net)
        breakdown[yr] = {"revenue": rev, "om": om_annual, "debt_svc": ds, "net": net}

    # Scale breakdown to per-MWh capacity for readability
    scale = 1.0 / e_cap if e_cap > 0 else 1.0
    bd_scaled = {
        yr: {k: v * scale for k, v in row.items()}
        for yr, row in breakdown.items()
    }
    return cfs, bd_scaled
```

With just the comment (the functions are now imported):

```python
# ── IRR computation — functions imported from irr_helpers ─────────────────────
```

- [ ] **Step 3: Verify app still loads**

```bash
cd "C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform/apps/bess-map"
py -c "
import ast, sys
with open('app.py') as f:
    src = f.read()
ast.parse(src)
print('Syntax OK')
"
```

Expected: `Syntax OK`

- [ ] **Step 4: Commit**

```bash
cd "C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform"
git add apps/bess-map/app.py
git commit -m "refactor(bess-map): replace inline IRR functions with irr_helpers import"
```

---

### Task 4: Add i18n strings to `app.py`

**Files:**
- Modify: `apps/bess-map/app.py` — `_T` dict, EN block (~line 158) and ZH block (~line 401)

- [ ] **Step 1: Add EN strings after `"irr_cf_net": "Net FCF",`**

```python
        "irr_cf_net":           "Net FCF",
        "irr_components_title": "Revenue & Cost Detail",
        "irr_fr_util":          "FR Utilisation (%)",
        "irr_cf_spot":          "Spot Arbitrage",
        "irr_cf_fr":            "Freq Reg",
        "irr_cf_cap_comp":      "Cap Comp",
        "irr_cf_sysopfee":      "Sys Op Fee",
        "geo_extra_items":      "Add Revenue/Cost Items (overlay on payback)",
        "geo_extra_sysopfee":   "Sys Op Fee (cost)",
        "geo_extra_cap_comp":   "Cap Comp (revenue)",
        "geo_extra_fr":         "Freq Reg (revenue)",
        "geo_fr_util":          "FR Utilisation (%)",
```

- [ ] **Step 2: Add ZH strings after `"irr_cf_net": "净自由现金流",`**

```python
        "irr_cf_net":           "净自由现金流",
        "irr_components_title": "收入/成本明细",
        "irr_fr_util":          "调频利用率 (%)",
        "irr_cf_spot":          "现货套利",
        "irr_cf_fr":            "调频",
        "irr_cf_cap_comp":      "容量补偿",
        "irr_cf_sysopfee":      "系统运行费",
        "geo_extra_items":      "额外收入/成本项（叠加至回收期）",
        "geo_extra_sysopfee":   "系统运行费（成本）",
        "geo_extra_cap_comp":   "容量补偿（收益）",
        "geo_extra_fr":         "调频（收益）",
        "geo_fr_util":          "调频利用率 (%)",
```

- [ ] **Step 3: Verify syntax**

```bash
cd "C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform/apps/bess-map"
py -c "import ast; ast.parse(open('app.py').read()); print('OK')"
```

Expected: `OK`

- [ ] **Step 4: Commit**

```bash
cd "C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform"
git add apps/bess-map/app.py
git commit -m "feat(bess-map): add i18n strings for full revenue IRR + geo overlay"
```

---

### Task 5: Update `chart_bess_revenue_map` to accept `extra_rev_map`

**Files:**
- Modify: `apps/bess-map/app.py:1314–1342`

- [ ] **Step 1: Replace `chart_bess_revenue_map` function**

Find and replace the entire function (lines 1314–1342):

```python
def chart_bess_revenue_map(rank_df: pd.DataFrame, duration_h: float,
                           col: str, geojson: dict | None,
                           capex_per_kwh: float = 600.0,
                           title: str | None = None,
                           extra_rev_map: "dict[str, float] | None" = None) -> plt.Figure:
    """Choropleth coloured by simple capex payback period (years).

    extra_rev_map: optional {province_name: annual ¥/MWh adjustment}.
    Positive = additional revenue (shorter payback), negative = cost (longer payback).
    """
    sub = rank_df[abs(rank_df["duration_h"] - duration_h) < 0.01].copy()
    sub["adcode"] = sub["province"].map(_ZH_PROV_ADCODE)
    sub = sub.dropna(subset=["adcode", col])

    # Build province → adcode lookup for applying extra_rev_map
    prov_to_adcode: dict[str, int] = {
        row["province"]: int(row["adcode"])
        for _, row in sub.iterrows()
        if pd.notna(row.get("adcode"))
    }
    extra_by_adcode: dict[int, float] = {}
    if extra_rev_map:
        for prov, adj in extra_rev_map.items():
            acode = prov_to_adcode.get(prov)
            if acode is not None:
                extra_by_adcode[acode] = adj

    rev_map: dict[int, float | None] = {}
    label_map: dict[int, str] = {}
    for _, row in sub.iterrows():
        acode = int(row["adcode"])
        rev = float(row[col]) if pd.notna(row[col]) else None
        adj = extra_by_adcode.get(acode, 0.0)
        adj_rev = (rev + adj) if rev is not None else None

        # Use adjusted revenue for colour
        rev_map[acode] = adj_rev

        if rev is not None and rev > 0:
            orig_pb = capex_per_kwh * 1000.0 / rev
            if adj_rev is not None and adj_rev > 0 and adj != 0.0:
                adj_pb = capex_per_kwh * 1000.0 / adj_rev
                sign = "+" if adj >= 0 else ""
                label_map[acode] = (
                    f"{rev:,.0f} ({sign}{adj:,.0f})\n"
                    f"({orig_pb:.1f}yr→{adj_pb:.1f}yr)"
                )
            else:
                label_map[acode] = f"{rev:,.0f}\n({orig_pb:.1f}yr)"
        elif rev is not None:
            label_map[acode] = f"{rev:,.0f}"

    _lang = st.session_state.get("lang_radio", "English")
    _rc_font = {"font.family": _CJK_FONT} if _lang == "中文" and _CJK_FONT else {}
    with plt.rc_context(_rc_font):
        return _chart_bess_revenue_map_inner(sub, rev_map, label_map, geojson,
                                             capex_per_kwh, title, duration_h)
```

- [ ] **Step 2: Verify syntax**

```bash
cd "C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform/apps/bess-map"
py -c "import ast; ast.parse(open('app.py').read()); print('OK')"
```

Expected: `OK`

- [ ] **Step 3: Commit**

```bash
cd "C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform"
git add apps/bess-map/app.py
git commit -m "feat(bess-map): chart_bess_revenue_map accepts extra_rev_map overlay"
```

---

### Task 6: Update `with tab_geo:` — add overlay multiselect

**Files:**
- Modify: `apps/bess-map/app.py` — `with tab_geo:` block (~lines 1854–1892)

- [ ] **Step 1: Replace the `with tab_geo:` block**

Find the existing block starting with `with tab_geo:` and ending before `# ── Tab 3: Price Profile PCA`. Replace with:

```python
with tab_geo:
    st.caption(
        "Colour = simple payback period (annual revenue ÷ capex).  "
        "Revenue = ¥/MWh of **installed capacity** (power × duration) per year."
    )
    geo_capex = st.slider("Assumed capex for payback (¥/kWh)", 400, 900, 600, step=25,
                          key="geo_capex")

    # ── Extra overlay items ───────────────────────────────────────────────────
    _geo_extra_options = {
        "sysopfee": _t("geo_extra_sysopfee"),
        "cap_comp":  _t("geo_extra_cap_comp"),
        "fr":        _t("geo_extra_fr"),
    }
    _geo_sel_labels = st.multiselect(
        _t("geo_extra_items"),
        options=list(_geo_extra_options.values()),
        default=[],
        key="geo_extra_sel",
    )
    # Map labels back to keys
    _label_to_key = {v: k for k, v in _geo_extra_options.items()}
    _geo_sel_keys = [_label_to_key[lbl] for lbl in _geo_sel_labels]

    _geo_fr_util = 0.30
    if "fr" in _geo_sel_keys:
        _geo_fr_util = st.slider(_t("geo_fr_util"), 5, 80, 30, step=5, key="geo_fr_util") / 100.0

    # Build extra_rev_map from already-loaded DataFrames
    _geo_extra_map = _build_extra_rev_map(
        _sof_df if "_sof_df" in dir() else pd.DataFrame(),
        _cc_df  if "_cc_df"  in dir() else pd.DataFrame(),
        _fr_df  if "_fr_df"  in dir() else pd.DataFrame(),
        duration_h=4.0,   # use 4h as reference for geo (most common)
        selected_items=_geo_sel_keys,
        fr_util_pct=_geo_fr_util,
    ) if _geo_sel_keys else {}

    geo_rank_df = load_province_ranking(_ENG_KEY, sel_start, sel_end, sel_model)
    _geojson_bess, _geo_err = _load_china_geojson_bess()
    if _geo_err:
        st.warning(f"{_t('geo_unavailable')} ({_geo_err})")

    if not geo_rank_df.empty and _geojson_bess:
        col_2h, col_4h = st.columns(2)
        with col_2h:
            st.subheader(_t("geo_2h_title"))
            fig_geo2 = chart_bess_revenue_map(
                geo_rank_df, 2.0, rank_annual_col, _geojson_bess,
                capex_per_kwh=geo_capex,
                title=_t("geo_2h_title"),
                extra_rev_map=_geo_extra_map if _geo_extra_map else None,
            )
            st.pyplot(fig_geo2, use_container_width=True)
            plt.close(fig_geo2)
        with col_4h:
            st.subheader(_t("geo_4h_title"))
            fig_geo4 = chart_bess_revenue_map(
                geo_rank_df, 4.0, rank_annual_col, _geojson_bess,
                capex_per_kwh=geo_capex,
                title=_t("geo_4h_title"),
                extra_rev_map=_geo_extra_map if _geo_extra_map else None,
            )
            st.pyplot(fig_geo4, use_container_width=True)
            plt.close(fig_geo4)

        st.caption(
            f"Revenue basis: **{_t('forecast_theoretical') if rank_annual_col == 'annual_theo' else _t('forecast_realized')}** · "
            f"{sel_start} → {sel_end} · Payback = capex ({geo_capex} ¥/kWh × 1000) ÷ annual rev"
        )
    elif geo_rank_df.empty:
        st.warning("No ranking data available for this period.")
```

Note: `_sof_df`, `_cc_df`, `_fr_df` are loaded later in `tab_sysopfee` and `tab_aux`. Because `tab_geo` renders before those tabs, these variables aren't yet assigned. Fix: move the three load calls **above** the tab definitions. See Step 2.

- [ ] **Step 2: Move the three load calls above the `st.tabs(...)` line**

Find the line:
```python
tab_ranking, tab_geo, tab_pca, tab_demand, tab_sysopfee, tab_aux, tab_dispatch, tab_irr, tab_mgmt, tab_agent = st.tabs([
```

Insert these three lines immediately **before** it:

```python
# Pre-load market data used across multiple tabs (sysopfee, aux, irr, geo)
_sof_df = load_sysopfee(_ENG_KEY)
_cc_df  = load_cap_comp(_ENG_KEY)
_fr_df  = load_fr_market(_ENG_KEY)

tab_ranking, tab_geo, tab_pca, tab_demand, tab_sysopfee, tab_aux, tab_dispatch, tab_irr, tab_mgmt, tab_agent = st.tabs([
```

Then in `with tab_sysopfee:`, remove the line:
```python
    _sof_df = load_sysopfee(_ENG_KEY)
```
(it now uses the pre-loaded variable)

And in `with tab_aux:`, remove:
```python
    _cc_df = load_cap_comp(_ENG_KEY)
    _fr_df = load_fr_market(_ENG_KEY)
```

- [ ] **Step 3: Simplify the `dir()` checks in tab_geo**

Now that the three DataFrames are guaranteed pre-loaded, simplify the `_build_extra_rev_map` call (remove the `if "_sof_df" in dir()` guards):

```python
    _geo_extra_map = _build_extra_rev_map(
        _sof_df, _cc_df, _fr_df,
        duration_h=4.0,
        selected_items=_geo_sel_keys,
        fr_util_pct=_geo_fr_util,
    ) if _geo_sel_keys else {}
```

- [ ] **Step 4: Verify syntax**

```bash
cd "C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform/apps/bess-map"
py -c "import ast; ast.parse(open('app.py').read()); print('OK')"
```

Expected: `OK`

- [ ] **Step 5: Commit**

```bash
cd "C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform"
git add apps/bess-map/app.py
git commit -m "feat(bess-map): geo map payback overlay for sysopfee/cap_comp/fr"
```

---

### Task 7: Update `with tab_irr:` — revenue components expander + new cashflow chart

**Files:**
- Modify: `apps/bess-map/app.py` — `with tab_irr:` block (~lines 3297–3443)

- [ ] **Step 1: Replace the `with col_irr_in:` block**

Find this section inside `with tab_irr:` → `with col_irr_in:`. Replace everything from the `st.divider()` + capex/rte/om/subsidy/dgrad block up to (but not including) the second `st.divider()`:

**Old** (lines ~3337–3342):
```python
        st.divider()
        capex   = st.slider(_t("irr_capex"),        400, 900, 600, step=25)
        rte_pct = st.slider(_t("irr_rte"),          70,  95,  85, step=1)
        om      = st.number_input(_t("irr_om"),      value=24000, step=1000)
        subsidy = st.number_input(_t("irr_subsidy"), value=0,     step=50)
        dgrad   = st.slider(_t("irr_degradation"),  0,   5,   2,  step=1) / 100.0
```

**New**:
```python
        st.divider()
        capex   = st.slider(_t("irr_capex"),       400, 900, 600, step=25)
        rte_pct = st.slider(_t("irr_rte"),          70,  95,  85, step=1)
        om      = st.number_input(_t("irr_om"),     value=24000, step=1000)
        dgrad   = st.slider(_t("irr_degradation"),  0,   5,   2,  step=1) / 100.0

        # ── Revenue/cost components expander ─────────────────────────────────
        with st.expander(_t("irr_components_title"), expanded=False):
            _irr_fr_util = st.slider(
                _t("irr_fr_util"), 5, 80, 30, step=5, key="irr_fr_util"
            ) / 100.0
            _defs = _irr_defaults_for_province(
                irr_prov, irr_dur_h, _sof_df, _cc_df, _fr_df,
                rte=rte_pct / 100.0, fr_util_pct=_irr_fr_util,
            )
            sysopfee_input = st.number_input(
                f"系统运行费 ¥/MWh/day  [{_defs['sysopfee_src']}]",
                value=round(_defs["sysopfee_day"], 4),
                step=0.01, format="%.4f", key="irr_sysopfee",
            )
            cap_comp_input = st.number_input(
                f"容量补偿 ¥/MWh/day  [{_defs['cap_comp_src']}]",
                value=round(_defs["cap_comp_day"], 4),
                step=0.01, format="%.4f", key="irr_cap_comp",
            )
            fr_input = st.number_input(
                f"调频 ¥/MWh/day  [{_defs['fr_src']}]",
                value=round(_defs["fr_day"], 4),
                step=0.01, format="%.4f", key="irr_fr",
            )
```

- [ ] **Step 2: Update the `build_cashflows` call inside `with col_irr_out:`**

Find:
```python
            cfs, bd = build_cashflows(
                theo_per_mwh_day=irr_rev_day,
                capture_rate=irr_cap_rate,
                duration_h=irr_dur_h,
                capex_per_kwh=capex,
                rte=rte_pct / 100.0,
                om_per_kw_yr=om,
                subsidy_per_mwh=subsidy,
                degradation=dgrad,
                equity_pct=equity,
                loan_rate=lr_pct,
                loan_tenure=tenure,
                project_life=life,
            )
```

Replace with:
```python
            cfs, bd = build_cashflows(
                theo_per_mwh_day=irr_rev_day,
                capture_rate=irr_cap_rate,
                duration_h=irr_dur_h,
                capex_per_kwh=capex,
                rte=rte_pct / 100.0,
                om_per_kw_yr=om,
                degradation=dgrad,
                equity_pct=equity,
                loan_rate=lr_pct,
                loan_tenure=tenure,
                project_life=life,
                sysopfee_per_mwh_day=sysopfee_input,
                cap_comp_per_mwh_day=cap_comp_input,
                fr_per_mwh_day=fr_input,
            )
```

- [ ] **Step 3: Replace the cashflow waterfall chart bars**

Find the chart section:
```python
            rev_s  = [bd[y]["revenue"]  for y in years]
            om_s   = [-bd[y]["om"]       for y in years]
            debt_s = [-bd[y]["debt_svc"] for y in years]
            net_s  = [bd[y]["net"]       for y in years]

            st.subheader(_t("irr_cashflow_title"))
            fig_cf = go.Figure()
            fig_cf.add_bar(x=years, y=rev_s,  name=_t("irr_cf_revenue"),
                           marker_color="#4CAF50")
            fig_cf.add_bar(x=years, y=om_s,   name=_t("irr_cf_om"),
                           marker_color="#E53935")
            fig_cf.add_bar(x=years, y=debt_s, name=_t("irr_cf_debt"),
                           marker_color="#FF7043")
            fig_cf.add_scatter(x=years, y=net_s, name=_t("irr_cf_net"),
                               line=dict(color="navy", width=2), mode="lines+markers")
```

Replace with:
```python
            spot_s    = [bd[y]["spot"]     for y in years]
            cap_s     = [bd[y]["cap_comp"] for y in years]
            fr_s      = [bd[y]["fr"]       for y in years]
            sof_s     = [bd[y]["sysopfee"] for y in years]  # already negative
            om_s      = [-bd[y]["om"]      for y in years]
            debt_s    = [-bd[y]["debt_svc"] for y in years]
            net_s     = [bd[y]["net"]      for y in years]

            st.subheader(_t("irr_cashflow_title"))
            fig_cf = go.Figure()
            fig_cf.add_bar(x=years, y=spot_s, name=_t("irr_cf_spot"),
                           marker_color="#2ecc71")
            fig_cf.add_bar(x=years, y=cap_s,  name=_t("irr_cf_cap_comp"),
                           marker_color="#1abc9c")
            fig_cf.add_bar(x=years, y=fr_s,   name=_t("irr_cf_fr"),
                           marker_color="#27ae60")
            fig_cf.add_bar(x=years, y=sof_s,  name=_t("irr_cf_sysopfee"),
                           marker_color="#e67e22")
            fig_cf.add_bar(x=years, y=om_s,   name=_t("irr_cf_om"),
                           marker_color="#e74c3c")
            fig_cf.add_bar(x=years, y=debt_s, name=_t("irr_cf_debt"),
                           marker_color="#c0392b")
            fig_cf.add_scatter(x=years, y=net_s, name=_t("irr_cf_net"),
                               line=dict(color="navy", width=2), mode="lines+markers")
```

- [ ] **Step 4: Update sensitivity table `build_cashflows` call**

Find the sensitivity table call (uses `subsidy` variable which no longer exists):
```python
                    cfs_s, _ = build_cashflows(
                        theo_per_mwh_day=irr_rev_day * rm,
                        capture_rate=irr_cap_rate,
                        duration_h=irr_dur_h,
                        capex_per_kwh=cx,
                        rte=rte_pct / 100.0,
                        om_per_kw_yr=om,
                        subsidy_per_mwh=subsidy,
                        degradation=dgrad,
                        equity_pct=equity,
                        loan_rate=lr_pct,
                        loan_tenure=tenure,
                        project_life=life,
                    )
```

Replace with (sensitivity varies only spot; other three components stay fixed):
```python
                    cfs_s, _ = build_cashflows(
                        theo_per_mwh_day=irr_rev_day * rm,
                        capture_rate=irr_cap_rate,
                        duration_h=irr_dur_h,
                        capex_per_kwh=cx,
                        rte=rte_pct / 100.0,
                        om_per_kw_yr=om,
                        degradation=dgrad,
                        equity_pct=equity,
                        loan_rate=lr_pct,
                        loan_tenure=tenure,
                        project_life=life,
                        sysopfee_per_mwh_day=sysopfee_input,
                        cap_comp_per_mwh_day=cap_comp_input,
                        fr_per_mwh_day=fr_input,
                    )
```

- [ ] **Step 5: Verify syntax**

```bash
cd "C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform/apps/bess-map"
py -c "import ast; ast.parse(open('app.py').read()); print('OK')"
```

Expected: `OK`

- [ ] **Step 6: Commit**

```bash
cd "C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform"
git add apps/bess-map/app.py
git commit -m "feat(bess-map): IRR full revenue stack — sysopfee/cap_comp/fr expander + split cashflow chart"
```

---

### Task 8: Build Docker image and deploy

**Files:**
- No file changes — build + ECS deploy

- [ ] **Step 1: Confirm current image tag**

```bash
cd "C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform"
grep "bess-map" docker-compose.local.yml | head -5
```

Current image should be `bess-map:v59`. New image will be `bess-map:v60`.

- [ ] **Step 2: Build image**

```bash
cd "C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform"
docker build -f apps/bess-map/Dockerfile -t 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-map:v60 apps/bess-map/
```

Expected: `Successfully tagged 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-map:v60`

- [ ] **Step 3: Push to ECR**

```bash
aws ecr get-login-password --region ap-southeast-1 | docker login --username AWS --password-stdin 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-map:v60
```

- [ ] **Step 4: Register td:91 with new image**

```bash
aws ecs describe-task-definition --task-definition bess-platform-bess-map:90 \
  --region ap-southeast-1 --query 'taskDefinition' > "$TEMP/td90b.json"

py -c "
import json, os
tmp = os.environ.get('TEMP', 'C:/Users/dipeng.chen/AppData/Local/Temp')
d = json.load(open(tmp + '/td90b.json'))
# Update image tag
c = d['containerDefinitions'][0]
c['image'] = c['image'].replace(':v59', ':v60').replace(':v60', ':v60')
# Strip read-only fields
for f in ['taskDefinitionArn','revision','status','requiresAttributes',
          'compatibilities','registeredAt','registeredBy','deregisteredAt']:
    d.pop(f, None)
json.dump(d, open(tmp + '/td91.json', 'w'), indent=2)
print('td91.json written, image:', c['image'])
"

aws ecs register-task-definition --cli-input-json "file://C:/Users/dipeng.chen/AppData/Local/Temp/td91.json" \
  --region ap-southeast-1 --query 'taskDefinition.{arn:taskDefinitionArn,status:status}'
```

Expected: `"arn": "...bess-platform-bess-map:91"`, `"status": "ACTIVE"`

- [ ] **Step 5: Deploy to ECS and wait for stable**

```bash
aws ecs update-service \
  --cluster bess-platform-cluster \
  --service bess-platform-bess-map-svc \
  --task-definition bess-platform-bess-map:91 \
  --force-new-deployment \
  --region ap-southeast-1 \
  --query 'service.{status:status,taskDef:taskDefinition}'

aws ecs wait services-stable --cluster bess-platform-cluster \
  --services bess-platform-bess-map-svc --region ap-southeast-1 && echo "STABLE"
```

Expected: `STABLE`

- [ ] **Step 6: Update docker-compose.local.yml image tag**

In `docker-compose.local.yml`, update bess-map image line from `v59` to `v60`.

- [ ] **Step 7: Final commit**

```bash
cd "C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform"
git add docker-compose.local.yml
git commit -m "chore: bump bess-map to v60 (full revenue IRR + geo overlay)"
```

---

## Self-Review

**Spec coverage check:**

| Spec requirement | Task |
|---|---|
| Auto-populate 系统运行费 from `_sof_df` | Task 1 (`_irr_defaults_for_province`) |
| Auto-populate 容量补偿 from `_cc_df` | Task 1 |
| Auto-populate 调频 from `_fr_df` | Task 1 |
| Skip 山东 NULL / 蒙东 ¥/kWh models | Task 1 (notes contains "kWh" guard) |
| Editable number_inputs with source labels | Task 7 Step 1 |
| FR utilisation slider | Task 7 Step 1 |
| `build_cashflows` extended, backwards-compat | Task 1 (`subsidy_per_mwh=0.0` default) |
| Cashflow chart split by source | Task 7 Step 3 |
| Sensitivity table uses new params | Task 7 Step 4 |
| Geo map multiselect overlay | Task 6 Step 1 |
| Geo map label shows orig→adjusted payback | Task 5 Step 1 |
| Data loaded before tab render (geo uses pre-loaded vars) | Task 6 Step 2 |
| Cache behaviour unchanged (30min TTL) | No change needed — TTL stays on load functions |
| i18n EN + ZH | Task 4 |
| Tests for all helpers | Task 2 |
| Build + deploy v60 | Task 8 |

**No placeholders or TBDs found.**

**Type consistency:** `_irr_defaults_for_province` returns `dict` with keys `sysopfee_day`, `cap_comp_day`, `fr_day`, `sysopfee_src`, `cap_comp_src`, `fr_src` — used consistently in Task 7 Step 1. `build_cashflows` breakdown keys `spot`, `cap_comp`, `fr`, `sysopfee`, `subsidy`, `om`, `debt_svc`, `net` — used consistently in Task 7 Step 3.
