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


def test_irr_defaults_sysopfee_uses_2026_average():
    # 12 months of 2025 at 0.10 + 7 months of 2026 at 0.05 → must use 2026 only
    sof_2025 = _make_sof_df("广东", fee=0.10, months=12)
    sof_2026 = _make_sof_df("广东", fee=0.05, months=19).iloc[12:]  # 2026-01..2026-07
    sof = pd.concat([sof_2025, sof_2026], ignore_index=True)
    d = _irr_defaults_for_province("广东", 4.0, sof, pd.DataFrame(), pd.DataFrame())
    assert d["sysopfee_day"] == pytest.approx(-(0.05 * 1000 / 0.85 / 365), rel=1e-4)
    assert "2026" in d["sysopfee_src"]


def test_irr_defaults_sysopfee_falls_back_when_no_2026():
    sof = _make_sof_df("广东", fee=0.08, months=12)  # all 2025
    d = _irr_defaults_for_province("广东", 4.0, sof, pd.DataFrame(), pd.DataFrame())
    assert d["sysopfee_day"] == pytest.approx(-(0.08 * 1000 / 0.85 / 365), rel=1e-4)
    assert "2026" in d["sysopfee_src"]  # label must disclose the fallback

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
