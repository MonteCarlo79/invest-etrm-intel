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
