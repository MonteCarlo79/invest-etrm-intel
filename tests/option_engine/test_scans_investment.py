# tests/option_engine/test_scans_investment.py
from services.option_engine.scout import scans_investment as si

def _snap(intrinsic, extrinsic, policy, total, capture_adj, province="蒙西", asset="bess", dur=2.0):
    return dict(province=province, asset_type=asset, duration_h=dur, asof_date="2026-10-26",
                intrinsic_yuan_kw=intrinsic, extrinsic_yuan_kw=extrinsic, policy_net_yuan_kw=policy,
                total_yuan_kw=total, capture_adj_yuan_kw=capture_adj,
                extrinsic_p10=None, extrinsic_p90=None, policy_json={}, mc_params={})

def test_permit_hunt_fires_only_on_optionality_gap():
    rows = [
        _snap(120.0, 50.0, 30.0, 200.0, 160.0),          # static (120+30)*8.5595=1283.9 ≥ capex 1200 → NOT a permit candidate (static clears)
        _snap(100.0, 60.0, 20.0, 180.0, 150.0, province="山西"),  # static 120*8.5595=1027.1 < 1200; full 150*8.5595=1283.9 ≥ 1200×1.05=1260 → CANDIDATE
        _snap(80.0, 30.0, 20.0, 130.0, 100.0, province="江苏"),   # static 100*8.5595=855.95 < 1200 AND full 855.95 < 1260 → no
    ]
    out = si.compute_permit_hunt(rows)
    assert [c["province"] for c in out] == ["山西"]
    assert out[0]["evidence"]["irr_uplift"] is not None

def test_underperformance_thresholds():
    rows = [
        {"scope": "nodal_agent", "model": "nodal_agent_v1", "province": "零碳46", "mean_capture_rate": 0.42,
         "delta_vs_champion": -0.18, "status": "candidate", "window_end": "2026-10-20"},
        {"scope": "nodal_agent", "model": "nodal_agent_v1", "province": "融水", "mean_capture_rate": 0.61,
         "delta_vs_champion": -0.02, "status": "candidate", "window_end": "2026-10-20"},
        {"scope": "bess_capture", "model": "ols_rt_time_v1", "province": "江苏", "mean_capture_rate": 0.48,
         "delta_vs_champion": 0.0, "status": "champion", "window_end": "2026-10-20"},
    ]
    out = si.compute_underperformance(rows)
    keys = {(c["evidence"]["scope"], c["evidence"]["province_or_plant"]) for c in out}
    assert ("nodal_agent", "零碳46") in keys          # fleet plant −18pp below champion
    assert ("bess_capture", "江苏") in keys           # weak monetization environment < 0.55
    assert ("nodal_agent", "融水") not in keys
