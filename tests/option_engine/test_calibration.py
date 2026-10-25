# test_calibration.py — flag semantics (no DB; readers/snapshots stubbed)
from datetime import date

import pandas as pd

import services.option_engine.calibration as cal


class _FakeEng:
    pass


def _snap(province, asset_type, duration_h, intrinsic, extrinsic, total):
    return {
        "province": province, "asset_type": asset_type,
        "duration_h": duration_h, "asof_date": date(2026, 10, 25),
        "window_start": date(2025, 10, 1), "window_end": date(2026, 10, 25),
        "intrinsic_yuan_kw": intrinsic, "extrinsic_yuan_kw": extrinsic,
        "total_yuan_kw": total, "capture_adj_yuan_kw": None,
        "mc_params": {
            "units": {"rt_price": {"days_scaled": 10, "days_total": 368},
                      "da_price": {"days_scaled": 20}},
            "fallback": {"filled": 5, "missing_both": 0, "negatives_kept": 3},
        },
    }


def test_flag_rides_energy_only_ratio(monkeypatch):
    # 蒙西-like row: big policy stack (total 510 vs energy 217) — under the
    # T8 gate this flagged 1.94 on the policy-inclusive ratio while the
    # energy-only ratio 0.82 was in band. flag_ok must ride ratio_energy.
    monkeypatch.setattr(cal, "latest_snapshots",
                        lambda eng, provs: [_snap("蒙西", "bess", 2.0,
                                                  117.2, 99.7, 510.4)])
    monkeypatch.setattr(cal.readers, "read_hist_pf_per_kw_yr",
                        lambda *a, **k: 263.571479)
    df = cal.calibration_report(_FakeEng(), ["蒙西"])
    row = df.iloc[0]
    assert abs(row["ratio_energy"] - (117.2 + 99.7) / 263.571479) < 1e-9
    assert row["flag_ok"] == True          # energy-only 0.823 ∈ [0.7, 1.4]
    assert row["ratio_with_policy"] > 1.4  # informational only, NOT flagged
    assert row["rt_days_scaled"] == 10 and row["fallback_filled"] == 5


def test_flag_false_when_energy_ratio_out_of_band(monkeypatch):
    monkeypatch.setattr(cal, "latest_snapshots",
                        lambda eng, provs: [_snap("假想省", "bess", 2.0,
                                                  100.0, 50.0, 155.0)])
    monkeypatch.setattr(cal.readers, "read_hist_pf_per_kw_yr",
                        lambda *a, **k: 300.0)
    df = cal.calibration_report(_FakeEng(), ["假想省"])
    row = df.iloc[0]
    assert abs(row["ratio_energy"] - 0.5) < 1e-9 and row["flag_ok"] == False


def test_wind_row_na(monkeypatch):
    monkeypatch.setattr(cal, "latest_snapshots",
                        lambda eng, provs: [_snap("山东", "wind", 0.0,
                                                  1280.0, 4.3, 1286.5)])
    monkeypatch.setattr(cal.readers, "read_hist_pf_per_kw_yr",
                        lambda *a, **k: 999.0)  # must not be called for wind
    df = cal.calibration_report(_FakeEng(), ["山东"])
    row = df.iloc[0]
    assert row["v_hist_yuan_kw"] is None or pd.isna(row["v_hist_yuan_kw"])
    assert pd.isna(row["ratio_energy"]) and pd.isna(row["flag_ok"])
