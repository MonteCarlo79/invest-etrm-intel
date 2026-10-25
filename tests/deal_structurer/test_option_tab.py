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
