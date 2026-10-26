# tests/deal_structurer/test_acquisition_panel.py
from datetime import date
from apps.deal_structurer.option_tab import (
    scenario_rows_to_scenarios, permit_verdict_text, to_ov_result, volume_from_ov,
)
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

def test_volume_from_ov_snapshot_column_passes_through():
    ov = to_ov_result({"province": "山东", "asset_type": "bess", "duration_h": 2.0,
                       "asof_date": "2026-10-26",
                       "mc_mean_volume_mwh_per_kw": 0.73}, power_mw=100.0)
    v, note = volume_from_ov(ov)
    assert v == pytest.approx(0.73)
    assert "mc_mean_volume_mwh_per_kw" in note

def test_volume_from_ov_legacy_row_zeroes_with_warning():
    v, note = volume_from_ov({"mc_mean_volume_mwh_per_kw": None, "mc_params": {}})
    assert v == 0.0
    assert "0 体量" in note        # loud warning retained for pre-column rows
