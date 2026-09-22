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
