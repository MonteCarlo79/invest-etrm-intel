# tests/exchange_reports/test_fundamentals_bridge.py
"""fundamentals_bridge: exchange_monthly_metrics → province_fundamentals mapping.

Key invariants:
- unit conversions: GW→万千瓦 (×100), GWh→亿千瓦时 (÷100)
- demand section never written (total_volume_gwh caliber ambiguity)
- None cells skipped, no placeholder rows
- existing confirmed cells (any source) left untouched
"""
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from services.exchange_reports.fundamentals_bridge import (  # noqa: E402
    SOURCE, build_rows,
)


FULL = {
    "installed_capacity_gw": 68.31,
    "thermal_capacity_gw": 47.06,
    "wind_capacity_gw": 10.07,
    "solar_capacity_gw": 11.79,
    "bess_capacity_gw": 2.92,
    "nuclear_capacity_gw": None,
    "thermal_volume_gwh": 3081.1,
    "wind_volume_gwh": 200.1,
    "solar_volume_gwh": 170.8,
    "hydro_volume_gwh": 70.5,
    "nuclear_volume_gwh": None,
    "incoming_volume_gwh": 512.3,
    "outgoing_volume_gwh": 88.4,
}


def _by_key(rows):
    return {(r.section, r.metric): r for r in rows}


def test_capacity_gw_to_wankw():
    rows = _by_key(build_rows(FULL, "安徽", date(2026, 9, 1)))
    assert rows[("capacity", "cap_total")].value == 6831.0
    assert rows[("capacity", "cap_thermal")].value == 4706.0
    assert rows[("capacity", "cap_storage")].value == 292.0
    assert rows[("capacity", "cap_total")].unit == "万千瓦"


def test_generation_gwh_to_yikwh():
    rows = _by_key(build_rows(FULL, "安徽", date(2026, 9, 1)))
    assert rows[("generation", "gen_thermal")].value == 30.81
    assert rows[("generation", "gen_solar")].value == 1.71
    assert rows[("generation", "gen_thermal")].unit == "亿千瓦时"


def test_interprov_mapped():
    rows = _by_key(build_rows(FULL, "安徽", date(2026, 9, 1)))
    assert rows[("interprov", "import_gwh")].value == 5.12
    assert rows[("interprov", "export_gwh")].value == 0.88


def test_none_cells_skipped():
    rows = _by_key(build_rows(FULL, "安徽", date(2026, 9, 1)))
    assert ("capacity", "cap_nuclear") not in rows
    assert ("generation", "gen_nuclear") not in rows


def test_no_demand_rows_ever():
    m = dict(FULL)
    m["total_volume_gwh"] = 999.0
    rows = build_rows(m, "安徽", date(2026, 9, 1))
    assert all(r.section != "demand" for r in rows)


def test_month_period_source():
    for r in build_rows(FULL, "安徽", date(2026, 9, 15)):
        assert r.month == date(2026, 9, 15)  # report_month passes through as-is
        assert r.period == "month"
        assert r.source == SOURCE
