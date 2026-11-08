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
    SOURCE, build_rows, _plausibility_split,
)


class _StubCur:
    """Answers the two guard queries from canned refs: latest= for the
    capacity LIMIT-1 query, trailing= for the volume median query."""
    def __init__(self, latest=None, trailing=()):
        self._latest = latest
        self._trailing = list(trailing)
    def execute(self, sql, params):
        pass
    def fetchone(self):
        return (self._latest,) if self._latest is not None else None
    def fetchall(self):
        return [(v,) for v in self._trailing]


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


def _cap_row(v):
    return build_rows({"installed_capacity_gw": v}, "安徽", date(2026, 9, 1))[0]


def _gen_row(v):
    return build_rows({"thermal_volume_gwh": v}, "安徽", date(2026, 9, 1))[0]


def test_capacity_mom_jump_guarded():
    # refs come back in table units (万千瓦): 7000 → 9000 = +28.6% MoM
    clean, suspect = _plausibility_split([_cap_row(90.0)], _StubCur(latest=7000.0))
    assert clean == [] and len(suspect) == 1


def test_capacity_normal_growth_clean():
    clean, suspect = _plausibility_split([_cap_row(73.5)], _StubCur(latest=7000.0))
    assert len(clean) == 1 and suspect == []  # 7350 vs 7000: +5% MoM — fine


def test_volume_outlier_guarded():
    # 安徽 thermal monthly ~250 亿kWh; the 2026-06 extraction (102,721 GWh →
    # 1,027 亿kWh) must be held
    clean, suspect = _plausibility_split([_gen_row(102721.0)], _StubCur(trailing=[250, 260, 240]))
    assert clean == [] and len(suspect) == 1


def test_volume_in_band_clean():
    clean, suspect = _plausibility_split([_gen_row(24000.0)], _StubCur(trailing=[250, 260, 240]))
    assert len(clean) == 1 and suspect == []


def test_no_reference_is_clean():
    rows = [_cap_row(68.31), _gen_row(25000.0)]
    clean, suspect = _plausibility_split(rows, _StubCur())
    assert len(clean) == 2 and suspect == []
