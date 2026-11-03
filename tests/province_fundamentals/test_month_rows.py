# tests/province_fundamentals/test_month_rows.py
"""月行宽表 parser tests — 山东月报工作簿 (月份为行, 燃料指标为列).

Fixture: tests/province_fundamentals/fixtures/山东电力市场信息披露月报-2022至2026年5月.xlsx
(copied once from data/exchange-monthly-reports/山东月报/ with `ditto` per the
controller ruling — tests must NOT read live data/ paths; OneDrive dataless
files hang the test run).

真值 (read from the real workbook, 2026-11-03, openpyxl data_only):
  sheet 10_装机与发电结构 (R2 表头, R3+ 数据, 17 个月 2025-01..2026-05):
    2025-01 | 火电装机 12137.63 | 风电装机 2668.78 | 太阳能装机 7905.16
            | 火电发电量 436.93 | 风电发电量 52.78 | 太阳能发电量 60.39
            | 水电装机/水电发电量/核电装机/核电发电量 全空 (2025 全年)
    2026-01 | 水电装机 406.85 | 核电装机 571.8 | 水电发电量 3.81 | 核电发电量 39.65
    2026-05 | 火电装机 11938.44 | 风电装机 3255.44 | 太阳能装机 9636.66
            | 火电发电量 345.99 | 风电发电量 66.72 | 太阳能发电量 111.58
            | 水电发电量 3.98 | 核电发电量 28.22
  sheet 01_用电与发电概况:
    2025-01 | 全社会用电量 708.24 | 同比 -3.53
    2026-05 | 全社会用电量 698.9  | 同比 3.66
"""
from datetime import date
from pathlib import Path

import openpyxl
import pytest

from services.province_fundamentals import registry
from services.province_fundamentals.parsers.month_rows import (
    MonthRowsConfig,
    MonthRowsParser,
    parse_month_rows,
    SHANDONG_CAP_GEN,
    SHANDONG_DEMAND,
    shandong_parser,
)

FIXTURE = (Path(__file__).parent / "fixtures"
           / "山东电力市场信息披露月报-2022至2026年5月.xlsx")
SRC = FIXTURE.name
JAN25 = date(2025, 1, 1)
MAY26 = date(2026, 5, 1)


def _by_value(rows):
    return {(r.month, r.metric): r.value for r in rows}


def _by_row(rows):
    return {(r.month, r.metric): r for r in rows}


def _all(rows):
    """山东 parser 全量 (两个 config 拼接, 同 registry 派发路径)."""
    return shandong_parser.parse(FIXTURE, "山东")


# ── 山东真值: sheet 10 装机 ───────────────────────────────────────────────────

def test_shandong_2025_01_capacity_truth():
    by = _by_value(parse_month_rows(FIXTURE, SHANDONG_CAP_GEN))
    assert by[(JAN25, "cap_thermal_full")] == pytest.approx(12137.63)
    assert by[(JAN25, "cap_wind_full")] == pytest.approx(2668.78)
    assert by[(JAN25, "cap_solar_full")] == pytest.approx(7905.16)


def test_shandong_capacity_row_conventions():
    r = _by_row(parse_month_rows(FIXTURE, SHANDONG_CAP_GEN))[
        (JAN25, "cap_thermal_full")]
    assert r.province == "山东"
    assert r.period == "month"
    assert r.section == "capacity"
    assert r.unit == "万千瓦"
    assert r.source == SRC                        # 当月直取: 无差分标


# ── 山东真值: sheet 10 发电量 (当月值, 不差分) ────────────────────────────────

def test_shandong_2025_01_generation_truth():
    by = _by_row(parse_month_rows(FIXTURE, SHANDONG_CAP_GEN))
    assert by[(JAN25, "gen_thermal_full")].value == pytest.approx(436.93)
    assert by[(JAN25, "gen_wind_full")].value == pytest.approx(52.78)
    assert by[(JAN25, "gen_solar_full")].value == pytest.approx(60.39)
    for m in ("gen_thermal_full", "gen_wind_full", "gen_solar_full"):
        r = by[(JAN25, m)]
        assert r.section == "generation"
        assert r.unit == "亿千瓦时"
        assert "(差分)" not in r.source           # 山东月报是当月值, 不差分


def test_shandong_2026_05_latest_month_truth():
    by = _by_value(parse_month_rows(FIXTURE, SHANDONG_CAP_GEN))
    assert by[(MAY26, "cap_thermal_full")] == pytest.approx(11938.44)
    assert by[(MAY26, "cap_wind_full")] == pytest.approx(3255.44)
    assert by[(MAY26, "cap_solar_full")] == pytest.approx(9636.66)
    assert by[(MAY26, "gen_thermal_full")] == pytest.approx(345.99)
    assert by[(MAY26, "gen_wind_full")] == pytest.approx(66.72)
    assert by[(MAY26, "gen_solar_full")] == pytest.approx(111.58)
    assert by[(MAY26, "gen_hydro_full")] == pytest.approx(3.98)
    assert by[(MAY26, "gen_nuclear_full")] == pytest.approx(28.22)


# ── 空燃料列 → value=None, 绝不写 0 (格式图已知陷阱) ─────────────────────────

def test_shandong_hydro_nuclear_2025_empty_is_none_not_zero():
    by = _by_row(parse_month_rows(FIXTURE, SHANDONG_CAP_GEN))
    for m in ("cap_hydro_full", "cap_nuclear_full",
              "gen_hydro_full", "gen_nuclear_full"):
        r = by[(JAN25, m)]
        assert r.value is None                    # 空列原样出 None
        assert r.value != 0
        assert r.unit in ("万千瓦", "亿千瓦时")    # 行仍在, 单位照常


def test_shandong_hydro_nuclear_filled_from_2026_01():
    by = _by_value(parse_month_rows(FIXTURE, SHANDONG_CAP_GEN))
    jan26 = date(2026, 1, 1)
    assert by[(jan26, "cap_hydro_full")] == pytest.approx(406.85)
    assert by[(jan26, "cap_nuclear_full")] == pytest.approx(571.8)
    assert by[(jan26, "gen_hydro_full")] == pytest.approx(3.81)
    assert by[(jan26, "gen_nuclear_full")] == pytest.approx(39.65)


# ── 山东真值: sheet 01 用电 ───────────────────────────────────────────────────

def test_shandong_demand_truth():
    by = _by_row(parse_month_rows(FIXTURE, SHANDONG_DEMAND))
    r = by[(JAN25, "demand_total")]
    assert r.value == pytest.approx(708.24)
    assert r.section == "demand"
    assert r.unit == "亿千瓦时"
    y = by[(JAN25, "demand_yoy")]
    assert y.value == pytest.approx(-3.53)
    assert y.section == "demand"
    assert y.unit == "%"
    assert by[(MAY26, "demand_total")].value == pytest.approx(698.9)
    assert by[(MAY26, "demand_yoy")].value == pytest.approx(3.66)


# ── 覆盖率与结构 ──────────────────────────────────────────────────────────────

def test_shandong_full_coverage_row_count():
    """sheet10: 17 月 × (5 装机 + 5 发电) = 170; sheet01: 17 月 × 2 = 34; 共 204."""
    rows = _all(None)
    assert len(rows) == 204
    assert all(r.period == "month" for r in rows)
    assert all(r.province == "山东" for r in rows)
    months = {r.month for r in rows}
    assert len(months) == 17
    assert min(months) == JAN25 and max(months) == MAY26


# ── 表头按名称匹配, 永不按下标 ────────────────────────────────────────────────

def test_header_mapping_by_name_not_index(tmp_path):
    """列序打乱 + 混入无关列, 值仍须落到正确 metric."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "10_装机与发电结构"
    ws.append(["山东电力市场月度数据库 - 装机与发电结构"])
    ws.append(["噪音列", "风电发电量\n(亿kWh)", "月份", "火电装机\n(万kW)",
               "水电装机(万kW)"])
    ws.append(["x", "52.78", "2025-01", "12137.63", None])
    p = tmp_path / "scrambled.xlsx"
    wb.save(p)
    rows = parse_month_rows(p, SHANDONG_CAP_GEN)
    by = {r.metric: r for r in rows}
    assert by["cap_thermal_full"].value == pytest.approx(12137.63)
    assert by["gen_wind_full"].value == pytest.approx(52.78)
    assert by["cap_hydro_full"].value is None     # 空列 → None 非 0
    assert len(rows) == 3                         # 噪音列不占位


# ── 累计差分 (cumulative_gen=True): 1 月原值, 不跨年 ─────────────────────────

def _cumulative_cfg():
    return MonthRowsConfig(
        province="测试省",
        sheet_select="累计表",
        header_markers=("月份", "累计发电量"),
        month_col=("月份",),
        fuel_col_map={"累计发电量": "gen_total_full",
                      "火电累计": "gen_thermal_full"},
        cumulative_gen=True,
    )


def test_cumulative_differencing_and_january_raw_no_cross_year(tmp_path):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "累计表"
    ws.append(["月份", "累计发电量(亿千瓦时)", "火电累计(亿千瓦时)"])
    ws.append(["2025-12", 500.0, 400.0])
    ws.append(["2026-01", 50.0, 40.0])            # 1 月: 原值 50, 不得 50−500
    ws.append(["2026-02", 130.0, 100.0])          # 差分: 130−50 = 80
    ws.append(["2026-03", None, 150.0])           # 本月空 → None (行靠火电存活)
    ws.append(["2026-04", 400.0, None])           # 上月空 → None
    p = tmp_path / "cum.xlsx"
    wb.save(p)
    rows = parse_month_rows(p, _cumulative_cfg())
    by = _by_row(rows)
    jan = by[(date(2026, 1, 1), "gen_total_full")]
    assert jan.value == pytest.approx(50.0)
    assert "(差分)" not in jan.source             # 1 月直取不打标
    feb = by[(date(2026, 2, 1), "gen_total_full")]
    assert feb.value == pytest.approx(80.0)
    assert feb.source == "cum.xlsx(差分)"
    assert by[(date(2026, 3, 1), "gen_total_full")].value is None
    assert by[(date(2026, 4, 1), "gen_total_full")].value is None
    # 火电列同规则: 3 月 = 150−100, 4 月本月空 → None
    assert by[(date(2026, 3, 1), "gen_thermal_full")].value == pytest.approx(50.0)
    assert by[(date(2026, 4, 1), "gen_thermal_full")].value is None


# ── 选错 sheet → 解析错误, 不出垃圾行 ────────────────────────────────────────

def test_wrong_sheet_name_raises():
    bad = MonthRowsConfig(
        province="山东", sheet_select="不存在的sheet",
        header_markers=("月份",), month_col=("月份",),
        fuel_col_map={"火电装机": "cap_thermal_full"})
    with pytest.raises(ValueError):
        parse_month_rows(FIXTURE, bad)


def test_right_sheet_wrong_markers_raises():
    """sheet 名对但表头标记不命中 → ValueError (不得静默出数)."""
    bad = MonthRowsConfig(
        province="山东", sheet_select="10_装机与发电结构",
        header_markers=("月份", "结算电价"),       # 该 sheet 无此列
        month_col=("月份",), fuel_col_map={"火电装机": "cap_thermal_full"})
    with pytest.raises(ValueError):
        parse_month_rows(FIXTURE, bad)


# ── 单位闸门: 非 万千瓦/亿千瓦时/小时/% 的列整列跳过并计数 ────────────────────

def test_unrecognized_unit_column_skipped(tmp_path, caplog):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "10_装机与发电结构"
    ws.append(["标题"])
    ws.append(["月份", "火电装机(万kW)", "风电发电量(万kWh)"])  # 万kWh 不认可
    ws.append(["2025-01", "100.0", "9.9"])
    p = tmp_path / "badunit.xlsx"
    wb.save(p)
    cfg = MonthRowsConfig(
        province="测试省", sheet_select="10_装机与发电结构",
        header_markers=("月份", "火电装机"), month_col=("月份",),
        fuel_col_map={"火电装机": "cap_thermal_full",
                      "风电发电量": "gen_wind_full"})
    with caplog.at_level("INFO"):
        rows = parse_month_rows(p, cfg)
    metrics = {r.metric for r in rows}
    assert metrics == {"cap_thermal_full"}        # 万kWh 列被闸门拦下
    assert any("风电发电量" in r.getMessage() or "unit" in r.getMessage().lower()
               for r in caplog.records)


# ── sniff / parser 类 / registry 派发 ─────────────────────────────────────────

def test_sniff_accepts_shandong_fixture():
    assert shandong_parser.sniff(FIXTURE) is True


def test_sniff_rejects_other_workbook(tmp_path):
    wb = openpyxl.Workbook()
    wb.active.title = "别的表"
    wb.active.append(["foo"])
    p = tmp_path / "junk.xlsx"
    wb.save(p)
    assert shandong_parser.sniff(p) is False
    assert shandong_parser.sniff(tmp_path / "nope.xlsx") is False


def test_registry_dispatches_shandong_fixture():
    fmt, rows = registry.dispatch(FIXTURE)
    assert fmt == "month_rows"
    assert rows, "山东 fixture must parse non-empty"
    assert {r.province for r in rows} == {"山东"}
    by = {(r.month, r.metric): r.value for r in rows}
    assert by[(JAN25, "cap_thermal_full")] == pytest.approx(12137.63)
    assert by[(JAN25, "demand_total")] == pytest.approx(708.24)


def test_registry_known_formats_lists_month_rows():
    assert "month_rows" in registry.known_formats()
