# tests/province_fundamentals/test_format_a.py
"""格式 A parser tests — 月报数据累计工作簿.

Fixture: tests/province_fundamentals/fixtures/安徽电力市场信息披露月报数据-截至2026年5月.xlsx
(copied once from data/exchange-monthly-reports/安徽月报/ with `ditto` per the
controller ruling — tests must NOT read live data/ paths; OneDrive dataless
files hang the test run).

真值 (read from the real workbook, sheet 装机及发电情况):
  2025-01 | 总 6831.49 | 火电 4321.6 | 水电 55.41 | 风电 905.31 | 光伏 1307.29
          | 储能 241.88 | 累计发电量 239.04
"""
from datetime import date
from pathlib import Path

import openpyxl
import pytest

from services.province_fundamentals.parsers.format_a import parse_format_a, sniff

FIXTURE = (Path(__file__).parent / "fixtures"
           / "安徽电力市场信息披露月报数据-截至2026年5月.xlsx")
SRC = FIXTURE.name


def _by_value(rows):
    return {(r.month, r.metric): r.value for r in rows}


def _by_row(rows):
    return {(r.month, r.metric): r for r in rows}


# ── 真值断言 (task brief Step 1) ──────────────────────────────────────────────

def test_format_a_anhui_2025_01_capacity_rows():
    rows = parse_format_a(FIXTURE, "安徽")
    by = _by_value(rows)
    assert by[(date(2025, 1, 1), "cap_total")] == pytest.approx(6831.49)
    assert by[(date(2025, 1, 1), "cap_thermal")] == pytest.approx(4321.6)
    assert by[(date(2025, 1, 1), "cap_storage")] == pytest.approx(241.88)


def test_format_a_january_generation_is_cumulative_value_not_diff():
    # 2025-01 gen_total 必须 == 239.04(累计原值), 不得减上年12月
    rows = parse_format_a(FIXTURE, "安徽")
    by = _by_row(rows)
    jan25 = by[(date(2025, 1, 1), "gen_total")]
    assert jan25.value == pytest.approx(239.04)
    assert "(差分)" not in jan25.source          # 直取行不打差分标
    jan26 = by[(date(2026, 1, 1), "gen_total")]
    assert jan26.value == pytest.approx(241.84)  # 不是 241.84 − 2489.68
    assert "(差分)" not in jan26.source


# ── 装机列: 期末值直取, 行级约定 ──────────────────────────────────────────────

def test_format_a_capacity_row_conventions():
    rows = parse_format_a(FIXTURE, "安徽")
    r = _by_row(rows)[(date(2025, 1, 1), "cap_total")]
    assert r.province == "安徽"
    assert r.period == "month"
    assert r.section == "capacity"
    assert r.unit == "万千瓦"
    assert r.source == SRC                        # 直取: 无差分标


def test_format_a_capacity_all_fuels_2025_01():
    by = _by_value(parse_format_a(FIXTURE, "安徽"))
    jan = date(2025, 1, 1)
    assert by[(jan, "cap_hydro")] == pytest.approx(55.41)
    assert by[(jan, "cap_wind")] == pytest.approx(905.31)
    assert by[(jan, "cap_solar")] == pytest.approx(1307.29)


# ── 发电量列: 年内累计 → 当月差分 ─────────────────────────────────────────────

def test_format_a_generation_diff_rows():
    rows = parse_format_a(FIXTURE, "安徽")
    by = _by_row(rows)
    feb = by[(date(2025, 2, 1), "gen_total")]
    assert feb.value == pytest.approx(467.77 - 239.04)
    assert feb.source == f"{SRC}(差分)"
    assert feb.section == "generation"
    assert feb.unit == "亿千瓦时"
    assert feb.period == "month"
    # 分电源同样差分
    assert by[(date(2025, 2, 1), "gen_thermal")].value == pytest.approx(
        368.28 - 189.24)
    assert by[(date(2025, 2, 1), "gen_wind")].value == pytest.approx(
        32.6 - 16.98)


def test_format_a_second_year_diff_never_crosses_year_boundary():
    """2026-02 的差分基准是 2026-01 (同年), 绝不是 2025-12."""
    by = _by_value(parse_format_a(FIXTURE, "安徽"))
    assert by[(date(2026, 2, 1), "gen_total")] == pytest.approx(408.24 - 241.84)


def test_format_a_2025_09_restatement_negative_diff_kept_as_is():
    """2025-09 累计发电量 1853.97 < 2025-08 2110.06 — 上游口径重述.
    解析器按规则原样差分 (负当月值), 交给 store 层 0.5% 冲突闸门处理,
    不做静默修正."""
    by = _by_value(parse_format_a(FIXTURE, "安徽"))
    assert by[(date(2025, 9, 1), "gen_total")] == pytest.approx(
        1853.97 - 2110.06)


# ── 覆盖率与结构 ──────────────────────────────────────────────────────────────

def test_format_a_full_coverage_row_count():
    """17 个月 (2025-01..2026-05) × 6 装机 + 5 发电量 = 187 行, 全部 month 期."""
    rows = parse_format_a(FIXTURE, "安徽")
    cap = [r for r in rows if r.section == "capacity"]
    gen = [r for r in rows if r.section == "generation"]
    assert len(cap) == 17 * 6
    assert len(gen) == 17 * 5
    assert all(r.period == "month" for r in rows)
    assert all(r.province == "安徽" for r in rows)
    assert all(r.value is not None for r in rows)  # 该工作簿无空格


def test_format_a_last_month_values():
    by = _by_value(parse_format_a(FIXTURE, "安徽"))
    may26 = date(2026, 5, 1)
    assert by[(may26, "cap_total")] == pytest.approx(7752.14)
    assert by[(may26, "cap_storage")] == pytest.approx(300.01)
    assert by[(may26, "gen_total")] == pytest.approx(991.98 - 800.55)


# ── sniff ─────────────────────────────────────────────────────────────────────

def test_sniff_accepts_format_a_fixture():
    assert sniff(FIXTURE) is True


def test_sniff_rejects_workbook_without_markers(tmp_path):
    wb = openpyxl.Workbook()
    wb.active.title = "别的表"
    wb.active.append(["foo", "bar"])
    p = tmp_path / "junk.xlsx"
    wb.save(p)
    assert sniff(p) is False


def test_sniff_rejects_missing_file(tmp_path):
    assert sniff(tmp_path / "nope.xlsx") is False


# ── 空行/NaT 跳过并计数 ───────────────────────────────────────────────────────

def test_parse_skips_blank_nat_and_empty_rows(tmp_path):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "xx装机及发电情况xx"
    ws.append(["标题行"])
    ws.append(["月份", "省调口径装机总容量(万千瓦)", "火电装机(万千瓦)",
               "累计发电量(亿千瓦时)"])
    ws.append(["2025-01", 100.0, 60.0, 10.0])   # good row
    ws.append([None, None, None, None])          # 空行 → skip
    ws.append(["not-a-month", 1.0, 1.0, 1.0])    # NaT → skip
    ws.append(["2025-02", None, None, None])     # 有月份但全空 → skip
    p = tmp_path / "mini.xlsx"
    wb.save(p)
    rows = parse_format_a(p, "测试省")
    assert len(rows) == 3                        # cap_total, cap_thermal, gen_total
    assert all(r.month == date(2025, 1, 1) for r in rows)
    by = {(r.metric): r for r in rows}
    assert by["cap_total"].value == pytest.approx(100.0)
    assert by["cap_thermal"].value == pytest.approx(60.0)
    assert by["gen_total"].value == pytest.approx(10.0)  # 1月直取
    assert by["gen_total"].source == "mini.xlsx"         # 直取无差分标
