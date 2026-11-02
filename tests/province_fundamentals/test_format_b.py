# tests/province_fundamentals/test_format_b.py
"""格式 B parser tests — N-省装机-月 利用小时月度工作簿.

Fixture: tests/province_fundamentals/fixtures/2-安徽装机-7月.xlsx
(ditto-copied from data/exchange-monthly-reports/安徽月报/ per the controller
ruling — tests must NOT read live data/ paths; OneDrive dataless files hang
the test run).

真值 (read from the real workbook, sheet 发电生产汇总-利用小时数数据):
  2025-01 总计: 期末容量 12635.64 万千瓦 | 本月发电量 299.53 亿千瓦时
               | 本月同比 -14.39% | 累计平均利用小时 239
  2025-01 火电: 6298.21 | 242.21 | -20.69% | 385
  2025-07 总计: 13879.153307 | 425.73606553 | 13% | 1712.67   (全精度浮点行)
  2026-01 总计: 14511.110154 | 361.88841919 | 20.8% | 252.15 (抽水蓄能无"其中:"前缀)

口径: 格式 B 为全口径 (2025-01 总计 12635.64 ≠ 格式 A 省调 6831.49) —
capacity 与 generation 指标一律 _full 后缀 (controller 裁定: gen 不与格式 A
省调 gen_* 撞键); 上报小时列原样入 hours_*; 推算小时入 hours_*_derived
(月度点估 ≠ 累计平均, 不混语义), source 另标 (推算).
"""
from datetime import date
from pathlib import Path

import openpyxl
import pytest

from services.province_fundamentals.parsers.format_b import parse_format_b, sniff

FIXTURE = (Path(__file__).parent / "fixtures" / "2-安徽装机-7月.xlsx")
FORMAT_A_FIXTURE = (Path(__file__).parent / "fixtures"
                    / "安徽电力市场信息披露月报数据-截至2026年5月.xlsx")
SRC = FIXTURE.name

JAN = date(2025, 1, 1)
STEMS = ("total", "hydro", "pumped", "thermal", "wind", "solar")


def _by_value(rows):
    return {(r.month, r.metric): r.value for r in rows}


def _by_row(rows):
    return {(r.month, r.metric): r for r in rows}


# ── 真值断言 (task brief Step 1) ──────────────────────────────────────────────

def test_format_b_anhui_2025_01_total_row():
    by = _by_row(parse_format_b(FIXTURE, "安徽"))
    cap = by[(JAN, "cap_total_full")]              # 全口径, 不是格式 A 的 6831.49
    assert cap.value == pytest.approx(12635.64)
    assert cap.section == "capacity"
    assert cap.unit == "万千瓦"
    assert cap.period == "month"
    assert cap.province == "安徽"
    assert cap.source == SRC
    gen = by[(JAN, "gen_total_full")]
    assert gen.value == pytest.approx(299.53)      # 当月值, 非差分
    assert gen.section == "generation"
    assert gen.unit == "亿千瓦时"
    assert "(差分)" not in gen.source and "(推算)" not in gen.source
    yoy = by[(JAN, "gen_total_full_yoy")]               # 同比列 → _yoy 后缀, 存数值
    assert yoy.value == pytest.approx(-14.39)
    assert yoy.unit == "%"
    hrs = by[(JAN, "hours_total")]
    assert hrs.value == pytest.approx(239)
    assert hrs.section == "util_hours"
    assert hrs.unit == "小时"


def test_format_b_anhui_2025_01_thermal_row():
    by = _by_value(parse_format_b(FIXTURE, "安徽"))
    assert by[(JAN, "cap_thermal_full")] == pytest.approx(6298.21)
    assert by[(JAN, "gen_thermal_full")] == pytest.approx(242.21)
    assert by[(JAN, "gen_thermal_full_yoy")] == pytest.approx(-20.69)
    assert by[(JAN, "hours_thermal")] == pytest.approx(385)


# ── 指标名映射 (词表) ──────────────────────────────────────────────────────────

def test_format_b_indicator_mapping_2025_01():
    """总计/水电/其中：抽水蓄能/火电/风电/太阳能发电 → total/hydro/pumped/thermal/wind/solar.
    抽水蓄能单列 pumped, 不并入 hydro."""
    by = _by_value(parse_format_b(FIXTURE, "安徽"))
    assert by[(JAN, "cap_hydro_full")] == pytest.approx(618.99)
    assert by[(JAN, "gen_hydro_full")] == pytest.approx(6.25)
    assert by[(JAN, "hours_hydro")] == pytest.approx(101)
    assert by[(JAN, "cap_pumped_full")] == pytest.approx(468)
    assert by[(JAN, "gen_pumped_full")] == pytest.approx(5.9)
    assert by[(JAN, "gen_pumped_full_yoy")] == pytest.approx(-6.47)
    assert by[(JAN, "hours_pumped")] == pytest.approx(126)
    assert by[(JAN, "cap_wind_full")] == pytest.approx(926.67)
    assert by[(JAN, "gen_wind_full")] == pytest.approx(17.26)
    assert by[(JAN, "hours_wind")] == pytest.approx(186)
    assert by[(JAN, "cap_solar_full")] == pytest.approx(4518.13)
    assert by[(JAN, "gen_solar_full")] == pytest.approx(33.32)
    assert by[(JAN, "hours_solar")] == pytest.approx(74)


def test_format_b_bare_pumped_label_2026():
    """2026 起 "抽水蓄能" 无 "其中：" 前缀 — 两种写法都要命中."""
    by = _by_value(parse_format_b(FIXTURE, "安徽"))
    jan26 = date(2026, 1, 1)
    assert by[(jan26, "cap_pumped_full")] == pytest.approx(468)
    assert by[(jan26, "gen_pumped_full")] == pytest.approx(5.84845354)
    assert by[(jan26, "gen_pumped_full_yoy")] == pytest.approx(-0.95)
    assert by[(jan26, "hours_pumped")] == pytest.approx(124.97)


def test_format_b_skips_subitem_rows():
    """燃煤机组/煤矸石/燃气/储能/6000千瓦以上/其中：光伏发电 等子项跳过,
    只取最粗口径 — 指标集合恰好 6  stems × 4 metrics."""
    rows = parse_format_b(FIXTURE, "安徽")
    metrics = {r.metric for r in rows}
    expected = ({f"cap_{s}_full" for s in STEMS}
                | {f"gen_{s}_full" for s in STEMS}
                | {f"gen_{s}_full_yoy" for s in STEMS}
                | {f"hours_{s}" for s in STEMS})
    assert metrics == expected


def test_format_b_solar_parent_row_wins_over_zhaoguang_dup():
    """2025 月同时有 太阳能发电 (父) 与 其中：光伏发电 (子, 同值) —
    同一 (month, metric) 只出一行, 取父行."""
    rows = parse_format_b(FIXTURE, "安徽")
    solar = [r for r in rows if r.month == JAN and r.metric == "cap_solar_full"]
    assert len(solar) == 1
    assert solar[0].value == pytest.approx(4518.13)


# ── 当月值 / 累计列 / 类型 ────────────────────────────────────────────────────

def test_format_b_generation_is_monthly_value_not_diff():
    """格式 B 本月发电量本身是当月值 — 2025-02 = 288.97 原值, 不做差分."""
    by = _by_row(parse_format_b(FIXTURE, "安徽"))
    feb = by[(date(2025, 2, 1), "gen_total_full")]
    assert feb.value == pytest.approx(288.97)
    assert feb.source == SRC
    jan26 = by[(date(2026, 1, 1), "gen_total_full")]
    assert jan26.value == pytest.approx(361.88841919)


def test_format_b_hours_column_taken_as_is():
    """设备累计平均利用小时 列直取 (不差不推算), source 不打标."""
    by = _by_row(parse_format_b(FIXTURE, "安徽"))
    feb = by[(date(2025, 2, 1), "hours_total")]
    assert feb.value == pytest.approx(468)
    assert feb.source == SRC


def test_format_b_string_typed_numbers():
    """2025-03..12 数值列为文本 ('12936.14') — 与浮点行同值解析."""
    by = _by_value(parse_format_b(FIXTURE, "安徽"))
    mar = date(2025, 3, 1)
    assert by[(mar, "cap_total_full")] == pytest.approx(12936.14)
    assert by[(mar, "gen_total_full")] == pytest.approx(316.00)
    assert by[(mar, "gen_total_full_yoy")] == pytest.approx(5.34)
    assert by[(mar, "hours_total")] == pytest.approx(717)


def test_format_b_full_precision_float_rows():
    """2025-07 行为全精度浮点."""
    by = _by_value(parse_format_b(FIXTURE, "安徽"))
    jul = date(2025, 7, 1)
    assert by[(jul, "cap_total_full")] == pytest.approx(13879.153307)
    assert by[(jul, "gen_total_full")] == pytest.approx(425.73606553)
    assert by[(jul, "gen_total_full_yoy")] == pytest.approx(13)
    assert by[(jul, "hours_total")] == pytest.approx(1712.67)


def test_format_b_full_suffix_on_capacity_and_generation():
    """全口径: capacity + generation 指标一律 _full; util_hours 不上报口径后缀
    (hours_* 上报原值 / hours_*_derived 推算)."""
    rows = parse_format_b(FIXTURE, "安徽")
    for r in rows:
        if r.section in ("capacity", "generation"):
            assert "_full" in r.metric
        else:
            assert "_full" not in r.metric


# ── 空格与推算 ────────────────────────────────────────────────────────────────

def test_format_b_reported_empty_yoy_emits_none():
    """2026-05 水电 本月同比为空 → 仍出行, value=None (reported-but-empty)."""
    by = _by_row(parse_format_b(FIXTURE, "安徽"))
    r = by[(date(2026, 5, 1), "gen_hydro_full_yoy")]
    assert r.value is None
    assert r.unit == "%"
    # 同行其余指标正常
    byv = _by_value(parse_format_b(FIXTURE, "安徽"))
    assert byv[(date(2026, 5, 1), "cap_hydro_full")] == pytest.approx(619.51)
    assert byv[(date(2026, 5, 1), "gen_hydro_full")] == pytest.approx(10.08)
    assert byv[(date(2026, 5, 1), "hours_hydro")] == pytest.approx(574)


def test_format_b_hours_derived_when_cell_missing(tmp_path):
    """小时列缺 → 本月发电量÷期末容量×10000 推算, 入 hours_<stem>_derived
    (月度点估不混入累计平均 hours_*), source 标 (推算)."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "发电生产汇总-利用小时数数据"
    ws.append(["年月", "年份", "月份", "指标名称", "期末发电设备容量（万千瓦）",
               "本月发电量（亿千瓦时）", "本月同比（%）", "本年累计发电量（亿千瓦时）",
               "本年累计同比（%）", "设备累计平均利用小时（小时）", "利用小时增减"])
    ws.append(["2025-01", 2025, "1月", "风电", 2000.0, 50.0, 10.0,
               50.0, 10.0, None, None])          # 小时缺 → 50/2000*10000 = 250
    p = tmp_path / "mini-b.xlsx"
    wb.save(p)
    rows = parse_format_b(p, "测试省")
    hrs = next(r for r in rows if r.metric == "hours_wind_derived")
    assert hrs.value == pytest.approx(250.0)
    assert hrs.source == "mini-b.xlsx(推算)"
    assert hrs.section == "util_hours"
    assert hrs.unit == "小时"
    # 语义隔离: 推算值绝不写 hours_*
    assert not [r for r in rows if r.metric == "hours_wind"]
    # 其余指标不打推算标
    assert next(r for r in rows if r.metric == "cap_wind_full").source == "mini-b.xlsx"


def test_format_b_hours_none_when_underivable(tmp_path):
    """小时缺且容量缺/为 0 → hours_<stem> value=None (上报格为空),
    source 不打 (推算) 标, 且不出 _derived 行 (未发生推算)."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "发电生产汇总-利用小时数数据"
    ws.append(["年月", "指标名称", "期末发电设备容量（万千瓦）",
               "本月发电量（亿千瓦时）", "本月同比（%）",
               "设备累计平均利用小时（小时）"])
    ws.append(["2025-01", "水电", None, 5.0, 1.0, None])
    p = tmp_path / "mini-b2.xlsx"
    wb.save(p)
    rows = parse_format_b(p, "测试省")
    hrs = next(r for r in rows if r.metric == "hours_hydro")
    assert hrs.value is None
    assert hrs.source == "mini-b2.xlsx"
    assert not [r for r in rows if r.metric == "hours_hydro_derived"]


# ── 覆盖率与结构 ──────────────────────────────────────────────────────────────

def test_format_b_full_coverage_row_count():
    """18 个月 (2025-01..07, 09..12, 2026-01..07) × 6 stems × 4 metrics = 432 行.
    全表仅两个空值: 2026-05 水电同比 + 2026-06 抽水蓄能同比 (源文件空格)."""
    rows = parse_format_b(FIXTURE, "安徽")
    assert len(rows) == 18 * 6 * 4
    assert all(r.period == "month" for r in rows)
    assert all(r.province == "安徽" for r in rows)
    nones = sorted((r.month, r.metric) for r in rows if r.value is None)
    assert nones == [(date(2026, 5, 1), "gen_hydro_full_yoy"),
                     (date(2026, 6, 1), "gen_pumped_full_yoy")]
    assert len({r.month for r in rows}) == 18     # 2025-08 源文件即缺, 不臆造


# ── sniff ─────────────────────────────────────────────────────────────────────

def test_sniff_accepts_format_b_fixture():
    assert sniff(FIXTURE) is True


def test_sniff_rejects_format_a_fixture():
    """格式 A 工作簿 (sheet 装机及发电情况) 不得命中格式 B."""
    assert sniff(FORMAT_A_FIXTURE) is False


def test_sniff_rejects_workbook_without_markers(tmp_path):
    wb = openpyxl.Workbook()
    wb.active.title = "利用小时数数据"            # sheet 名对但表头不对
    wb.active.append(["foo", "bar"])
    p = tmp_path / "junk.xlsx"
    wb.save(p)
    assert sniff(p) is False


def test_sniff_rejects_missing_file(tmp_path):
    assert sniff(tmp_path / "nope.xlsx") is False


# ── 空行/NaT/占位符跳过 ────────────────────────────────────────────────────────

def test_parse_skips_blank_nat_placeholder_and_empty_rows(tmp_path):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "发电生产汇总-利用小时数数据"
    ws.append(["年月", "指标名称", "期末发电设备容量（万千瓦）",
               "本月发电量（亿千瓦时）", "本月同比（%）",
               "设备累计平均利用小时（小时）"])
    ws.append(["2025-01", "总计", 100.0, 10.0, 5.0, 100.0])   # good
    ws.append([None, None, None, None, None, None])            # 空行 → skip
    ws.append(["not-a-month", "总计", 1.0, 1.0, 1.0, 1.0])     # NaT → skip
    ws.append(["2025-02", "总计", None, None, None, None])     # 全空 → skip
    ws.append(["2025-03", "火电", "—", "–", "-", None])        # 占位符 → 全空 skip
    ws.append(["2025-04", "燃煤机组", 1.0, 1.0, 1.0, 1.0])     # 子项 → 忽略
    p = tmp_path / "mini-b3.xlsx"
    wb.save(p)
    rows = parse_format_b(p, "测试省")
    assert len(rows) == 4                        # cap/gen/yoy/hours 各一
    assert all(r.month == date(2025, 1, 1) for r in rows)
    by = {r.metric: r for r in rows}
    assert by["cap_total_full"].value == pytest.approx(100.0)
    assert by["gen_total_full"].value == pytest.approx(10.0)
    assert by["gen_total_full_yoy"].value == pytest.approx(5.0)
    assert by["hours_total"].value == pytest.approx(100.0)


def test_all_empty_row_does_not_block_later_data_row(tmp_path):
    """同 (month, stem) 先出现全空行后出现数据行: 全空行只计数不占位,
    数据行必须正常出 (全空不视为 "先见者")."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "发电生产汇总-利用小时数数据"
    ws.append(["年月", "指标名称", "期末发电设备容量（万千瓦）",
               "本月发电量（亿千瓦时）", "本月同比（%）",
               "设备累计平均利用小时（小时）"])
    ws.append(["2025-01", "风电", None, None, None, None])     # 全空 → skip
    ws.append(["2025-01", "风电", 926.67, 17.26, 33.04, 186])  # 数据行
    p = tmp_path / "mini-b4.xlsx"
    wb.save(p)
    rows = parse_format_b(p, "测试省")
    assert len(rows) == 4
    by = {r.metric: r for r in rows}
    assert by["cap_wind_full"].value == pytest.approx(926.67)
    assert by["hours_wind"].value == pytest.approx(186)
