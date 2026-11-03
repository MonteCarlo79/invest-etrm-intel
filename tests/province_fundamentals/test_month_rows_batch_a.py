# tests/province_fundamentals/test_month_rows_batch_a.py
"""月行宽表 parser tests — Wave-1 Batch A: 山西 / 陕西 / 蒙西 / 辽宁.

Fixtures ditto'd from data/exchange-monthly-reports/<省>月报/ (controller ruling:
tests never read live data/ paths).

真值 (read from the real workbooks, 2026-11-04, openpyxl data_only):

山西 山西信息披露月报数据_2025至2026年5月.xlsx (12 sheets; 表头 R1; datetime 月份):
  装机容量   2025-01 | 总装机 10781.02 | 常规火电 5001.5 | 风电 2513.24
                     | 光伏 2566.38 | 储能 199.98
             2026-05 | 总装机 12087.83 | 常规火电 4992 | 风电 2742.96 | 光伏 3450.29
  月度发电量 2025-01 | 总 298.32 | 火电 208.07 | 水电 3.95 | 风电 63.39 | 光伏 22.11
             2026-04 | 水电 -2.53 (累计重述倒挤负值, 原样出) | 总 243.98
             2026-05 | 总 252.56 | 火电 160.08
  (累计发电量 双表不解析 — 当月表直取)

陕西 陕西-信息披露月报数据_2025年1月-2026年5月.xlsx (23 sheets; 表头 R2/R3 漂移):
  装机规模(R3 表头) 2025-01 | 水电 505.52 | 火电 6267.85 | 风电 1497.77
                     | 太阳能 3524.55 | 新型储能 空 | 合计 11795.65
             2025-04~08 空行 → 不出行; R44 备注说明行跳过;
             2026-05 搁浅于 R45 (备注行之下) | 火电 6356.33 | 风电 1774.12
                     | 太阳能 4276.65 | 新型储能 9.8 | 合计 12923.26
  全网发电量(R3) 2025-01 | 火电 252.03 | 风电 19.63 | 光伏 25.48 | 水电 3.97
                     | 合计 301.1 | 火电同比 -13.11
             2026-05 | 火电 165.47 | 合计 249.88 | 合计同比 -6.1
  全社会用电(R2) 2025-01 | 合计 238.42 | 同比 -0.84
             2026-05 | 合计 204.6  | 同比 -0.68

蒙西 蒙西信息披露月报数据汇总_2022至2026年5月.xlsx (11 sheets; 表头 R2):
  1_电网概况 行预留 2022-01→2026-05; 2025-03 及之前全空 (整行跳过);
             2025-04 | 风电 2851.826, 其余全空 → None (不写 0)
             2025-06 | 总装机 12944.598 | 火电 5283.95 | 水电 208.8
                     | 风电 3713.39 | 光伏 3128.16 | 储能 610.29
             2026-05 | 总装机 16769.4 | 火电 5700 | 风电 4976 | 光伏 4371.2
                     | 储能 1513.4

辽宁 辽宁-信息披露月报-2025至2026年5月.xlsx (6 sheets; 表头 R2; 总量版无分燃料):
  月度概况   2025-01 | 全社会用电量 255.74 | 同比 -2.43 | 全省发电量 217.28
                     | 发电量同比 -2.27 | 发电装机容量 8212
             2025-05 全空行 → 跳过
             2026-05 | 全社会用电量 228.46 | 同比 2.31 | 全省发电量 213.4
                     | 发电量同比 6.95 | 发电装机容量 9428.15
"""
from datetime import date
from pathlib import Path

import openpyxl
import pytest

from services.province_fundamentals import registry
from services.province_fundamentals.parsers.month_rows import (
    MonthRowsConfig,
    parse_month_rows,
    LIAONING_OVERVIEW,
    MENGXI_CAP,
    SHAANXI_CAP,
    SHAANXI_DEMAND,
    SHAANXI_GEN,
    SHANXI_CAP,
    SHANXI_GEN,
    liaoning_parser,
    mengxi_parser,
    shaanxi_parser,
    shanxi_parser,
)

FIX = Path(__file__).parent / "fixtures"
SX_PATH = FIX / "山西信息披露月报数据_2025至2026年5月.xlsx"
SN_PATH = FIX / "陕西-信息披露月报数据_2025年1月-2026年5月.xlsx"
MX_PATH = FIX / "蒙西信息披露月报数据汇总_2022至2026年5月.xlsx"
LN_PATH = FIX / "辽宁-信息披露月报-2025至2026年5月.xlsx"

JAN25 = date(2025, 1, 1)
MAY26 = date(2026, 5, 1)


def _by_value(rows):
    return {(r.month, r.metric): r.value for r in rows}


def _by_row(rows):
    return {(r.month, r.metric): r for r in rows}


# ── 山西: datetime 月份单元格 + 当月/累计双表取当月 ────────────────────────────

def test_shanxi_2025_01_capacity_truth():
    by = _by_value(parse_month_rows(SX_PATH, SHANXI_CAP))
    assert by[(JAN25, "cap_total_full")] == pytest.approx(10781.02)
    assert by[(JAN25, "cap_thermal_full")] == pytest.approx(5001.5)
    assert by[(JAN25, "cap_wind_full")] == pytest.approx(2513.24)
    assert by[(JAN25, "cap_solar_full")] == pytest.approx(2566.38)
    assert by[(JAN25, "cap_storage_full")] == pytest.approx(199.98)


def test_shanxi_datetime_month_cells_parse():
    """月份列是真实 datetime 单元格 (非字符串) — 必须解析为月初 date."""
    rows = parse_month_rows(SX_PATH, SHANXI_CAP)
    r = _by_row(rows)[(JAN25, "cap_thermal_full")]
    assert r.month == date(2025, 1, 1)
    assert r.province == "山西"
    assert r.section == "capacity"
    assert r.unit == "万千瓦"
    assert r.source == SX_PATH.name


def test_shanxi_generation_truth_and_negative_hydro_as_is():
    by = _by_value(parse_month_rows(SX_PATH, SHANXI_GEN))
    assert by[(JAN25, "gen_total_full")] == pytest.approx(298.32)
    assert by[(JAN25, "gen_thermal_full")] == pytest.approx(208.07)
    assert by[(JAN25, "gen_hydro_full")] == pytest.approx(3.95)
    # 2026-04 水电累计重述倒挤为负 — 原样出, 不截断不置 None
    apr26 = date(2026, 4, 1)
    assert by[(apr26, "gen_hydro_full")] == pytest.approx(-2.53)
    assert by[(apr26, "gen_total_full")] == pytest.approx(243.98)
    assert by[(MAY26, "gen_total_full")] == pytest.approx(252.56)
    assert by[(MAY26, "gen_thermal_full")] == pytest.approx(160.08)


def test_shanxi_monthly_sheet_only_no_cumulative_diff():
    """当月表直取: source 无 (差分) 标; 行数 17月×(6装机+6发电)=204,
    累计发电量 sheet 若被误解析行数必超."""
    rows = shanxi_parser.parse(SX_PATH, "山西")
    assert len(rows) == 204
    assert all("(差分)" not in r.source for r in rows)
    months = {r.month for r in rows}
    assert len(months) == 17
    assert min(months) == JAN25 and max(months) == MAY26


def test_shanxi_registry_dispatch():
    fmt, rows = registry.dispatch(SX_PATH)
    assert fmt == "month_rows"
    assert {r.province for r in rows} == {"山西"}
    by = {(r.month, r.metric): r.value for r in rows}
    assert by[(JAN25, "cap_total_full")] == pytest.approx(10781.02)
    assert by[(JAN25, "gen_total_full")] == pytest.approx(298.32)


# ── 陕西: 表头 R2/R3 漂移 + 空行缺口 + 备注行下搁浅 2026-05 ────────────────────

def test_shaanxi_capacity_truth_header_at_r3():
    by = _by_value(parse_month_rows(SN_PATH, SHAANXI_CAP))
    assert by[(JAN25, "cap_hydro_full")] == pytest.approx(505.52)
    assert by[(JAN25, "cap_thermal_full")] == pytest.approx(6267.85)
    assert by[(JAN25, "cap_wind_full")] == pytest.approx(1497.77)
    assert by[(JAN25, "cap_solar_full")] == pytest.approx(3524.55)
    assert by[(JAN25, "cap_total_full")] == pytest.approx(11795.65)


def test_shaanxi_stranded_2026_05_below_note_row_captured():
    """2026-05 在 R44 备注行之下 (R45): 不得当作备注跳过, 不得被首个空行截断."""
    by = _by_value(parse_month_rows(SN_PATH, SHAANXI_CAP))
    assert by[(MAY26, "cap_thermal_full")] == pytest.approx(6356.33)
    assert by[(MAY26, "cap_wind_full")] == pytest.approx(1774.12)
    assert by[(MAY26, "cap_solar_full")] == pytest.approx(4276.65)
    assert by[(MAY26, "cap_storage_full")] == pytest.approx(9.8)
    assert by[(MAY26, "cap_total_full")] == pytest.approx(12923.26)


def test_shaanxi_blank_gap_months_emit_nothing_and_storage_none():
    rows = parse_month_rows(SN_PATH, SHAANXI_CAP)
    months = {r.month for r in rows}
    assert len(months) == 12                    # 17 − 5 空行 (2025-04..08)
    for m in (date(2025, k, 1) for k in range(4, 9)):
        assert m not in months                  # 空行整行跳过, 不出 None 行
    # 新型储能 2025-01 单元格为空 → value=None, 绝不写 0
    r = _by_row(rows)[(JAN25, "cap_storage_full")]
    assert r.value is None
    assert r.unit == "万千瓦"


def test_shaanxi_generation_truth_header_at_r3():
    by = _by_value(parse_month_rows(SN_PATH, SHAANXI_GEN))
    assert by[(JAN25, "gen_thermal_full")] == pytest.approx(252.03)
    assert by[(JAN25, "gen_wind_full")] == pytest.approx(19.63)
    assert by[(JAN25, "gen_solar_full")] == pytest.approx(25.48)
    assert by[(JAN25, "gen_hydro_full")] == pytest.approx(3.97)
    assert by[(JAN25, "gen_total_full")] == pytest.approx(301.1)
    assert by[(JAN25, "gen_thermal_full_yoy")] == pytest.approx(-13.11)
    assert by[(MAY26, "gen_thermal_full")] == pytest.approx(165.47)
    assert by[(MAY26, "gen_total_full")] == pytest.approx(249.88)
    assert by[(MAY26, "gen_total_full_yoy")] == pytest.approx(-6.1)


def test_shaanxi_demand_truth_header_at_r2():
    by = _by_row(parse_month_rows(SN_PATH, SHAANXI_DEMAND))
    r = by[(JAN25, "demand_total")]
    assert r.value == pytest.approx(238.42)
    assert r.section == "demand"
    assert r.unit == "亿千瓦时"
    assert by[(JAN25, "demand_yoy")].value == pytest.approx(-0.84)
    assert by[(MAY26, "demand_total")].value == pytest.approx(204.6)
    assert by[(MAY26, "demand_yoy")].value == pytest.approx(-0.68)


def test_shaanxi_registry_dispatch_23_sheet_workbook():
    """23 个 sheet 的工作簿: 按名选 3 个目标 sheet, 不被长表/双表头 sheet 干扰."""
    fmt, rows = registry.dispatch(SN_PATH)
    assert fmt == "month_rows"
    assert {r.province for r in rows} == {"陕西"}
    # 12月×6装机 + 17月×10发电(含同比) + 17月×2用电 = 72+170+34
    assert len(rows) == 276
    by = {(r.month, r.metric): r.value for r in rows}
    assert by[(JAN25, "cap_thermal_full")] == pytest.approx(6267.85)
    assert by[(JAN25, "gen_total_full")] == pytest.approx(301.1)
    assert by[(JAN25, "demand_total")] == pytest.approx(238.42)


# ── 蒙西: 稀疏填充 (2025-06 起) + 限定语括号注记 + 省名规范化 ──────────────────

def test_mengxi_2025_06_first_full_month_truth():
    by = _by_value(parse_month_rows(MX_PATH, MENGXI_CAP))
    jun25 = date(2025, 6, 1)
    assert by[(jun25, "cap_total_full")] == pytest.approx(12944.598)
    assert by[(jun25, "cap_thermal_full")] == pytest.approx(5283.95)
    assert by[(jun25, "cap_hydro_full")] == pytest.approx(208.8)
    assert by[(jun25, "cap_wind_full")] == pytest.approx(3713.39)
    assert by[(jun25, "cap_solar_full")] == pytest.approx(3128.16)
    assert by[(jun25, "cap_storage_full")] == pytest.approx(610.29)
    assert by[(MAY26, "cap_total_full")] == pytest.approx(16769.4)
    assert by[(MAY26, "cap_wind_full")] == pytest.approx(4976)
    assert by[(MAY26, "cap_storage_full")] == pytest.approx(1513.4)


def test_mengxi_sparse_fill_none_not_zero():
    """2025-04/05 仅风电有值: 其余 metric 出 None (绝不写 0);
    2025-03 及之前全空行整行跳过 (不出行)."""
    rows = parse_month_rows(MX_PATH, MENGXI_CAP)
    months = {r.month for r in rows}
    assert len(months) == 14                    # 2025-04 .. 2026-05
    assert min(months) == date(2025, 4, 1)      # 更早月份不出行
    by = _by_row(rows)
    apr = date(2025, 4, 1)
    assert by[(apr, "cap_wind_full")].value == pytest.approx(2851.826)
    for m in ("cap_total_full", "cap_thermal_full", "cap_hydro_full",
              "cap_solar_full", "cap_storage_full"):
        assert by[(apr, m)].value is None
        assert by[(apr, m)].value != 0


def test_mengxi_qualifier_notes_survive_unit_gate():
    """光伏(含光热)/储能(含配储) 括号是限定语非单位: ignore_notes 放行,
    若被单位闸门误杀这两列将整列缺席."""
    rows = parse_month_rows(MX_PATH, MENGXI_CAP)
    metrics = {r.metric for r in rows}
    assert "cap_solar_full" in metrics
    assert "cap_storage_full" in metrics
    for r in rows:
        assert r.unit == "万千瓦"
        assert r.section == "capacity"


def test_mengxi_province_canonical_not_neimenggu():
    """province 恒为注册名 蒙西 (cfg.province 决定; sheet 内容不参与命名)."""
    rows = mengxi_parser.parse(MX_PATH, "蒙西")
    assert rows
    assert {r.province for r in rows} == {"蒙西"}
    assert len(rows) == 84                      # 14 月 × 6 装机


def test_mengxi_registry_dispatch():
    fmt, rows = registry.dispatch(MX_PATH)
    assert fmt == "month_rows"
    assert {r.province for r in rows} == {"蒙西"}
    by = {(r.month, r.metric): r.value for r in rows}
    assert by[(date(2025, 6, 1), "cap_total_full")] == pytest.approx(12944.598)


def test_mengxi_sniff_rejects_liaoning_fixture():
    assert mengxi_parser.sniff(LN_PATH) is False
    assert liaoning_parser.sniff(MX_PATH) is False


# ── 辽宁: 总量版 (无分燃料列) ──────────────────────────────────────────────────

def test_liaoning_totals_truth():
    by = _by_value(parse_month_rows(LN_PATH, LIAONING_OVERVIEW))
    assert by[(JAN25, "demand_total")] == pytest.approx(255.74)
    assert by[(JAN25, "demand_yoy")] == pytest.approx(-2.43)
    assert by[(JAN25, "gen_total_full")] == pytest.approx(217.28)
    assert by[(JAN25, "gen_total_full_yoy")] == pytest.approx(-2.27)
    assert by[(JAN25, "cap_total_full")] == pytest.approx(8212)
    assert by[(MAY26, "demand_total")] == pytest.approx(228.46)
    assert by[(MAY26, "demand_yoy")] == pytest.approx(2.31)
    assert by[(MAY26, "gen_total_full")] == pytest.approx(213.4)
    assert by[(MAY26, "gen_total_full_yoy")] == pytest.approx(6.95)
    assert by[(MAY26, "cap_total_full")] == pytest.approx(9428.15)


def test_liaoning_totals_only_no_fuel_metrics():
    """无分燃料列: 只出 5 个总量 metric, 不出 cap_thermal_full 等键 (键缺席)."""
    rows = liaoning_parser.parse(LN_PATH, "辽宁")
    assert {r.metric for r in rows} == {
        "demand_total", "demand_yoy",
        "gen_total_full", "gen_total_full_yoy", "cap_total_full"}
    sections = {r.metric: r.section for r in rows}
    assert sections["cap_total_full"] == "capacity"
    assert sections["gen_total_full"] == "generation"
    assert sections["demand_total"] == "demand"
    units = {r.metric: r.unit for r in rows}
    assert units["cap_total_full"] == "万千瓦"
    assert units["gen_total_full"] == "亿千瓦时"
    assert units["demand_yoy"] == "%"


def test_liaoning_blank_month_skipped_coverage():
    """2025-05 全空行跳过: 16 个月 × 5 metric = 80 行."""
    rows = liaoning_parser.parse(LN_PATH, "辽宁")
    assert len(rows) == 80
    months = {r.month for r in rows}
    assert len(months) == 16
    assert date(2025, 5, 1) not in months


def test_liaoning_registry_dispatch():
    fmt, rows = registry.dispatch(LN_PATH)
    assert fmt == "month_rows"
    assert {r.province for r in rows} == {"辽宁"}
    by = {(r.month, r.metric): r.value for r in rows}
    assert by[(JAN25, "demand_total")] == pytest.approx(255.74)
    assert by[(JAN25, "cap_total_full")] == pytest.approx(8212)


# ── 累计差分 × datetime 月份键 (山西型单元格走差分路径) ────────────────────────

def test_cumulative_differencing_with_datetime_month_cells(tmp_path):
    """cumulative_gen=True + datetime 月份: 1 月原值不跨年, 2 月差分打标."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "累计表"
    ws.append(["月份", "累计总发电量(亿kWh)", "火电累计(亿kWh)"])
    from datetime import datetime
    ws.append([datetime(2025, 12, 1), 500.0, 400.0])
    ws.append([datetime(2026, 1, 1), 50.0, 40.0])    # 1 月: 原值, 不得减上年 12 月
    ws.append([datetime(2026, 2, 1), 130.0, 100.0])  # 差分: 130−50 = 80
    ws.append([datetime(2026, 3, 1), None, 150.0])   # 本月空 → None (行靠火电存活)
    p = tmp_path / "cum_dt.xlsx"
    wb.save(p)
    cfg = MonthRowsConfig(
        province="测试省", sheet_select="累计表",
        header_markers=("月份", "累计总发电量"), month_col=("月份",),
        fuel_col_map={"累计总发电量": "gen_total_full",
                      "火电累计": "gen_thermal_full"},
        cumulative_gen=True)
    by = _by_row(parse_month_rows(p, cfg))
    jan = by[(date(2026, 1, 1), "gen_total_full")]
    assert jan.value == pytest.approx(50.0)
    assert "(差分)" not in jan.source
    feb = by[(date(2026, 2, 1), "gen_total_full")]
    assert feb.value == pytest.approx(80.0)
    assert feb.source == "cum_dt.xlsx(差分)"
    assert by[(date(2026, 3, 1), "gen_total_full")].value is None
    # 火电列同规则: 3 月 = 150−100
    assert by[(date(2026, 3, 1), "gen_thermal_full")].value == pytest.approx(50.0)


# ── 全四省 registry 派发不互相截胡 ─────────────────────────────────────────────

def test_all_four_provinces_registered_and_isolated():
    for path, prov in ((SX_PATH, "山西"), (SN_PATH, "陕西"),
                       (MX_PATH, "蒙西"), (LN_PATH, "辽宁")):
        fmt, rows = registry.dispatch(path)
        assert fmt == "month_rows"
        assert rows
        assert {r.province for r in rows} == {prov}
