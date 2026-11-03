# tests/province_fundamentals/test_month_rows_batch_b.py
"""月行宽表 parser tests — Wave-1 Batch B: 冀南(河北南网) / 蒙东 / 甘肃 / 冀北.

Fixtures ditto'd from data/exchange-monthly-reports/<省>月报/ (controller ruling:
tests never read live data/ paths).

真值 (read from the real workbooks, 2026-11-04, openpyxl data_only):

冀南 河北南网-电力市场信息报告数据汇总_2025-01_2026-05.xlsx (10 sheets; 表头 R3):
  纯交易册 — 全书关键词扫描 ("装机/发电量/用电量/利用小时/全社会/统调") 仅
  7.建设违规 说明文本 1 命中, 无任何 fundamentals 数据列.
  0.综合概览 R3 表头: 月份 | 注册主体 | 发电企业 | 电力用户 | 售电企业
                     | 年度合同分月 | 月度交易合计 | 月内交易合计 | 日前现货电量
                     | 实时现货电量 | 发电侧结算 | 发电侧电费 | 售电公司结算 | 售电公司电费
  R4: 2025-01 | 9270 | 547 | 8239 | 466 | 105.75 | 28.2 | 53.41 | ... | 113.45
  → 注册但一行不出 (sniff 命中, fuel_col_map 空 → parse 抛错 → registry 接 []).

蒙东 蒙东-电力交易市场信息披露数据_截至2026年5月.xlsx (11 sheets; 表头 R1):
  电网装机情况 (年份 int + 月份 int 双列复合键):
    2025-04 | 总装机 5449 | 火电 2376 | 新能源 3036 | 水电 37 | 储能 164
    2025-12 | 总装机 6350 | 火电 2440 | 新能源 3586 | 水电 37 | 储能 287
    2026-01 | 装机 5 列全空 (仅注册计数列有值) → 整行跳过
    2026-02 | 总装机 4506.162 | 火电 757.55 | 新能源 3736.852 | 水电 11.76 | 储能 空
    2026-05 | 总装机 6574.73 | 火电 2447.17 | 新能源 4091.07 | 水电/储能 空
  (口径断点 2026-04→05 为原册重述, 原样出; 新能源合并无风光拆分 → cap_newenergy_full)

甘肃 甘肃-信息披露月报数据-截至2026年5月.xlsx (21 sheets; 表头 R1):
  装机容量 (年份 int + 月份 "1月" 双列复合键):
    2025-01 | 总装机 9575.86 | 水电 763.75 | 火电 2439.2 | 风电 2939.77 | 光电 2779.54
    2026-05 | 总装机 12628.74 | 水电 971.99 | 火电 3496.9 | 风电 4166.6 | 光电 3993.25
  上网电量_月度 (上网口径 → _ongrid; _累计 表不解析):
    2025-01 | 总上网 170.43 | 同比 5.14 | 购火电 86.03 | 购水电 15.33
            | 购风电 38.27 | 购光电 23.91
    2026-05 | 总上网 163.43
  全社会用电量_月度:
    2025-01 | 全社会 156.61 | 同比 1.95 | 一产 1.83 | 二产 111.63
            | 三产 26.25 | 居民 16.9
    2026-05 | 全社会 156.8 | 同比 8.35 | 二产 114.97
  (核心指标总览 与三主题表同值重复 → 不解析; 发电利用小时_累计 累计口径 → 不解析)

冀北 冀北-信息披露月报_截至2026年5月.xlsx (11 sheets; 表头 R2, 标题 R1):
  新能源保障性收购 (月份×电源类型 长表 → pivot; 上网口径 → _ongrid):
    2025-01 | 风电 63.67 | 光伏 21.7
    2025-12 | 风电 87.41 | 光伏 22.34
    2026-02 | 风电 58.72 | 光伏 18.69
    2026-05 | 风电 55.66 | 光伏 32.14
  (装机/用电主题本册不存在; 2026-03~05 保障性收购/交易列 NOT_FOUND — 不映射,
   不影响上网电量列; 统调发受电量/最大负荷 无标准词表项 → 电网运行数据 不取)
"""
from datetime import date
from pathlib import Path

import openpyxl
import pytest

from services.province_fundamentals import registry
from services.province_fundamentals.parsers.month_rows import (
    MonthRowsConfig,
    parse_month_rows,
    GANSU_CAP,
    GANSU_DEMAND,
    GANSU_GEN,
    JIBEI_GEN,
    JINAN_OVERVIEW,
    MENGDONG_CAP,
    gansu_parser,
    jibei_parser,
    jinan_parser,
    mengdong_parser,
)

FIX = Path(__file__).parent / "fixtures"
JN_PATH = FIX / "河北南网-电力市场信息报告数据汇总_2025-01_2026-05.xlsx"
MD_PATH = FIX / "蒙东-电力交易市场信息披露数据_截至2026年5月.xlsx"
GS_PATH = FIX / "甘肃-信息披露月报数据-截至2026年5月.xlsx"
JB_PATH = FIX / "冀北-信息披露月报_截至2026年5月.xlsx"

JAN25 = date(2025, 1, 1)
MAY26 = date(2026, 5, 1)


def _by_value(rows):
    return {(r.month, r.metric): r.value for r in rows}


def _by_row(rows):
    return {(r.month, r.metric): r for r in rows}


# ── 冀南(河北南网): 纯交易册 — 注册识别但一行不出 ───────────────────────────────

def test_jinan_sniff_hits_own_workbook():
    """sniff 命中 0.综合概览 (表头 R3, 月份+年度合同分月+月度交易合计)."""
    assert jinan_parser.sniff(JN_PATH) is True


def test_jinan_dispatch_recognized_but_zero_rows():
    """工作簿被确定性识别 (fmt=month_rows) 但无 fundamentals 列 → 空 (绝不编造)."""
    fmt, rows = registry.dispatch(JN_PATH)
    assert fmt == "month_rows"
    assert rows == []
    assert registry.parse_file(JN_PATH) == []


def test_jinan_parse_raises_on_empty_colmap():
    """fuel_col_map 空 → parse_month_rows 抛 ValueError (registry 兜底接住)."""
    with pytest.raises(ValueError, match="月份|metric"):
        parse_month_rows(JN_PATH, JINAN_OVERVIEW)


# ── 蒙东: 年份+月份 int 双列复合键 + 新能源合并列 ──────────────────────────────

def test_mengdong_2025_04_first_month_truth():
    by = _by_value(parse_month_rows(MD_PATH, MENGDONG_CAP))
    apr25 = date(2025, 4, 1)
    assert by[(apr25, "cap_total_full")] == pytest.approx(5449)
    assert by[(apr25, "cap_thermal_full")] == pytest.approx(2376)
    assert by[(apr25, "cap_newenergy_full")] == pytest.approx(3036)
    assert by[(apr25, "cap_hydro_full")] == pytest.approx(37)
    assert by[(apr25, "cap_storage_full")] == pytest.approx(164)
    dec25 = date(2025, 12, 1)
    assert by[(dec25, "cap_total_full")] == pytest.approx(6350)
    assert by[(dec25, "cap_storage_full")] == pytest.approx(287)


def test_mengdong_composite_int_key_months_are_month_start():
    """年份(int)+月份(int) 复合 → 月初 date; province/unit/section 正确."""
    r = _by_row(parse_month_rows(MD_PATH, MENGDONG_CAP))[(date(2025, 4, 1), "cap_total_full")]
    assert r.month == date(2025, 4, 1)
    assert r.province == "蒙东"
    assert r.section == "capacity"
    assert r.unit == "万千瓦"
    assert r.source == MD_PATH.name


def test_mengdong_2026_01_all_capacity_empty_row_skipped():
    """2026-01 装机 5 列全空 (注册计数列有值也不救命): 整行跳过, 不出 None 行."""
    rows = parse_month_rows(MD_PATH, MENGDONG_CAP)
    months = {r.month for r in rows}
    assert date(2026, 1, 1) not in months
    assert len(months) == 13                     # 14 数据行 − 2026-01
    assert min(months) == date(2025, 4, 1) and max(months) == MAY26


def test_mengdong_empty_cells_none_not_zero():
    """2026-02 储能空 / 2026-05 水电储能空 → None 原样出, 绝不写 0."""
    by = _by_row(parse_month_rows(MD_PATH, MENGDONG_CAP))
    feb26 = date(2026, 2, 1)
    assert by[(feb26, "cap_total_full")].value == pytest.approx(4506.162)
    assert by[(feb26, "cap_storage_full")].value is None
    assert by[(feb26, "cap_storage_full")].value != 0
    assert by[(MAY26, "cap_total_full")].value == pytest.approx(6574.73)
    assert by[(MAY26, "cap_thermal_full")].value == pytest.approx(2447.17)
    assert by[(MAY26, "cap_hydro_full")].value is None
    assert by[(MAY26, "cap_storage_full")].value is None


def test_mengdong_newenergy_combined_metric_no_wind_solar_split():
    """新能源合并列 → cap_newenergy_full; 不出 cap_wind_full/cap_solar_full."""
    rows = mengdong_parser.parse(MD_PATH, "蒙东")
    metrics = {r.metric for r in rows}
    assert metrics == {"cap_total_full", "cap_thermal_full", "cap_newenergy_full",
                       "cap_hydro_full", "cap_storage_full"}
    assert len(rows) == 65                       # 13 月 × 5 装机


def test_mengdong_registry_dispatch_and_sniff_isolation():
    fmt, rows = registry.dispatch(MD_PATH)
    assert fmt == "month_rows"
    assert {r.province for r in rows} == {"蒙东"}
    by = {(r.month, r.metric): r.value for r in rows}
    assert by[(date(2025, 4, 1), "cap_total_full")] == pytest.approx(5449)
    # 蒙东/甘肃同为双列键族: sniff 互不命中
    assert mengdong_parser.sniff(GS_PATH) is False
    assert gansu_parser.sniff(MD_PATH) is False


# ── 甘肃: 年份 int + "1月" 双列复合键; 装机 + 上网电量 + 用电量 三主题 ──────────

def test_gansu_capacity_truth():
    by = _by_value(parse_month_rows(GS_PATH, GANSU_CAP))
    assert by[(JAN25, "cap_total_full")] == pytest.approx(9575.86)
    assert by[(JAN25, "cap_hydro_full")] == pytest.approx(763.75)
    assert by[(JAN25, "cap_thermal_full")] == pytest.approx(2439.2)
    assert by[(JAN25, "cap_wind_full")] == pytest.approx(2939.77)
    assert by[(JAN25, "cap_solar_full")] == pytest.approx(2779.54)
    assert by[(MAY26, "cap_total_full")] == pytest.approx(12628.74)
    assert by[(MAY26, "cap_hydro_full")] == pytest.approx(971.99)
    assert by[(MAY26, "cap_thermal_full")] == pytest.approx(3496.9)
    assert by[(MAY26, "cap_wind_full")] == pytest.approx(4166.6)
    assert by[(MAY26, "cap_solar_full")] == pytest.approx(3993.25)


def test_gansu_composite_key_month_string_with_suffix():
    """月份单元格是 "1月" 字符串 (非 int): 复合键解析为月初 date."""
    rows = parse_month_rows(GS_PATH, GANSU_CAP)
    r = _by_row(rows)[(JAN25, "cap_thermal_full")]
    assert r.month == date(2025, 1, 1)
    assert r.province == "甘肃"
    assert r.unit == "万千瓦"
    months = {x.month for x in rows}
    assert len(months) == 17
    assert min(months) == JAN25 and max(months) == MAY26


def test_gansu_generation_ongrid_caliber_truth():
    """上网口径 → _ongrid 后缀 (与 _full 全口径分键); 同比列随行出 (%)."""
    by = _by_row(parse_month_rows(GS_PATH, GANSU_GEN))
    assert by[(JAN25, "gen_total_ongrid")].value == pytest.approx(170.43)
    assert by[(JAN25, "gen_total_ongrid_yoy")].value == pytest.approx(5.14)
    assert by[(JAN25, "gen_total_ongrid_yoy")].unit == "%"
    assert by[(JAN25, "gen_thermal_ongrid")].value == pytest.approx(86.03)
    assert by[(JAN25, "gen_hydro_ongrid")].value == pytest.approx(15.33)
    assert by[(JAN25, "gen_wind_ongrid")].value == pytest.approx(38.27)
    assert by[(JAN25, "gen_solar_ongrid")].value == pytest.approx(23.91)
    assert by[(JAN25, "gen_thermal_ongrid_yoy")].value == pytest.approx(-8.0)
    r = by[(MAY26, "gen_total_ongrid")]
    assert r.value == pytest.approx(163.43)
    assert r.section == "generation"
    assert r.unit == "亿千瓦时"


def test_gansu_demand_truth_with_sector_split():
    by = _by_row(parse_month_rows(GS_PATH, GANSU_DEMAND))
    assert by[(JAN25, "demand_total")].value == pytest.approx(156.61)
    assert by[(JAN25, "demand_yoy")].value == pytest.approx(1.95)
    assert by[(JAN25, "demand_p1")].value == pytest.approx(1.83)
    assert by[(JAN25, "demand_p2")].value == pytest.approx(111.63)
    assert by[(JAN25, "demand_p3")].value == pytest.approx(26.25)
    assert by[(JAN25, "demand_residential")].value == pytest.approx(16.9)
    assert by[(JAN25, "demand_total")].section == "demand"
    assert by[(JAN25, "demand_total")].unit == "亿千瓦时"
    assert by[(MAY26, "demand_total")].value == pytest.approx(156.8)
    assert by[(MAY26, "demand_yoy")].value == pytest.approx(8.35)
    assert by[(MAY26, "demand_p2")].value == pytest.approx(114.97)


def test_gansu_registry_dispatch_21_sheet_workbook():
    """21 个 sheet: 按名选 3 目标表, 累计双表/核心指标总览/长表 sheet 不干扰.
    行数 = 17月×(5装机 + 10上网含同比 + 6用电) = 357."""
    fmt, rows = registry.dispatch(GS_PATH)
    assert fmt == "month_rows"
    assert {r.province for r in rows} == {"甘肃"}
    assert len(rows) == 357
    assert all("(差分)" not in r.source for r in rows)
    by = {(r.month, r.metric): r.value for r in rows}
    assert by[(JAN25, "cap_total_full")] == pytest.approx(9575.86)
    assert by[(JAN25, "gen_total_ongrid")] == pytest.approx(170.43)
    assert by[(JAN25, "demand_total")] == pytest.approx(156.61)


# ── 冀北: 电源类型透视列 (月份×燃料 长表) ──────────────────────────────────────

def test_jibei_pivot_generation_truth():
    by = _by_value(parse_month_rows(JB_PATH, JIBEI_GEN))
    assert by[(JAN25, "gen_wind_ongrid")] == pytest.approx(63.67)
    assert by[(JAN25, "gen_solar_ongrid")] == pytest.approx(21.7)
    assert by[(date(2025, 12, 1), "gen_wind_ongrid")] == pytest.approx(87.41)
    assert by[(date(2025, 12, 1), "gen_solar_ongrid")] == pytest.approx(22.34)
    assert by[(date(2026, 2, 1), "gen_wind_ongrid")] == pytest.approx(58.72)
    assert by[(date(2026, 2, 1), "gen_solar_ongrid")] == pytest.approx(18.69)
    assert by[(MAY26, "gen_wind_ongrid")] == pytest.approx(55.66)
    assert by[(MAY26, "gen_solar_ongrid")] == pytest.approx(32.14)


def test_jibei_pivot_rows_merge_per_month_and_meta():
    """同月风行+光行合并 (同 metric 后写覆盖, 不同 metric 并存): 17 月 × 2 = 34."""
    rows = jibei_parser.parse(JB_PATH, "冀北")
    assert len(rows) == 34
    months = {r.month for r in rows}
    assert len(months) == 17
    assert min(months) == JAN25 and max(months) == MAY26
    assert {r.metric for r in rows} == {"gen_wind_ongrid", "gen_solar_ongrid"}
    r = _by_row(rows)[(JAN25, "gen_wind_ongrid")]
    assert r.province == "冀北"
    assert r.section == "generation"
    assert r.unit == "亿千瓦时"
    assert "(差分)" not in r.source


def test_jibei_no_capacity_no_demand_metrics():
    """装机/用电主题本册不存在 → 不出行 (绝不编造); 统调发受电量不取."""
    rows = jibei_parser.parse(JB_PATH, "冀北")
    assert {r.section for r in rows} == {"generation"}
    assert not any(r.metric.startswith("cap_") for r in rows)
    assert not any(r.metric.startswith("demand_") for r in rows)


def test_jibei_registry_dispatch():
    fmt, rows = registry.dispatch(JB_PATH)
    assert fmt == "month_rows"
    assert {r.province for r in rows} == {"冀北"}
    by = {(r.month, r.metric): r.value for r in rows}
    assert by[(JAN25, "gen_wind_ongrid")] == pytest.approx(63.67)


# ── 框架: 复合键 + 透视列 全按表头名定位 (列序打乱合成册) ───────────────────────

def test_composite_key_and_pivot_name_not_index(tmp_path):
    """复合键+透视全部按表头名匹配: 列序打乱 (类型首列, 月份/年份末列) 仍解析;
    未知燃料类型行跳过; 全指标空行跳过."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "长表"
    ws.append(["电源类型", "上网电量(亿kWh)", "月份", "年份"])   # 故意乱序
    ws.append(["风电", 10.5, "3月", 2025])
    ws.append(["光伏", 4.25, "3月", 2025])
    ws.append(["风电", 11.0, 4, 2025])
    ws.append(["核电", 99.0, 4, 2025])       # pivot_map 外类型 → 跳过
    ws.append(["光伏", None, 4, 2025])       # 全指标空 → 跳过
    p = tmp_path / "comp_pivot.xlsx"
    wb.save(p)
    cfg = MonthRowsConfig(
        province="测试省", sheet_select="长表",
        header_markers=("电源类型", "上网电量", "月份", "年份"),
        month_col=("月份",), year_col=("年份",),
        pivot_col=("电源类型",), pivot_map={"风电": "wind", "光伏": "solar"},
        fuel_col_map={"上网电量": "gen_{pivot}_ongrid"})
    by = _by_row(parse_month_rows(p, cfg))
    assert len(by) == 3
    assert by[(date(2025, 3, 1), "gen_wind_ongrid")].value == pytest.approx(10.5)
    assert by[(date(2025, 3, 1), "gen_solar_ongrid")].value == pytest.approx(4.25)
    assert by[(date(2025, 4, 1), "gen_wind_ongrid")].value == pytest.approx(11.0)
    assert (date(2025, 4, 1), "gen_solar_ongrid") not in by    # 空值行整行跳过


def test_pivot_template_without_pivot_col_rejected(tmp_path):
    """metric 含 {pivot} 但未配置 pivot_col → ValueError (防配置笔误静默出错)."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "t"
    ws.append(["月份", "上网电量(亿kWh)"])
    ws.append(["2025-01", 1.0])
    p = tmp_path / "bad_tpl.xlsx"
    wb.save(p)
    cfg = MonthRowsConfig(
        province="测试省", sheet_select="t",
        header_markers=("月份", "上网电量"), month_col=("月份",),
        fuel_col_map={"上网电量": "gen_{pivot}_ongrid"})
    with pytest.raises(ValueError, match="pivot"):
        parse_month_rows(p, cfg)


# ── 全四省 registry 派发不互相截胡 ─────────────────────────────────────────────

def test_all_four_provinces_registered_and_isolated():
    expected = {JN_PATH: "河北南网", MD_PATH: "蒙东", GS_PATH: "甘肃", JB_PATH: "冀北"}
    for path, prov in expected.items():
        fmt, rows = registry.dispatch(path)
        assert fmt == "month_rows"
        if prov == "河北南网":
            assert rows == []                  # 纯交易册: 识别但零行
        else:
            assert rows
            assert {r.province for r in rows} == {prov}
