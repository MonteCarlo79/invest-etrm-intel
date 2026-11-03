# tests/province_fundamentals/test_wave2_bespoke.py
"""Wave-2 bespoke parser tests — 广东 (long/narrow) / 广西 (混型月键+转置) / 江苏 (无装机+口径陷阱).

Fixtures ditto'd from data/exchange-monthly-reports/<省>月报/ (controller ruling:
tests never read live data/ paths).

真值 (read from the real workbooks, 2026-11-04, openpyxl data_only):

广东 广东电力市场运营简报_2026年1-5月.xlsx (7 sheets; 窄表_指标 长表, 表头 R1):
  指标类别=机组类型注册 的 装机规模 行 (单位全为 万千瓦; 全书 发电量/用电量/全社会
  关键词零命中 — 发电/用电主题不存在, 一行不出):
    2026-01 | 合计 22932 | 燃煤 9393 | 燃气 5660 | 核电 1067 | 风电 1937
            | 太阳能 3744 | 抽水蓄能 360 | 储能 440
    2026-02 | 合计 23242 | 风电 2052 | 储能 461
    2026-03 | 合计 232686.73 | 燃煤 95169.49 | 风电 20516.68  ← 源 PDF ×10 异常,
            册内 异常提示 sheet 逐行备案 "未擅自修正" → parser 原值透传不打标
    2026-04 | 燃煤 9188.449 | 储能 491.2 (仅 6 指标: 无 合计/抽水蓄能/生物质/综合)
    2026-05 | 机组类型注册 整月缺席 → 0 行
  (生物质/综合 不在燃料词表不映射; 注册数量(家) 非 fundamentals 不映射)

广西 广西-信息披露-2025-2026.5.xlsx (16 sheets; 标题 R1 + 表头 R2):
  月末装机容量 (年份 int + 统计时点 int/'5' 复合键; 统调 → _full):
    2025-01 | 合计 7394.8 | 燃煤 2049.5 | 燃气 48 | 水电 1323.8 | 核电 454.8
            | 风电 1904.6 | 光伏 1242 | 储能 239.1
    2025-12 | 合计 8749.24
    2026-05 (统计时点='5' 字符串) | 合计 9000.21 | 水电 1325.05 | 储能 269.95
  月度发受电量 (单列混型月键: 2025 行 serial int, 2026 行 '2026年4月';
    统调发受电 → _dispatch; 合计=分电源+西电/云电−送广东海南):
    45658=2025-01 | 煤电 40.31 | 水电 26.82 | 气电 0.32 | 合计 155.28
    45992=2025-12 | 煤电 43.33 | 合计 175.94
    '2026年5月'   | 煤电 40.33 | 风电 33.87 | 合计 189.08
  (数据汇总 转置重复表仅 2025-01..2026-02 → 显式不解析; 生物质/西电/云电/送广东海南
   不在词表不映射; 用电无干净主题表 → 不出行)

江苏 江苏-信息披露月报数据_截至2026年5月.xlsx (9 sheets; 表头 R1, 无装机 sheet):
  全省月度用电量 (⚠名为月度实为年内累计 → 差分; 同比存小数分数 → ×100 出 %):
    全社会用电量: 2025-01=667.07(原值) → 2025-02=1283.06 → … → 2025-12=8803.31,
      2026-01 重置 815.58(原值) → … → 2026-05=3520.76
      差分: 2025-02=615.99, 2025-12=749.08, 2026-05=728.59
      二产差分 2025-02 = 831.35−420.17 = 411.18
    全社会用电量同比: 2025 全年+2026-01 空 → None; 2026-02=0.0619→6.19%,
      2026-05=0.0618→6.18% (不差分)
  发电侧结算-只有电量 (当月, 上网口径 → _ongrid; NOT_FOUND → None):
    2025-01 | 上网 423.02 | 火电 318.52 | 核电 40.14 | 风电 45.13 | 太阳能 12.95
            | 水电 0.19
    2025-11 | 上网 474.53 | 分燃料全空 → None 原样出
    2026-02 | 上网 370.35 | 火电 236.82 | 太阳能 16.69 | 水电='NOT_FOUND' → None
    2026-01 全空 / 2026-03..05 全 NOT_FOUND → 整月跳过 (共 13 个有数月)
"""
from datetime import date
from pathlib import Path

import openpyxl
import pytest

from services.province_fundamentals import registry
from services.province_fundamentals.parsers import (
    prov_guangdong,
    prov_guangxi,
    prov_jiangsu,
)

FIX = Path(__file__).parent / "fixtures"
GD_PATH = FIX / "广东电力市场运营简报_2026年1-5月.xlsx"
GX_PATH = FIX / "广西-信息披露-2025-2026.5.xlsx"
JS_PATH = FIX / "江苏-信息披露月报数据_截至2026年5月.xlsx"

JAN25 = date(2025, 1, 1)
MAY26 = date(2026, 5, 1)


def _by_value(rows):
    return {(r.month, r.metric): r.value for r in rows}


def _by_row(rows):
    return {(r.month, r.metric): r for r in rows}


# ── 广东: unique long/narrow (指标类别/指标名称/数值) ─────────────────────────────

def test_guangdong_capacity_truth_2026_01():
    by = _by_value(prov_guangdong.parse(GD_PATH, "广东"))
    jan = date(2026, 1, 1)
    assert by[(jan, "cap_total_registered")] == pytest.approx(22932)
    assert by[(jan, "cap_thermal_registered")] == pytest.approx(9393)
    assert by[(jan, "cap_gas_registered")] == pytest.approx(5660)
    assert by[(jan, "cap_nuclear_registered")] == pytest.approx(1067)
    assert by[(jan, "cap_wind_registered")] == pytest.approx(1937)
    assert by[(jan, "cap_solar_registered")] == pytest.approx(3744)
    assert by[(jan, "cap_pumped_registered")] == pytest.approx(360)
    assert by[(jan, "cap_storage_registered")] == pytest.approx(440)


def test_guangdong_months_and_meta():
    """机组类型注册 仅 2026-01..04 (2026-05 整月缺席); 8 指标 × (8+8+8+6) = 30 行."""
    rows = prov_guangdong.parse(GD_PATH, "广东")
    assert len(rows) == 30
    months = {r.month for r in rows}
    assert months == {date(2026, m, 1) for m in (1, 2, 3, 4)}
    assert date(2026, 5, 1) not in months
    r = _by_row(rows)[(date(2026, 2, 1), "cap_wind_registered")]
    assert r.value == pytest.approx(2052)
    assert r.province == "广东"
    assert r.section == "capacity"
    assert r.unit == "万千瓦"
    assert r.period == "month"
    assert r.source == GD_PATH.name
    assert _by_value(rows)[(date(2026, 2, 1), "cap_storage_registered")] == pytest.approx(461)


def test_guangdong_2026_03_anomaly_passthrough_untagged():
    """×10 源 PDF 异常 (异常提示 sheet 备案): 原值透传, 不修正不打标."""
    rows = prov_guangdong.parse(GD_PATH, "广东")
    by = _by_row(rows)
    mar = date(2026, 3, 1)
    assert by[(mar, "cap_total_registered")].value == pytest.approx(232686.73)
    assert by[(mar, "cap_wind_registered")].value == pytest.approx(20516.68)
    assert by[(mar, "cap_thermal_registered")].value == pytest.approx(95169.49)
    assert all("异常" not in r.source and "#" not in r.source for r in rows)


def test_guangdong_2026_04_partial_indicators():
    """2026-04 仅 6 个装机规模指标: 无 合计/抽水蓄能 → 缺什么不出什么 (绝不编造)."""
    rows = prov_guangdong.parse(GD_PATH, "广东")
    apr_rows = [r for r in rows if r.month == date(2026, 4, 1)]
    assert len(apr_rows) == 6
    metrics = {r.metric for r in apr_rows}
    assert "cap_total_registered" not in metrics
    assert "cap_pumped_registered" not in metrics
    by = _by_value(apr_rows)
    assert by[(date(2026, 4, 1), "cap_thermal_registered")] == pytest.approx(9188.449)
    assert by[(date(2026, 4, 1), "cap_storage_registered")] == pytest.approx(491.2)


def test_guangdong_no_generation_no_demand_no_counts():
    """发电/用电主题全书不存在 → 不出行; 注册数量(家)/生物质/综合 不映射."""
    rows = prov_guangdong.parse(GD_PATH, "广东")
    assert {r.section for r in rows} == {"capacity"}
    metrics = {r.metric for r in rows}
    assert not any(m.startswith("gen_") for m in metrics)
    assert not any(m.startswith("demand_") for m in metrics)
    assert not any("biomass" in m or "count" in m for m in metrics)


def test_guangdong_name_not_index(tmp_path):
    """列序打乱 (数值首列, 年月末列) + 非万千瓦单位行拦截, 仍按表头名解析."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "窄表_指标"
    ws.append(["数值", "指标名称", "单位", "指标类别", "年月"])     # 故意乱序
    ws.append([100.5, "燃煤装机规模", "万千瓦", "机组类型注册", "2026-01"])
    ws.append([200.0, "风电装机规模", "万千瓦", "机组类型注册", "2026-01"])
    ws.append([999.0, "水电装机规模", "万千瓦", "市场交易成交", "2026-01"])  # 类别不符
    ws.append([50.0, "燃气装机规模", "MW", "机组类型注册", "2026-01"])       # 单位闸门
    ws.append([None, "储能装机规模", "万千瓦", "机组类型注册", "2026-02"])  # 空值 → None
    p = tmp_path / "gd_shuffled.xlsx"
    wb.save(p)
    rows = prov_guangdong.parse(p, "广东")
    by = _by_row(rows)
    assert len(rows) == 3
    assert by[(date(2026, 1, 1), "cap_thermal_registered")].value == pytest.approx(100.5)
    assert by[(date(2026, 1, 1), "cap_wind_registered")].value == pytest.approx(200.0)
    # 水电类别不符 / 燃气单位 MW → 均不出行; 储能空值 → None 非 0
    assert (date(2026, 1, 1), "cap_gas_registered") not in by
    assert by[(date(2026, 2, 1), "cap_storage_registered")].value is None
    assert by[(date(2026, 2, 1), "cap_storage_registered")].value != 0


# ── 广西: 混型月键 (serial int + '2026年4月') + 双列复合键 + 转置汇总排除 ──────────

def test_guangxi_capacity_truth():
    by = _by_value(prov_guangxi.parse(GX_PATH, "广西"))
    assert by[(JAN25, "cap_total_full")] == pytest.approx(7394.8)
    assert by[(JAN25, "cap_thermal_full")] == pytest.approx(2049.5)
    assert by[(JAN25, "cap_gas_full")] == pytest.approx(48)
    assert by[(JAN25, "cap_hydro_full")] == pytest.approx(1323.8)
    assert by[(JAN25, "cap_nuclear_full")] == pytest.approx(454.8)
    assert by[(JAN25, "cap_wind_full")] == pytest.approx(1904.6)
    assert by[(JAN25, "cap_solar_full")] == pytest.approx(1242)
    assert by[(JAN25, "cap_storage_full")] == pytest.approx(239.1)
    assert by[(date(2025, 12, 1), "cap_total_full")] == pytest.approx(8749.24)
    assert by[(MAY26, "cap_total_full")] == pytest.approx(9000.21)
    assert by[(MAY26, "cap_hydro_full")] == pytest.approx(1325.05)
    assert by[(MAY26, "cap_storage_full")] == pytest.approx(269.95)


def test_guangxi_generation_truth():
    by = _by_value(prov_guangxi.parse(GX_PATH, "广西"))
    assert by[(JAN25, "gen_thermal_dispatch")] == pytest.approx(40.31)
    assert by[(JAN25, "gen_hydro_dispatch")] == pytest.approx(26.82)
    assert by[(JAN25, "gen_gas_dispatch")] == pytest.approx(0.32)
    assert by[(JAN25, "gen_total_dispatch")] == pytest.approx(155.28)
    assert by[(date(2025, 12, 1), "gen_thermal_dispatch")] == pytest.approx(43.33)
    assert by[(date(2025, 12, 1), "gen_total_dispatch")] == pytest.approx(175.94)
    assert by[(MAY26, "gen_thermal_dispatch")] == pytest.approx(40.33)
    assert by[(MAY26, "gen_wind_dispatch")] == pytest.approx(33.87)
    assert by[(MAY26, "gen_total_dispatch")] == pytest.approx(189.08)


def test_guangxi_twin_month_key_forms_both_normalize():
    """同列双型: serial int 45658 → 2025-01-01; '2026年5月' → 2026-05-01 (月初 date)."""
    assert prov_guangxi._parse_mixed_month(45658) == date(2025, 1, 1)
    assert prov_guangxi._parse_mixed_month(45992) == date(2025, 12, 1)
    assert prov_guangxi._parse_mixed_month("2026年4月") == date(2026, 4, 1)
    assert prov_guangxi._parse_mixed_month("2026年5月") == date(2026, 5, 1)
    # 非 serial 窗的 int / 乱串 → None (不误判)
    assert prov_guangxi._parse_mixed_month(5) is None
    assert prov_guangxi._parse_mixed_month(True) is None
    assert prov_guangxi._parse_mixed_month("五月") is None
    # 复合键: 统计时点 int 与 '5' 字符串 (2026-05 实勘混型)
    assert prov_guangxi._parse_composite_month(2026, 5) == date(2026, 5, 1)
    assert prov_guangxi._parse_composite_month(2026, "5") == date(2026, 5, 1)
    assert prov_guangxi._parse_composite_month("2026", "5月") == date(2026, 5, 1)
    assert prov_guangxi._parse_composite_month(2026, None) is None


def test_guangxi_row_counts_and_transposed_summary_excluded():
    """17 月 × (8 装机 + 7 发受电) = 255 行; 数据汇总 (转置, 仅 ..2026-02) 若被解析
    会双写同月同 metric → 行数即证明其被排除; 2026-03..05 有数 (= 汇总表覆盖之外)."""
    rows = prov_guangxi.parse(GX_PATH, "广西")
    assert len(rows) == 255
    months = {r.month for r in rows}
    assert len(months) == 17
    assert min(months) == JAN25 and max(months) == MAY26
    # 双写检测: 同 (month, metric) 至多一行
    keys = [(r.month, r.metric) for r in rows]
    assert len(keys) == len(set(keys))
    assert "数据汇总" in prov_guangxi._SKIP_SHEETS


def test_guangxi_no_demand_no_biogas_no_import_export():
    """用电无干净主题表 → 不出 demand; 生物质/西电/云电/送广东海南 不在词表."""
    rows = prov_guangxi.parse(GX_PATH, "广西")
    assert {r.section for r in rows} == {"capacity", "generation"}
    metrics = {r.metric for r in rows}
    assert not any(m.startswith("demand_") for m in metrics)
    assert not any("biomass" in m or "import" in m or "export" in m for m in metrics)
    # 全部当月值, 无差分标
    assert all("(差分)" not in r.source for r in rows)


def test_guangxi_name_not_index(tmp_path):
    """表头乱序 (合计首列, 月份末列) 仍按名解析; 空行/不可解析月键跳过."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "月度发受电量"
    ws.append(["月度发受电量"])                                    # 标题 R1
    ws.append(["合计", "煤电", "气电", "月份"])                    # 表头 R2 乱序
    ws.append([100.0, 60.0, 1.5, 45658])                          # serial
    ws.append([200.0, 120.0, 2.5, "2026年3月"])                   # 中文字符串
    ws.append([None, None, None, "2026年4月"])                    # 全指标空 → 跳过
    ws.append([300.0, 150.0, 3.0, "乱码"])                        # 月键不可解析 → 跳过
    p = tmp_path / "gx_shuffled.xlsx"
    wb.save(p)
    header, data = prov_guangxi._read_sheet(p, "月度发受电量", ("月份", "煤电"))
    keys, colmap = prov_guangxi._map_columns(
        header, prov_guangxi._GEN_COL_MAP, ("月份",))
    rows = prov_guangxi._collect(
        data, colmap, "广西", p.name, section="generation", unit="亿千瓦时",
        month_of=lambda row: prov_guangxi._parse_mixed_month(
            prov_guangxi._cell(row, keys["月份"])))
    by = _by_value(rows)
    assert len(by) == 6                                          # 2 月 × 3 指标
    assert by[(date(2025, 1, 1), "gen_total_dispatch")] == pytest.approx(100.0)
    assert by[(date(2026, 3, 1), "gen_thermal_dispatch")] == pytest.approx(120.0)
    assert by[(date(2026, 3, 1), "gen_gas_dispatch")] == pytest.approx(2.5)


# ── 江苏: 无装机 + 用电累计差分 + 同比分数×100 + NOT_FOUND ─────────────────────────

def test_jiangsu_demand_cumulative_diff_truth():
    by = _by_row(prov_jiangsu.parse(JS_PATH, "江苏"))
    # 1 月 = 累计原值直取, 不打标
    jan = by[(JAN25, "demand_total")]
    assert jan.value == pytest.approx(667.07)
    assert jan.source == JS_PATH.name
    assert "(差分)" not in jan.source
    # 差分月: 本月 − 上月 (同年), 打标
    feb = by[(date(2025, 2, 1), "demand_total")]
    assert feb.value == pytest.approx(615.99)
    assert feb.source == f"{JS_PATH.name}(差分)"
    dec = by[(date(2025, 12, 1), "demand_total")]
    assert dec.value == pytest.approx(749.08)
    # 跨年不差分: 2026-01 又是原值
    jan26 = by[(date(2026, 1, 1), "demand_total")]
    assert jan26.value == pytest.approx(815.58)
    assert "(差分)" not in jan26.source
    may26 = by[(MAY26, "demand_total")]
    assert may26.value == pytest.approx(728.59)
    assert may26.unit == "亿千瓦时"
    assert may26.section == "demand"
    # 行业拆分同规则差分
    assert by[(date(2025, 2, 1), "demand_p2")].value == pytest.approx(411.18)
    assert by[(JAN25, "demand_p2")].value == pytest.approx(420.17)


def test_jiangsu_demand_yoy_fraction_to_percent_not_diffed():
    """同比存小数分数 → ×100 出百分数 (unit %); 不差分; 空格 → None."""
    by = _by_row(prov_jiangsu.parse(JS_PATH, "江苏"))
    feb26 = by[(date(2026, 2, 1), "demand_yoy")]
    assert feb26.value == pytest.approx(6.19)
    assert feb26.unit == "%"
    assert "(差分)" not in feb26.source
    assert by[(MAY26, "demand_yoy")].value == pytest.approx(6.18)
    jan25 = by[(JAN25, "demand_yoy")]
    assert jan25.value is None
    assert jan25.value != 0


def test_jiangsu_generation_ongrid_truth_and_not_found():
    by = _by_row(prov_jiangsu.parse(JS_PATH, "江苏"))
    jan = date(2025, 1, 1)
    assert by[(jan, "gen_total_ongrid")].value == pytest.approx(423.02)
    assert by[(jan, "gen_thermal_ongrid")].value == pytest.approx(318.52)
    assert by[(jan, "gen_nuclear_ongrid")].value == pytest.approx(40.14)
    assert by[(jan, "gen_wind_ongrid")].value == pytest.approx(45.13)
    assert by[(jan, "gen_solar_ongrid")].value == pytest.approx(12.95)
    assert by[(jan, "gen_hydro_ongrid")].value == pytest.approx(0.19)
    r = by[(jan, "gen_total_ongrid")]
    assert r.unit == "亿千瓦时"
    assert "(差分)" not in r.source                     # 发电为当月值, 不差分
    # 2025-11 分燃料全空 → None 原样出 (绝不写 0)
    nov = date(2025, 11, 1)
    assert by[(nov, "gen_total_ongrid")].value == pytest.approx(474.53)
    assert by[(nov, "gen_thermal_ongrid")].value is None
    assert by[(nov, "gen_thermal_ongrid")].value != 0
    # 2026-02: 水电 = 'NOT_FOUND' → None; 其余正常出
    feb26 = date(2026, 2, 1)
    assert by[(feb26, "gen_total_ongrid")].value == pytest.approx(370.35)
    assert by[(feb26, "gen_solar_ongrid")].value == pytest.approx(16.69)
    assert by[(feb26, "gen_hydro_ongrid")].value is None


def test_jiangsu_all_not_found_months_skipped():
    """2026-01 全空 / 2026-03..05 全 NOT_FOUND → 整月跳过 (gen 共 13 个有数月)."""
    rows = prov_jiangsu.parse(JS_PATH, "江苏")
    gen_months = {r.month for r in rows if r.section == "generation"}
    assert len(gen_months) == 13
    assert date(2026, 1, 1) not in gen_months
    assert date(2026, 3, 1) not in gen_months
    assert date(2026, 4, 1) not in gen_months
    assert date(2026, 5, 1) not in gen_months
    assert date(2026, 2, 1) in gen_months


def test_jiangsu_no_capacity_rows_and_counts():
    """装机 sheet 本册不存在 → 一行不出 (绝不编造). 17月×6用电 + 13月×6发电 = 180."""
    rows = prov_jiangsu.parse(JS_PATH, "江苏")
    assert len(rows) == 180
    assert {r.section for r in rows} == {"demand", "generation"}
    assert not any(r.metric.startswith("cap_") for r in rows)


def test_jiangsu_name_not_index_and_unit_gate(tmp_path):
    """表头乱序 (累计值末列) 仍按名解析 + 差分跨行成立; 单位注记不识别的列被闸门拦下."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "全省月度用电量"
    ws.append(["一产同比(%)", "月份", "全社会用电量\n(亿kWh)", "第二产业\n(万kWh)"])
    ws.append([None, "2025-01", 100.0, 50.0])
    ws.append([None, "2025-02", 250.0, 120.0])
    ws.append([None, "垃圾", 999.0, 999.0])                    # 月键不可解析 → 跳过
    ws2 = wb.create_sheet("发电侧结算-只有电量")
    ws2.append(["结算月份", "发电企业上网电量\n(亿kWh)"])
    ws2.append(["2025-01", 1.0])
    p = tmp_path / "js_shuffled.xlsx"
    wb.save(p)
    rows = prov_jiangsu.parse(p, "江苏")
    by = _by_row(rows)
    # demand_total 差分: 250−100=150; 第二产业 单位注记 万kWh≠亿千瓦时 → 整列拦截
    assert by[(JAN25, "demand_total")].value == pytest.approx(100.0)
    feb = by[(date(2025, 2, 1), "demand_total")]
    assert feb.value == pytest.approx(150.0)
    assert feb.source.endswith("(差分)")
    assert (date(2025, 2, 1), "demand_p2") not in by
    assert len([r for r in rows if r.section == "demand"]) == 2
    assert by[(JAN25, "gen_total_ongrid")].value == pytest.approx(1.0)


# ── registry 派发 + 三省互不漏/互不截胡 (dispatch isolation) ──────────────────────

def test_registry_dispatch_three_provinces():
    expected = {GD_PATH: ("prov_guangdong", "广东", 30),
                GX_PATH: ("prov_guangxi", "广西", 255),
                JS_PATH: ("prov_jiangsu", "江苏", 180)}
    for path, (fmt, prov, n) in expected.items():
        got_fmt, rows = registry.dispatch(path)
        assert got_fmt == fmt
        assert len(rows) == n
        assert {r.province for r in rows} == {prov}
        assert registry.parse_file(path) == rows


def test_sniff_isolation_cross_province():
    """三省 bespoke sniff 互不中对方册, 也不中 month_rows 代表册 (山东)."""
    sd = FIX / "山东电力市场信息披露月报-2022至2026年5月.xlsx"
    ah = FIX / "安徽电力市场信息披露月报数据-截至2026年5月.xlsx"
    own = {prov_guangdong: GD_PATH, prov_guangxi: GX_PATH, prov_jiangsu: JS_PATH}
    for parser, mine in own.items():
        assert parser.sniff(mine) is True
        for other, path in own.items():
            if other is not parser:
                assert parser.sniff(path) is False
        assert parser.sniff(sd) is False
        assert parser.sniff(ah) is False


def test_existing_parsers_do_not_steal_new_fixtures():
    """既有 parser (format_a/format_b/month_rows/全国册) 不得命中三本新册."""
    from services.province_fundamentals.parsers import (
        format_a, format_b, jiaojie_2025, summary_2025)
    from services.province_fundamentals.parsers.month_rows import (
        shandong_parser, gansu_parser, mengdong_parser, jibei_parser)
    for f in (GD_PATH, GX_PATH, JS_PATH):
        assert format_a.sniff(f) is False
        assert format_b.sniff(f) is False
        assert jiaojie_2025.sniff(f) is False
        assert summary_2025.sniff(f) is False
        assert shandong_parser.sniff(f) is False
        assert gansu_parser.sniff(f) is False
        assert mengdong_parser.sniff(f) is False
        assert jibei_parser.sniff(f) is False
