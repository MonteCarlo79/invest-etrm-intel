# tests/province_fundamentals/test_interprov_wave2.py
"""省间 (interprov) 月度受入/外送 parser tests — Wave 2: 聚合册内省间 sheet
蒙东 / 甘肃 / 山西 / 陕西 / 辽宁 / 山东 / 广西.

Fixtures ditto'd from data/exchange-monthly-reports/<省>月报/ (controller ruling:
tests never read live data/ paths). 陕西用第二册 (结算及分类构成) — 格式图
§陕西: prefer 长格式结算 sheet; 披露册 省间市场交易结算 宽表不解析.

真值 (read from the real fixtures, 2026-11-04, openpyxl data_only):

蒙东 蒙东-电力交易市场信息披露数据_截至2026年5月.xlsx · 外送结算(月度)
  (表头 R1, 数据类型 ∈ {当月,累计} 混合; 外送 only, 总外送列):
  当月: 2024-01 199.71 | 2024-02 134.17 | 2024-06 174.72 | 2024-12 208.84
       | 2025-01 82.96
  差分 (source 打 "(差分)"): 2024-03 = 累计1-3月495.77 − 累计1-2月333.88 = 161.89
       (当月行存在但为空) | 2025-02 = 累计1-2月366.44 − 当月2025-01·82.96 = 283.48
       (1 月累计 ≡ 1 月当月). 共 9 个月; 册止 2025-02 (原册即如此).

甘肃 甘肃-信息披露月报数据-截至2026年5月.xlsx · 跨省售电_月度 + 跨省购电_月度
  (年份 int + 'N月' 复合键, 合计列; 双向各 17 个月 2025-01→2026-05):
  售电(export): 2025-01 55.33 | 2025-08 88.65 | 2026-01 99.14 | 2026-05 66.35
  购电(import): 2025-01 4.98 | 2025-08 2.17 | 2026-01 1.75 | 2026-05 3.66
  分项通道列现 'NOT_FOUND' 字符串但不入合计 → 不影响; 计划 sheet 不解析.

山西 山西信息披露月报数据_2025至2026年5月.xlsx · 外送交易
  (月份为 datetime; 外送 only, 外送总量列; 17 个月 2025-01→2026-05):
  2025-01 54.75 | 2025-07 63.85 | 2026-02 31.01 | 2026-05 51.92

陕西 陕西结算及分类构成_2025至2026年5月.xlsx · 结算及分类构成表-省间交易结算
  (长表, 交易类别 ∈ {主网外送,主网外购}, 取 当月结算电量; 年份+月份 int 复合键):
  外送: 2025-01 43.4 | 2025-08 67.88 | 2026-01 60.86 | 2026-05 26.04
  外购: 2025-01 15.8 | 2025-08 17.94 | 2026-01 11.67 | 2026-04 24.44
  ⚠ 2026-05 外购整行缺失 → 33 行 = 17 外送 + 16 外购 (原样出, 不补).

辽宁 辽宁-信息披露月报-2025至2026年5月.xlsx · 联络线电量结算（当月）
  (双行复合表头 R2 方向组+R3 通道; 活通道加总; 蒙东送出 全列空→死通道剔除;
  15 个月 2025-02→2026-05, 缺 2025-01/2025-05):
  受入 = Σ(统分水电,东北分部火电,蒙东,吉林,黑龙江):
    2025-02 5.13+26.9+11+2.16+5.96=51.15 | 2025-12 72.12 | 2026-05 33.56
  送出 = Σ(高岭通道外送,鲁固通道外送,东北区域内外送):
    2025-02 0.8+2.72+0.02=3.54 | 2025-12 12.78 | 2026-05 2.02+4.467+3.86=10.347

山东 山东电力市场信息披露月报-2022至2026年5月.xlsx · 06_跨省跨区交易
  (R1 标题/R2 表头含字面\\n; import only, 购入合计; 17 个月 2025-01→2026-05):
    2025-01 124.2 | 2025-12 254.72 | 2026-01 174.39 | 2026-05 138.37
    (2026-01 与 14_跨省购入明细 合计 174.39 独立互证)
  + 15_跨省外送明细 (长表 通道名称=合计 行; ⚠ 单位 万kWh → ÷10000;
    5 个月 2026-01→2026-05):
    2026-01 4469.15万→0.446915 | 2026-02 10119.5万→1.01195 | 2026-05 9872.327万→0.9872327

广西 广西-信息披露-2025-2026.5.xlsx · 月度发受电量 (西电+云电双腿加总;
  月份列 Excel 序列日 45658=2025-01 与 '2026年1月' 混杂; 17 个月):
  2025-01 9.94+8.21=18.15 | 2025-07 22.44+28.87=51.31 | 2025-09 11.95+42.63=54.58
  | 2026-05 13.85+8.98=22.83
  2026-03 西电 None (云电 10.35 在) → 当月 None (绝不半腿出数; 册内显式 0
  与 None 有别).
"""
from datetime import date
from pathlib import Path

import pytest

from services.province_fundamentals import registry
from services.province_fundamentals.parsers import prov_interprov2 as p2

FIX = Path(__file__).parent / "fixtures"
MD_PATH = FIX / "蒙东-电力交易市场信息披露数据_截至2026年5月.xlsx"
GS_PATH = FIX / "甘肃-信息披露月报数据-截至2026年5月.xlsx"
SX_PATH = FIX / "山西信息披露月报数据_2025至2026年5月.xlsx"
SAX_PATH = FIX / "陕西结算及分类构成_2025至2026年5月.xlsx"
SAX_DISC = FIX / "陕西-信息披露月报数据_2025年1月-2026年5月.xlsx"
LN_PATH = FIX / "辽宁-信息披露月报-2025至2026年5月.xlsx"
SD_PATH = FIX / "山东电力市场信息披露月报-2022至2026年5月.xlsx"
GX_PATH = FIX / "广西-信息披露-2025-2026.5.xlsx"

# wave 1 旁册 (sniff 隔离对照)
W1_SIDES = [
    FIX / "安徽跨区跨省省间购入电量_截至2026年5月.xlsx",
    FIX / "江苏跨区跨省月度交易计划分类汇总_2025-09至2026-05.xlsx",
    FIX / "河北南网省间交易结算汇总-2025.4-2026.5.xlsx",
    FIX / "冀北2024年以来电力市场交易结算通报数据（含省间）-2026-05.xlsx",
]

ALL_PARSERS2 = [p2.mengdong_parser, p2.gansu_parser, p2.shanxi_parser,
                p2.shaanxi_parser, p2.liaoning_parser, p2.shandong_parser,
                p2.guangxi_parser]

MAY26 = date(2026, 5, 1)


def _by_value(rows):
    return {(r.month, r.metric): r.value for r in rows}


def _by_row(rows):
    return {(r.month, r.metric): r for r in rows}


# ── 蒙东: 当月/累计混合 → 差分规则 ─────────────────────────────────────────────

def test_mengdong_truth_direct_months():
    by = _by_value(p2.mengdong_parser.parse(MD_PATH, "蒙东"))
    assert by[(date(2024, 1, 1), "export_gwh")] == pytest.approx(199.71)
    assert by[(date(2024, 2, 1), "export_gwh")] == pytest.approx(134.17)
    assert by[(date(2024, 6, 1), "export_gwh")] == pytest.approx(174.72)
    assert by[(date(2024, 12, 1), "export_gwh")] == pytest.approx(208.84)
    assert by[(date(2025, 1, 1), "export_gwh")] == pytest.approx(82.96)


def test_mengdong_diff_rule_tagged():
    """当月空 → 同年相邻累计差分; 差分出数 source 打 (差分) 标 (format_a 同规).
    2024-03: 累计1-3月 495.77 − 累计1-2月 333.88 = 161.89.
    2025-02: 累计1-2月 366.44 − 当月2025-01 82.96 = 283.48 (1月累计≡1月当月)."""
    rows = p2.mengdong_parser.parse(MD_PATH, "蒙东")
    by = _by_row(rows)
    r3 = by[(date(2024, 3, 1), "export_gwh")]
    assert r3.value == pytest.approx(161.89)
    assert "(差分)" in r3.source
    r2 = by[(date(2025, 2, 1), "export_gwh")]
    assert r2.value == pytest.approx(283.48)
    assert "(差分)" in r2.source
    # 直读月不打标
    assert "(差分)" not in by[(date(2024, 1, 1), "export_gwh")].source


def test_mengdong_9_months_export_only_meta():
    rows = p2.mengdong_parser.parse(MD_PATH, "蒙东")
    assert len(rows) == 9
    assert {r.metric for r in rows} == {"export_gwh"}
    months = {r.month for r in rows}
    assert min(months) == date(2024, 1, 1) and max(months) == date(2025, 2, 1)
    # 累计-only 且无法差分的月份 (2024-04/05/09/10/11) 不出 phantom 行
    assert date(2024, 4, 1) not in months and date(2024, 5, 1) not in months
    assert date(2024, 9, 1) not in months
    r = _by_row(rows)[(date(2024, 1, 1), "export_gwh")]
    assert r.province == "蒙东" and r.section == "interprov"
    assert r.period == "month" and r.unit == "亿千瓦时"


# ── 甘肃: 双向双 sheet, 合计列 ─────────────────────────────────────────────────

def test_gansu_truth_both_directions():
    by = _by_value(p2.gansu_parser.parse(GS_PATH, "甘肃"))
    assert by[(date(2025, 1, 1), "export_gwh")] == pytest.approx(55.33)
    assert by[(date(2025, 1, 1), "import_gwh")] == pytest.approx(4.98)
    assert by[(date(2025, 8, 1), "export_gwh")] == pytest.approx(88.65)
    assert by[(date(2025, 8, 1), "import_gwh")] == pytest.approx(2.17)
    assert by[(date(2026, 1, 1), "export_gwh")] == pytest.approx(99.14)
    assert by[(date(2026, 1, 1), "import_gwh")] == pytest.approx(1.75)
    assert by[(MAY26, "export_gwh")] == pytest.approx(66.35)
    assert by[(MAY26, "import_gwh")] == pytest.approx(3.66)


def test_gansu_34_rows_meta():
    rows = p2.gansu_parser.parse(GS_PATH, "甘肃")
    assert len(rows) == 34                            # 17 月 × 双向
    assert {r.metric for r in rows} == {"import_gwh", "export_gwh"}
    months = {r.month for r in rows}
    assert len(months) == 17
    assert min(months) == date(2025, 1, 1) and max(months) == MAY26
    assert all(r.unit == "亿千瓦时" and r.section == "interprov" for r in rows)
    assert all(r.source == GS_PATH.name for r in rows)


# ── 山西: datetime 月份, 外送 only ─────────────────────────────────────────────

def test_shanxi_truth_datetime_months():
    by = _by_value(p2.shanxi_parser.parse(SX_PATH, "山西"))
    assert by[(date(2025, 1, 1), "export_gwh")] == pytest.approx(54.75)
    assert by[(date(2025, 7, 1), "export_gwh")] == pytest.approx(63.85)
    assert by[(date(2026, 2, 1), "export_gwh")] == pytest.approx(31.01)
    assert by[(MAY26, "export_gwh")] == pytest.approx(51.92)


def test_shanxi_17_months_export_only_meta():
    rows = p2.shanxi_parser.parse(SX_PATH, "山西")
    assert len(rows) == 17
    assert {r.metric for r in rows} == {"export_gwh"}
    months = {r.month for r in rows}
    assert min(months) == date(2025, 1, 1) and max(months) == MAY26
    assert all(r.province == "山西" and r.unit == "亿千瓦时" for r in rows)


# ── 陕西: 长表 主网外送/主网外购, 2026-05 外购整行缺失 ─────────────────────────

def test_shaanxi_truth_both_directions():
    by = _by_value(p2.shaanxi_parser.parse(SAX_PATH, "陕西"))
    assert by[(date(2025, 1, 1), "export_gwh")] == pytest.approx(43.4)
    assert by[(date(2025, 1, 1), "import_gwh")] == pytest.approx(15.8)
    assert by[(date(2025, 8, 1), "export_gwh")] == pytest.approx(67.88)
    assert by[(date(2025, 8, 1), "import_gwh")] == pytest.approx(17.94)
    assert by[(date(2026, 1, 1), "export_gwh")] == pytest.approx(60.86)
    assert by[(date(2026, 1, 1), "import_gwh")] == pytest.approx(11.67)
    assert by[(MAY26, "export_gwh")] == pytest.approx(26.04)


def test_shaanxi_2026_05_import_row_absent_not_none():
    """原册 2026-05 主网外购整行缺失 → 不出 None 行, 当月只有外送."""
    rows = p2.shaanxi_parser.parse(SAX_PATH, "陕西")
    assert len(rows) == 33                             # 17 外送 + 16 外购
    by = _by_row(rows)
    assert (MAY26, "import_gwh") not in by
    assert (MAY26, "export_gwh") in by
    months = {r.month for r in rows}
    assert len(months) == 17
    assert min(months) == date(2025, 1, 1) and max(months) == MAY26


def test_shaanxi_sniff_misses_disclosure_workbook():
    """陕西披露册的 省间市场交易结算 宽表不解析 (格式图: prefer 结算长表)."""
    assert p2.shaanxi_parser.sniff(SAX_DISC) is False


# ── 辽宁: 双行复合表头, 活通道加总 ─────────────────────────────────────────────

def test_liaoning_truth_composite_header_sums():
    by = _by_value(p2.liaoning_parser.parse(LN_PATH, "辽宁"))
    # 受入 = Σ(统分水电,东北分部火电,蒙东,吉林,黑龙江)
    assert by[(date(2025, 2, 1), "import_gwh")] == pytest.approx(51.15)
    assert by[(date(2025, 12, 1), "import_gwh")] == pytest.approx(72.12)
    assert by[(MAY26, "import_gwh")] == pytest.approx(33.56)
    # 送出 = Σ(高岭通道外送,鲁固通道外送,东北区域内外送) — 蒙东送出死通道剔除
    assert by[(date(2025, 2, 1), "export_gwh")] == pytest.approx(3.54)
    assert by[(date(2025, 12, 1), "export_gwh")] == pytest.approx(12.78)
    assert by[(MAY26, "export_gwh")] == pytest.approx(10.347)


def test_liaoning_15_months_missing_2025_01_05():
    rows = p2.liaoning_parser.parse(LN_PATH, "辽宁")
    assert len(rows) == 30                             # 15 月 × 双向
    assert {r.metric for r in rows} == {"import_gwh", "export_gwh"}
    months = {r.month for r in rows}
    assert len(months) == 15
    assert date(2025, 1, 1) not in months and date(2025, 5, 1) not in months
    assert min(months) == date(2025, 2, 1) and max(months) == MAY26
    assert all(r.unit == "亿千瓦时" and r.section == "interprov" for r in rows)


def test_liaoning_dead_channel_excluded_not_none():
    """蒙东送出 全列 None/'NOT_FOUND' → 死通道剔除: 送出仍出数 (非 None)."""
    by = _by_value(p2.liaoning_parser.parse(LN_PATH, "辽宁"))
    for m in {r.month for r in p2.liaoning_parser.parse(LN_PATH, "辽宁")}:
        assert by[(m, "export_gwh")] is not None


# ── 山东: 购入合计 + 外送合计 万kWh→亿 ─────────────────────────────────────────

def test_shandong_import_truth():
    by = _by_value(p2.shandong_parser.parse(SD_PATH, "山东"))
    assert by[(date(2025, 1, 1), "import_gwh")] == pytest.approx(124.2)
    assert by[(date(2025, 12, 1), "import_gwh")] == pytest.approx(254.72)
    assert by[(date(2026, 1, 1), "import_gwh")] == pytest.approx(174.39)
    assert by[(MAY26, "import_gwh")] == pytest.approx(138.37)


def test_shandong_export_unit_conversion_wan_to_yi():
    """15_跨省外送明细 单位 万kWh → ÷10000: 2026-01 合计 4469.15万 = 0.446915亿."""
    by = _by_value(p2.shandong_parser.parse(SD_PATH, "山东"))
    assert by[(date(2026, 1, 1), "export_gwh")] == pytest.approx(0.446915)
    assert by[(date(2026, 2, 1), "export_gwh")] == pytest.approx(1.01195)
    assert by[(MAY26, "export_gwh")] == pytest.approx(0.9872327)
    # 若未换算, 值会大 4 个数量级 — 显式断言换算确实发生
    assert by[(date(2026, 1, 1), "export_gwh")] < 1


def test_shandong_22_rows_meta():
    rows = p2.shandong_parser.parse(SD_PATH, "山东")
    assert len(rows) == 22                             # 17 购入 + 5 外送
    assert {r.metric for r in rows} == {"import_gwh", "export_gwh"}
    in_months = {r.month for r in rows if r.metric == "import_gwh"}
    out_months = {r.month for r in rows if r.metric == "export_gwh"}
    assert len(in_months) == 17 and len(out_months) == 5
    assert min(in_months) == date(2025, 1, 1) and max(in_months) == MAY26
    assert min(out_months) == date(2026, 1, 1) and max(out_months) == MAY26
    assert all(r.unit == "亿千瓦时" for r in rows)     # 换算后统一单位


# ── 广西: 西电+云电双腿加总, Excel 序列/中文串混合月键 ─────────────────────────

def test_guangxi_truth_two_leg_sum():
    by = _by_value(p2.guangxi_parser.parse(GX_PATH, "广西"))
    assert by[(date(2025, 1, 1), "import_gwh")] == pytest.approx(18.15)  # 9.94+8.21
    assert by[(date(2025, 7, 1), "import_gwh")] == pytest.approx(51.31)  # 22.44+28.87
    assert by[(date(2025, 9, 1), "import_gwh")] == pytest.approx(54.58)  # 11.95+42.63
    assert by[(MAY26, "import_gwh")] == pytest.approx(22.83)  # 13.85+8.98


def test_guangxi_none_leg_propagates_not_zero():
    """2026-03 西电 None (云电 10.35 在) → 当月 None (绝不半腿出数), 行仍在."""
    rows = p2.guangxi_parser.parse(GX_PATH, "广西")
    r = _by_row(rows)[(date(2026, 3, 1), "import_gwh")]
    assert r.value is None
    assert r.value != 0


def test_guangxi_mixed_month_keys_excel_serial_and_string():
    """月份列: 2025 为 Excel 序列日 (45658=2025-01-01), 2026 为 '2026年1月'."""
    rows = p2.guangxi_parser.parse(GX_PATH, "广西")
    assert len(rows) == 17
    assert {r.metric for r in rows} == {"import_gwh"}   # partial import only
    months = {r.month for r in rows}
    assert len(months) == 17
    assert min(months) == date(2025, 1, 1) and max(months) == MAY26
    r = _by_row(rows)[(date(2025, 1, 1), "import_gwh")]
    assert r.province == "广西" and r.section == "interprov"
    assert r.period == "month" and r.unit == "亿千瓦时"


# ── registry 派发 + sniff 隔离 (双向) ──────────────────────────────────────────

def test_dispatch_aggregate_workbooks_unchanged():
    """聚合册 dispatch 仍归披露 parser (本波注册在后, 首个命中者解析)."""
    expected = {MD_PATH: "month_rows", GS_PATH: "month_rows", SX_PATH: "month_rows",
                SAX_DISC: "month_rows", LN_PATH: "month_rows", SD_PATH: "month_rows",
                GX_PATH: "prov_guangxi"}
    for path, fmt in expected.items():
        got, rows = registry.dispatch(path)
        assert got == fmt, f"{path.name}: {got} != {fmt}"
        assert rows, f"{path.name}: disclosure parser returned no rows"


def test_dispatch_shaanxi_settlement_workbook_lands_interprov2():
    """陕西结算册无披露 parser 命中 → dispatch 自然落 prov_interprov2."""
    fmt, rows = registry.dispatch(SAX_PATH)
    assert fmt == "prov_interprov2"
    assert len(rows) == 33
    assert {r.province for r in rows} == {"陕西"}
    assert {r.section for r in rows} == {"interprov"}


def test_interprov2_sniff_misses_wave1_side_files():
    """本波 sniff 不得命中 wave 1 旁册 (sheet 集互不相同)."""
    for path in W1_SIDES:
        for parser in ALL_PARSERS2:
            assert parser.sniff(path) is False, \
                f"{type(parser).__name__} sniff-hit {path.name}"


def test_interprov2_sniff_misses_other_provinces_workbooks():
    """本波 sniff 不得跨省命中 (目标 sheet 名在各册唯一, 实勘验证)."""
    own = {p2.mengdong_parser: MD_PATH, p2.gansu_parser: GS_PATH,
           p2.shanxi_parser: SX_PATH, p2.shaanxi_parser: SAX_PATH,
           p2.liaoning_parser: LN_PATH, p2.shandong_parser: SD_PATH,
           p2.guangxi_parser: GX_PATH}
    others = [MD_PATH, GS_PATH, SX_PATH, SAX_PATH, SAX_DISC, LN_PATH, SD_PATH,
              GX_PATH, FIX / "蒙西信息披露月报数据汇总_2022至2026年5月.xlsx",
              FIX / "广东电力市场运营简报_2026年1-5月.xlsx",
              FIX / "江苏-信息披露月报数据_截至2026年5月.xlsx",
              FIX / "安徽电力市场信息披露月报数据-截至2026年5月.xlsx",
              FIX / "河北南网-电力市场信息报告数据汇总_2025-01_2026-05.xlsx",
              FIX / "冀北-信息披露月报_截至2026年5月.xlsx"]
    for parser, mine in own.items():
        for path in others:
            if path == mine:
                continue
            assert parser.sniff(path) is False, \
                f"{type(parser).__name__} sniff-hit {path.name}"
        assert parser.sniff(mine) is True


def test_all_rows_share_interprov_vocabulary():
    """七省统一词汇表: section=interprov, unit=亿千瓦时, period=month."""
    cases = [(p2.mengdong_parser, MD_PATH, "蒙东"),
             (p2.gansu_parser, GS_PATH, "甘肃"),
             (p2.shanxi_parser, SX_PATH, "山西"),
             (p2.shaanxi_parser, SAX_PATH, "陕西"),
             (p2.liaoning_parser, LN_PATH, "辽宁"),
             (p2.shandong_parser, SD_PATH, "山东"),
             (p2.guangxi_parser, GX_PATH, "广西")]
    for parser, path, prov in cases:
        rows = parser.parse(path, prov)
        assert rows, f"{prov}: no rows"
        assert all(r.section == "interprov" and r.period == "month"
                   and r.unit == "亿千瓦时" and r.province == prov
                   for r in rows), prov
        assert all(r.metric in ("import_gwh", "export_gwh") for r in rows), prov
