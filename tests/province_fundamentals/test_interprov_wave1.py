# tests/province_fundamentals/test_interprov_wave1.py
"""省间 (interprov) 月度受入/外送 parser tests — Wave 1: 安徽 / 江苏 / 河北南网 / 冀北.

Fixtures ditto'd from data/exchange-monthly-reports/<省>月报/ (controller ruling:
tests never read live data/ paths). 全部当月值 (无累计表) → 无 (差分) 标.

真值 (read from the real fixtures, 2026-11-04, openpyxl data_only):

安徽 安徽跨区跨省省间购入电量_截至2026年5月.xlsx · 省间购入电量 (表头 R1, 14 数据行):
  年份 int + 月份 "N月" 复合键; 购入 only (中长期购入实际执行, 结算口径).
  2025-04 31.15 | 2025-07 59.94 | 2025-12 44.23 | 2026-01 47.59 | 2026-05 36.27

江苏 江苏跨区跨省月度交易计划分类汇总_2025-09至2026-05.xlsx · 月度明细
  (R1 标题 / R2 单位来源注记 / R3 空 / R4 真表头, 9 数据行; 计划口径 → source 打
  " (计划)" 标):
  2025-09 受进 189.24 / 送出 4.02 | 2026-01 145.24 / 4 | 2026-05 104.71 / 5.79
  净受进 = 派生列 → 不取.

河北南网 河北南网省间交易结算汇总-2025.4-2026.5.xlsx · 窄表
  (R1 标题 / R2 表头, 91 数据行 = 13 月 × 7 行; 结算口径, 外送 only):
  月出口 = 省间现货合计 + 省间中长期外送合计 (只取 发电类别=合计 行):
  2025-04 0.14+0.61=0.75 | 2025-09 0.18+0.5=0.68 | 2025-12 0.04+0.01=0.05
  2026-01 现货 0.22 + 中长期 '—' → None (任一空腿 → 无法加总)
  2026-03 0.44+0.62=1.06 | 2026-05 0.25+0.02=0.27
  ⚠ 原册缺 2026-04 整月 → 13 个月.

冀北 冀北2024年以来电力市场交易结算通报数据（含省间）-2026-05.xlsx (19 sheets):
  购入 = 01_电源侧总览 滤 电源类型=购华北:
    2024-01 38.36 | 2024-03 22.7 | 2025-05 21.1 | 2025-09 37.93
    2025-12 15.69 | 2026-01 21.51 | 2026-05 19.77
  外送 = 12_省间外送 滤 类别=合计 (合计行逐月唯一, 分项行不读 — 分项含同值重复行
    2024-01 跨京津唐中长期火电 0.06×2 且重复值计入合计: 去重求和 23.32 ≠ 合计
    23.38, 证明重复行实为同值两笔, 合计行唯一权威):
    2024-01 23.38 | 2024-02 31.8 | 2025-09 0.85 | 2025-12 29.72
    2026-01 28.26 | 2026-04 20.48
  ⚠ 2026-05 合计行单元格右移 (结算电量格空, 17.36 错落均价列) → 按表头名列读取
    得 None (reported-but-empty), 不做位移修正.
  两表均 27 个月 (2024-01→2026-05, 全书缺 2025-10/2025-11).
"""
from datetime import date
from pathlib import Path

import pytest

from services.province_fundamentals import registry
from services.province_fundamentals.parsers import prov_interprov as pi

FIX = Path(__file__).parent / "fixtures"
AH_PATH = FIX / "安徽跨区跨省省间购入电量_截至2026年5月.xlsx"
JS_PATH = FIX / "江苏跨区跨省月度交易计划分类汇总_2025-09至2026-05.xlsx"
JN_PATH = FIX / "河北南网省间交易结算汇总-2025.4-2026.5.xlsx"
JB_PATH = FIX / "冀北2024年以来电力市场交易结算通报数据（含省间）-2026-05.xlsx"

# 披露主册 fixtures (sniff 隔离对照)
AH_DISC = FIX / "安徽电力市场信息披露月报数据-截至2026年5月.xlsx"
JS_DISC = FIX / "江苏-信息披露月报数据_截至2026年5月.xlsx"
JN_DISC = FIX / "河北南网-电力市场信息报告数据汇总_2025-01_2026-05.xlsx"
JB_DISC = FIX / "冀北-信息披露月报_截至2026年5月.xlsx"

MAY26 = date(2026, 5, 1)


def _by_value(rows):
    return {(r.month, r.metric): r.value for r in rows}


def _by_row(rows):
    return {(r.month, r.metric): r for r in rows}


# ── 安徽: 购入 only, 年份 int + "N月" 复合键 ────────────────────────────────────

def test_anhui_import_truth():
    by = _by_value(pi.anhui_parser.parse(AH_PATH, "安徽"))
    assert by[(date(2025, 4, 1), "import_gwh")] == pytest.approx(31.15)
    assert by[(date(2025, 7, 1), "import_gwh")] == pytest.approx(59.94)
    assert by[(date(2025, 12, 1), "import_gwh")] == pytest.approx(44.23)
    assert by[(date(2026, 1, 1), "import_gwh")] == pytest.approx(47.59)
    assert by[(MAY26, "import_gwh")] == pytest.approx(36.27)


def test_anhui_import_only_14_months_meta():
    rows = pi.anhui_parser.parse(AH_PATH, "安徽")
    assert len(rows) == 14
    assert {r.metric for r in rows} == {"import_gwh"}        # 购入 only
    months = {r.month for r in rows}
    assert min(months) == date(2025, 4, 1) and max(months) == MAY26
    r = _by_row(rows)[(date(2025, 4, 1), "import_gwh")]
    assert r.province == "安徽"
    assert r.section == "interprov"
    assert r.period == "month"
    assert r.unit == "亿千瓦时"
    assert r.source == AH_PATH.name                          # 结算口径不打标
    assert "(差分)" not in r.source and "(计划)" not in r.source


# ── 江苏: 双向, 计划口径打标, 3 行前导 ──────────────────────────────────────────

def test_jiangsu_both_directions_truth():
    by = _by_value(pi.jiangsu_parser.parse(JS_PATH, "江苏"))
    assert by[(date(2025, 9, 1), "import_gwh")] == pytest.approx(189.24)
    assert by[(date(2025, 9, 1), "export_gwh")] == pytest.approx(4.02)
    assert by[(date(2026, 1, 1), "import_gwh")] == pytest.approx(145.24)
    assert by[(date(2026, 1, 1), "export_gwh")] == pytest.approx(4)
    assert by[(MAY26, "import_gwh")] == pytest.approx(104.71)
    assert by[(MAY26, "export_gwh")] == pytest.approx(5.79)


def test_jiangsu_plan_tag_and_no_derived_metric():
    """计划口径: source 打 " (计划)" 标 (口径可追溯), metric 不变; 净受进派生列不取."""
    rows = pi.jiangsu_parser.parse(JS_PATH, "江苏")
    assert len(rows) == 18                                   # 9 月 × 双向
    assert {r.metric for r in rows} == {"import_gwh", "export_gwh"}
    assert all(r.source == JS_PATH.name + " (计划)" for r in rows)
    assert all(r.unit == "亿千瓦时" and r.section == "interprov" for r in rows)
    months = {r.month for r in rows}
    assert min(months) == date(2025, 9, 1) and max(months) == MAY26
    assert len(months) == 9


def test_jiangsu_header_below_preamble_found():
    """3 行前导 (标题/单位注记/空行) 后 R4 真表头必须被扫描窗找到."""
    assert pi.jiangsu_parser.sniff(JS_PATH) is True


# ── 河北南网: 外送 only, 窄表合计双腿加总 ───────────────────────────────────────

def test_jinan_export_truth_two_leg_sum():
    by = _by_value(pi.jinan_parser.parse(JN_PATH, "河北南网"))
    assert by[(date(2025, 4, 1), "export_gwh")] == pytest.approx(0.75)   # 0.14+0.61
    assert by[(date(2025, 9, 1), "export_gwh")] == pytest.approx(0.68)   # 0.18+0.5
    assert by[(date(2025, 12, 1), "export_gwh")] == pytest.approx(0.05)  # 0.04+0.01
    assert by[(date(2026, 3, 1), "export_gwh")] == pytest.approx(1.06)   # 0.44+0.62
    assert by[(MAY26, "export_gwh")] == pytest.approx(0.27)              # 0.25+0.02


def test_jinan_none_leg_propagates_not_zero():
    """2026-01 中长期外送合计 '—' → 当月 None (绝不半腿出数, 绝不写 0), 行仍在."""
    rows = pi.jinan_parser.parse(JN_PATH, "河北南网")
    r = _by_row(rows)[(date(2026, 1, 1), "export_gwh")]
    assert r.value is None
    assert r.value != 0
    # 对照: 同月现货腿 0.22 真实存在 — None 合计拒绝把 0.22 当全月出口
    assert r.unit == "亿千瓦时" and r.section == "interprov"


def test_jinan_export_only_13_months_missing_2026_04():
    """原册缺 2026-04 整月 → 13 个月, 外送 only, 结算口径不打标."""
    rows = pi.jinan_parser.parse(JN_PATH, "河北南网")
    assert len(rows) == 13
    assert {r.metric for r in rows} == {"export_gwh"}
    months = {r.month for r in rows}
    assert date(2026, 4, 1) not in months
    assert min(months) == date(2025, 4, 1) and max(months) == MAY26
    r = _by_row(rows)[(date(2025, 4, 1), "export_gwh")]
    assert r.province == "河北南网"
    assert r.source == JN_PATH.name


# ── 冀北: 双向双 sheet; 合计行权威; 2026-05 位移格 ─────────────────────────────

def test_jibei_import_truth():
    by = _by_value(pi.jibei_parser.parse(JB_PATH, "冀北"))
    assert by[(date(2024, 1, 1), "import_gwh")] == pytest.approx(38.36)
    assert by[(date(2024, 3, 1), "import_gwh")] == pytest.approx(22.7)
    assert by[(date(2025, 5, 1), "import_gwh")] == pytest.approx(21.1)
    assert by[(date(2025, 9, 1), "import_gwh")] == pytest.approx(37.93)
    assert by[(date(2025, 12, 1), "import_gwh")] == pytest.approx(15.69)
    assert by[(date(2026, 1, 1), "import_gwh")] == pytest.approx(21.51)
    assert by[(MAY26, "import_gwh")] == pytest.approx(19.77)


def test_jibei_export_truth_total_row_authoritative():
    """类别=合计 行权威: 2024-01 = 23.38 (册内合计), 非分项去重求和 23.32 —
    分项含同值重复行且重复值计入合计 (同值两笔), 故分项绝不读取."""
    by = _by_value(pi.jibei_parser.parse(JB_PATH, "冀北"))
    assert by[(date(2024, 1, 1), "export_gwh")] == pytest.approx(23.38)
    assert by[(date(2024, 1, 1), "export_gwh")] != pytest.approx(23.32)
    assert by[(date(2024, 2, 1), "export_gwh")] == pytest.approx(31.8)
    assert by[(date(2025, 9, 1), "export_gwh")] == pytest.approx(0.85)
    assert by[(date(2025, 12, 1), "export_gwh")] == pytest.approx(29.72)
    assert by[(date(2026, 1, 1), "export_gwh")] == pytest.approx(28.26)
    assert by[(date(2026, 4, 1), "export_gwh")] == pytest.approx(20.48)


def test_jibei_2026_05_shifted_cell_export_none():
    """2026-05 合计行单元格右移 (电量格空, 17.36 错落均价列) → 按表头名列读取
    得 None, 不做位移修正; 同月购入腿正常出 19.77."""
    rows = pi.jibei_parser.parse(JB_PATH, "冀北")
    by = _by_row(rows)
    assert by[(MAY26, "export_gwh")].value is None
    assert by[(MAY26, "import_gwh")].value == pytest.approx(19.77)


def test_jibei_27_months_missing_2025_10_11_meta():
    rows = pi.jibei_parser.parse(JB_PATH, "冀北")
    assert len(rows) == 54                                   # 27 月 × 双向
    assert {r.metric for r in rows} == {"import_gwh", "export_gwh"}
    months = {r.month for r in rows}
    assert len(months) == 27
    assert min(months) == date(2024, 1, 1) and max(months) == MAY26
    assert date(2025, 10, 1) not in months and date(2025, 11, 1) not in months
    assert all(r.unit == "亿千瓦时" and r.section == "interprov" for r in rows)
    assert all(r.source == JB_PATH.name for r in rows)       # 结算口径不打标
    assert all(r.province == "冀北" for r in rows)


# ── registry 派发 + sniff 隔离 (披露主册互不截胡) ───────────────────────────────

def test_dispatch_four_side_files():
    expected = {AH_PATH: ("安徽", 14), JS_PATH: ("江苏", 18),
                JN_PATH: ("河北南网", 13), JB_PATH: ("冀北", 54)}
    for path, (prov, n) in expected.items():
        fmt, rows = registry.dispatch(path)
        assert fmt == "prov_interprov"
        assert len(rows) == n
        assert {r.province for r in rows} == {prov}
        assert {r.section for r in rows} == {"interprov"}


def test_interprov_sniff_misses_disclosure_workbooks():
    """旁册 sniff 不得命中披露主册 (sheet 集互不相同)."""
    assert pi.anhui_parser.sniff(AH_DISC) is False
    assert pi.jiangsu_parser.sniff(JS_DISC) is False
    assert pi.jinan_parser.sniff(JN_DISC) is False
    assert pi.jibei_parser.sniff(JB_DISC) is False


def test_disclosure_workbooks_still_dispatch_to_original_parsers():
    """披露主册派发不变 (旁册 parser 注册在后, 披露 parser 先命中)."""
    assert registry.dispatch(AH_DISC)[0] == "format_a"
    assert registry.dispatch(JS_DISC)[0] == "prov_jiangsu"
    assert registry.dispatch(JN_DISC)[0] == "month_rows"
    assert registry.dispatch(JB_DISC)[0] == "month_rows"
