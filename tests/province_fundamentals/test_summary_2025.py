# tests/province_fundamentals/test_summary_2025.py
"""2023-2025 全国汇总册年度解析器 tests — summary_2025 (loader-reuse parser).

Fixture: tests/province_fundamentals/fixtures/2023-2025 全国各省电力市场基础信息汇总2026-03-30.xlsx
(ditto-copied from data/market-fundamentals/ per the controller ruling — tests must
NOT read live data/ paths; OneDrive dataless files hang the test run.
md5 0a0f165f43e93e0102a2b7efc151d9c3 verified identical to the live source, 2026-11-03;
fixture size 1,423,950 bytes ≈ 1.4 MB, under the 5 MB bar).

真值 (read from the real loader output on the fixture, 2026-11-03):
  安徽 2024 cap 火电 6340.07 万千瓦 / 2025 cap 火电 4706.1 (loader 位置年份约定)
  北京 2024 cap_total_full 1408.0 — 与册内陈述总计逐值相等 (核验加总派生)
  山东 2025 cap_total_full 25742.88 / gen_thermal_full 4902.89
  广东 2025 cap_storage_full 1343.0
  30 省 2025 cap_total_full > 0 (缺 上海/重庆)

task-brief 年份差异裁定 (binding): 任务书 pin "安徽 2025 cap_thermal_full ≈ 6340.07"
与 loader 实际输出不符 — loader 位置约定 (锚点按列排序第 i 块 → 2024+i) 把
6340.07 记入 2024 (该块 统计截止=2025-03-31, 系源册 vintage 错乱), 2025 块
实为 4706.1。按 "verify against the actual loader output" 条款, pin loader 真值
两年各一条, 差异记录于 parser docstring 与 task 报告。

跳过清单 (实勘): 重庆 整省缺席 — 其 sheet 用 需求5/需求6 锚点标签而非 需求13/14,
loader 探不到 → 零数据块, 不猜映射; 江西 整省缺席 (sheet 14江西-暂未上线 被
loader 跳过); 上海 2025 capacity 块 6 燃料行全空 (源册缺报) → 该块跳过。
"""
from datetime import date
from pathlib import Path

import pytest

from services.province_fundamentals import registry
from services.province_fundamentals.parsers import summary_2025
from services.province_fundamentals.parsers.summary_2025 import (
    parse_summary_2025, sniff)

FIXTURE = (Path(__file__).parent / "fixtures"
           / "2023-2025 全国各省电力市场基础信息汇总2026-03-30.xlsx")
FIXTURE_JIAOJIE = (Path(__file__).parent / "fixtures"
                   / "2025年1-12月各省分类型利用小时.xlsx")
FIXTURE_FORMAT_A = (Path(__file__).parent / "fixtures"
                    / "安徽电力市场信息披露月报数据-截至2026年5月.xlsx")
SRC = FIXTURE.name
Y2024 = date(2024, 1, 1)
Y2025 = date(2025, 1, 1)


@pytest.fixture(scope="module")
def rows():
    return parse_summary_2025(FIXTURE)


def _by_key(rows):
    return {(r.province, r.month.year, r.metric): r for r in rows}


# ── 真值断言 (task brief, binding; 年份按 loader 实际输出) ─────────────────────

def test_anhui_thermal_capacity_both_years(rows):
    """安徽 火电 6340.07 在 loader 的 2024 (task pin 写的 2025 系年份差异,
    见模块 docstring); loader 的 2025 实为 4706.1."""
    by = _by_key(rows)
    r24 = by[("安徽", 2024, "cap_thermal_full")]
    assert r24.value == pytest.approx(6340.07)
    assert r24.period == "year"
    assert r24.month == Y2024
    assert r24.section == "capacity"
    assert r24.unit == "万千瓦"
    assert r24.source == SRC
    r25 = by[("安徽", 2025, "cap_thermal_full")]
    assert r25.value == pytest.approx(4706.1)
    assert r25.month == Y2025


def test_beijing_2024_total_matches_workbook_stated_total(rows):
    """北京 2024: 六燃料加总 24+130+1153+101 = 1408, 与册内陈述总计逐值相等 —
    核验 cap_total_full 加总派生规则."""
    by = _by_key(rows)
    total = by[("北京", 2024, "cap_total_full")]
    assert total.value == pytest.approx(1408.0)
    assert total.section == "capacity"
    assert total.unit == "万千瓦"
    assert by[("北京", 2024, "gen_total_full")].value == pytest.approx(432.37)


def test_shandong_2025_values(rows):
    by = _by_key(rows)
    assert by[("山东", 2025, "cap_total_full")].value == pytest.approx(25742.88)
    assert by[("山东", 2025, "cap_thermal_full")].value == pytest.approx(12266.33)
    assert by[("山东", 2025, "cap_solar_full")].value == pytest.approx(9484.86)
    assert by[("山东", 2025, "cap_nuclear_full")].value == pytest.approx(571.8)
    assert by[("山东", 2025, "gen_thermal_full")].value == pytest.approx(4902.89)
    assert by[("山东", 2025, "gen_total_full")].unit == "亿千瓦时"


def test_guangdong_2025_storage(rows):
    """储能单列: 广东 2025 cap_storage_full=1343; gen 储能缺报不出行."""
    by = _by_key(rows)
    assert by[("广东", 2025, "cap_storage_full")].value == pytest.approx(1343.0)
    assert ("广东", 2025, "gen_storage_full") not in by


def test_at_least_three_provinces_2025_cap_total_full(rows):
    """task pin: ≥3 省 2025 cap_total_full > 0 — 实为 30 省 (缺 上海/重庆)."""
    provs = {r.province for r in rows
             if r.metric == "cap_total_full" and r.month == Y2025
             and r.value is not None and r.value > 0}
    assert len(provs) == 30
    assert "上海" not in provs and "重庆" not in provs


# ── 两年同出 (loader 提供 2024 的省必出 2025) ─────────────────────────────────

def test_both_year_emission(rows):
    """loader 只产 2024/2025 (全册无 2023); 凡有 2024 行的省必有 2025 行."""
    assert rows
    assert {r.month for r in rows} == {Y2024, Y2025}
    p24 = {r.province for r in rows if r.month == Y2024}
    p25 = {r.province for r in rows if r.month == Y2025}
    assert p24 == p25
    assert len(p24) == 31


# ── 跳过清单 (不猜映射) ─────────────────────────────────────────────────────────

def test_chongqing_and_jiangxi_absent(rows):
    """重庆 (需求5/6 锚点标签, loader 探不到) 与 江西 (暂未上线 sheet) 整省缺席."""
    provs = {r.province for r in rows}
    assert "重庆" not in provs
    assert "江西" not in provs
    assert len(provs) == 31


def test_shanghai_2025_capacity_block_empty(rows):
    """上海 2025 capacity 块源册缺报 (6 燃料行全空) → 块跳过;
    2024 capacity 与两年 generation 照出."""
    by = _by_key(rows)
    assert ("上海", 2025, "cap_total_full") not in by
    assert ("上海", 2025, "cap_thermal_full") not in by
    assert by[("上海", 2024, "cap_thermal_full")].value == pytest.approx(2542)
    assert by[("上海", 2025, "gen_thermal_full")].value == pytest.approx(958.3)
    sh_2025 = [r for r in rows if r.province == "上海" and r.month == Y2025]
    assert sh_2025 and all(r.section == "generation" for r in sh_2025)


# ── 结构不变量 ─────────────────────────────────────────────────────────────────

def test_all_rows_annual_with_required_fields(rows):
    """全部行 period='year', month∈{2024-01-01, 2025-01-01}, source=文件名,
    空格跳过不出 value=None 行 (年度表空格即缺报)."""
    assert rows
    for r in rows:
        assert r.period == "year"
        assert r.month in (Y2024, Y2025)
        assert r.source == SRC
        assert r.value is not None
        assert r.province
        assert r.section in ("capacity", "generation")
        assert r.unit in ("万千瓦", "亿千瓦时")


def test_metric_vocabulary(rows):
    """metric 全集 ⊆ 词表: {cap,gen} × {thermal,hydro,nuclear,wind,solar,
    storage,total}_full (全册全口径, 一律 _full 后缀)."""
    stems = {"thermal", "hydro", "nuclear", "wind", "solar", "storage", "total"}
    allowed = ({f"cap_{s}_full" for s in stems}
               | {f"gen_{s}_full" for s in stems})
    metrics = {r.metric for r in rows}
    assert metrics <= allowed
    # capacity / generation 两 section 都出了数, 总计行都派生了
    assert {r.section for r in rows} == {"capacity", "generation"}
    assert "cap_total_full" in metrics and "gen_total_full" in metrics


def test_section_unit_consistent(rows):
    for r in rows:
        if r.section == "capacity":
            assert r.unit == "万千瓦"
        else:
            assert r.unit == "亿千瓦时"


def test_no_duplicate_keys(rows):
    """同 (province, month, section, metric) 只出一行 (唯一键含 source, 全表同 source)."""
    keys = [(r.province, r.month, r.section, r.metric) for r in rows]
    assert len(keys) == len(set(keys))


def test_rows_sorted(rows):
    keys = [(r.province, r.month, r.section, r.metric) for r in rows]
    assert keys == sorted(keys)


# ── 结构嗅探 + registry 全国册通道 ─────────────────────────────────────────────

def test_sniff_hits_summary_fixture():
    assert sniff(FIXTURE)


def test_sniff_rejects_other_workbooks(tmp_path):
    """全国册 sniff 不得误吃交接表/省月报/垃圾工作簿."""
    import openpyxl
    assert not sniff(FIXTURE_JIAOJIE)
    assert not sniff(FIXTURE_FORMAT_A)
    wb = openpyxl.Workbook()
    wb.active.title = "无关sheet"
    wb.active.append(["a", "b"])
    p = tmp_path / "junk.xlsx"
    wb.save(p)
    assert not sniff(p)
    assert not sniff(tmp_path / "不存在.xlsx")


def test_registry_dispatch_hits_summary():
    """路径无省份名 → national 通道: jiaojie miss 后 summary_2025 命中."""
    assert "summary_2025" in registry.known_formats()
    fmt, rows = registry.dispatch(FIXTURE)
    assert fmt == "summary_2025"
    assert rows
    assert "安徽" in {r.province for r in rows}


def test_registry_jiaojie_fixture_still_jiaojie():
    """交接表 fixture 仍命中 jiaojie_2025 — 两个 national parser 互不抢食."""
    fmt, rows = registry.dispatch(FIXTURE_JIAOJIE)
    assert fmt == "jiaojie_2025"
    assert rows


# ── 垃圾工作簿报错 (registry 捕获转 []; 直调抛 ValueError) ─────────────────────

def test_junk_workbook_raises(tmp_path):
    import openpyxl
    wb = openpyxl.Workbook()
    wb.active.title = "无关sheet"
    wb.active.append(["a", "b"])
    p = tmp_path / "junk.xlsx"
    wb.save(p)
    with pytest.raises(ValueError):
        parse_summary_2025(p)
