# tests/province_fundamentals/test_jiaojie_2025.py
"""交接表 2025 年度解析器 tests — 2025年1-12月各省分类型利用小时.xlsx.

Fixture: tests/province_fundamentals/fixtures/2025年1-12月各省分类型利用小时.xlsx
(ditto-copied from data/market-fundamentals/【！飔合资料交接】/交易数据/ per the
controller ruling — tests must NOT read live data/ paths; OneDrive dataless files
hang the test run. md5 verified identical to the live source, 2026-11-02).

真值 (read from the real workbook, 2026-11-02):
  sheet 2025各省年度利用小时数: 华北|山东|火电|3997.03; 华东|安徽|总计|2724|-389
    (同比变化列单位为小时, -389 是小时数变化不是 %)
  sheet 全社会用电量: 广东 全社会用电量(合计) 9589.73 亿千瓦时 (该 sheet 仅广东)
  sheet 全国发电量: 山西省 累计发电量 4405.8 亿千瓦时; 福建省 装机/年度小时全行

口径裁定 (task-4 context): 全国发电量 sheet 装机/发电量/年度小时为全口径,
一律 _full 后缀 (cap_*_full / gen_*_full / hours_*_full, 与格式 B 全口径约定一致,
且 gen_* 不与格式 A 省调口径撞键). 年度行: period='year', month=date(2025,1,1).
"""
from datetime import date
from pathlib import Path

import pytest

from services.province_fundamentals.parsers.jiaojie_2025 import parse_jiaojie

FIXTURE = (Path(__file__).parent / "fixtures"
           / "2025年1-12月各省分类型利用小时.xlsx")
SRC = FIXTURE.name
Y2025 = date(2025, 1, 1)


@pytest.fixture(scope="module")
def rows():
    return parse_jiaojie(FIXTURE)


def _by_key(rows):
    return {(r.province, r.metric): r for r in rows}


# ── 真值断言 (task brief Step 1, binding) ─────────────────────────────────────

def test_shandong_thermal_annual_hours(rows):
    """华北|山东|火电|3997.03 — period='year', month=2025-01-01."""
    r = _by_key(rows)[("山东", "hours_thermal")]
    assert r.value == pytest.approx(3997.03)
    assert r.period == "year"
    assert r.month == Y2025
    assert r.section == "util_hours"
    assert r.unit == "小时"
    assert r.source == SRC


def test_anhui_total_hours_and_yoy(rows):
    """华东|安徽|总计|2724|-389 → 两行: hours_total=2724, hours_total_yoy=-389.
    同比变化列为小时数变化 (非 %) — unit 按实存 小时."""
    by = _by_key(rows)
    total = by[("安徽", "hours_total")]
    assert total.value == pytest.approx(2724)
    assert total.unit == "小时"
    yoy = by[("安徽", "hours_total_yoy")]
    assert yoy.value == pytest.approx(-389)
    assert yoy.section == "util_hours"
    assert yoy.period == "year"
    assert yoy.month == Y2025


def test_guangdong_demand_split(rows):
    """广东 全社会用电量合计 9589.73 亿千瓦时 + 产业拆分."""
    by = _by_key(rows)
    total = by[("广东", "demand_total")]
    assert total.value == pytest.approx(9589.73)
    assert total.section == "demand"
    assert total.unit == "亿千瓦时"
    assert total.period == "year"
    assert total.month == Y2025
    assert by[("广东", "demand_p1")].value == pytest.approx(171.51)
    assert by[("广东", "demand_p2")].value == pytest.approx(5506.04)
    assert by[("广东", "demand_p3")].value == pytest.approx(2285.99)
    assert by[("广东", "demand_residential")].value == pytest.approx(1626.19)


def test_shanxi_generation_full(rows):
    """全国发电量 sheet: 山西省 4405.8 亿千瓦时 (全口径 → gen_*_full)."""
    by = _by_key(rows)
    r = by[("山西", "gen_total_full")]
    assert r.value == pytest.approx(4405.8)
    assert r.section == "generation"
    assert r.unit == "亿千瓦时"
    assert r.period == "year"
    assert r.month == Y2025
    assert by[("山西", "gen_thermal_full")].value == pytest.approx(3545.8)
    assert by[("山西", "gen_hydro_full")].value == pytest.approx(45.5)
    assert by[("山西", "gen_wind_full")].value == pytest.approx(555.8)
    assert by[("山西", "gen_solar_full")].value == pytest.approx(258.7482)
    # 山西 核电为空 → 跳过不出行
    assert ("山西", "gen_nuclear_full") not in by


# ── 全国发电量 sheet: _full 口径与全行覆盖 ─────────────────────────────────────

def test_fujian_cap_and_hours_full(rows):
    """福建省是全发电量 sheet 中装机/小时填全的省: cap/hours 全 _full."""
    by = _by_key(rows)
    assert by[("福建", "cap_total_full")].value == pytest.approx(9577.2)
    assert by[("福建", "cap_thermal_full")].value == pytest.approx(4085.1)
    assert by[("福建", "cap_hydro_full")].value == pytest.approx(1720.4)
    assert by[("福建", "cap_wind_full")].value == pytest.approx(1804)
    assert by[("福建", "cap_solar_full")].value == pytest.approx(1633.6)
    assert by[("福建", "cap_nuclear_full")].value == pytest.approx(1287.4)
    assert by[("福建", "cap_storage_full")].value == pytest.approx(46.5)
    assert by[("福建", "cap_total_full")].unit == "万千瓦"
    assert by[("福建", "cap_total_full")].section == "capacity"
    assert by[("福建", "hours_total_full")].value == pytest.approx(3593.22)
    assert by[("福建", "hours_thermal_full")].value == pytest.approx(4795.23)
    assert by[("福建", "hours_hydro_full")].value == pytest.approx(1207.86)
    assert by[("福建", "hours_wind_full")].value == pytest.approx(1352.55)
    assert by[("福建", "hours_solar_full")].value == pytest.approx(151.52)
    assert by[("福建", "hours_nuclear_full")].value == pytest.approx(7812.65)
    assert by[("福建", "hours_total_full")].section == "util_hours"
    assert by[("福建", "hours_total_full")].unit == "小时"


def test_jiangxi_cap_and_hours_full(rows):
    """江西 核电装机/小时为空 → 跳过; 储能 cap=123 出, 储能 hours 不在词表不出."""
    by = _by_key(rows)
    assert by[("江西", "cap_total_full")].value == pytest.approx(8011)
    assert by[("江西", "cap_solar_full")].value == pytest.approx(2872)
    assert by[("江西", "cap_storage_full")].value == pytest.approx(123)
    assert by[("江西", "hours_total_full")].value == pytest.approx(2245.66)
    assert ("江西", "cap_nuclear_full") not in by
    assert ("江西", "hours_nuclear_full") not in by
    assert not [r for r in rows if "storage" in r.metric and "hours" in r.metric]


def test_gen_sheet_only_fujian_jiangxi_have_cap(rows):
    """源文件仅福建/江西填了装机列."""
    provs = {r.province for r in rows if r.section == "capacity"}
    assert provs == {"福建", "江西"}


# ── 省份规范化 ─────────────────────────────────────────────────────────────────

def test_hebei_skipped_unmapped_province(rows):
    """河北省 在全国发电量 sheet 中 → 河北南网/冀北 归属不定, 整行跳过 (不猜映射)."""
    assert not [r for r in rows if r.province in ("河北", "河北省")]


def test_province_suffix_normalized(rows):
    """山东省→山东, 北京市→北京, 内蒙古自治区→内蒙古 等后缀剥除."""
    provs = {r.province for r in rows}
    assert "山东" in provs and "山东省" not in provs
    assert "北京" in provs and "北京市" not in provs
    assert "内蒙古" in provs
    assert "广西壮族自治区" not in provs and "广西" in provs


def test_subregion_names_kept(rows):
    """冀南/冀北/蒙东/蒙西 在利用小时 sheet 中是独立行 — 原样保留."""
    by = _by_key(rows)
    assert by[("冀南", "hours_total")].value == pytest.approx(3335)
    assert by[("冀北", "hours_total")].value == pytest.approx(1894.92)
    assert by[("蒙东", "hours_wind")].value == pytest.approx(2243.07)
    assert by[("蒙西", "hours_thermal")].value == pytest.approx(4048.14)


# ── 利用小时 sheet: 类型映射与口径行 ──────────────────────────────────────────

def test_sichuan_full_scope_wins_over_shengdiao(rows):
    """四川同类型有 全口径/省调 双行 (备注列) — 只取全口径, 省调跳过."""
    by = _by_key(rows)
    assert by[("四川", "hours_total")].value == pytest.approx(3693)     # 全口径
    assert by[("四川", "hours_thermal")].value == pytest.approx(4150)   # 非 4335
    assert by[("四川", "hours_solar")].value == pytest.approx(1187)
    assert by[("四川", "hours_total_yoy")].value == pytest.approx(-285)
    sichuan = [r for r in rows if r.province == "四川"
               and r.section == "util_hours"]
    assert len(sichuan) == 10           # 5 stems × (hours + yoy), 无省调副本


def test_type_label_variants_map_to_canonical_stems(rows):
    """合计/全省平均→total; 太阳能/太阳能发电/太阳→solar."""
    by = _by_key(rows)
    assert by[("陕西", "hours_total")].value == pytest.approx(2959)     # 合计
    assert by[("河南", "hours_total")].value == pytest.approx(2478)     # 全省平均
    assert by[("河南", "hours_solar")].value == pytest.approx(1051)     # 太阳能
    assert by[("天津", "hours_solar")].value == pytest.approx(1098.29)  # 太阳能发电
    assert by[("湖南", "hours_solar")].value == pytest.approx(861.6)    # 太阳


def test_unmapped_type_labels_skipped(rows):
    """煤电/燃气/燃煤/垃圾/集中式光伏/分布式光伏/光伏（集中式）/抽水蓄能/储能/
    统调机组（含自备）/省调机组平均 等子项或口径行一律跳过 — 不出现双计."""
    by = _by_key(rows)
    # 广东只有 煤电/燃气发电 (火电子项) — 不得合成 hours_thermal
    assert ("广东", "hours_thermal") not in by
    assert ("广东", "hours_total") not in by      # 无总计行
    assert by[("广东", "hours_nuclear")].value == pytest.approx(7795)
    # 云南 有父行 光伏=904.3; 集中式/分布式光伏 子项跳过, 只出一条 solar 行
    yunnan_solar = [r for r in rows if r.province == "云南"
                    and r.metric == "hours_solar"]
    assert len(yunnan_solar) == 1
    assert yunnan_solar[0].value == pytest.approx(904.3)
    # 山西 省调机组平均 不是 total
    assert ("山西", "hours_total") not in by
    # 蒙西 总计（统调）不取
    assert ("蒙西", "hours_total") not in by
    # 冀北 抽水蓄能/煤矸石/燃气/垃圾/生物质 等子项全跳过: 只剩 5 个 stem
    jibei = {r.metric for r in rows if r.province == "冀北"}
    assert jibei == {"hours_total", "hours_hydro", "hours_thermal",
                     "hours_wind", "hours_solar"}


def test_no_province_metric_collision(rows):
    """同 (province, metric) 只出一行 (唯一键含 source, 全表同 source)."""
    keys = [(r.province, r.metric) for r in rows]
    assert len(keys) == len(set(keys))


# ── 结构不变量 ─────────────────────────────────────────────────────────────────

def test_all_rows_annual_with_required_fields(rows):
    """全部行 period='year', month=2025-01-01, source=文件名, 空格跳过不出 None 行."""
    assert rows
    for r in rows:
        assert r.period == "year"
        assert r.month == Y2025
        assert r.source == SRC
        assert r.value is not None
        assert r.province
        assert r.unit in ("小时", "万千瓦", "亿千瓦时")


def test_metric_vocabulary(rows):
    """metric 全集 ⊆ 词表: hours_*(_yoy) / demand_* / gen_*_full / cap_*_full /
    hours_*_full."""
    stems = {"total", "thermal", "hydro", "nuclear", "wind", "solar"}
    allowed = ({f"hours_{s}" for s in stems}
               | {f"hours_{s}_yoy" for s in stems}
               | {f"hours_{s}_full" for s in stems}
               | {f"gen_{s}_full" for s in stems}
               | {f"cap_{s}_full" for s in stems}
               | {"cap_storage_full"}
               | {"demand_total", "demand_p1", "demand_p2", "demand_p3",
                  "demand_residential"})
    metrics = {r.metric for r in rows}
    assert metrics <= allowed
    # 三个 sheet 都出了数
    assert {r.section for r in rows} == {"util_hours", "demand", "generation",
                                         "capacity"}


def test_demand_sheet_only_guangdong(rows):
    """全社会用电量 sheet 源文件仅广东一省."""
    assert {r.province for r in rows if r.section == "demand"} == {"广东"}


# ── 缺失 sheet 报错 ────────────────────────────────────────────────────────────

def test_missing_sheet_raises(tmp_path):
    import openpyxl
    wb = openpyxl.Workbook()
    wb.active.title = "无关sheet"
    wb.active.append(["a", "b"])
    p = tmp_path / "junk.xlsx"
    wb.save(p)
    with pytest.raises(ValueError):
        parse_jiaojie(p)
