from datetime import date
from pathlib import Path

import pytest
from pptx import Presentation
from pptx.util import Inches

from libs.asset_risk.review.aliases import STATION_ALIASES, asset_code_for
from libs.asset_risk.review.parse_bess import deck_month, parse_bess_deck


def _add_table(prs, grid, merges=()):
    s = prs.slides.add_slide(prs.slide_layouts[6])
    t = s.shapes.add_table(len(grid), len(grid[0]), Inches(0), Inches(0), Inches(9), Inches(3)).table
    for i, r in enumerate(grid):
        for j, v in enumerate(r):
            t.cell(i, j).text = v
    for a, b in merges:
        t.cell(*a).merge(t.cell(*b))


def _title(prs, label, date_line):
    s = prs.slides.add_slide(prs.slide_layouts[6])
    for txt in (label, date_line):
        s.shapes.add_textbox(0, 0, Inches(3), Inches(1)).text_frame.text = txt


OV_H = ["场站名称", "月度总收益（万元）", "额定容量收益\n（元/MWh）", "日均充电次数", "现货价差\n（元/MWh）",
        "实发容量补偿单价\n（元/MWh）", "系统运行费单价（元/MWh)"]
OV = [["", "关键收益指标", "", "", "", "", ""], OV_H,
      ["苏右", "466.70", "11668", "1.24", "73.40", "352.07", "35.5"],
      ["谷山梁", "2830.47", "14152", "1.30", "123.62", "349.58", ""],
      ["武川", "-890.44", "", "0.53", "21.92", "0", ""],
      ["神秘站", "1", "2", "3", "4", "5", "6"]]
BD = [["曲线", "远景景蓝", "", "", "裕昭新能源", "", ""],
      ["", "收益\n(万元)", "充电量(MWh)", "放电量(MWh)"] * 1 + ["收益", "充电量", "放电量"],
      ["策略申报收益", "651.7", "16,596.0", "16,151.2", "3,323.3", "83,562.6", "75,552.9"],
      ["实时出清收益", "598.7", "16,133.3", "14,856.5", "3,234.2", "80,618.2", "72,595.5"],
      ["实际执行收益", "540.2", "15,471.8", "13,867.3", "3,255.1", "79,784.9", "72,369.7"]]
TWO = [["", "关键收益指标"] + [""] * 4,
       ["场站名称", "月度总收益（万元）", "", "容量补贴单价\n（元/MWh）", "", "日均充电次数"],
       ["月份", "8月", "9月", "8月", "9月", "8月"],
       ["苏右", "383.75", "466.70", "351.69", "352.07", "1.22"]]


def _deck(tmp_path, tables, label="蒙西储能场站复盘（9月）", dl="\n2026.10", name="x.pptx"):
    prs = Presentation()
    _title(prs, label, dl)
    for g in tables:
        _add_table(prs, g)
    p = tmp_path / name
    prs.save(p)
    return p


def _v(df, code, metric):
    r = df[(df.asset_code == code) & (df.metric == metric)]
    assert len(r) == 1, (code, metric)
    return r.value.iloc[0]


def test_alias_longest_match():
    assert asset_code_for("乌海康富") == "wuhai"
    assert asset_code_for("远景乌拉特") == "wulate"
    assert asset_code_for("固山梁") == "gushanliang"
    assert asset_code_for("四子王旗") == "siziwangqi"
    assert asset_code_for("nobody") is None
    flat = {a for v in STATION_ALIASES.values() for a in v}
    for a in "苏右 远景景蓝 景蓝乌尔图 杭锦旗 悦杭独贵 四子王旗 四子王 景通四益堂 谷山梁 固山梁 裕昭新能源 裕昭沙子坝 乌海 乌海康富 乌拉特 远景乌拉特 巴盟 景怡查干哈达 武川 阿拉善".split():
        assert a in flat


def test_deck_month_labels():
    assert deck_month(["蒙西储能场站复盘（9月）", "\n2026.10"]) == date(2026, 9, 1)
    assert deck_month(["复盘（12月）", "2027.1"]) == date(2026, 12, 1)
    assert deck_month(["no label"], "蒙西6月内部复盘.pptx") is None  # no year anywhere
    assert deck_month(["2026.7"], "蒙西6月内部复盘.pptx") == date(2026, 6, 1)
    assert deck_month(["上半年复盘"], "上半年.pptx") is None
    assert deck_month(["蒙西储能场站复盘（9月）"], "2026蒙西复盘.pptx") == date(2026, 9, 1)  # no date line
    assert deck_month(["蒙西储能场站复盘（9月）"], "x.pptx") is None


def test_overview_and_breakdown(tmp_path):
    df, notes = parse_bess_deck(_deck(tmp_path, [OV, BD]), "x.pptx")
    assert set(df.month) == {date(2026, 9, 1)}
    assert _v(df, "suyou", "total_revenue_cny") == pytest.approx(4_667_000)
    assert _v(df, "suyou", "unit_capacity_revenue") == 11668
    assert _v(df, "suyou", "daily_cycles") == 1.24
    assert _v(df, "suyou", "spot_spread") == 73.40
    assert _v(df, "suyou", "capcomp_unit_price") == 352.07
    assert _v(df, "suyou", "sysop_fee_unit") == 35.5
    assert _v(df, "suyou", "executed_revenue_cny") == pytest.approx(5_402_000)
    assert _v(df, "suyou", "strategy_revenue_cny") == pytest.approx(6_517_000)
    assert _v(df, "suyou", "cleared_revenue_cny") == pytest.approx(5_987_000)
    assert _v(df, "suyou", "strategy_charge_mwh") == 16596.0
    assert _v(df, "suyou", "executed_discharge_mwh") == 13867.3
    assert _v(df, "gushanliang", "strategy_revenue_cny") == pytest.approx(33_233_000)
    assert any("神秘站" in n for n in notes)
    assert (df.unit[df.metric == "executed_charge_mwh"] == "MWh").all()


def test_blank_cells_absent_zero_kept(tmp_path):
    df, _ = parse_bess_deck(_deck(tmp_path, [OV]), "x.pptx")
    assert df[(df.asset_code == "wuchuan") & (df.metric == "unit_capacity_revenue")].empty
    assert df[(df.asset_code == "wuchuan") & (df.metric == "sysop_fee_unit")].empty
    assert _v(df, "wuchuan", "capcomp_unit_price") == 0
    assert _v(df, "wuchuan", "total_revenue_cny") == pytest.approx(-8_904_400)
    assert df[(df.asset_code == "gushanliang") & (df.metric == "sysop_fee_unit")].empty


def test_two_month_takes_deck_month_only(tmp_path):
    df, _ = parse_bess_deck(_deck(tmp_path, [TWO]), "x.pptx")
    assert _v(df, "suyou", "total_revenue_cny") == pytest.approx(4_667_000)
    assert _v(df, "suyou", "capcomp_unit_price") == 352.07
    assert len(df) == 2  # 日均充电次数 only has an 8月 column -> absent


def test_undetected_month_gives_note(tmp_path):
    df, notes = parse_bess_deck(_deck(tmp_path, [OV], label="上半年复盘", dl="2026.7"), "上半年.pptx")
    assert df.empty and notes


REAL = Path("/Users/chenzhuqi/Library/CloudStorage/OneDrive-Personal/ETRM/bess-platform/assets/operating/"
            "复盘/储能/蒙西/蒙西储能场站9月内部复盘.pptx")


@pytest.mark.skipif(not REAL.exists(), reason="real 9月 deck not available")
def test_real_sept_deck():
    df, _ = parse_bess_deck(str(REAL), REAL.name)
    assert set(df.month) == {date(2026, 9, 1)}
    assert _v(df, "suyou", "total_revenue_cny") == 4_667_000
    assert _v(df, "suyou", "executed_revenue_cny") == 5_402_000
    assert _v(df, "suyou", "strategy_revenue_cny") == 6_517_000
