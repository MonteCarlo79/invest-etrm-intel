import importlib.util
import math
import pathlib
from datetime import date, datetime

import pytest
from openpyxl import Workbook
from sqlalchemy import create_engine, event, text

from libs.asset_risk.ic_case import (IC_COLUMNS, SOH_TABLE_I, ensure_table, ic_month_value, load_ic,
                                     parse_ic_workbook, save_ic)

REAL_DIR = pathlib.Path("/Users/chenzhuqi/Library/CloudStorage/OneDrive-Personal/ETRM/bess-platform/reports/投决")
REAL_A = REAL_DIR / "内蒙古储能项目经评边界值  20260729.xlsx"
REAL_B = REAL_DIR / "蒙西独立储能-投委会预审会-0509-sent.xlsx"


def _format_a_sheet(ws, cycles, y1, y5, tail, cap="0.5 / 2", year_col=5, top=2):
    """Layout of 武川-IPMT 边界 (year table in E..I, SOH table in B..C)."""
    r = top
    for i, row in enumerate([("项目", "单位", "数值"), ("建设容量", "GW/GWh", cap), ("运营期", "年", 20),
                             ("放电深度（DOD)", "%", 0.9), ("充放电效率（RTE)", "%", 0.85),
                             ("电池健康度", "/", "Table I"), ("收益",), ("第1-5年：电能量充放价差", "元/kWh", "x"),
                             ("第6年后： 电能量充放价差", "元/kWh", 0.18), ("年充放电次数", "次/年", cycles),
                             ("容量补偿", "元/kWh", 0.35), ("补偿年限", "年", 10)]):
        for j, v in enumerate(row):
            ws.cell(r + i, 2 + j, v)
    h = top + 15
    ws.cell(h - 1, 2, "Table I")
    ws.cell(h, 2, "年份"); ws.cell(h, 3, "SOH")
    for j, t in enumerate(["年度", "全年充放次数", "充电价格\n(元/MWh)", "放电价格\n(元/MWh)", "价差\n(元/MWh)"]):
        ws.cell(h, year_col + j, t)
    for y in range(1, 21):
        ws.cell(h + y, 2, y); ws.cell(h + y, 3, SOH_TABLE_I[y - 1])
    for i, (yr, ch, dis) in enumerate([(1, *y1), (2, 141.6, 273.84), (3, 109.46, 273.64), (4, 85.21, 263.65),
                                       (5, *y5), ("后15年", *tail)]):
        r2 = h + 1 + i
        ws.cell(r2, year_col, yr); ws.cell(r2, year_col + 1, cycles)
        ws.cell(r2, year_col + 2, ch); ws.cell(r2, year_col + 3, dis); ws.cell(r2, year_col + 4, dis - ch)


def _format_b_sheet(ws, two_cols=False):
    ws["B2"] = "项目基本信息"
    ws["E2"] = "杭锦旗-投委会历史上会版" if two_cols else "四子王旗"
    if two_cols:
        ws["F2"] = "杭锦旗-更新边界"; ws["G2"] = "数据来源"
    else:
        ws["F2"] = "更新来源"
    rows = [("项目运营日", "年/月/日", datetime(2025, 10, 15)), ("额定功率", "万kW", 10),
            ("额定容量", "万kWh", 40), ("放电深度DOD", "%", 0.9), ("充放电效率", "%", 0.86),
            ("放电电价", "元/kWh", 0.42964), ("充电电价", "元/kWh", 0.07467), ("电网收费", "元/kWh", 0.0273),
            ("日均充放电次数", "次/日", 1)]
    for i, (lab, unit, v) in enumerate(rows):
        r = 5 + i
        ws.cell(r, 2 if i < 5 else 3, lab); ws.cell(r, 4, unit); ws.cell(r, 5, v)
        if two_cols:   # E = 历史上会版, F = 更新边界
            ws.cell(r, 5, 0.87 if lab == "充放电效率" else v)
            ws.cell(r, 6, datetime(2025, 11, 1) if lab == "项目运营日" else v)
        elif lab == "充放电效率":
            ws.cell(r, 6, "储能产品技术")  # text in the 来源 column of the 四子王 layout (must be ignored)


def _format_c_sheet(ws, trend1="平均预期：2027年 0.4，逐年线性递减至 0.3"):
    for c, v in zip("CDFGHIJ", ["序号", "项目", "单位", "苏尼特右旗", "杭锦旗", "乌拉特中旗", "备注"]):
        ws[f"{c}5"] = v
    ws["D6"], ws["F6"] = "储能系统规模", "MW/MWh"
    ws["D7"], ws["F7"], ws["J7"] = "容量补偿收入", "元/kWh", "补贴年限 10年"
    ws["D9"], ws["D12"] = "日循环次数", "度电价差"
    for r, per in ((9, "2025-2026"), (10, "2027-2034"), (11, "2035-2044"),
                   (12, "2025-2026"), (13, "2027-2034"), (14, "2035-2044")):
        ws.cell(r, 5, per)
    for col, cyc, sp in (("G", 1.32, 0.415), ("H", 1.37, 0.388), ("I", 1.13, 0.414)):
        ws[f"{col}6"], ws[f"{col}7"] = "100/400", 0.35
        ws[f"{col}9"], ws[f"{col}10"], ws[f"{col}11"], ws[f"{col}12"] = cyc, 1.2, 1.2, sp
    ws["G13"], ws["G14"] = trend1, "平均预期：2035年0.3，逐年线性递减至 0.2"
    ws["D17"], ws["E17"] = "放电深度DOD", 0.9
    ws["D18"], ws["E18"] = "RTE", 0.87
    ws["D19"], ws["E19"] = "SOH(10年）", "99% - 81%"


@pytest.fixture()
def seed(tmp_path):
    wb = Workbook()
    wb.remove(wb.active)
    _format_a_sheet(wb.create_sheet("武川-IPMT 边界"), 234, (177.73, 275.77), (61.72, 246.24), (61.72, 246.24))
    _format_b_sheet(wb.create_sheet("四子王旗-测算边界汇总"))
    wb.create_sheet("锡盟-IPMT 边界")["B2"] = "项目基本信息"
    _format_c_sheet(wb.create_sheet("苏尼特-杭锦旗-乌拉特"))
    p = tmp_path / "ic.xlsx"
    wb.save(p)
    return p


def test_format_a_武川(seed):
    df, notes = parse_ic_workbook(seed, "ic.xlsx")
    assert list(df.columns) == IC_COLUMNS
    w = df[df.asset_code == "wuchuan"]
    assert len(w) == 20 and w.energy_mwh.iloc[0] == 4000 and w.capacity_mw.iloc[0] == 1000  # owner override 0.5/2 -> 1/4 GW(h)
    y1 = w[w.op_year == 1].iloc[0]
    assert (y1.charge_price, y1.discharge_price, y1.cycles_per_year, y1.soh) == (177.73, 275.77, 234, 0.946)
    y7 = w[w.op_year == 7].iloc[0]                       # 后15年 row applies to years >= 6
    assert (y7.charge_price, y7.discharge_price) == (61.72, 246.24) and y7.soh == 0.844
    assert y7.capcomp_rate == 350 and y7.capcomp_years == 10 and y7.grid_fee == 0
    assert w[w.op_year == 6].iloc[0].charge_price == 61.72
    assert w[w.op_year == 2].iloc[0].charge_price == 141.6


def test_format_b_四子王旗(seed):
    df, notes = parse_ic_workbook(seed, "ic.xlsx")
    s = df[df.asset_code == "siziwangqi"]
    assert len(s) == 20 and (s.cycles_per_year == 365).all()
    r = s.iloc[0]
    assert math.isclose(r.grid_fee, 27.3) and math.isclose(r.charge_price, 74.67)
    assert math.isclose(r.discharge_price, 429.64)
    assert r.capacity_mw == 100 and r.energy_mwh == 400 and r.rte == 0.86
    assert r.ic_cod == date(2025, 10, 15) and r.capcomp_rate == 350
    assert s[s.op_year == 20].iloc[0].soh == 0.706


def test_skips_have_notes(seed):
    df, notes = parse_ic_workbook(seed, "ic.xlsx")
    assert set(df.asset_code) == {"wuchuan", "siziwangqi", "suyou", "wulate"}
    assert any("锡盟" in n for n in notes)
    assert "杭锦旗: covered by 预审会 case — not taken from this sheet" in notes
    assert "hangjinqi" not in set(df.asset_code)
    assert any("SOH years 11-20 extrapolated" in n for n in notes)


def _row(df, code, y):
    return df[(df.asset_code == code) & (df.op_year == y)].iloc[0]


def test_format_c_suyou_wulate(seed):
    df, _ = parse_ic_workbook(seed, "ic.xlsx")
    r1 = _row(df, "suyou", 1)
    assert math.isclose(r1.cycles_per_year, 1.32 * 365) and math.isclose(r1.discharge_price, 415)
    assert r1.charge_price == 0 and r1.capacity_mw == 100 and r1.energy_mwh == 400
    assert r1.capcomp_rate == 350 and r1.capcomp_years == 10 and r1.ic_cod == date(2025, 1, 1)
    assert r1.case_label.startswith("经评边界值汇总") and r1.dod == 0.9 and r1.rte == 0.87
    assert math.isclose(_row(df, "wulate", 1).discharge_price, 414)
    assert math.isclose(_row(df, "wulate", 1).cycles_per_year, 1.13 * 365)
    for y, sp in ((3, 400), (10, 300), (11, 300), (20, 200)):
        assert math.isclose(_row(df, "suyou", y).discharge_price, sp), y
    assert math.isclose(_row(df, "suyou", 2).discharge_price, 415)
    assert math.isclose(_row(df, "suyou", 6).cycles_per_year, 1.2 * 365)
    assert math.isclose(_row(df, "suyou", 10).soh, 0.81) and math.isclose(_row(df, "suyou", 20).soh, 0.61)
    assert math.isclose(_row(df, "suyou", 1).soh, 0.99)


def test_format_c_arbitrage_is_discharge_times_spread(seed):
    ic, _ = parse_ic_workbook(seed, "ic.xlsx")
    v = ic_month_value(ic, "suyou", date(2025, 3, 1), date(24, 12, 31))   # invalid cod -> ic_cod
    assert v["op_year"] == 1 and math.isclose(v["arbitrage_cny"], v["discharge_mwh"] * 415)


def test_format_c_unparseable_trend_fails_with_note(tmp_path):
    wb = Workbook(); wb.remove(wb.active)
    _format_c_sheet(wb.create_sheet("苏尼特-杭锦旗-乌拉特"), trend1="待定")
    p = tmp_path / "c.xlsx"; wb.save(p)
    df, notes = parse_ic_workbook(p, "c.xlsx")
    assert df.empty and any("苏尼特" in n and "not parseable" in n for n in notes)


def test_capacity_price_from_year_11(tmp_path):
    wb = Workbook(); wb.remove(wb.active)
    ws = wb.create_sheet("巴盟-IPMT")
    _format_a_sheet(ws, 203, (100, 200), (100, 200), (100, 200), cap="1 / 2")
    ws["B14"], ws["D14"] = "容量电价", 110
    p = tmp_path / "b.xlsx"; wb.save(p)
    ic, _ = parse_ic_workbook(p, "b.xlsx")
    assert math.isclose(_row(ic, "bameng", 1).capacity_price_yuan_kw, 110)
    cod = date(2020, 1, 1)
    # 30-day months: op_year 10 = Jun 2029 (cod+9y); op_year 11 = Jun 2030
    v10 = ic_month_value(ic, "bameng", date(2029, 6, 1), cod)
    v11 = ic_month_value(ic, "bameng", date(2030, 6, 1), cod)
    assert v10["op_year"] == 10 and v10["capacity_price_cny"] == 0 and v10["capcomp_cny"] > 0
    assert v11["op_year"] == 11 and v11["capcomp_cny"] == 0
    assert math.isclose(v11["capacity_price_cny"], 110 * 1000 * 1000 * 30 / 365)
    assert math.isclose(v11["total_cny"], v11["arbitrage_cny"] + v11["capacity_price_cny"])


def test_format_b_two_columns_uses_last(tmp_path):
    wb = Workbook(); wb.remove(wb.active)
    _format_b_sheet(wb.create_sheet("杭锦旗-测算边界汇总"), two_cols=True)
    p = tmp_path / "h.xlsx"; wb.save(p)
    df, _ = parse_ic_workbook(p, "h.xlsx")
    r = df.iloc[0]
    assert r.asset_code == "hangjinqi" and r.case_label == "杭锦旗-更新边界"
    assert r.rte == 0.86 and r.ic_cod == date(2025, 11, 1)


def test_trailing_space_duplicate_sheet_skipped(tmp_path):
    wb = Workbook(); wb.remove(wb.active)
    old = wb.create_sheet("四子王旗-测算边界汇总 "); _format_b_sheet(old); old["E12"] = 0.99  # old RTE row differs
    _format_b_sheet(wb.create_sheet("四子王旗-测算边界汇总"))
    p = tmp_path / "d.xlsx"; wb.save(p)
    df, notes = parse_ic_workbook(p, "d.xlsx")
    assert len(df) == 20 and any("duplicate" in n for n in notes)


def test_month_value_武川(seed):
    ic, _ = parse_ic_workbook(seed, "ic.xlsx")
    v = ic_month_value(ic, "wuchuan", date(2026, 9, 1), date(2026, 7, 20))
    cycles = 234 * 30 / 365
    dis_c = 4000 * 0.9 * 0.946 * math.sqrt(0.85)
    chg_c = 4000 * 0.9 * 0.946 / math.sqrt(0.85)
    assert v["op_year"] == 1
    assert math.isclose(v["discharge_mwh"], cycles * dis_c)
    assert math.isclose(v["charge_mwh"], cycles * chg_c)
    assert math.isclose(v["arbitrage_cny"], cycles * (dis_c * 275.77 - chg_c * 177.73))
    assert math.isclose(v["capcomp_cny"], cycles * dis_c * 350)
    assert v["grid_fee_cny"] == 0
    assert math.isclose(v["total_cny"], v["arbitrage_cny"] + v["capcomp_cny"])


def test_month_value_prorates_first_month_and_precod(seed):
    ic, _ = parse_ic_workbook(seed, "ic.xlsx")
    v = ic_month_value(ic, "wuchuan", date(2026, 7, 1), date(2026, 7, 20))
    full = ic_month_value(ic, "wuchuan", date(2026, 8, 1), date(2026, 7, 20))
    assert math.isclose(v["discharge_mwh"] / full["discharge_mwh"], 12 / 31)   # 20..31 Jul = 12 days
    assert ic_month_value(ic, "wuchuan", date(2026, 6, 1), date(2026, 7, 20)) is None


def test_month_value_cod_fallback_capcomp_expiry_and_gridfee(seed):
    ic, _ = parse_ic_workbook(seed, "ic.xlsx")
    v = ic_month_value(ic, "siziwangqi", date(2025, 11, 1), date(24, 12, 31))   # 0024-12-31 -> ic_cod
    assert v is not None and v["op_year"] == 1 and v["grid_fee_cny"] > 0
    late = ic_month_value(ic, "siziwangqi", date(2036, 1, 1), None)             # op_year 11 > capcomp_years
    assert late["op_year"] == 11 and late["capcomp_cny"] == 0
    assert ic_month_value(ic, "siziwangqi", date(2090, 1, 1), None)["op_year"] == 20


def test_month_value_none_cases(seed):
    ic, _ = parse_ic_workbook(seed, "ic.xlsx")
    assert ic_month_value(ic, "alashan", date(2026, 9, 1), date(2026, 7, 20)) is None   # no rows
    assert ic_month_value(ic, "wuchuan", date(2026, 9, 1), None) is None                # no cod, no ic_cod
    assert ic_month_value(ic, "wuchuan", date(2026, 9, 1), date(24, 12, 31)) is None
    assert ic_month_value(ic.iloc[0:0], "wuchuan", date(2026, 9, 1), date(2026, 7, 20)) is None


@pytest.fixture()
def sqlite_engine(tmp_path):
    eng = create_engine(f"sqlite:///{tmp_path / 'main.db'}")

    @event.listens_for(eng, "connect")
    def _attach(dbapi_conn, _):
        dbapi_conn.execute(f"ATTACH DATABASE '{tmp_path / 'md.db'}' AS marketdata")

    ensure_table(eng)
    return eng


def test_store_roundtrip_and_replace(seed, sqlite_engine):
    ic, _ = parse_ic_workbook(seed, "ic.xlsx")
    save_ic(sqlite_engine, ic)
    save_ic(sqlite_engine, ic[ic.asset_code == "wuchuan"].assign(case_label="v2"))   # replaces only wuchuan
    out = load_ic(sqlite_engine)
    assert len(out) == 80
    assert set(out[out.asset_code == "wuchuan"].case_label) == {"v2"}
    s = out[out.asset_code == "siziwangqi"].iloc[0]
    assert s.ic_cod == date(2025, 10, 15)
    assert out[out.asset_code == "wuchuan"].ic_cod.iloc[0] is None
    assert ic_month_value(out, "siziwangqi", date(2025, 11, 1), None)["op_year"] == 1


def test_migration_script_dry_run_has_both_tables(sqlite_engine):
    p = pathlib.Path(__file__).resolve().parents[2] / "scripts/asset_risk/migrate_ic_review.py"
    spec = importlib.util.spec_from_file_location("migrate_ic_review", p)
    m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
    lines = []
    assert m.run(sqlite_engine, apply=False, out=lines.append) == 0
    joined = "\n".join(lines)
    assert "rm_ic_assumptions" in joined and "rm_review_metrics" in joined and "dry-run" in joined
    assert len(m.build_statements()) == 2


@pytest.mark.skipif(not (REAL_A.exists() and REAL_B.exists()), reason="real IC workbooks not present")
def test_real_workbooks_asset_codes():
    a, na = parse_ic_workbook(REAL_A, REAL_A.name)
    b, nb = parse_ic_workbook(REAL_B, REAL_B.name)
    assert {"wuchuan", "alashan", "bameng"} <= set(a.asset_code)
    assert {"siziwangqi", "hangjinqi"} == set(b.asset_code)
    assert {"suyou", "wulate"} <= set(a.asset_code) and "wuhai" not in set(a.asset_code)
    assert "hangjinqi" not in set(a.asset_code)
    assert a[(a.asset_code == "bameng") & (a.op_year == 11)].capacity_price_yuan_kw.iloc[0] == 110
    assert any("锡盟" in n for n in na)
    assert a[(a.asset_code == "bameng") & (a.op_year == 1)].cycles_per_year.iloc[0] == 203
    h = b[b.asset_code == "hangjinqi"].iloc[0]
    assert h.case_label == "杭锦旗-更新边界" and h.ic_cod == date(2025, 11, 1)
    assert math.isclose(h.grid_fee, 27.3)


def test_save_ic_all_none_column_inserts_as_null(tmp_path):
    # Seed incident 2026-11-16: Format B rows have capacity_price_yuan_kw all None.
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import event, text
    from libs.asset_risk import ic_case
    md = tmp_path / "md.db"
    eng = sqlalchemy.create_engine(f"sqlite:///{tmp_path / 'm.db'}")

    @event.listens_for(eng, "connect")
    def _a(conn, _):
        conn.execute(f"ATTACH DATABASE '{md}' AS marketdata")
    ic_case.ensure_table(eng)
    row = {c: None for c in ic_case.IC_COLUMNS}
    row.update(asset_code="siziwangqi", op_year=1, soh=0.946, cycles_per_year=365.0, charge_price=74.67,
               discharge_price=429.64, dod=0.9, rte=0.86, capcomp_rate=350.0, capcomp_years=10,
               grid_fee=27.3, capacity_mw=100.0, energy_mwh=400.0, case_label="四子王旗", source_file="b.xlsx")
    ic_case.save_ic(eng, pd.DataFrame([row]))
    with eng.connect() as c:
        got = c.execute(text("SELECT capacity_price_yuan_kw, discharge_price FROM marketdata.rm_ic_assumptions")).fetchone()
    assert got[0] is None and got[1] == 429.64


def test_capacity_override_scales_three_assets():
    # Owner decision 2026-11-16: 武川/阿拉善/巴盟 IC case at 1 GW / 4 GWh.
    from pathlib import Path
    from libs.asset_risk import ic_case
    f = Path("/Users/chenzhuqi/Library/CloudStorage/OneDrive-Personal/ETRM/bess-platform/reports/投决/"
             "内蒙古储能项目经评边界值  20260729.xlsx")
    if not f.exists():
        import pytest
        pytest.skip("real workbook not present")
    df, notes = ic_case.parse_ic_workbook(str(f), f.name)
    for code in ("wuchuan", "alashan", "bameng"):
        sub = df[df.asset_code == code]
        assert set(sub.capacity_mw) == {1000.0} and set(sub.energy_mwh) == {4000.0}
        assert any(n.startswith(f"{code}: capacity scaled") for n in notes)
    assert set(df[df.asset_code == "suyou"].energy_mwh) == {400.0}   # others untouched
