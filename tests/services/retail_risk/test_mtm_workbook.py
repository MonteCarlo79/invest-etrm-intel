# tests/services/retail_risk/test_mtm_workbook.py
from pathlib import Path
import pandas as pd
import pytest
from services.retail_risk.parsers import mtm_workbook as m


def test_scenario_for_filename():
    assert m.scenario_for_filename("8 【山东测算】零售合同Mark to Market利润测算【分月分时-10】.xlsx") == "spot_m10"
    assert m.scenario_for_filename("1 【广东测算】零售合同Mark to Market利润测算【分月-不分月分时+10】.xlsx") == "spot_p10"
    assert m.scenario_for_filename("8 【山东测算】零售合同Mark to Market利润测算【分月分时】.xlsx") == "spot_base"
    assert m.scenario_for_filename("4 【江苏测算-更新】2026生效用户-利润测算&电量统计-汇总-10.xlsx") == "spot_m10"


def test_province_for_filename():
    assert m.province_for_filename("8 【山东测算】零售合同Mark to Market利润测算【分月分时】.xlsx") == "山东"
    assert m.province_for_filename("4 【江苏测算-更新】2026生效用户-利润测算&电量统计-汇总.xlsx") == "江苏"


def _make_workbook(path: Path):
    price = pd.DataFrame([["1月"] + [300.0 + h for h in range(24)],
                          ["月度备注"] + [None] * 24,      # junk label row: skipped
                          ["2月"] + [310.0 + h for h in range(24)]],
                         columns=["现货价格（中价假设）"] + list(range(24)))
    contracts = pd.DataFrame([
        [1, "测试用户A", "单元1", "2025-12-22", "2026-01-01", "2026-03-31",
         "景融参考价格联动类0792", "联动+上浮", "已生效", 6.0, 100.0, 200.0, 300.0, 600.0, "渠道X", 0.9],
    ], columns=["序号", "零售用户名称", "交易单元名称", "建立时间", "生效时间", "失效时间",
                "套餐名称", "套餐类别", "状态", "套餐价格", "1月电量", "2月电量", "3月电量",
                "年度电量/万度（匹配原始台账）", "渠道归属", "渠道分成比例"])
    ratio = pd.DataFrame([["1月"] + [1 / 24] * 24], columns=["分月分时比例"] + list(range(24)))
    with pd.ExcelWriter(path, engine="openpyxl") as w:
        price.to_excel(w, sheet_name="山东模型价格预测", index=False)
        contracts.to_excel(w, sheet_name="新山东零售合约-总表", index=False)
        ratio.to_excel(w, sheet_name="山东分月分时比例", index=False)


def test_curves_expand_month_hour_to_days(tmp_path):
    f = tmp_path / "8 【山东测算】零售合同Mark to Market利润测算【分月分时】.xlsx"
    _make_workbook(f)
    out = m.parse_mtm_workbook(f)
    assert out["province"] == "山东" and out["product"] == "spot_base"
    curves = out["curves"]
    jan = curves[(curves.delivery_date.astype(str).str.startswith("2026-01"))]
    assert jan.delivery_date.nunique() == 31 and set(jan.delivery_hour) == set(range(24))
    h0 = jan[(jan.delivery_date.astype(str) == "2026-01-01") & (jan.delivery_hour == 0)]
    assert h0.price_cny_kwh.iloc[0] == pytest.approx(0.300)   # CNY/MWh -> /1000


def test_contracts_parsed(tmp_path):
    f = tmp_path / "8 【山东测算】零售合同Mark to Market利润测算【分月分时】.xlsx"
    _make_workbook(f)
    out = m.parse_mtm_workbook(f)
    c = out["contracts"].iloc[0]
    assert c.customer_name == "测试用户A" and c.contract_type == "indexed"
    assert c.contract_ref == "山东-1"      # I3: province-prefixed, no cross-province collision
    assert c.price_cny_mwh == 6.0 and c.share_ratio == 0.9
    assert c.annual_mwh == 6000.0          # 万度 -> MWh ×10
    assert '"1": 1000.0' in c.monthly_mwh  # JSON string, 万度 -> MWh


def test_ratios_normalised(tmp_path):
    f = tmp_path / "8 【山东测算】零售合同Mark to Market利润测算【分月分时】.xlsx"
    _make_workbook(f)
    r = m.load_hourly_ratios(f)
    assert r[r.month == 1].ratio.sum() == pytest.approx(1.0)


def _make_transposed_workbook(path: Path):
    """广东 layout: rows = price series x month columns (1..12 as numbers)."""
    df = pd.DataFrame([
        ["广东"] + list(range(1, 13)),
        ["批发成本"] + [360.0] * 12,
        ["年度中长期价格"] + [372.0] * 12,
        ["月度现货价格（日前）"] + [290.0] * 12,
        ["月度现货价格（实时）"] + [285.0] * 12,
    ])
    with pd.ExcelWriter(path, engine="openpyxl") as w:
        df.to_excel(w, sheet_name="广东模型价格预测", index=False, header=False)


def test_transposed_monthly_layout_broadcast_24h(tmp_path):
    """广东/福建 layout: monthly flat price from the 现货 series row, all 24 hours."""
    f = tmp_path / "1 【广东测算】零售合同Mark to Market利润测算【分月-不分月分时】.xlsx"
    _make_transposed_workbook(f)
    out = m.parse_mtm_workbook(f)
    curves = out["curves"]
    assert not curves.empty
    jan = curves[curves.delivery_date.astype(str).str.startswith("2026-01")]
    assert set(jan.delivery_hour) == set(range(24))          # broadcast
    h8 = jan[(jan.delivery_date.astype(str) == "2026-01-01") & (jan.delivery_hour == 8)]
    assert h8.price_cny_kwh.iloc[0] == pytest.approx(0.285)  # 实时 series, not 日前


def _make_flat_param_workbook(path: Path):
    """江苏 layout: 参数 sheet with 预估现货均价 scalar (CNY/kWh)."""
    df = pd.DataFrame([["预估长协均价", 0.344190, "年度"],
                       ["预估月度均价", 0.337100, None],
                       ["预估现货均价", 0.319770, None]])
    with pd.ExcelWriter(path, engine="openpyxl") as w:
        df.to_excel(w, sheet_name="参数", index=False, header=False)


def test_flat_param_layout_jiangsu(tmp_path):
    f = tmp_path / "4 【江苏测算-更新】2026生效用户-利润测算&电量统计-汇总.xlsx"
    _make_flat_param_workbook(f)
    out = m.parse_mtm_workbook(f)
    curves = out["curves"]
    assert not curves.empty
    assert curves.price_cny_kwh.unique().tolist() == [pytest.approx(0.319770)]


def _make_grid_workbook(path: Path, header_label: str, hour_headers: list,
                        extra_cols: dict | None = None, junk_cell: str | None = None):
    """Generic month x hour grid fixture: header row + 1月/2月 rows."""
    cols = [header_label] + hour_headers
    rows = [list(cols)]
    for mi, (mname, base) in enumerate([("1月", 300.0), ("2月", 310.0)]):
        row = [mname] + [base + i for i in range(len(hour_headers))]
        if junk_cell and mi == 0:
            row[1] = junk_cell
        rows.append(row)
    df = pd.DataFrame(rows)
    if extra_cols:
        for name, vals in extra_cols.items():
            df[name] = vals
    with pd.ExcelWriter(path, engine="openpyxl") as w:
        df.to_excel(w, sheet_name=path.stem[:2] + "模型价格预测", index=False, header=False)


def test_grid_with_float_hour_headers_not_transposed(tmp_path):
    """安徽 layout: header col0='批发侧成本（总）' with float hours 0..23 — must NOT
    be detected as transposed (regression: numeric headers != month numbers)."""
    f = tmp_path / "2 【安徽测算】零售合同Mark to Market利润测算【分月分时】.xlsx"
    _make_grid_workbook(f, "批发侧成本（总）", [float(h) for h in range(24)])
    out = m.parse_mtm_workbook(f)
    assert not out["curves"].empty
    assert set(out["curves"].delivery_hour) == set(range(24))


def test_grid_with_hNN_hour_headers(tmp_path):
    """冀南 layout: hour columns named h00..h23."""
    f = tmp_path / "6 【冀南测算】零售合同Mark to Market利润测算【分月分时】.xlsx"
    _make_grid_workbook(f, "批发市场参考价（零售）", [f"h{h:02d}" for h in range(24)])
    out = m.parse_mtm_workbook(f)
    assert not out["curves"].empty
    assert set(out["curves"].delivery_hour) == set(range(24))


def test_grid_with_trailing_unnamed_and_junk_cells(tmp_path):
    """上海/浙江/山东 defects: trailing 'Unnamed: N' columns skipped; junk value
    cells ('一季度340') dropped without crashing."""
    f = tmp_path / "5 【上海测算】零售合同Mark to Market利润测算【分月不分时】.xlsx"
    _make_grid_workbook(f, "月竞价格（中价假设）", [float(h) for h in range(24)],
                        extra_cols={"Unnamed: 25": ["备注", "x", "y"]},
                        junk_cell="一季度340")
    out = m.parse_mtm_workbook(f)
    assert not out["curves"].empty
    # 不分时 broadcast: every day has 24 hours despite the junk cell
    jan1 = out["curves"][out["curves"].delivery_date.astype(str) == "2026-01-01"]
    assert len(jan1) == 24


def test_multi_block_grid_dedups_to_first_block(tmp_path):
    """冀南/浙江/山东 price sheets carry 2-4 side-by-side (or stacked) 24h blocks;
    curves must dedup (month, hour) keeping the FIRST block — 8760 rows, not N×."""
    f = tmp_path / "6 【冀南测算】零售合同Mark to Market利润测算【分月分时】.xlsx"
    # two side-by-side 24h blocks with different prices
    cols = ["批发市场参考价（零售）"] + [f"h{h:02d}" for h in range(24)] \
        + [f"h{h:02d}" for h in range(24)]
    rows = [cols]
    for mname, base in [("1月", 300.0), ("2月", 310.0)]:
        rows.append([mname] + [base + i for i in range(24)] + [base + 100 + i for i in range(24)])
    with pd.ExcelWriter(f, engine="openpyxl") as w:
        pd.DataFrame(rows).to_excel(w, sheet_name="冀南模型价格预测", index=False, header=False)
    out = m.parse_mtm_workbook(f)
    jan = out["curves"][out["curves"].delivery_date.astype(str).str.startswith("2026-01")]
    assert len(jan) == 31 * 24                       # exactly one block
    h0 = jan[(jan.delivery_date.astype(str) == "2026-01-01") & (jan.delivery_hour == 0)]
    assert h0.price_cny_kwh.iloc[0] == pytest.approx(0.300)   # FIRST block's price


def test_contracts_junk_numeric_cells_skipped(tmp_path):
    """Contract sheets with junk text in numeric cells ('一季度340') must not crash."""
    f = tmp_path / "8 【山东测算】零售合同Mark to Market利润测算【分月分时】.xlsx"
    _make_workbook(f)
    # rewrite the contract sheet with a junk cell in a month column
    cdf = pd.DataFrame([
        [1, "测试用户A", "单元1", "2025-12-22", "2026-01-01", "2026-03-31",
         "景融参考价格联动类0792", "联动+上浮", "已生效", 6.0, "一季度340", 200.0, 300.0, 600.0, "渠道X", 0.9],
    ], columns=["序号", "零售用户名称", "交易单元名称", "建立时间", "生效时间", "失效时间",
                "套餐名称", "套餐类别", "状态", "套餐价格", "1月电量", "2月电量", "3月电量",
                "年度电量/万度（匹配原始台账）", "渠道归属", "渠道分成比例"])
    with pd.ExcelWriter(f, engine="openpyxl", mode="a", if_sheet_exists="replace") as w:
        cdf.to_excel(w, sheet_name="新山东零售合约-总表", index=False)
    out = m.parse_mtm_workbook(f)
    c = out["contracts"].iloc[0]
    assert '"1"' not in c.monthly_mwh or "340" not in c.monthly_mwh   # junk month dropped
    assert '"2": 2000.0' in c.monthly_mwh


def test_contracts_nan_month_cells_produce_valid_json(tmp_path):
    """Empty month cells (NaN floats) must be dropped from monthly JSON —
    json.dumps emits 'NaN' tokens which PostgreSQL jsonb rejects."""
    import json as _json
    f = tmp_path / "8 【山东测算】零售合同Mark to Market利润测算【分月分时】.xlsx"
    _make_workbook(f)
    cdf = pd.DataFrame([
        [1, "测试用户A", "单元1", "2025-12-22", "2026-01-01", "2026-03-31",
         "景融参考价格联动类0792", "联动+上浮", "已生效", 6.0, float("nan"), 200.0, float("nan"), 600.0, "渠道X", 0.9],
    ], columns=["序号", "零售用户名称", "交易单元名称", "建立时间", "生效时间", "失效时间",
                "套餐名称", "套餐类别", "状态", "套餐价格", "1月电量", "2月电量", "3月电量",
                "年度电量/万度（匹配原始台账）", "渠道归属", "渠道分成比例"])
    with pd.ExcelWriter(f, engine="openpyxl", mode="a", if_sheet_exists="replace") as w:
        cdf.to_excel(w, sheet_name="新山东零售合约-总表", index=False)
    out = m.parse_mtm_workbook(f)
    monthly = _json.loads(out["contracts"].iloc[0].monthly_mwh)   # must parse as valid JSON
    assert monthly == {"2": 2000.0}
