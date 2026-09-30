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
    assert c.price_cny_mwh == 6.0 and c.share_ratio == 0.9
    assert c.annual_mwh == 6000.0          # 万度 -> MWh ×10
    assert '"1": 1000.0' in c.monthly_mwh  # JSON string, 万度 -> MWh


def test_ratios_normalised(tmp_path):
    f = tmp_path / "8 【山东测算】零售合同Mark to Market利润测算【分月分时】.xlsx"
    _make_workbook(f)
    r = m.load_hourly_ratios(f)
    assert r[r.month == 1].ratio.sum() == pytest.approx(1.0)
