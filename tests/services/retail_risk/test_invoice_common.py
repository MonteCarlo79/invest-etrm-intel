# tests/services/retail_risk/test_invoice_common.py
from pathlib import Path
import pandas as pd
import pytest
from services.retail_risk import schemas
from services.retail_risk.parsers.invoice_common import (
    parse_subject_lines, parse_total_from_summary,
)
from services.retail_risk.parsers.invoice_shandong import parse_shandong_invoice

JINAN_TEXT = """
结算单编号：HEPX-2026-03-SD0100
本月 175251.68
结算科目编码 结算科目 分月交易计划电量 结算电量 结算均价 结算电费 备注
01 电量清分 17770.000 21906.460 331.580 7263741.37
0101 中长期交易 17770.000 17770.000 337.685 6000657.47
010105 合同交易 1418.946 1418.946 -60.718 -86155.80
0102 现货交易 - 4136.460 305.354 1263083.90
0202030002 中长期偏差收益回收（差额） - - - 10383.71
0211030001 零售市场超额收益费用 - - - 193111.28
第1页，共6页
"""


def test_subject_lines_categories_and_signs():
    items = parse_subject_lines(JINAN_TEXT, schemas.JINAN_CATEGORY_RULES)
    ml = [i for i in items if i["label_cn"] == "中长期交易"][0]
    assert ml["category"] == "midlong_energy" and ml["amount_cny"] == 6000657.47
    neg = [i for i in items if i["label_cn"] == "合同交易"][0]
    assert neg["price_cny_mwh"] == -60.718 and neg["amount_cny"] == -86155.80
    spot = [i for i in items if i["label_cn"] == "现货交易"][0]
    assert spot["category"] == "spot_energy"
    dev = [i for i in items if "偏差收益回收" in i["label_cn"]][0]
    assert dev["category"] == "imbalance"
    claw = [i for i in items if "超额收益" in i["label_cn"]][0]
    assert claw["category"] == "rule_charges"


def test_total_from_summary():
    assert parse_total_from_summary(JINAN_TEXT) == 175251.68


def test_total_from_summary_multi_number_line():
    """浙江 summary: 本月 <实际用电量> <结算电量> <合同电量> <偏差> <结算电费> —
    the money total is the LAST number on the line, not the first."""
    text = "期间 实际用电量 结算电量 合同电量 偏差考核电量 结算电费\n本月 27142.626 27142.626 24069.4070 - 9163096.47\n"
    assert parse_total_from_summary(text) == 9163096.47


def test_shandong_7021_excel(tmp_path):
    rows = [["2026年3月月清算临时结果单"] + [None] * 7,
            [None] * 8, [None] * 8, [None] * 8,
            ["结算单元名称", "日期", "省内实时市场结算", None, None, "省内日前市场结算", None, None],
            [None, None, "实时用电量", "实时市场电价", "实时电能量电费", "日前出清电量", "日前市场出清电价", "日前电能量电费"],
            ["景融绿色能源科技有限公司", "2026-03-01", 7546.9, 436.69, 3295685.07, 0, 0, 0],
            [None, "2026-03-02", 7662.2, 375.92, 2880362.44, 0, 0, 0],
            ["合计", None, 15209.1, None, 6176047.51, 0, 0, 0]]
    f = tmp_path / "7021-2026-03景融绿色能源科技有限公司结算单.xlsx"
    with pd.ExcelWriter(f, engine="openpyxl") as w:
        pd.DataFrame(rows).to_excel(w, sheet_name="结算依据", index=False, header=False)
    doc = parse_shandong_invoice(f)
    assert doc.settlement_month.isoformat() == "2026-03-01"
    assert len(doc.items) == 2
    assert all(i["category"] == "spot_energy" for i in doc.items)
    assert doc.items[0]["delivery_date"].isoformat() == "2026-03-01"
    assert doc.total_amount_cny == pytest.approx(6176047.51)
    assert doc.total_kind == "spot_subtotal"    # 合计 = Σ spot energy, NOT a margin
