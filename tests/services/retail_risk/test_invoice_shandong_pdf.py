# tests/services/retail_risk/test_invoice_shandong_pdf.py
from pathlib import Path
from unittest.mock import patch
import pytest
from services.retail_risk import schemas
from services.retail_risk.parsers.invoice_shandong_pdf import parse_shandong_pdf_invoice

SHANDONG_TEXT = """
结算单编号：SDPX-2026-03-7021
期间 购电侧/售电侧 结算电量 合同电量 偏差电量 售电公司收益
购电侧 230,493.735 40,872.000 189,166.735
本月 -145,012.50
购电侧
01 电量清分 230,493.735 326.650 75290789.50
0101 中长期交易 40,872.000 -22.498 -919525.75
0101020301 中长期合约交易 40,872.000 -22.498 -919525.75
0102 现货交易 230,476.827 328.696 75756734.69
0102020301 实时分时交易 230,506.784 328.696 75766581.44
0103 辅助服务交易 453580.56
0202 市场运营费用 851043.21
03 结算调整 16.908 - 4220.12
01 电量清分 230,476.827 330.680 76214189.38
0101020303 零售电能量交易 230,476.827 330.680 76214189.38
0204 偏差费用 26477.70
0211030104 封顶结算差额费用 -245531.70
03 结算调整 16.908 - 5904.95
"""


def test_shandong_pdf_two_sided(tmp_path):
    f = tmp_path / "2026年03月景融绿色能源科技有限公司结算单.pdf"
    f.write_bytes(b"%PDF-1.4 fake")
    with patch("services.retail_risk.parsers.invoice_shandong_pdf.extract_pdf_text",
               return_value=SHANDONG_TEXT):
        doc = parse_shandong_pdf_invoice(f)
    assert doc.settlement_month.isoformat() == "2026-03-01"
    assert doc.total_amount_cny == -145012.50            # negative margin preserved
    assert doc.settled_volume_mwh == 230493.735

    buy = [i for i in doc.items if i.get("side") == "buy"]
    sell = [i for i in doc.items if i.get("side") == "sell"]

    # midlong CfD adjustment: negative price AND amount survive
    ml = [i for i in buy if i["label_cn"] == "中长期交易"][0]
    assert ml["category"] == "midlong_energy"
    assert ml["price_cny_mwh"] == -22.498 and ml["amount_cny"] == -919525.75
    # single-numeric ancillary line
    anc = [i for i in buy if i["notes"] == "0103"][0]
    assert anc["category"] == "frequency" and anc["amount_cny"] == 453580.56

    # second '01 电量清分' (no marker) flips to sell -> retail revenue 330.68 line
    sell01 = [i for i in sell if i["notes"] == "01"]
    assert len(sell01) == 1 and sell01[0]["category"] == "retail_revenue"
    assert sell01[0]["amount_cny"] == 76214189.38
    retail = [i for i in sell if i["label_cn"] == "零售电能量交易"][0]
    assert retail["category"] == "retail_revenue"
    # sell-side fee codes keep rule-mapped categories, not retail_revenue
    fee = [i for i in sell if i["notes"] == "0204"][0]
    assert fee["category"] == "imbalance"

    # margin identity: sell 01 + sell fees - (buy 01 + buy 02 + buy 03) == printed margin
    sell_total = 76214189.38 + 26477.70 - 245531.70 + 5904.95
    buy_total = 75290789.50 + 851043.21 + 4220.12
    assert (sell_total - buy_total) == pytest.approx(-145012.50, abs=1.0)
