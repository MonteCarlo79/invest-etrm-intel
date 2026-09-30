# tests/services/retail_risk/test_invoice_pdfs.py
from pathlib import Path
from unittest.mock import patch
import pytest
from services.retail_risk.parsers.invoice_jinan import parse_jinan_invoice
from services.retail_risk.parsers.invoice_zhejiang import parse_zhejiang_invoice

JINAN_TEXT = """
结算单编号：HEPX-2026-03-SD0100
本月 175251.68
01 电量清分 17770.000 21906.460 331.580 7263741.37
0101 中长期交易 17770.000 17770.000 337.685 6000657.47
0102 现货交易 - 4136.460 305.354 1263083.90
0202 市场运营费用 - - - 176.41
"""

ZHEJIANG_TEXT = """
结算单编号：
本月 9163096.47
01 电量清分 24069.407 27007.772 337.848 9124509.97
0101 中长期交易 24069.407 - - 1395926.90
010104 省间送受电交易 3661.792 - - 111114.51
0102 现货交易 - 27007.772 286.161 7728583.07
0201 权益和凭证交易 - 4955.000 3.989 19763.99
"""


def test_jinan_invoice(tmp_path):
    f = tmp_path / "景融绿色能源科技有限公司2026年03月现货月结算.pdf"
    f.write_bytes(b"%PDF-1.4 fake")
    with patch("services.retail_risk.parsers.invoice_jinan.extract_pdf_text",
               return_value=JINAN_TEXT):
        doc = parse_jinan_invoice(f)
    assert doc.settlement_month.isoformat() == "2026-03-01"
    assert doc.total_amount_cny == 175251.68
    cats = {i["label_cn"]: i["category"] for i in doc.items}
    assert cats["中长期交易"] == "midlong_energy"
    assert cats["现货交易"] == "spot_energy"
    assert cats["市场运营费用"] == "market_redistribution"


def test_zhejiang_invoice(tmp_path):
    f = tmp_path / "景融绿色能源科技有限公司2026年03月结算单-25年现货月依据.pdf"
    f.write_bytes(b"%PDF-1.4 fake")
    with patch("services.retail_risk.parsers.invoice_zhejiang.extract_pdf_text",
               return_value=ZHEJIANG_TEXT):
        doc = parse_zhejiang_invoice(f)
    cats = {i["label_cn"]: i["category"] for i in doc.items}
    assert cats["省间送受电交易"] == "midlong_energy"
    assert cats["权益和凭证交易"] == "green_premium"
