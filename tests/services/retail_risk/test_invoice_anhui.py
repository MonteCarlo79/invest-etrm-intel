# tests/services/retail_risk/test_invoice_anhui.py
from pathlib import Path
from unittest.mock import patch
import pytest
from services.retail_risk import schemas
from services.retail_risk.parsers.invoice_anhui import (
    clean_anhui_watermark, parse_anhui_invoice,
)

RAW = """司
公
源
科
技
有
限
日
15:58:36
售电公司交易结算单
结算单编号：AHPX-2026-3-R0615
景
融 2026
单位：兆瓦时、元 年4
本月 462471.22
结算科目编码 结算科目 分月交易计划电量 结算电量/容量 结算电价/均价 结算电费 备注
购电侧
01 电量清分
01010201 中长期交易 38101.323 38101.323 347.902 13255508.85
0102 现货交易 - 19411.918 288.5 5601534.2
色
能 月14
绿 年4
"""


def test_clean_watermark():
    cleaned = clean_anhui_watermark(RAW)
    assert "AHPX-2026-3-R0615" in cleaned
    assert "15:58:36" not in cleaned
    for frag in ["司", "公", "源", "技"]:
        assert frag not in cleaned.splitlines()


def test_parse_anhui_invoice(tmp_path):
    f = tmp_path / "景融绿色能源科技有限公司2026年03月统推售电公司结算单结算单 (1).pdf"
    f.write_bytes(b"%PDF-1.4 fake")
    with patch("services.retail_risk.parsers.invoice_anhui.extract_pdf_text",
               return_value=RAW):
        doc = parse_anhui_invoice(f)
    assert doc.settlement_month.isoformat() == "2026-03-01"
    assert doc.total_amount_cny == 462471.22
    cats = {i["label_cn"]: i["category"] for i in doc.items}
    assert cats["中长期交易"] == "midlong_energy"
    assert cats["现货交易"] == "spot_energy"


# --- degraded real-file shapes (seen in 安徽统推 2026-03 after font filtering) ---

DEGRADED = """01010201 中长期交易 38101.323 38101.323 347.902 13255508.85
01010802 超(少)用电量 0.000 0.000 0.00
01010902 少用电量
01020201 19635.109 277.070 5440248.49
94527.74
02010200 6837.235 4816.000 4.246 20447.29
02010200 4816.000 4.246 20447.29
0202010001 0.00
0202010002 308421.24
0211030001 -105165.91
"""


def test_subject_lines_degraded_shapes():
    """3-numeric lines, name-missing lines, orphan amounts, page-repeat dedup."""
    from services.retail_risk.parsers.invoice_common import parse_subject_lines
    items = parse_subject_lines(DEGRADED, schemas.ANHUI_CATEGORY_RULES)
    amounts = {(i["notes"], i["amount_cny"]) for i in items}
    # 4-numeric line: standard mapping
    assert ("01010201", 13255508.85) in amounts
    # 3-numeric line: amount=last, volume/price None
    assert ("01010802", 0.00) in amounts
    # name-missing 1-numeric line: amount captured, label empty
    assert ("0202010002", 308421.24) in amounts
    # page-repeat: 02010200 amount 20447.29 appears ONCE despite two raw rows
    assert sum(1 for i in items if i["notes"] == "02010200") == 1
    # orphan amount (no code) never parses
    assert all(i["amount_cny"] != 94527.74 for i in items)
    # negative amount kept
    assert ("0211030001", -105165.91) in amounts
