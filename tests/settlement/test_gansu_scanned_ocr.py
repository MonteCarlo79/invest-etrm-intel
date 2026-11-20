"""Scanned 国网甘肃 bills route to OCR + the deterministic 甘肃 parser, not vision (民勤 2026-08)."""
from pathlib import Path

import pytest

pytest.importorskip("rapidocr_onnxruntime")
pytest.importorskip("pymupdf")

from libs.asset_risk.settlement.ocr import _lines
from libs.asset_risk.settlement.router import read_bill, route
from services.settlement_ingest.scanner import classify_pdf

SCAN = Path("/Users/chenzhuqi/Library/CloudStorage/OneDrive-Personal/ETRM/bess-platform/debug/"
            "民勤储能2026年08月上网电费结算单(1).pdf")


def test_scanned_gansu_bill_uses_ocr_parser_and_ties():
    if not SCAN.exists():
        pytest.skip("scanned bill not present")
    fn = "民勤储能2026年08月上网电费结算单.pdf"
    bill = read_bill(str(SCAN), fn)
    assert bill.is_scanned
    r = route(bill, classify_pdf(fn), "upload")
    assert r is not None and "OCR + deterministic 甘肃" in r.label, r and r.label
    got = {i["category"]: i["amount_cny"] for i in r.parse(bill)}
    assert got == {"discharge_energy": 328362.99, "frequency": 623071.17, "two_rules": -44323.2}


def test_lines_rebuilds_rows_left_to_right():
    box = lambda x, y, t: ([[x, y], [x + 50, y], [x + 50, y + 10], [x, y + 10]], t, 0.99)
    res = [box(200, 101, "623071.17"), box(10, 100, "（二）系统运行费用"), box(10, 130, "（七）两个细则费用"), box(200, 131, "-44323.2")]
    assert _lines(res).splitlines() == ["（二）系统运行费用 623071.17", "（七）两个细则费用 -44323.2"]


def test_zhongchangqi_statement_is_skipped():
    from libs.asset_risk.settlement.router import BillText
    for scanned, text in ((True, ""), (False, "国网甘肃 ... 中长期月结算 结算单 实时交易正电量 220.163 44046.26")):
        bill = BillText("民勤景肃新能源有限公司2026年06月中长期月结算结算单.pdf", "/tmp/x.pdf", scanned, text, text)
        r = route(bill, classify_pdf(bill.filename), "upload")
        assert r is not None and r.kind == "skip" and "中长期月结算" in r.label
        assert r.parse(bill) == []
