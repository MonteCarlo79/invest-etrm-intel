"""网上国网 watermarked bills (滨州 2026-08/09) parse deterministically after the grey-glyph filter."""
from pathlib import Path

import pytest

from libs.asset_risk.settlement.router import read_bill, route
from services.settlement_ingest.scanner import classify_pdf

D = Path("/Users/chenzhuqi/Library/CloudStorage/OneDrive-Personal/ETRM/bess-platform/data/raw/settlement/invoices/"
         "B-2 山东滨州/2026年结算单")
CASES = {
    "B-2-上北海远凌2026年9月份电费账单.pdf": 1296863.45,
    "B-2-下北海远凌2026年9月份电费账单.pdf": -356314.27,
    "B-2-上北海远凌2026年8月份电费账单.pdf": 1124187.89,
    "B-2-下北海远凌2026年8月份电费账单.pdf": -646748.85,
}


@pytest.mark.parametrize("fn,total", CASES.items())
def test_watermarked_bill_routes_deterministically_and_ties(fn, total):
    f = D / fn
    if not f.exists():
        pytest.skip("invoice PDF not present")
    bill = read_bill(str(f), fn)
    r = route(bill, classify_pdf(fn), "upload")
    assert r is not None and "deterministically" in r.label, r and r.label
    items = r.parse(bill)
    assert abs(sum(i["amount_cny"] for i in items) - total) < 0.01
    keys = [(i["category"], i.get("notes"), i["amount_cny"]) for i in items]
    assert len(keys) == len(set(keys))            # no page-2 duplicates


def test_two_rules_separate_from_frequency():
    # Owner 2026-11-19: 两个细则 is its own category, not 调频.
    fn = "B-2-上北海远凌2026年9月份电费账单.pdf"
    f = D / fn
    if not f.exists():
        pytest.skip("invoice PDF not present")
    bill = read_bill(str(f), fn)
    items = route(bill, classify_pdf(fn), "upload").parse(bill)
    two = [i for i in items if i["category"] == "two_rules"]
    assert len(two) == 1 and abs(two[0]["amount_cny"] - 11343.89) < 0.01
    assert not any(i["category"] == "frequency" and "两个细则" in (i.get("notes") or "") for i in items)
