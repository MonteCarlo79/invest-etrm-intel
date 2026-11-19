"""国网江苏 电费核查联 (射阳 B-4-下) parses deterministically to −金额合计 and Σ 有功总."""
from pathlib import Path

import pytest

from libs.asset_risk.settlement.router import read_bill, route
from services.settlement_ingest.scanner import classify_pdf

D = Path("/Users/chenzhuqi/Library/CloudStorage/OneDrive-Personal/ETRM/bess-platform/data/raw/settlement/invoices/B-4 江苏射阳")
CASES = {  # file stem -> (金额合计, Σ有功总 kWh)
    "B-4下-射阳远汇智慧能源有限公司2026年9月": (3858506.13, 634 + 16445440),
    "B-4下-射阳远汇智慧能源有限公司2026年8月": (390.21, 634 + 29440928),       # peak month: main meter billed at 0
    "2025年3月 【B-4-下】-射阳远汇智慧能源有限公司25.3-OK": (4098162.05, 643 + 17466944),
}


def _find(stem):
    hits = list(D.rglob(stem + ".pdf"))
    return hits[0] if hits else None


@pytest.mark.parametrize("stem,exp", CASES.items())
def test_grid_check_bill(stem, exp):
    f = _find(stem)
    if f is None:
        pytest.skip("invoice PDF not present")
    bill = read_bill(str(f), f.name)
    r = route(bill, classify_pdf(f.name), "upload")
    assert r is not None and "国网江苏电费核查联" in r.label, r and r.label
    items = r.parse(bill)
    assert len(items) == 1 and items[0]["category"] == "charge_energy"
    assert abs(items[0]["amount_cny"] + exp[0]) < 0.01
    assert abs(items[0]["volume_mwh"] - exp[1] / 1000) < 1e-6


def test_parser_on_synthetic_text():
    from services.settlement_ingest.parser_jiangsu_grid import is_jiangsu_grid_check, parse_jiangsu_grid_check
    t = ("国 网 江 苏 省 电 力 有 限 公 司 电 费 核 查 联\n有功总 100\n金额合计（元） 1,234.50 合计\n"
         "国 网 江 苏 省 电 力 有 限 公 司 电 费 核 查 联\n有功总 900\n金额合计（元） 1,234.50")
    assert is_jiangsu_grid_check(t)
    it = parse_jiangsu_grid_check(t)[0]
    assert it["amount_cny"] == -1234.5 and it["volume_mwh"] == 1.0
    assert not is_jiangsu_grid_check("国网江苏省电力有限公司 电费核查联 (no total)")
