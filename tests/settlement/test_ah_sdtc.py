"""安徽电力交易中心(统推) 新型经营主体交易结算单 (定远储能): gross 售电侧, tie to headline.

The headline 结算电费 between the 本月（购电侧）/本月（售电侧）rows is the NET
(售电侧 合计 − 购电侧 合计). Before the fix it was stored as the 售电侧 amount.
"""
import re
from pathlib import Path

import pytest

from libs.asset_risk.settlement.router import read_bill
from services.settlement_ingest.parser_stategrid import parse_ah_sdtc

D = Path("/Users/chenzhuqi/Library/CloudStorage/OneDrive-Personal/ETRM/bess-platform/data/raw/settlement/invoices/"
         "B-3 安徽定远")
FILES = sorted(D.glob("*/*统推独立储能结算单*.pdf")) if D.exists() else []
_NUM = r"-?\d[\d,]*\.\d+"


def _month(p: Path) -> str:
    # leading "2025年5月 【…】" prefix — the 2025-05 body says "025年5月统推"
    m = re.match(r"(?:B-3\D*)?(20\d\d)年(\d{1,2})月", p.name)
    return f"{m.group(1)}-{int(m.group(2)):02d}"


def _headline(text: str) -> float:
    m = (re.search(r"本月（购电侧）[^\n]*\n(" + _NUM + r")\s*\n本月（售电侧）", text)
         or re.search(r"^本月\s+(" + _NUM + r")\s*$", text, re.MULTILINE))
    return float(m.group(1).replace(",", ""))


def _parse(month: str):
    hits = [p for p in FILES if _month(p) == month]
    if not hits:
        pytest.skip(f"定远 statement {month} not available")
    return parse_ah_sdtc(read_bill(str(hits[0]), hits[0].name).full_text)


@pytest.mark.skipif(not FILES, reason="定远 statements not available")
@pytest.mark.parametrize("path", FILES, ids=[_month(p) for p in FILES])
def test_every_statement_ties_gross_minus_buy_to_headline(path):
    text = read_bill(str(path), path.name).full_text
    out = parse_ah_sdtc(text)
    assert set(out) == {"售电侧", "购电侧"}, f"{path.name} not parsed"
    sell, buy = out["售电侧"], out["购电侧"]
    assert sell["notes"] == "放电结算: 交易中心售电侧合计"
    assert buy["notes"] == "充电结算: 交易中心购电侧合计"
    assert buy["amount_cny"] <= 0  # charge stored negative
    assert sell["volume_mwh"] and buy["volume_mwh"]
    assert sell["amount_cny"] + buy["amount_cny"] == pytest.approx(_headline(text), abs=0.01)


def test_2026_03_exact_values():
    out = _parse("2026-03")
    assert out["售电侧"]["amount_cny"] == pytest.approx(3459747.08, abs=0.005)  # = grid 上网 bill 结算金额
    assert out["购电侧"]["amount_cny"] == pytest.approx(-1582061.71, abs=0.005)
    assert out["售电侧"]["volume_mwh"] == pytest.approx(8371.330)
    assert out["购电侧"]["volume_mwh"] == pytest.approx(10076.846)


# previously stored (wrong) values: (headline stored as sell, buy)
_STORED = {"2026-02": (3601657.94, -1376523.94),
           "2026-07": (2190646.03, -2699396.62),
           "2026-09": (3006685.53, -2419935.09)}


@pytest.mark.parametrize("month,stored", _STORED.items())
def test_gross_sell_equals_old_sell_plus_abs_buy(month, stored):
    out = _parse(month)
    old_sell, old_buy = stored
    assert out["售电侧"]["amount_cny"] == pytest.approx(old_sell + abs(old_buy), abs=0.01)
    assert out["购电侧"]["amount_cny"] == pytest.approx(old_buy, abs=0.01)


_SYNTH = """新型经营主体交易结算单
安徽电力交易中心2026年3月交易结算单
期间 实际用电量 结算电量 合同电量 偏差电量 结算电费
本月（购电侧） 100.000 100.000 0.000 100.000
{net}
本月（售电侧） 80.000 80.000 0.000 80.000
大写金额
结算科目编码 结算科目 分月交易计划电量 结算电量/容量 结算电价/均价 结算电费 备注
购电侧
01020201 省内现货交易-日前 100.000 300.00 30000.00
{buy_total}
售电侧
01020201 省内现货交易-日前 80.000 600.00 48000.00
合计 48000.00
"""


def test_synthetic_consistent_statement_parses():
    out = parse_ah_sdtc(_SYNTH.format(net="18000.00", buy_total="合计 30000.00"))
    assert out["售电侧"]["amount_cny"] == pytest.approx(48000.00)
    assert out["购电侧"]["amount_cny"] == pytest.approx(-30000.00)


def test_synthetic_buy_total_missing_is_derived_from_headline():
    out = parse_ah_sdtc(_SYNTH.format(net="18000.00", buy_total=""))
    assert out["购电侧"]["amount_cny"] == pytest.approx(-30000.00)


def test_synthetic_inconsistent_statement_returns_empty():
    # 48000 − 30000 = 18000, headline says 17000 → refuse to store anything
    assert parse_ah_sdtc(_SYNTH.format(net="17000.00", buy_total="合计 30000.00")) == {}


def test_synthetic_missing_headline_returns_empty():
    text = _SYNTH.format(net="", buy_total="合计 30000.00")
    assert parse_ah_sdtc(text) == {}
