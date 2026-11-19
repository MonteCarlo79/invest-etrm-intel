"""江苏电力交易中心 交易结算单 (射阳储能 B-4 discharge) — deterministic route.

Vision stored level-1 "01 电量清分" AND its leaf "优先发电", double-counting every
month. The parser emits leaves only and must tie to 合计 to the cent.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from libs.asset_risk.settlement.router import read_bill, route
from services.settlement_ingest.parser_jiangsu import (
    is_jiangsu_trade_statement, jiangsu_total, parse_jiangsu_trade_statement)

_DIR = Path("/Users/chenzhuqi/Library/CloudStorage/OneDrive-Personal/ETRM/bess-platform/"
            "data/raw/settlement/invoices/B-4 江苏射阳")
_FILES = sorted(p for p in _DIR.rglob("*.pdf") if "【B-4-上】" in p.name or "B-4上" in p.name) \
    if _DIR.exists() else []
_LABEL = "江苏电力交易中心交易结算单 detected — parsed deterministically (no vision)."
_TOTALS = {"2025-08": 9062791.94, "2025-12": 17795704.43,
           "2026-01": 17217177.12, "2026-08": 17130285.81}


def _month(p: Path) -> str:
    m = re.search(r"(\d{4})-(\d{2})-\s?01", p.name)
    return f"{m.group(1)}-{m.group(2)}"


@pytest.fixture(scope="module", params=_FILES or [None], ids=lambda p: _month(p) if p else "absent")
def bill(request):
    if request.param is None:
        pytest.skip("B-4 江苏射阳 statements not available")
    return read_bill(str(request.param), request.param.name), _month(request.param)


def test_real_files_present():
    if not _DIR.exists():
        pytest.skip("settlement folder not available")
    assert len(_FILES) == 26


def test_real_statement_parses_and_ties(bill):
    b, month = bill
    r = route(b, "discharge", "upload")
    assert r is not None and r.label == _LABEL and r.kind == "discharge"
    items = r.parse(b)
    total = jiangsu_total(b.full_text)
    assert total is not None and items
    assert abs(sum(i["amount_cny"] for i in items) - total) <= 0.01
    keys = [(i["category"], i["notes"], i["amount_cny"]) for i in items]
    assert len(keys) == len(set(keys))
    energy = [i for i in items if i["category"] == "discharge_energy"]
    assert energy and all(i["volume_mwh"] and i["volume_mwh"] > 0 for i in energy)
    assert not any("电量清分" in i["notes"] or "清分" in i["notes"] for i in items)
    if month in _TOTALS:
        assert total == pytest.approx(_TOTALS[month], abs=0.005)
    if month == "2025-12":
        cap = [i for i in items if i["category"] == "capacity_compensation"]
        assert [i["amount_cny"] for i in cap] == [pytest.approx(7726065.60, abs=0.005)]


_SYNTH = """江苏电力交易中心2026年10月交易结算单
单位：兆瓦时、元
期间 实际上网电量 结算电量 合同电量 偏差电量 结算电费
本月 100.000 100.000 0.000 - 40100.00
电费大写金额 肆万零壹佰圆整
结算科目编码 结算科目 结算电费 备注
01 电量清分 0.000 100.000 391.000000 39100.00
0101010301 优先发电 0.000 100.000 391.000000 39100.00
02 0.000 0.000 - -
03 结算调整 0.000 0.000 - 1000.00
合计 0.000 100.000 401.000000 40100.00
备注：
"""


def test_level1_without_leaf_is_emitted_once():
    assert is_jiangsu_trade_statement(_SYNTH)
    assert jiangsu_total(_SYNTH) == pytest.approx(40100.00)
    items = parse_jiangsu_trade_statement(_SYNTH)
    assert [(i["category"], i["notes"], i["amount_cny"]) for i in items] == [
        ("discharge_energy", "放电结算: 优先发电", 39100.00),
        ("other", "放电结算: 结算调整", 1000.00),
    ]
    assert items[0]["volume_mwh"] == 100.0 and items[0]["price_cny_kwh"] == 0.391


def test_mismatched_total_returns_nothing():
    bad = _SYNTH.replace("0101010301 优先发电 0.000 100.000 391.000000 39100.00",
                         "0101010301 优先发电 0.000 100.000 391.000000 39000.00")
    assert parse_jiangsu_trade_statement(bad) == []
