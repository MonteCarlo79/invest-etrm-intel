# services/deal_committee/tests/test_sections.py
import pytest

from services.deal_committee.brief import DealBrief
from services.deal_committee.sections import SECTION_DEFS, build_question, _asset_desc

BRIEF = DealBrief(deal_name="蒙西储能一期", asset_type="bess", province="蒙西",
                  capacity_mw=100, capacity_mwh=200)


def test_seven_sections_in_order():
    assert [s.key for s in SECTION_DEFS] == [
        "market_background", "policy", "economics",
        "ops_mengxi", "ops_asset_risk", "ops_retail_risk", "risk",
    ]


def test_agent_keys_match_bridge():
    agents = {s.key: s.agent for s in SECTION_DEFS}
    assert agents["market_background"] == "spot"
    assert agents["policy"] == "spot"
    assert agents["ops_mengxi"] == "mengxi"
    assert agents["ops_asset_risk"] == "asset-risk"
    assert agents["ops_retail_risk"] == "retail-risk"
    assert agents["economics"] == "" and agents["risk"] == ""


def test_questions_contain_province_and_asset():
    for key in ("market_background", "policy", "ops_mengxi", "ops_asset_risk", "ops_retail_risk"):
        q = build_question(key, BRIEF)
        assert "蒙西" in q, key
    assert "100" in build_question("market_background", BRIEF)  # capacity in MW


def test_non_agent_sections_raise_keyerror():
    with pytest.raises(KeyError):
        build_question("economics", BRIEF)
    with pytest.raises(KeyError):
        build_question("risk", BRIEF)


def test_asset_desc_variants():
    assert _asset_desc(BRIEF) == "100MW/200MWh 储能"
    w = DealBrief(asset_type="wind", province="山东", installed_mw=200)
    assert _asset_desc(w) == "200MW 风电"
    wb = DealBrief(asset_type="wind_bess", province="山西", installed_mw=150,
                   capacity_mw=50, capacity_mwh=100)
    assert "风电" in _asset_desc(wb) and "储能" in _asset_desc(wb)
