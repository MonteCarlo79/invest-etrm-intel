# services/deal_committee/tests/test_brief.py
from services.deal_committee.brief import DealBrief, parse_brief_json, low_confidence_fields


def test_defaults_are_sane():
    b = DealBrief()
    assert b.asset_type == "bess"
    assert b.confirmed is False
    assert b.efficiency == 0.85
    assert b.debt_ratio == 0.70
    assert b.field_confidence == {}


def test_parse_brief_json_tolerates_missing_and_extra():
    b = parse_brief_json(
        {"deal_name": "蒙西储能一期", "province": "蒙西", "capacity_mw": 100,
         "capacity_mwh": 200, "unknown_field": "ignored",
         "field_confidence": {"province": 0.95, "capacity_mw": 0.3}},
        source_files=["deal.docx"],
    )
    assert b.deal_name == "蒙西储能一期"
    assert b.province == "蒙西"
    assert b.capacity_mw == 100.0
    assert b.source_files == ["deal.docx"]
    assert b.field_confidence["province"] == 0.95


def test_parse_brief_json_coerces_numeric_strings():
    b = parse_brief_json({"capex_total_yuan": "1200000000", "debt_ratio": "0.7"})
    assert b.capex_total_yuan == 1.2e9
    assert b.debt_ratio == 0.7


def test_low_confidence_fields_only_core_below_threshold():
    b = parse_brief_json({"field_confidence": {"province": 0.2, "structure_notes": 0.1,
                                               "capacity_mw": 0.9}})
    low = low_confidence_fields(b, threshold=0.6)
    assert "province" in low
    assert "capacity_mw" not in low
    assert "structure_notes" not in low  # not a core field


def test_asset_type_literal_validated():
    import pytest
    with pytest.raises(Exception):
        DealBrief(asset_type="nuclear")
