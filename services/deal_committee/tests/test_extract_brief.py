# services/deal_committee/tests/test_extract_brief.py
import json
from types import SimpleNamespace

import pytest

from services.deal_committee.brief import build_extraction_prompt, extract_brief


class _FakeClient:
    """Mimics anthropic client: .messages.create(...) → content[0].text"""
    def __init__(self, text: str):
        self._text = text
    @property
    def messages(self):
        return self
    def create(self, **kwargs):
        return SimpleNamespace(content=[SimpleNamespace(text=self._text)])


def test_prompt_contains_fields_and_text():
    p = build_extraction_prompt("蒙西 100MW/200MWh 储能,总投资 12 亿元")
    for field in ("deal_name", "asset_type", "province", "capex_total_yuan", "field_confidence"):
        assert field in p
    assert "蒙西 100MW/200MWh" in p
    assert "12 亿元" in p


def test_extract_brief_parses_fenced_json():
    payload = {"deal_name": "蒙西储能一期", "province": "蒙西", "asset_type": "bess",
               "capacity_mw": 100, "capacity_mwh": 200, "capex_total_yuan": 1.2e9,
               "field_confidence": {"province": 0.95}}
    client = _FakeClient("```json\n" + json.dumps(payload) + "\n```")
    brief = extract_brief("无关文本", ["deal.docx"], api_key="", client=client)
    assert brief.deal_name == "蒙西储能一期"
    assert brief.province == "蒙西"
    assert brief.capex_total_yuan == 1.2e9
    assert brief.source_files == ["deal.docx"]


def test_extract_brief_invalid_json_raises():
    client = _FakeClient("这不是 JSON")
    with pytest.raises(ValueError, match="JSON"):
        extract_brief("文本", [], api_key="", client=client)
