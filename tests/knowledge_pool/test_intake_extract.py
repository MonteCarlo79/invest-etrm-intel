# tests/knowledge_pool/test_intake_extract.py
import json
import pytest
from services.knowledge_pool.intake_extract import (
    IntakeError, Page, extract_batch, batch_hash,
)


_CANNED = {
    "title": "宁夏储能市场分析",
    "province": "宁夏",
    "category": "capacity_pipeline",
    "summary": "宁夏独立储能在库19.93GW，备案合计31.63GW，规划缺口11.46GW。",
    "pages": [{"page_no": 1, "text": "第一页转录"}],
    "routes": [
        {"type": "pipeline_stat", "province": "宁夏", "content": "在库19.93GW",
         "structured": {"rows": [{"metric": "registry_gw", "value": 19.93,
                                   "unit": "GW", "as_of_date": "2026-07-21",
                                   "source": "国网宁电[2026]70号"}]}},
        {"type": "quant_note", "province": "宁夏",
         "content": "AGC二次调频需求仅17.65~35.3万kW，调频收入空间受限。",
         "structured": {}},
        {"type": "nonsense_type", "province": None, "content": "dropped", "structured": {}},
    ],
}


def _tool_resp(payload, stop_reason="end_turn"):
    """Response shaped like a forced-tool-call reply: one tool_use block."""
    block = type("B", (), {"type": "tool_use", "name": "submit_extraction",
                           "input": payload})()
    return type("R", (), {"content": [block], "stop_reason": stop_reason})()


def _text_resp(text, stop_reason="end_turn"):
    """Legacy text-only response (no tool_use block)."""
    block = type("B", (), {"type": "text", "text": text})()
    return type("R", (), {"content": [block], "stop_reason": stop_reason})()


class _FakeClient:
    def __init__(self, resp): self._resp = resp; self.calls = []
    @property
    def messages(self): return self
    def create(self, **kwargs): self.calls.append(kwargs); return self._resp


def test_extract_batch_parses_and_filters_routes():
    client = _FakeClient(_tool_resp(_CANNED))
    pages = [Page(filename="IMG_1.jpg", data=b"\xff\xd8img1", kind="image"),
             Page(filename="IMG_2.jpg", data=b"\xff\xd8img2", kind="image")]
    prop = extract_batch(pages, api_key="k", _client=client)
    assert prop.title == "宁夏储能市场分析"
    assert prop.province == "宁夏"
    assert prop.category == "capacity_pipeline"
    assert [r.type for r in prop.routes] == ["pipeline_stat", "quant_note"]  # unknown dropped
    assert prop.batch_hash == batch_hash([b"\xff\xd8img1", b"\xff\xd8img2"])
    assert len(client.calls) == 1                      # one vision call for 2 images
    assert client.calls[0]["max_tokens"] >= 16384   # dense multi-slide batches need the headroom
    content = client.calls[0]["messages"][0]["content"]
    assert sum(1 for b in content if b.get("type") == "image") == 2


def test_extract_batch_forces_extraction_tool():
    """Free-text JSON parsing broke on unescaped quotes in long transcriptions
    (辽宁 batch 2026-10-14). The extractor must constrain output via a forced
    tool call so the API guarantees schema-valid JSON."""
    client = _FakeClient(_tool_resp(_CANNED))
    extract_batch([Page(filename="a.jpg", data=b"x", kind="image")], api_key="k", _client=client)
    call = client.calls[0]
    assert call["tool_choice"] == {"type": "tool", "name": "submit_extraction"}
    tool_names = [t["name"] for t in call["tools"]]
    assert tool_names == ["submit_extraction"]
    schema = call["tools"][0]["input_schema"]
    for field_name in ("title", "province", "category", "summary", "pages", "routes"):
        assert field_name in schema["properties"], field_name


def test_extract_batch_no_tool_block_raises():
    client = _FakeClient(_text_resp(json.dumps(_CANNED, ensure_ascii=False)))
    with pytest.raises(IntakeError):
        extract_batch([Page(filename="a.jpg", data=b"x", kind="image")], api_key="k", _client=client)


def test_extract_batch_max_tokens_raises_split_hint():
    client = _FakeClient(_tool_resp(_CANNED, stop_reason="max_tokens"))
    with pytest.raises(IntakeError, match="[Ss]plit"):
        extract_batch([Page(filename="a.jpg", data=b"x", kind="image")], api_key="k", _client=client)


def test_extract_batch_groups_over_20_images():
    client = _FakeClient(_tool_resp(_CANNED))
    pages = [Page(filename=f"IMG_{i}.jpg", data=f"img{i}".encode(), kind="image") for i in range(25)]
    prop = extract_batch(pages, api_key="k", _client=client)
    assert len(client.calls) == 2                      # 20 + 5
    assert prop.title == "宁夏储能市场分析"            # from first group


def test_extract_batch_text_pages_skip_vision():
    client = _FakeClient(_tool_resp(_CANNED))
    pages = [Page(filename="doc.pdf", data=b"pdf", kind="text", text="政策全文……")]
    prop = extract_batch(pages, api_key="k", _client=client)
    content = client.calls[0]["messages"][0]["content"]
    assert not [b for b in content if b.get("type") == "image"]
    assert "政策全文" in content[-1]["text"]           # text pages inlined in prompt


def test_extract_batch_page_no_null_falls_back_to_cursor():
    canned = dict(_CANNED, pages=[{"page_no": None, "text": "空页码转录"}])
    client = _FakeClient(_tool_resp(canned))
    pages = [Page(filename="IMG_1.jpg", data=b"\xff\xd8img1", kind="image")]
    prop = extract_batch(pages, api_key="k", _client=client)   # must not raise TypeError
    assert prop.pages == [(1, "空页码转录")]                   # cursor fallback = 1


def test_prompt_carries_metric_vocabulary():
    from services.knowledge_pool.intake_extract import _PROMPT_TMPL
    for token in ("registry_gw", "installed_new_storage_gw", "planning_gap_gw",
                  "target_gw", "禁止出现日期"):
        assert token in _PROMPT_TMPL, token
