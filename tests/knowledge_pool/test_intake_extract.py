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


class _FakeResp:
    def __init__(self, text): self.content = [type("B", (), {"text": text})()]


class _FakeClient:
    def __init__(self, text): self._text = text; self.calls = []
    @property
    def messages(self): return self
    def create(self, **kwargs): self.calls.append(kwargs); return _FakeResp(self._text)


def test_extract_batch_parses_and_filters_routes():
    client = _FakeClient(json.dumps(_CANNED, ensure_ascii=False))
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


def test_extract_batch_bad_json_raises():
    client = _FakeClient("not json at all")
    with pytest.raises(IntakeError):
        extract_batch([Page(filename="a.jpg", data=b"x", kind="image")], api_key="k", _client=client)


def test_extract_batch_groups_over_20_images():
    client = _FakeClient(json.dumps(_CANNED, ensure_ascii=False))
    pages = [Page(filename=f"IMG_{i}.jpg", data=f"img{i}".encode(), kind="image") for i in range(25)]
    prop = extract_batch(pages, api_key="k", _client=client)
    assert len(client.calls) == 2                      # 20 + 5
    assert prop.title == "宁夏储能市场分析"            # from first group


def test_extract_batch_text_pages_skip_vision():
    client = _FakeClient(json.dumps(_CANNED, ensure_ascii=False))
    pages = [Page(filename="doc.pdf", data=b"pdf", kind="text", text="政策全文……")]
    prop = extract_batch(pages, api_key="k", _client=client)
    content = client.calls[0]["messages"][0]["content"]
    assert not [b for b in content if b.get("type") == "image"]
    assert "政策全文" in content[-1]["text"]           # text pages inlined in prompt


def test_prompt_carries_metric_vocabulary():
    from services.knowledge_pool.intake_extract import _PROMPT_TMPL
    for token in ("registry_gw", "installed_new_storage_gw", "planning_gap_gw",
                  "target_gw", "禁止出现日期"):
        assert token in _PROMPT_TMPL, token
