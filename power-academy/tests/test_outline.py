import json

from academy.models import SourceEntry
from academy.outline import (SYSTEM_PROMPT, chunk_text, outline_source,
                             render_outline_md, run_outlines)
from tests.fakes import FakeClient


def _e(id_):
    return SourceEntry(id=id_, path="/x", title=id_.upper(), type="pdf",
                       source_class="library", cleared=False, license_risk="high")


def _reply(names, **kw):
    d = {"topic": "valuation", "level": "advanced", "market": "EU", "year": 2011,
         "concepts": [{"name": n, "scope": f"{n} scope"} for n in names],
         "methods": ["monte carlo"], "implied_prerequisites": ["option basics"],
         "has_worked_examples": False, "has_code": False}
    d.update(kw)
    return json.dumps(d)


def test_prompt_forbids_summarising():
    assert "do not summarise" in SYSTEM_PROMPT.lower()


def test_chunk_text_caps_and_flags_truncation():
    chunks, trunc = chunk_text("a" * 250, size=100, max_chunks=2)
    assert [len(c) for c in chunks] == [100, 100] and trunc is True
    chunks, trunc = chunk_text("a" * 50, size=100, max_chunks=2)
    assert len(chunks) == 1 and trunc is False


def test_outline_merges_chunks_dedupes_concepts():
    # 70 000 chars with the default 60 000 chunk size -> exactly two chunks
    c = FakeClient([_reply(["Spark Spread", "Tolling"]),
                    _reply(["spark spread", "Swing"], has_code=True)])
    out = outline_source(c, "m", _e("a"), "x" * 70_000)
    names = [x["name"] for x in out["concepts"]]
    assert names == ["Spark Spread", "Tolling", "Swing"]
    assert out["has_code"] is True and out["truncated"] is False


def test_render_md_has_concepts_and_no_body_text():
    out = json.loads(_reply(["Tolling"]))
    out["truncated"] = False
    md = render_outline_md(_e("a"), out)
    assert "Tolling" in md and "Tolling scope" in md and "A" in md


def test_run_outlines_isolates_failures_and_resumes(tmp_path):
    cache, outd = tmp_path / "cache", tmp_path / "inv"
    cache.mkdir()
    (cache / "good.txt").write_text("t" * 300, encoding="utf-8")
    (cache / "bad.txt").write_text("t" * 300, encoding="utf-8")
    es = [_e("good"), _e("bad"), _e("missing")]
    c = FakeClient([_reply(["A"]), "garbage", "garbage"])
    rep = run_outlines(es, cache, outd, c, "m")
    assert rep["ok"] == ["good"] and list(rep["failed"]) == ["bad"]
    assert rep["skipped"] == {"missing": "no cached text"}
    assert (outd / "good.md").exists()
    # resume: good already done, bad retried and now succeeds
    c2 = FakeClient([_reply(["B"])])
    rep2 = run_outlines(es, cache, outd, c2, "m")
    assert rep2["ok"] == ["bad"]
    assert set(json.loads((outd / "_outlines.json").read_text())) == {"good", "bad"}


def test_chunk_outline_requests_enough_tokens():
    c = FakeClient([_reply(["A"])])
    outline_source(c, "m", _e("a"), "x" * 100)
    assert c.calls[0]["max_tokens"] >= 4000
