import json

from academy.column.editor import (EDITOR_SYSTEM, append_backlog, gather_context,
                                   propose)
from academy.io import load_yaml
from tests.fakes import FakeClient


def _ctx():
    return {"briefings": [{"file": "2026-10-03-morning.md", "text": "山西现货转入正式运行…"}],
            "kb_docs": [{"title": "关于深化新能源上网电价市场化改革的通知", "created_at": "2026-09-30"}],
            "scan_md": "- **山东** [rt_max_spike] 周实时最高 2.5…",
            "concept_ids": ["merit_order", "spark_spread_option"],
            "backlog_titles": ["已有选题"]}


def test_system_prompt_forbids_fabricated_hooks():
    assert "只能引用" in EDITOR_SYSTEM or "only" in EDITOR_SYSTEM.lower()


def test_propose_drops_unverifiable_hooks():
    good = {"working_title": "从英国容量市场看山东", "hook": {"source": "2026-10-03-morning.md",
            "item": "山西现货正式运行"}, "thesis_hypothesis": "t",
            "western": {"concept_ids": ["merit_order"], "angle": "a"},
            "china": {"provinces": ["山东"], "topics": ["现货"]},
            "evidence_candidates": ["spot_prices_hourly"], "timeliness": "本周"}
    bad = dict(good, working_title="编造来源", hook={"source": "不存在的来源", "item": "x"})
    c = FakeClient([json.dumps({"proposals": [good, bad]}, ensure_ascii=False)])
    props = propose(c, "m", _ctx())
    assert [p["working_title"] for p in props] == ["从英国容量市场看山东"]


def test_append_backlog_dedupes(tmp_path):
    (tmp_path / "topics").mkdir()
    (tmp_path / "topics" / "backlog.yaml").write_text(
        "topics:\n  - working_title: 已有选题\n", encoding="utf-8")
    props = [{"working_title": "新选题", "hook": {"source": "s", "item": "i"}},
             {"working_title": "已有选题", "hook": {"source": "s", "item": "i"}}]
    n = append_backlog(tmp_path, props, today="2026-10-06")
    assert n == 1
    titles = [t["working_title"] for t in load_yaml(tmp_path / "topics" / "backlog.yaml")["topics"]]
    assert titles == ["已有选题", "新选题"]
    entry = load_yaml(tmp_path / "topics" / "backlog.yaml")["topics"][1]
    assert entry["status"] == "idea" and entry["proposed_at"] == "2026-10-06"


def test_gather_context_reads_recent_briefings(tmp_path):
    b = tmp_path / "briefings"
    b.mkdir()
    (b / "2026-10-03-morning.md").write_text("新闻一", encoding="utf-8")
    (b / "2026-09-01-morning.md").write_text("旧闻", encoding="utf-8")
    ctx = gather_context(b, None, "扫描", ["merit_order"], ["已有"], briefing_days=7,
                         kb_limit=10, today="2026-10-06")
    assert len(ctx["briefings"]) == 1 and ctx["briefings"][0]["text"] == "新闻一"
    assert ctx["kb_docs"] == [] and ctx["scan_md"] == "扫描"


def test_propose_keeps_scan_cited_hooks():
    good = {"working_title": "甘肃基差异动分析", "hook": {"source": "weekly_scan",
            "item": "甘肃基差 1.8x"}, "thesis_hypothesis": "t",
            "western": {"concept_ids": [], "angle": "a"},
            "china": {"provinces": ["甘肃"], "topics": ["现货"]},
            "evidence_candidates": ["spot_prices_hourly"], "timeliness": "本周"}
    c = FakeClient([json.dumps({"proposals": [good]}, ensure_ascii=False)])
    props = propose(c, "m", _ctx())
    assert [p["working_title"] for p in props] == ["甘肃基差异动分析"]
