import json
import re
from datetime import date, timedelta
from pathlib import Path

import sqlalchemy as sa

from ..io import dump_yaml, load_yaml
from ..llm import call_json

EDITOR_SYSTEM = (
    "你是专栏「西洋镜看中国电力市场」的编辑。根据提供的近期素材，提出3-5个选题。"
    "只输出一个JSON对象：{\"proposals\": [{\"working_title\", \"hook\": {\"source\", \"item\"}, "
    "\"thesis_hypothesis\", \"western\": {\"concept_ids\": [], \"angle\"}, "
    "\"china\": {\"provinces\": [], \"topics\": []}, \"evidence_candidates\": [], \"timeliness\"}]}。"
    "hook.source 只能引用提供的素材文件名或文档标题，禁止编造来源或新闻。"
    "每个选题必须包含西方市场机制视角与可获取的中国数据支撑。")

KB_RECENT_SQL = ("select title, created_at from staging.spot_knowledge_docs "
                 "where active order by created_at desc limit :n")


def gather_context(briefings_dir, kb_engine, scan_md, concept_ids, backlog_titles,
                   briefing_days=7, kb_limit=10, today=None) -> dict:
    today = today or date.today().isoformat()
    cut = date.fromisoformat(today) - timedelta(days=briefing_days)
    briefings = []
    for p in sorted(Path(briefings_dir).glob("*.md"), reverse=True):
        m = re.match(r"(\d{4}-\d{2}-\d{2})", p.name)
        if m and date.fromisoformat(m.group(1)) >= cut:
            briefings.append({"file": p.name, "text": p.read_text(encoding="utf-8")[:2000]})
    kb_docs = []
    if kb_engine is not None:
        with kb_engine.connect() as c:
            kb_docs = [{"title": t, "created_at": str(d)}
                       for t, d in c.execute(sa.text(KB_RECENT_SQL), {"n": kb_limit})]
    return {"briefings": briefings, "kb_docs": kb_docs, "scan_md": scan_md,
            "concept_ids": concept_ids, "backlog_titles": backlog_titles, "today": today}


def propose(client, model, context) -> list:
    sources = {b["file"] for b in context["briefings"]} | {d["title"] for d in context["kb_docs"]}
    data = call_json(client, model, EDITOR_SYSTEM, json.dumps(context, ensure_ascii=False),
                     max_tokens=3000)
    out = []
    for p in data.get("proposals", []):
        if p.get("hook", {}).get("source") in sources:
            out.append(p)
    return out


def append_backlog(root, proposals, today) -> int:
    path = Path(root) / "topics" / "backlog.yaml"
    data = load_yaml(path) or {"topics": []}
    topics = data.get("topics") or []
    have = {t.get("working_title") for t in topics}
    n = 0
    for p in proposals:
        if p.get("working_title") in have:
            continue
        topics.append({**p, "proposed_at": today, "status": "idea"})
        n += 1
    dump_yaml({"topics": topics}, path)
    return n
