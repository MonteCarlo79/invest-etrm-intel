"""Weekly 胖橘花 topic proposal card (v1).

Gathers context (KB recent docs + weekly price anomaly scan + curriculum
concepts + recent proposal history), asks the editor model for 3-5 topic
proposals, stores them in marketdata.professor_topic_proposals, and sends a
Feishu card to the owner.

Owner picks by replying on the card or telling the professor session the
seq number; the professor session writes the pick into topics/backlog.yaml
with status: chosen (see columns/xiyangjing/topics/CHOOSING.md).

Modes:
  --dry-run   print the card JSON; no DB write, no Feishu send
  --send      full run (default on ECS)
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import sqlalchemy as sa
import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("topic_card")

# power-academy is copied into the image at /opt/power-academy
sys.path.insert(0, "/opt/power-academy")
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "power-academy"))
# hermes feishu client: copied into the image at /opt/services/hermes
sys.path.insert(0, "/opt/services/hermes")

from academy.column import editor as ed  # noqa: E402
from academy.column import scan as cscan  # noqa: E402

DDL = """
CREATE TABLE IF NOT EXISTS marketdata.professor_topic_proposals (
  id SERIAL PRIMARY KEY,
  week DATE NOT NULL,
  seq INT NOT NULL,
  working_title TEXT NOT NULL,
  hook_source TEXT,
  hook_item TEXT,
  thesis TEXT,
  western JSONB,
  china JSONB,
  evidence_candidates JSONB,
  notes TEXT,
  status TEXT NOT NULL DEFAULT 'proposed',
  chosen_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (week, seq)
)
"""

KB_RECENT_SQL = """
select title, created_at from staging.spot_knowledge_docs
where active order by created_at desc limit :n
"""

PROPOSAL_SYSTEM = (
    "你是公众号「胖橘花丈量电价」的编辑。读者是电力市场同业/投资者与政策研究者。"
    "根据提供的近期素材（最新政策文件、周度价格异动扫描、课程概念库、历史选题），"
    "提出3-5个公众号选题。只输出一个JSON对象："
    "{\"proposals\": [{\"working_title\", \"hook\": {\"source\", \"item\"}, "
    "\"thesis\", \"western\": {\"concept_ids\": [], \"angle\"}, "
    "\"china\": {\"provinces\": [], \"topics\": []}, "
    "\"evidence_candidates\": [], \"notes\"}]}。"
    "hook.source 只能引用提供的素材（文档标题或 weekly_scan），禁止编造。"
    "每个选题须包含西方市场机制视角与中国数据支撑点；避免与历史选题重复。")


def gather(engine, weeks_back: int = 4) -> dict:
    with engine.connect() as c:
        kb = [{"title": t, "created_at": str(d)}
              for t, d in c.execute(sa.text(KB_RECENT_SQL), {"n": 12})]
        recent = [r[0] for r in c.execute(sa.text(
            "select working_title from marketdata.professor_topic_proposals "
            "where week >= :cut order by week desc limit 30"),
            {"cut": date.today() - timedelta(weeks=weeks_back)})]
    scan_md = "(scan unavailable)"
    try:
        df = cscan.load_prices(engine, days=35)
        scan_md = cscan.render_scan_md(cscan.scan_anomalies(df, date.today().isoformat()),
                                       date.today().isoformat())
    except Exception as exc:
        logger.warning("anomaly scan failed: %s", exc)
    concepts = []
    tracks_path = Path("/opt/power-academy/syllabus/tracks.yaml")
    if not tracks_path.exists():
        tracks_path = Path("power-academy/syllabus/tracks.yaml")
    if tracks_path.exists():
        concepts = [c["id"] for t in (yaml.safe_load(tracks_path.read_text()) or {}).get("tracks", [])
                    for c in t.get("concepts", [])]
    return {"kb_docs": kb, "scan_md": scan_md, "concept_ids": concepts,
            "recent_titles": recent, "today": date.today().isoformat()}


def propose(ctx: dict, model: str, api_key: str) -> list[dict]:
    import anthropic
    client = anthropic.Anthropic(api_key=api_key)
    sources = {d["title"] for d in ctx["kb_docs"]} | {"weekly_scan"}
    data = ed.call_json(client, model, PROPOSAL_SYSTEM,
                        json.dumps(ctx, ensure_ascii=False), max_tokens=4000)
    return [p for p in data.get("proposals", [])
            if p.get("hook", {}).get("source") in sources]


def store_proposals(engine, week: date, proposals: list[dict]) -> None:
    with engine.begin() as c:
        c.execute(sa.text(DDL))
        c.execute(sa.text("delete from marketdata.professor_topic_proposals where week = :w"),
                  {"w": week})
        for i, p in enumerate(proposals, 1):
            c.execute(sa.text(
                "insert into marketdata.professor_topic_proposals "
                "(week, seq, working_title, hook_source, hook_item, thesis, western, china, "
                " evidence_candidates, notes) values (:w, :s, :t, :hs, :hi, :th, :we, :ch, :ec, :no)"),
                {"w": week, "s": i, "t": p["working_title"],
                 "hs": p.get("hook", {}).get("source"), "hi": p.get("hook", {}).get("item"),
                 "th": p.get("thesis", ""), "we": json.dumps(p.get("western", {})),
                 "ch": json.dumps(p.get("china", {})),
                 "ec": json.dumps(p.get("evidence_candidates", [])),
                 "no": p.get("notes", "")})
    logger.info("stored %d proposals for week %s", len(proposals), week)


def build_card(week: date, proposals: list[dict], scan_md: str) -> dict:
    elements = []
    for i, p in enumerate(proposals, 1):
        hook = p.get("hook", {})
        elements.append({
            "tag": "div",
            "text": {"tag": "lark_md",
                     "content": f"**{i}. {p['working_title']}**\n"
                                f"由头：{hook.get('item', '')}（{hook.get('source', '')}）\n"
                                f"论点：{p.get('thesis', '')[:120]}"}})
    elements.append({"tag": "hr"})
    elements.append({"tag": "div",
                     "text": {"tag": "lark_md",
                              "content": "回复序号或告诉 professor 选题编号；"
                                         "选定后会写入 backlog 的 chosen，"
                                         "下次 professor 会话直接接续出稿。"}})
    return {"config": {"wide_screen_mode": True},
            "header": {"title": {"tag": "plain_text",
                                 "content": f"胖橘花 · 本周选题建议（{week}）"},
                       "template": "blue"},
            "elements": elements}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--send", action="store_true")
    args = ap.parse_args()

    pg_url = os.environ["PGURL"]
    engine = sa.create_engine(pg_url)
    ctx = gather(engine)
    logger.info("context: %d kb docs, %d recent titles, %d concepts",
                len(ctx["kb_docs"]), len(ctx["recent_titles"]), len(ctx["concept_ids"]))

    if args.dry_run:
        proposals = [{"working_title": "(dry-run) 示例选题",
                      "hook": {"source": ctx["kb_docs"][0]["title"] if ctx["kb_docs"] else "weekly_scan",
                               "item": "示例由头"},
                      "thesis": "示例论点", "western": {}, "china": {},
                      "evidence_candidates": [], "notes": ""}]
    else:
        model = os.environ.get("ACADEMY_EDITOR_MODEL", "claude-sonnet-4-6")
        proposals = propose(ctx, model, os.environ["ANTHROPIC_API_KEY"])
        logger.info("editor proposed %d topics", len(proposals))
        if not proposals:
            logger.error("editor returned no valid proposals; aborting without send")
            sys.exit(1)

    week = date.today() - timedelta(days=date.today().weekday())  # Monday of this week
    card = build_card(week, proposals, ctx["scan_md"])
    if args.dry_run:
        print(json.dumps(card, ensure_ascii=False, indent=1))
        return

    store_proposals(engine, week, proposals)
    if args.send or os.environ.get("FEISHU_APP_ID"):
        from feishu_client import FeishuClient
        feishu = FeishuClient(os.environ["FEISHU_APP_ID"], os.environ["FEISHU_APP_SECRET"])
        feishu.send_card(open_id=os.environ["FEISHU_OWNER_OPEN_ID"], card=card)
        logger.info("card sent to owner")


if __name__ == "__main__":
    main()
