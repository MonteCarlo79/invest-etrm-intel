# services/citic_futures/weekly_digest.py
"""CITIC Futures weekly digest — Claude summary of newly ingested relevant
reports, delivered as a Feishu card.

One digest per weekly folder. Structure: per-group one-liners (direction +
driver + key level), then a 电力/储能视角 section — gas-price read-across to
the merit-order fuel model, 碳酸锂 → BESS capex, 铜铝锌 → capex inputs.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

_BJ = timezone(timedelta(hours=8))
_GROUP_LABELS = {
    "energy_chain": "能源化工",
    "new_materials": "新材料(锂/新能源金属)",
    "basic_metals": "基本金属",
}
_MAX_CHARS_PER_DOC = 4000
_MAX_DOCS_PER_GROUP = 6

_SYSTEM = (
    "你是电力与储能投资分析师的期货周报助理。基于给定的中信期货周报内容，"
    "输出中文摘要，只依据提供的文本，不要引入外部数据。每个商品一行：方向 + 核心驱动 + 关键位/点位（若文中有）。"
    "最后写一段「电力/储能视角」：天然气/燃油价格对气电燃料成本与电力现货价格的传导、"
    "碳酸锂对储能系统造价（BESS capex）的影响、铜铝锌对电力设备成本的影响。"
    "总长度控制在 600 字以内，直接给正文，不要客套。"
)


def collect_relevant_text(relevant_docs: list[tuple[int, str, str]], pg_url: str) -> dict[str, list[tuple[str, str]]]:
    """Pull chunk text per doc from the KB, grouped by relevance group.

    Returns {group: [(filename, text)]}, each text truncated to
    _MAX_CHARS_PER_DOC, capped at _MAX_DOCS_PER_GROUP per group.
    """
    import psycopg2

    by_group: dict[str, list[tuple[str, str]]] = {}
    if not relevant_docs:
        return by_group
    conn = psycopg2.connect(pg_url)
    try:
        with conn.cursor() as cur:
            for doc_id, filename, group in relevant_docs:
                if len(by_group.setdefault(group, [])) >= _MAX_DOCS_PER_GROUP:
                    continue
                cur.execute(
                    "SELECT chunk_text FROM staging.spot_knowledge_chunks "
                    "WHERE doc_id = %s ORDER BY chunk_index LIMIT 40",
                    (doc_id,),
                )
                text = "\n".join(r[0] for r in cur.fetchall())[:_MAX_CHARS_PER_DOC]
                if text.strip():
                    by_group[group].append((filename, text))
    finally:
        conn.close()
    return by_group


def build_prompt(texts_by_group: dict[str, list[tuple[str, str]]], week_label: str) -> str:
    parts = [f"以下是中信期货 {week_label} 周报中与本业务相关的报告内容：\n"]
    for group, docs in texts_by_group.items():
        parts.append(f"\n===== {_GROUP_LABELS.get(group, group)} =====")
        for filename, text in docs:
            parts.append(f"\n--- {filename} ---\n{text}")
    parts.append("\n请按系统提示的格式输出本周摘要。")
    return "\n".join(parts)


def generate_digest(prompt: str, api_key: str) -> str:
    from shared.anthropic_client import make_client
    client = make_client(api_key)
    resp = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1500,
        system=_SYSTEM,
        messages=[{"role": "user", "content": prompt}],
    )
    return resp.content[0].text.strip()


def build_card(week_label: str, digest_text: str, n_docs: int, n_skipped: int) -> dict:
    now = datetime.now(_BJ)
    date_str = f"{now.year}年{now.month}月{now.day}日"
    body = digest_text + f"\n\n---\n📥 本周入库 {n_docs} 篇相关报告（另 {n_skipped} 篇已去重）· 全文已入知识库，可向策略师/投资委员会追问"
    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": "blue",
            "title": {"content": f"📊 中信期货周报摘要 — {week_label}（{date_str}）", "tag": "plain_text"},
        },
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md", "content": body}},
        ],
    }


def send_weekly_digest(feishu, owner_open_id: str, week_label: str,
                       texts_by_group: dict, api_key: str,
                       n_ingested: int = 0, n_skipped: int = 0) -> str:
    """Generate and send the digest card. Returns the digest text."""
    total_docs = sum(len(v) for v in texts_by_group.values())
    if total_docs == 0:
        logger.info("weekly_digest: no relevant docs, nothing to send")
        return ""
    prompt = build_prompt(texts_by_group, week_label)
    digest = generate_digest(prompt, api_key)
    if feishu and owner_open_id:
        card = build_card(week_label, digest, total_docs, n_skipped)
        feishu.send_card(open_id=owner_open_id, card=card)
    return digest
