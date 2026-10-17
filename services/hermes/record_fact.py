"""RECORD_FACT ("记一下") — one-sentence field intel → draft DB row + pending slot.

Owner workflow (2026-10-17): field intel like "记一下:安徽调频池3000万/月,
规则未发布" lands as a draft row in the right province_* table (trust ladder:
draft → owner confirms 「确认」 → confirmed; bess-map reads confirmed only),
plus a KB doc and an agent_memory entry (handled by the caller).

Extraction uses a forced tool call (same pattern as intake_extract) so the
payload is always schema-valid.
"""
from __future__ import annotations

import json
import logging
from typing import Optional

from services.knowledge_pool.intake_extract import PROVINCES
from services.knowledge_pool.intake_routes import (
    write_ancillary_draft,
    write_capcomp_draft,
    write_fr_market_draft,
    write_pipeline_rows,
)

logger = logging.getLogger(__name__)

FACT_TYPES = ("fr_pool", "fr_price", "cap_comp", "ancillary_revenue",
              "storage_stat", "sysopfee", "other")

_EXTRACT_MODEL = "claude-haiku-4-5-20251001"

_SUBMIT_FACT_TOOL = {
    "name": "submit_fact",
    "description": "提交一条现场调研情报的结构化提取结果。",
    "input_schema": {
        "type": "object",
        "properties": {
            "province": {"type": ["string", "null"], "description": "省份(限选词表;无法判断为 null)"},
            "fact_type": {"type": "string", "enum": list(FACT_TYPES)},
            "value": {"type": ["number", "null"], "description": "归一后的数值(见单位规则)"},
            "unit": {"type": ["string", "null"], "description": "归一后的单位"},
            "as_of_date": {"type": ["string", "null"], "description": "信息获得日期 YYYY-MM-DD"},
            "effective_date": {"type": ["string", "null"], "description": "政策/数据生效日期 YYYY-MM-DD(无法判断=as_of)"},
            "metric": {"type": ["string", "null"], "description": "指标名(仅 storage_stat/ancillary_revenue 需要)"},
            "note": {"type": "string", "description": "用户原话的关键限定(如 规则未发布/零和/未传导用户侧)"},
            "kb_title": {"type": "string", "description": "一句话知识库标题(省份+主题+关键数字)"},
        },
        "required": ["province", "fact_type", "value", "unit", "as_of_date",
                     "effective_date", "metric", "note", "kb_title"],
    },
}

_PROMPT_TMPL = """从用户的现场调研情报中提取一条结构化事实并提交。
省份限选: {provinces}(无法判断为 null)。
fact_type 定义与单位归一规则:
- fr_pool: 调频资金池 → 亿元/年(万元/月 ×12÷10000,如 3000万/月 → 3.6)
- fr_price: 调频容量价格 → 元/kW·h
- cap_comp: 容量补偿标准 → 元/kW·年
- ancillary_revenue: 辅助服务/调频已结算收入 → 元(万元 ×10000)
- storage_stat: 储能装机/在库/备案统计 → 数值与单位保持原文(如 GW)
- sysopfee: 系统运行费 → 元/kWh
- other: 不属于以上(只进知识库,不写结构化表)
as_of_date: 信息获得日期(用户未说明用今天);effective_date: 政策/数据生效日期(无法判断 = as_of_date)。
metric: 仅 storage_stat/ancillary_revenue 需要(如 registry_gw / 调频收入);其余为 null。
note: 用户原话中的关键限定(如"规则未发布""两个细则零和""未传导用户侧"),简洁保留。
kb_title: 省份+主题+关键数字的一句话标题。

用户情报: {text}"""


class RecordFactError(Exception):
    """Extraction or write failed."""


def extract_fact(text: str, client) -> dict:
    """One forced-tool-call extraction; always returns schema-valid dict."""
    resp = client.messages.create(
        model=_EXTRACT_MODEL,
        max_tokens=1024,
        tools=[_SUBMIT_FACT_TOOL],
        tool_choice={"type": "tool", "name": "submit_fact"},
        messages=[{"role": "user",
                   "content": _PROMPT_TMPL.format(provinces="、".join(PROVINCES), text=text)}],
    )
    for block in resp.content:
        if (getattr(block, "type", None) == "tool_use"
                and getattr(block, "name", None) == "submit_fact"):
            return block.input
    raise RecordFactError(
        f"extractor returned no fact payload "
        f"(content types: {[getattr(b, 'type', '?') for b in resp.content]}).")


def write_fact_drafts(fact: dict, *, pg_url: str, source: str) -> list[str]:
    """Route the fact to the matching province_* draft writer. Returns labels.

    storage_stat / sysopfee / other → no structured write in v1 (KB only):
    pipeline stats belong to the document-backed Intel Intake path (source_doc_id
    lineage), and sysopfee has its own monthly ETL.
    """
    ft = fact.get("fact_type")
    prov = fact.get("province")
    eff = fact.get("effective_date") or fact.get("as_of_date")
    v = fact.get("value")
    out: list[str] = []
    if ft in ("fr_pool", "fr_price"):
        r = write_fr_market_draft(
            province=prov, effective_date=eff,
            fr_price_yuan_kw_h=v if ft == "fr_price" else None,
            fr_pool_billion_yuan=v if ft == "fr_pool" else None,
            source=source)
        out.append(f"province_fr_market: {r}")
    elif ft == "cap_comp":
        r = write_capcomp_draft(
            province=prov, effective_date=eff, cap_comp_yuan_kw=v, source=source)
        out.append(f"province_cap_comp: {r}")
    elif ft == "ancillary_revenue":
        month = (fact.get("as_of_date") or "")[:7] + "-01"
        r = write_ancillary_draft(
            province=prov, month=month,
            metric=fact.get("metric") or "调频收入",
            amount_yuan=v, source_file=source)
        out.append(f"province_ancillary_revenue: {r}")
    return out


def pending_writes_for(fact: dict, source: str) -> list[dict]:
    """Natural-key refs of the draft rows (for the confirm flip)."""
    ft = fact.get("fact_type")
    prov = fact.get("province")
    eff = fact.get("effective_date") or fact.get("as_of_date")
    if ft in ("fr_pool", "fr_price"):
        return [{"table": "marketdata.province_fr_market",
                 "key": {"province": prov, "effective_date": eff, "source": source}}]
    if ft == "cap_comp":
        return [{"table": "marketdata.province_cap_comp",
                 "key": {"province": prov, "effective_date": eff, "source": source}}]
    if ft == "ancillary_revenue":
        month = (fact.get("as_of_date") or "")[:7] + "-01"
        return [{"table": "marketdata.province_ancillary_revenue",
                 "key": {"province": prov, "month": month,
                         "metric": fact.get("metric") or "调频收入"}}]
    return []


def load_pending(*, pg_url: str) -> Optional[dict]:
    """Latest active pending_record memory row → {writes, memory_id}, or None."""
    import psycopg2
    try:
        conn = psycopg2.connect(pg_url)
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, content FROM marketdata.agent_memory "
                    "WHERE app = 'hermes' AND category = 'pending_record' AND active "
                    "ORDER BY id DESC LIMIT 1")
                row = cur.fetchone()
        conn.close()
    except Exception as exc:
        logger.warning("load_pending failed: %s", exc)
        return None
    if not row:
        return None
    try:
        payload = json.loads(row[1])
    except Exception:
        return None
    payload["memory_id"] = row[0]
    return payload


def confirm_pending(pending: dict, *, pg_url: str) -> str:
    """Flip pending draft rows to confirmed and deactivate the memory slot."""
    import psycopg2
    tables: list[str] = []
    conn = psycopg2.connect(pg_url)
    try:
        with conn:
            with conn.cursor() as cur:
                for w in pending.get("writes", []):
                    where = " AND ".join(f"{k} = %s" for k in w["key"])
                    cur.execute(
                        f"UPDATE {w['table']} SET status = 'confirmed' "
                        f"WHERE {where} AND status = 'draft'",
                        tuple(w["key"].values()))
                    tables.append(w["table"].rsplit(".", 1)[-1])
                mid = pending.get("memory_id")
                if mid:
                    cur.execute(
                        "UPDATE marketdata.agent_memory SET active = FALSE WHERE id = %s",
                        (mid,))
    finally:
        conn.close()
    return "、".join(dict.fromkeys(tables)) or "无"
