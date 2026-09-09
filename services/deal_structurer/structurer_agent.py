"""Structurer agent tools — DB-driven challenge/revise loop for deal-structurer tabs."""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_PAGE_CHARS = 4000
_MAX_UPLOADED = 5

# filename -> {"text": str, "ts": float}
# NOTE: module-level (single-operator app, same convention as _last_mc_result).
_UPLOADED_DOCS: dict[str, dict] = {}

_session_revisions: list[dict] = []


def add_uploaded_doc(filename: str, text: str) -> None:
    import time as _time
    _UPLOADED_DOCS[filename] = {"text": text, "ts": _time.monotonic()}
    while len(_UPLOADED_DOCS) > _MAX_UPLOADED:
        oldest = min(_UPLOADED_DOCS, key=lambda k: _UPLOADED_DOCS[k]["ts"])
        _UPLOADED_DOCS.pop(oldest, None)


def list_uploaded_docs() -> list[str]:
    return list(_UPLOADED_DOCS)


def tool_read_uploaded_doc(filename: str, page: int | None = None,
                           query: str | None = None) -> dict:
    doc = _UPLOADED_DOCS.get(filename)
    if doc is None:
        return {"error": f"未找到上传文档 {filename!r}", "available": list_uploaded_docs()}
    text = doc["text"]
    total = max(1, (len(text) + _PAGE_CHARS - 1) // _PAGE_CHARS)
    if query:
        i = text.lower().find(query.lower())
        if i < 0:
            return {"error": f"文档中未找到 {query!r}", "total_pages": total}
        lo, hi = max(0, i - 1500), min(len(text), i + 1500)
        return {"filename": filename, "page": None, "total_pages": total,
                "excerpt": text[lo:hi]}
    p = page or 1
    p = max(1, min(p, total))
    return {"filename": filename, "page": p, "total_pages": total,
            "excerpt": text[(p - 1) * _PAGE_CHARS: p * _PAGE_CHARS]}


def _note_revision(result_id: int, label: str) -> None:
    _session_revisions.append({"result_id": result_id, "label": label})


def get_session_revisions() -> list[dict]:
    return list(_session_revisions)


def _engine():
    from services.common.db_utils import get_engine
    return get_engine()


def tool_list_deals(limit: int = 10) -> dict:
    from services.deal_committee.library import list_briefs
    rows = list_briefs(_engine(), limit=limit)
    return {"deals": [
        {"brief_id": b["id"], "deal_name": b["deal_name"],
         "province": (b["brief"] or {}).get("province"),
         "asset_type": (b["brief"] or {}).get("asset_type"),
         "result_id": b["result_id"], "recommendation": b["recommendation"],
         "created_at": b["created_at"][:16]}
        for b in rows
    ]}


def _resolve_brief_row(deal) -> dict | None:
    from services.deal_committee.library import list_briefs
    for b in list_briefs(_engine(), limit=20):
        if isinstance(deal, int) and b["id"] == deal:
            return b
        if isinstance(deal, str) and b["deal_name"] == deal:
            return b
    return None


def tool_get_deal_result(deal) -> dict:
    row = _resolve_brief_row(deal)
    if row is None:
        return {"error": f"找不到交易 {deal!r} — 先用 list_deals 查看"}
    if not row["result_id"]:
        return {"error": f"{row['deal_name']} 尚无分析结果", "brief_id": row["id"]}
    from services.deal_committee.library import load_result
    rec = load_result(_engine(), row["result_id"])
    return {
        "brief_id": row["id"], "deal_name": row["deal_name"],
        "result_id": row["result_id"], "brief": rec["brief"],
        "economics": rec["economics"],
        "sections": [
            {"key": s["key"], "title": s["title"], "status": s.get("status", "ok"),
             "markdown_head": (s.get("markdown") or "")[:400]}
            for s in rec["sections"]
        ],
        "synthesis": (rec["synthesis"] or "")[:1500],
        "recommendation": rec["recommendation"], "daf_id": rec["daf_id"],
    }


_UPDATE_WHITELIST = {
    "province", "node", "capacity_mw", "capacity_mwh", "efficiency",
    "cycles_per_day", "installed_mw", "capex_total_yuan", "commissioning_year",
    "tenor_years", "debt_ratio", "loan_rate", "loan_term_years", "deal_name",
    "comp_rate_yuan_mwh",
}


def tool_update_deal_parameters(brief_id: int, updates: dict,
                                challenge_note: str = "") -> dict:
    if not isinstance(updates, dict) or not updates:
        return {"error": "updates 必须是非空 {field: value} 字典"}
    bad = [f for f in updates if f not in _UPDATE_WHITELIST]
    if bad:
        return {"error": f"字段不在白名单: {bad}。可改: {sorted(_UPDATE_WHITELIST)}"}

    from services.deal_committee.brief import DealBrief
    from services.deal_committee.library import (
        count_results_for_brief, load_brief, update_brief,
    )
    engine = _engine()
    row = load_brief(engine, brief_id)
    brief = DealBrief(**(row["brief"] or {}))

    changed = {}
    for field, value in updates.items():
        old = getattr(brief, field)
        if old == value and value is not None:
            continue
        try:
            setattr(brief, field, value)
            brief = DealBrief(**brief.model_dump())  # pydantic re-validate
        except Exception as e:
            return {"error": f"字段 {field} 取值非法 ({value!r}): {e}"}
        changed[field] = [old, value]
    if not changed:
        return {"error": "updates 与现值相同,无变化"}

    if challenge_note:
        note = f"rev{count_results_for_brief(engine, brief_id) + 1}: {challenge_note}"
        brief.structure_notes = ((brief.structure_notes or "") + "\n" + note).strip()

    update_brief(engine, brief_id, brief)
    return {
        "ok": True, "brief_id": brief_id, "changed": changed,
        "rev_name": f"{brief.deal_name} (rev {count_results_for_brief(engine, brief_id) + 1})",
    }
