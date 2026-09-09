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
