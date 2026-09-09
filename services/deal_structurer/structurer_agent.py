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
