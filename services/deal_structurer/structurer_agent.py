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


def _load_current_result(engine, brief_id: int):
    """(latest_result_row, CommitteeResult-like parts) for a brief."""
    from services.deal_committee.library import list_briefs, load_result
    from services.deal_committee.result_store import dict_to_economics, sections_from_dicts
    b = next((x for x in list_briefs(engine, limit=20) if x["id"] == brief_id), None)
    if b is None or not b["result_id"]:
        return None, None
    rec = load_result(engine, b["result_id"])
    return rec, {
        "brief": rec["brief"],
        "sections": sections_from_dicts(rec["sections"]),
        "economics": dict_to_economics(rec["economics"]),
        "synthesis": rec["synthesis"], "recommendation": rec["recommendation"],
        "result_id": b["result_id"],
    }


def _save_revision(engine, brief_id: int, brief, sections, economics,
                   synthesis: str, recommendation: str, challenge: str = ""):
    from services.deal_committee.library import count_results_for_brief, save_result
    from services.deal_committee.orchestrator import CommitteeResult
    n = count_results_for_brief(engine, brief_id) + 1
    brief.deal_name = f"{brief.deal_name.split(' (rev ')[0]} (rev {n})"
    result = CommitteeResult(brief=brief, sections=sections, economics=economics,
                             synthesis=synthesis, recommendation=recommendation)
    # CommitteeResult carries deal_name only via .brief; mirror the rev-suffixed
    # name onto the object so savers/callers can read it directly.
    result.deal_name = brief.deal_name
    result_id = save_result(engine, brief_id, result)
    _note_revision(result_id, brief.deal_name)
    return result_id, brief.deal_name, result


def tool_rerun_analysis(brief_id: int, scope: str, confirmed: bool = False,
                        api_key: str = "") -> dict:
    engine = _engine()
    from services.deal_committee.brief import DealBrief
    from services.deal_committee.library import load_brief
    brief = DealBrief(**(load_brief(engine, brief_id)["brief"] or {}))

    rec, cur = _load_current_result(engine, brief_id)
    sections = cur["sections"] if cur else []
    economics = cur["economics"] if cur else None
    synthesis = cur["synthesis"] if cur else ""
    recommendation = cur["recommendation"] if cur else ""

    if scope == "economics":
        from services.deal_committee.economics import (
            economics_section_markdown, run_economics,
        )
        res = run_economics(brief, n_simulations=500)
        economics = res
        sections = [s if s.key != "economics" else type(s)(
            key=s.key, title=s.title,
            markdown=economics_section_markdown(res, brief), status="ok")
            for s in sections]
        if not sections:
            from services.deal_committee.sections import SectionResult
            sections = [SectionResult(key="economics", title="经济性测算",
                                      markdown=economics_section_markdown(res, brief))]
        rid, rev_name, _ = _save_revision(engine, brief_id, brief, sections,
                                          economics, synthesis, recommendation)
        mc = res.mc
        # mc is always a real MCResult from run_economics in production; the
        # None-guard only keeps mc-less EconomicsResult stubs (tests) usable.
        summary = (f"收入 P50 {mc.revenue_p50/1e6:.1f}M · 股权IRR {mc.equity_irr_p50:.1%} "
                   f"· NPV {mc.npv_p50/1e6:.1f}M · 容量补偿 {res.comp_rate_yuan_mwh:.0f} ¥/MWh"
                   if mc is not None else
                   f"容量补偿 {res.comp_rate_yuan_mwh:.0f} ¥/MWh")
        return {"ok": True, "result_id": rid, "rev_name": rev_name,
                "summary": summary}

    if scope.startswith("section:"):
        key = scope.split(":", 1)[1]
        from services.deal_committee.orchestrator import default_query_fn, run_single_section
        sec, econ2 = run_single_section(key, brief, default_query_fn, api_key, timeout_s=600)
        if sec.status != "ok":
            return {"error": f"章节 {key} 重算失败: {sec.error}"}
        sections = [s if s.key != key else sec for s in sections]
        if key == "economics" and econ2 is not None:
            economics = econ2
        rid, rev_name, _ = _save_revision(engine, brief_id, brief, sections,
                                          economics, synthesis, recommendation)
        return {"ok": True, "result_id": rid, "rev_name": rev_name,
                "summary": f"章节 {sec.title} 已重算 ({len(sec.markdown)} chars)"}

    if scope == "synthesis":
        from services.deal_committee.synthesis import run_synthesis
        synthesis, recommendation = run_synthesis(brief, sections, economics, api_key)
        rid, rev_name, _ = _save_revision(engine, brief_id, brief, sections,
                                          economics, synthesis, recommendation)
        return {"ok": True, "result_id": rid, "rev_name": rev_name,
                "summary": f"新结论: {recommendation or '—'}"}

    if scope == "daf":
        from services.deal_committee.daf_builder import build_daf
        from services.deal_committee.library import link_result_pdf, save_daf
        from services.deal_committee.orchestrator import CommitteeResult
        # DB-loaded economics carries scalars only (revenue_paths=[] by design);
        # the distribution chart needs real paths. simulate_prices(seed=42) makes
        # a fresh local run deterministic, so scalars match the stored result.
        _paths = getattr(getattr(economics, "mc", None), "revenue_paths", None)
        if economics is None or _paths is None or len(_paths) == 0:
            from services.deal_committee.economics import run_economics
            economics = run_economics(brief, n_simulations=500)
        result = CommitteeResult(brief=brief, sections=sections,
                                 economics=economics, synthesis=synthesis,
                                 recommendation=recommendation)
        pdf = build_daf(result)
        fname = f"DAF_{brief.deal_name or 'deal'}_{brief.province}.pdf"
        daf_id = save_daf(engine, brief_id, brief, pdf, fname, recommendation)
        if cur and cur.get("result_id"):
            link_result_pdf(engine, cur["result_id"], daf_id)
        return {"ok": True, "daf_id": daf_id, "filename": fname}

    if scope == "full":
        if not confirmed:
            return {"needs_confirmation": "重跑全部 7 个章节约 5-10 分钟。"
                                          "请用户明确回复「确认」后再以 confirmed=true 调用"}
        from services.deal_committee.orchestrator import default_query_fn, run_committee
        from services.deal_committee.synthesis import run_synthesis
        result = run_committee(brief, query_fn=default_query_fn,
                               api_key=api_key, timeout_s=600)
        result.synthesis, result.recommendation = run_synthesis(
            brief, result.sections, result.economics, api_key)
        rid, rev_name, _ = _save_revision(
            engine, brief_id, brief, result.sections, result.economics,
            result.synthesis, result.recommendation)
        ok = sum(1 for s in result.sections if s.status == "ok")
        return {"ok": True, "result_id": rid, "rev_name": rev_name,
                "summary": f"全量重算完成 {ok}/7 · 结论 {result.recommendation or '—'}"}

    return {"error": f"未知 scope {scope!r} — 支持: economics | section:<key> | synthesis | daf | full"}
