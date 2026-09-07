# services/hermes/daf_command.py
"""DAF chat command — run the investment committee from Feishu/Telegram.

Paths:
  1. `/daf`               → list recent saved briefs, reply with a number to run
  2. `/daf 名称 | 省份 | MW/MWh | 总投资亿 [| asset_type]`  → new compact brief

Both paths converge on a confirm card; replying 「确认」starts the committee in
a background thread, with per-section progress and the DAF PDF delivered as a
file. Field overrides in confirm stage: `效率=0.88`, `利率=0.04` …
Owner-only (committee runs spend real API money): FEISHU_OWNER_OPEN_ID on
Feishu, TELEGRAM_OWNER_CHAT_ID on Telegram. A denied caller sees its own id
echoed so the admin can whitelist it.
"""
from __future__ import annotations

import logging
import os
import re
import threading
import time

from services.deal_committee.brief import DealBrief

logger = logging.getLogger(__name__)

_TTL_S = 900  # pending state lifetime (15 min)
# sender_id -> {"stage": "pick"|"confirm", "candidates": [...], "brief": DealBrief, "ts": float}
_pending: dict[str, dict] = {}

_CONFIRM_WORDS = {"确认", "confirm", "yes", "y", "是"}
_CANCEL_WORDS = {"取消", "cancel", "no", "n", "否"}

# 中文字段名 → DealBrief field (confirm-stage overrides)
_FIELD_MAP = {
    "名称": "deal_name", "项目": "deal_name", "项目名称": "deal_name",
    "省份": "province", "节点": "node",
    "效率": "efficiency", "循环": "cycles_per_day", "次数": "cycles_per_day",
    "装机": "installed_mw", "总投资": "capex_total_yuan", "capex": "capex_total_yuan",
    "投运": "commissioning_year", "年份": "commissioning_year",
    "期限": "tenor_years", "负债率": "debt_ratio", "利率": "loan_rate",
    "贷款期限": "loan_term_years", "对手方": "counterparty",
}
_NUM_FIELDS = {"efficiency", "cycles_per_day", "installed_mw", "capex_total_yuan",
               "commissioning_year", "tenor_years", "debt_ratio", "loan_rate",
               "loan_term_years"}
_ASSET_TYPES = {"bess", "wind", "solar", "wind_bess", "solar_bess"}


# ── parsing ───────────────────────────────────────────────────────────────────

def parse_compact(arg: str) -> DealBrief | None:
    """'乌兰察布二期 | 蒙西 | 500/2000 | 12.5亿 [| bess]' → DealBrief (defaults rest).

    Segments: name | province | mw/mwh | capex亿 [| asset_type]. None if malformed.
    """
    parts = [p.strip() for p in arg.split("|") if p.strip()]
    if len(parts) < 4:
        return None
    name, province, cap, capex = parts[:4]
    m = re.match(r"^([\d.]+)\s*/\s*([\d.]+)\s*(?:mw|mwh)?$", cap, re.I)
    if not m:
        return None
    mw, mwh = float(m.group(1)), float(m.group(2))
    c = re.match(r"^¥?\s*([\d.]+)\s*亿?$", capex)
    capex_yuan = float(c.group(1)) * 1e8 if c else None
    asset_type = "bess"
    if len(parts) > 4 and parts[4].lower() in _ASSET_TYPES:
        asset_type = parts[4].lower()
    return DealBrief(
        deal_name=name, province=province, asset_type=asset_type,
        capacity_mw=mw, capacity_mwh=mwh, capex_total_yuan=capex_yuan,
        confirmed=True,
    )


def apply_override(brief: DealBrief, text: str) -> str | None:
    """Apply one `字段=值` override in confirm stage. Returns a human-readable
    confirmation line, or None when the text is not a recognised override."""
    m = re.match(r"^\s*([一-鿿A-Za-z_]+)\s*[=:：]\s*(\S+)\s*$", text)
    if not m:
        return None
    key_raw, val = m.group(1), m.group(2)
    field = _FIELD_MAP.get(key_raw) or (key_raw if key_raw in _NUM_FIELDS else None)
    if field is None or not hasattr(brief, field):
        return None
    try:
        if field in _NUM_FIELDS:
            num = float(val.rstrip("亿"))
            if field == "capex_total_yuan" and ("亿" in text or num < 1e6):
                num *= 1e8
            if field in ("commissioning_year", "tenor_years", "loan_term_years"):
                num = int(num)
            setattr(brief, field, num)
        else:
            setattr(brief, field, val)
    except (ValueError, TypeError):
        return None
    return f"✏️ 已更新 {key_raw} → {val}"


def _brief_card(brief: DealBrief) -> str:
    capex = f"¥{brief.capex_total_yuan/1e8:.1f}亿" if brief.capex_total_yuan else "—"
    return (
        "📋 交易要素确认\n"
        f"项目:{brief.deal_name or '(未命名)'}\n"
        f"省份/类型:{brief.province or '—'} · {brief.asset_type}"
        + (f" · 节点 {brief.node}" if brief.node else "") + "\n"
        f"规模:{brief.capacity_mw:g}MW / {brief.capacity_mwh:g}MWh\n"
        f"总投资:{capex}\n"
        f"(默认:效率 {brief.efficiency:g} · {brief.cycles_per_day:g} 次/天 · "
        f"负债率 {brief.debt_ratio:.0%} · 利率 {brief.loan_rate:.1%} · "
        f"期限 {brief.tenor_years} 年 · 投运 {brief.commissioning_year})\n\n"
        "回复「确认」开始投委会分析(约5-10分钟,消耗API算力);\n"
        "回复 字段=值 修改(如 效率=0.88);回复「取消」退出。"
    )


def _candidate_line(i: int, b: dict) -> str:
    bd = b.get("brief") or {}
    capex = bd.get("capex_total_yuan")
    return (f"{i}. {b['deal_name']} · {bd.get('province') or '—'} · "
            f"{bd.get('asset_type') or '—'} · {bd.get('capacity_mw', 0):g}MW/"
            f"{bd.get('capacity_mwh', 0):g}MWh"
            + (f" · ¥{capex/1e8:.1f}亿" if capex else ""))


# ── channel helpers ───────────────────────────────────────────────────────────

def _reply(msg, feishu, telegram, text: str) -> None:
    try:
        if msg.source == "feishu" and feishu:
            feishu.send_text(open_id=msg.sender_id, text=text)
        elif msg.source == "telegram" and telegram:
            telegram.send_text(chat_id=msg.sender_id, text=text)
    except Exception as e:
        logger.error("daf reply failed: %s", e)


def _send_pdf(msg, feishu, telegram, pdf: bytes, filename: str, caption: str) -> None:
    try:
        if msg.source == "feishu" and feishu:
            key = feishu.upload_file(pdf, filename)
            feishu.send_file(open_id=msg.sender_id, file_key=key)
        elif msg.source == "telegram" and telegram:
            telegram.send_document(chat_id=msg.sender_id, data=pdf,
                                   filename=filename, caption=caption)
    except Exception as e:
        logger.error("daf pdf send failed: %s", e)
        _reply(msg, feishu, telegram, f"⚠️ PDF 发送失败:{e}")


def _is_owner(source: str, sender_id: str) -> bool:
    if source == "feishu":
        return bool(sender_id) and sender_id == os.environ.get("FEISHU_OWNER_OPEN_ID", "")
    if source == "telegram":
        owner = os.environ.get("TELEGRAM_OWNER_CHAT_ID", "")
        return bool(owner) and str(sender_id) == owner
    return False


# ── committee run (background thread) ─────────────────────────────────────────

def _run_and_deliver(msg, feishu, telegram, brief: DealBrief, api_key: str) -> None:
    try:
        from services.common.db_utils import get_engine
        from services.deal_committee.daf_builder import build_daf
        from services.deal_committee.library import (
            link_result_pdf, save_brief, save_daf, save_result,
        )
        from services.deal_committee.orchestrator import default_query_fn, run_committee
        from services.deal_committee.synthesis import run_synthesis

        _reply(msg, feishu, telegram,
               f"▶ 投委会分析开始:{brief.deal_name or '(未命名)'}({brief.province})…")
        result = run_committee(
            brief, api_key=api_key,
            on_section_done=lambda s: _reply(
                msg, feishu, telegram,
                f"{'✅' if s.status == 'ok' else '❌'} {s.title}"))
        ok = sum(1 for s in result.sections if s.status == "ok")
        result.synthesis, result.recommendation = run_synthesis(
            brief, result.sections, result.economics, api_key)
        pdf = build_daf(result)

        engine = get_engine()
        brief_id = save_brief(engine, brief)
        result_id = save_result(engine, brief_id, result)
        fname = f"DAF_{brief.deal_name or 'deal'}_{brief.province}.pdf"
        daf_id = save_daf(engine, brief_id, brief, pdf, fname, result.recommendation)
        link_result_pdf(engine, result_id, daf_id)

        _reply(msg, feishu, telegram,
               f"🏁 分析完成 · 结论:**{result.recommendation or '—'}**({ok}/7 章节成功)\n\n"
               f"{result.synthesis[:900]}")
        _send_pdf(msg, feishu, telegram, pdf, fname,
                  f"DAF · {brief.deal_name} · {result.recommendation or ''}")
    except Exception as e:
        logger.exception("daf run failed")
        _reply(msg, feishu, telegram, f"❌ 投委会分析失败:{e}")


# ── state machine ─────────────────────────────────────────────────────────────

def _start(msg, feishu, telegram, text: str, api_key: str) -> None:
    arg = re.sub(r"^\s*/?daf\s*", "", text, flags=re.I).strip()
    sender = msg.sender_id or ""
    if not arg:
        # Path 1 — list recent saved briefs
        try:
            from services.common.db_utils import get_engine
            from services.deal_committee.library import list_briefs
            candidates = list_briefs(get_engine(), limit=8)
        except Exception as e:
            _reply(msg, feishu, telegram, f"⚠️ 要素库不可用:{e}")
            return
        if not candidates:
            _reply(msg, feishu, telegram,
                   "暂无历史要素。新建:/daf 名称 | 省份 | MW/MWh | 总投资亿")
            return
        _pending[sender] = {"stage": "pick", "candidates": candidates,
                            "ts": time.monotonic()}
        lines = "\n".join(_candidate_line(i + 1, b) for i, b in enumerate(candidates))
        _reply(msg, feishu, telegram,
               f"📂 最近交易要素(15分钟内回复编号运行):\n{lines}\n\n"
               "新建:/daf 名称 | 省份 | MW/MWh | 总投资亿")
        return

    # Path 2 — compact new brief (or name match against saved briefs)
    brief = parse_compact(arg)
    if brief is None and "|" not in arg:
        # try name match: /daf 谷山梁二期
        try:
            from services.common.db_utils import get_engine
            from services.deal_committee.library import list_briefs
            for b in list_briefs(get_engine(), limit=20):
                if b["deal_name"] == arg:
                    brief = DealBrief(**(b["brief"] or {}))
                    break
        except Exception:
            pass
    if brief is None:
        _reply(msg, feishu, telegram,
               "⚠️ 格式:/daf 名称 | 省份 | MW/MWh | 总投资亿 [| bess]\n"
               "例:/daf 乌兰察布二期 | 蒙西 | 500/2000 | 12.5亿\n"
               "或 /daf <已有项目名称> 直接复用要素")
        return
    _pending[sender] = {"stage": "confirm", "brief": brief, "ts": time.monotonic()}
    _reply(msg, feishu, telegram, _brief_card(brief))


def _continue(msg, feishu, telegram, st: dict, api_key: str) -> bool:
    text = msg.text.strip()
    low = text.lower()
    sender = msg.sender_id or ""

    if low in _CANCEL_WORDS:
        _pending.pop(sender, None)
        _reply(msg, feishu, telegram, "已取消。")
        return True

    if st["stage"] == "pick":
        if low in _CONFIRM_WORDS:
            return False  # nothing to confirm yet — let the agent handle it
        if re.match(r"^\d{1,2}$", text):
            idx = int(text) - 1
            candidates = st.get("candidates", [])
            if not (0 <= idx < len(candidates)):
                _reply(msg, feishu, telegram, f"编号超出范围(1-{len(candidates)})。")
                return True
            brief = DealBrief(**(candidates[idx]["brief"] or {}))
            _pending[sender] = {"stage": "confirm", "brief": brief,
                                "ts": time.monotonic()}
            _reply(msg, feishu, telegram, _brief_card(brief))
            return True
        return False  # not a pick — let normal routing handle

    # stage == "confirm"
    if low in _CONFIRM_WORDS:
        brief = st["brief"]
        _pending.pop(sender, None)
        threading.Thread(target=_run_and_deliver,
                         args=(msg, feishu, telegram, brief, api_key),
                         daemon=True).start()
        return True
    update = apply_override(st["brief"], text)
    if update:
        st["ts"] = time.monotonic()
        _reply(msg, feishu, telegram, f"{update}\n\n{_brief_card(st['brief'])}")
        return True
    return False


def try_handle(msg, feishu, telegram, api_key: str) -> bool:
    """Intercept /daf commands and pending-state replies. True = handled."""
    text = (msg.text or "").strip()
    sender = msg.sender_id or ""
    if re.match(r"^/?daf\b", text, re.I):
        if not _is_owner(msg.source, sender):
            _reply(msg, feishu, telegram,
                   f"⛔ /daf 仅限管理员(每次运行消耗API算力)。\n"
                   f"你的 {msg.source} id:`{sender}` — 请管理员配置"
                   f" {'FEISHU_OWNER_OPEN_ID' if msg.source == 'feishu' else 'TELEGRAM_OWNER_CHAT_ID'}。")
            return True
        _start(msg, feishu, telegram, text, api_key)
        return True

    st = _pending.get(sender)
    if st and time.monotonic() - st["ts"] < _TTL_S and _is_owner(msg.source, sender):
        return _continue(msg, feishu, telegram, st, api_key)
    if st and time.monotonic() - st["ts"] >= _TTL_S:
        _pending.pop(sender, None)
    return False
