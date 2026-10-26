# services/option_engine/scout/thesis.py
"""Grounded LLM thesis drafter + delta gate (generative scout, Task 4).

This module is the ONLY place the scout spends LLM tokens. The delta gate
(``needs_draft``) is the cost control: a candidate whose numeric evidence has
not drifted beyond tolerance since the latest same-signature card is carried
forward without a new draft (spec §5.9: ~70-80% of calls eliminated in
steady state).

Grounding (hard rule): the thesis text is generated ONLY from the
candidate's evidence pack — every number must trace to it, and the LLM never
invents candidates. The system prompt states this in Chinese instructions.

This module writes nothing itself — the orchestrator writes cards.
"""
from __future__ import annotations

import json
import math
import re

import numpy as np

from services.option_engine.scout import store
from shared.usage_meter import usage_tag

MODEL = "claude-sonnet-4-6"
MAX_TOKENS = 400
USAGE_TAG = "deal_scout_thesis"
DEFAULT_TOLERANCE = 0.05

_SYSTEM = """你是一名新能源投资分析师,为投资委员会撰写投资机会卡片。

铁律(grounding,必须遵守):
- 只能用证据包中的数据;不得引用外部知识、市场价格记忆或行业常识数字
- 所有数字必须来自 evidence;不得推算、估算出证据包之外的数值
- 不得发明证据包中不存在的候选机会、政策或事件

卡片格式(五节,顺序固定,每节以节名加冒号开头):
机会 / 为什么被低估 / 政策赌注 / 证伪条件 / 风险

写作要求:
- 中文正文合计不超过 200 字(节标题不计)
- 直接、量化,写给投资委员会;只输出卡片正文,不要任何解释或前后缀"""


def draft_thesis(client, candidate: dict) -> str:
    """Draft one Chinese opportunity card for a deterministic candidate.

    One grounded ``claude-sonnet-4-6`` call (max_tokens 400): system prompt
    carries the grounding rule + card format; the user message is the JSON
    evidence pack (identity fields + evidence, numpy/pandas-safe via
    ``store._json_safe``). The ≤200-character limit is enforced by the
    prompt, not truncated post-hoc. ``client`` is the test seam; pass None
    to build one via ``make_client`` (fails loudly when no LLM configured).
    """
    if client is None:
        from shared.anthropic_client import is_llm_available, make_client
        if not is_llm_available():
            raise RuntimeError(
                "LLM unavailable: set ANTHROPIC_API_KEY or BEDROCK_REGION"
            )
        client = make_client()
    pack = {
        "kind": candidate.get("kind"),
        "province": candidate.get("province"),
        "asset_type": candidate.get("asset_type"),
        "scan_key": candidate.get("scan_key"),
        "evidence": candidate.get("evidence") or {},
    }
    content = json.dumps(store._json_safe(pack), ensure_ascii=False)
    with usage_tag(USAGE_TAG):
        resp = client.messages.create(
            model=MODEL, max_tokens=MAX_TOKENS, system=_SYSTEM,
            messages=[{"role": "user", "content": content}],
        )
    raw = resp.content[0].text.strip()
    # Tolerant fence-stripping (deal_committee.brief.extract_brief pattern):
    # the model occasionally wraps prose in a code fence despite the prompt.
    return re.sub(r"^```(?:\w+)?\s*|\s*```$", "", raw, flags=re.MULTILINE).strip()


def needs_draft(eng, candidate: dict, tolerance: float = DEFAULT_TOLERANCE
                ) -> tuple[bool, dict | None]:
    """Delta gate: does this candidate need a fresh LLM draft?

    Looks up the latest same-signature card within 40 days
    (``store.recent_same_signature`` default). Compares the candidate's
    NUMERIC evidence fields against the stored card's evidence_json — text
    fields are ignored. All within ``tolerance`` (relative) →
    ``(False, prior_card)`` and the caller marks the card carried. Any new
    or missing key, any drift beyond tolerance, or no prior → ``(True, None)``.

    A lookup failure (DB error etc.) also returns ``(True, None)`` — never
    silently suppress a candidate.
    """
    try:
        prior = store.recent_same_signature(eng, candidate.get("signature"))
    except Exception:
        return True, None
    if prior is None:
        return True, None
    prior_ev = prior.get("evidence_json") or {}
    if isinstance(prior_ev, str):  # some drivers return jsonb as raw text
        try:
            prior_ev = json.loads(prior_ev)
        except (json.JSONDecodeError, TypeError):
            return True, None
    if not isinstance(prior_ev, dict):
        return True, None
    cur = candidate.get("evidence") or {}
    for key, val in cur.items():
        f = _as_float(val)
        if f is None:
            continue  # text field — ignored by the delta comparison
        if key not in prior_ev:  # new key → draft
            return True, None
        g = _as_float(prior_ev.get(key))
        # missing/unparseable prior value or non-finite either side → draft
        if g is None or not math.isfinite(f) or not math.isfinite(g):
            return True, None
        if abs(f - g) > tolerance * max(abs(g), 1e-12):
            return True, None
    for key, val in prior_ev.items():
        if _as_float(val) is not None and key not in cur:
            return True, None  # evidence shape shrank → draft
    return False, prior


def _as_float(v) -> float | None:
    """Numeric coercion for the delta comparison; bools and non-numbers → None."""
    if isinstance(v, (bool, np.bool_)):
        return None
    if isinstance(v, (int, float, np.integer, np.floating)):
        return float(v)
    return None
