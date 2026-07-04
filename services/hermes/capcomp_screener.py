"""
Province Capacity Compensation + FR Market Screener
====================================================
Searches the knowledge base (staging.spot_knowledge_chunks) for per-province
policy documents, then uses Claude to extract:
  - 储能容量补偿标准 (yuan/kW) and 年最高净负荷峰值时段 (hours)
  - 调频容量价格 (yuan/kW·h) and 全省调频总资金池 (亿元/年)

Falls back to Claude's training knowledge if no KB documents found.

Entry points:
  screen_capcomp(pg_url, api_key, feishu, owner_open_id)  — full province loop
"""
from __future__ import annotations

import json
import logging
import re
import time
from datetime import date, datetime
from typing import Optional

import anthropic
import psycopg2

_QUERY_TIMEOUT = 30  # seconds per Claude call before skipping

logger = logging.getLogger(__name__)

# ── Province list ─────────────────────────────────────────────────────────────

_SEARCH_PROVINCES = [
    # 南方电网
    "广东", "广西", "贵州", "云南", "海南",
    # 国网 华北
    "北京", "天津", "河北", "冀北", "山西",
    # 国网 华东
    "上海", "江苏", "浙江", "安徽", "福建",
    # 国网 华中
    "湖北", "湖南", "河南", "江西", "重庆",
    # 国网 东北
    "辽宁", "吉林", "黑龙江",
    # 国网 西北
    "陕西", "甘肃", "宁夏", "新疆", "青海",
    # 国网 华北/西北 special
    "内蒙古（蒙西）", "内蒙古（蒙东）",
    # 国网 华东
    "山东",
    # 国网 西南
    "四川",
]

# ── KB keywords ────────────────────────────────────────────────────────────────

_CAP_COMP_KEYWORDS = ["容量补偿", "容量电价"]
_FR_KEYWORDS = ["调频辅助服务", "调频市场", "调频价格"]

_RATE_DELAY_SECONDS = 1  # delay between province queries


# ── KB search ──────────────────────────────────────────────────────────────────

def _search_kb(province: str, keywords: list, pg_url: str, limit: int = 12) -> list:
    """
    Search knowledge base for chunks relevant to province + keywords.
    Returns list of (chunk_text, file_name) tuples.

    Strategy:
    1. Prefer chunks from docs whose filename contains the province name
    2. Fall back to chunks that mention the province in their text
    Keyword filter: at least one keyword must match in the chunk.
    """
    # Normalise province for search (strip parentheses variants)
    prov_short = province.split("（")[0].split("(")[0]

    # Build keyword filter — at least one keyword must match in chunk
    kw_parts = " OR ".join(["c.chunk_text ILIKE %s"] * len(keywords))

    # Priority 1: doc filename contains province + keyword in chunk
    # Priority 2: chunk text contains province + keyword in chunk
    sql = f"""
        SELECT c.chunk_text, COALESCE(d.file_name, '') AS file_name,
               CASE WHEN d.file_name ILIKE %s THEN 1 ELSE 2 END AS priority
        FROM staging.spot_knowledge_chunks c
        LEFT JOIN staging.spot_knowledge_docs d ON d.id = c.doc_id
        WHERE ({kw_parts})
          AND (d.file_name ILIKE %s OR c.chunk_text ILIKE %s
               OR d.region_province ILIKE %s)
        ORDER BY priority ASC, d.published_at DESC NULLS LAST, c.id DESC
        LIMIT %s
    """
    params = (
        [f"%{prov_short}%"]          # priority CASE WHEN
        + [f"%{kw}%" for kw in keywords]  # keyword filter
        + [f"%{prov_short}%", f"%{prov_short}%", f"%{prov_short}%", limit]
    )
    try:
        conn = psycopg2.connect(pg_url, connect_timeout=10)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = [(r[0], r[1]) for r in cur.fetchall()]  # drop priority column
        conn.close()
        return rows
    except Exception as exc:
        logger.warning("KB search error for %s: %s", province, exc)
        return []


# ── Claude extraction ──────────────────────────────────────────────────────────

_CAP_COMP_SYSTEM = (
    "你是中国电力市场政策分析专家。从提供的政策文本中提取储能容量补偿相关数据。"
    "只提取明确出现在文本中的数据，不要猜测。若文本无相关数据，返回null值。"
    "Respond ONLY with valid JSON, no other text."
)

_FR_SYSTEM = (
    "你是中国电力市场政策分析专家。从提供的政策文本中提取调频辅助服务市场相关数据。"
    "只提取明确出现在文本中的数据，不要猜测。若文本无相关数据，返回null值。"
    "Respond ONLY with valid JSON, no other text."
)

_CAP_COMP_PROMPT = """
省份：{province}
目标年份：{year}

以下是知识库中找到的相关政策文本（共{n_chunks}段）：

{context}

任务：从上述文本中提取{province}储能容量补偿（capacity compensation）的具体数值。

规则：
- 如果文本中**明确出现**{province}的容量补偿标准（如"X元/kW"或"X元/千瓦"），填入数值，confidence=high。
- 如果文本**间接提到**{province}的大致补偿水平（如"约X元"、"参考价X元"），填入估算值，confidence=medium。
- 如果文本提到了某个**省份范围或区域**（如南方电网各省、华中地区）并有数值，可用于{province}，confidence=medium。
- 只有完全没有任何数字线索时，才返回null，confidence=low。

请以JSON格式回答（不要包含任何其他文字）：
{{
  "cap_comp_yuan_kw": <数字或null>,
  "peak_duration_hours": <年最高净负荷峰值小时数，数字或null>,
  "effective_year": <年份整数或null>,
  "source_url": "<文件名>",
  "confidence": "high|medium|low"
}}
"""

_FR_PROMPT = """
省份：{province}
目标年份：{year}

以下是知识库中找到的相关政策文本（共{n_chunks}段）：

{context}

任务：从上述文本中提取{province}调频辅助服务市场的具体数值。

规则：
- 如果文本中**明确出现**调频容量价格（如"X元/kW/h"或"X元/千瓦·时"），填入数值，confidence=high。
- 如果文本**间接提到**大致价格区间或参考值，填入估算值，confidence=medium。
- 如果文本提到了某个**省份范围或区域**的调频价格，可用于{province}，confidence=medium。
- 只有完全没有任何数字线索时，才返回null，confidence=low。

请以JSON格式回答（不要包含任何其他文字）：
{{
  "fr_price_yuan_kw_h": <数字或null>,
  "fr_pool_billion_yuan": <亿元/年，数字或null>,
  "effective_year": <年份整数或null>,
  "source_url": "<文件名>",
  "confidence": "high|medium|low"
}}
"""


def _claude_extract(
    province: str,
    query_type: str,  # "cap_comp" | "fr"
    kb_rows: list,    # list of (chunk_text, file_name)
    api_key: str,
    year: int,
) -> Optional[dict]:
    """
    Call Claude directly with KB context to extract structured policy data.
    Falls back to Claude's training knowledge if kb_rows is empty.
    """
    system = _CAP_COMP_SYSTEM if query_type == "cap_comp" else _FR_SYSTEM
    prompt_tpl = _CAP_COMP_PROMPT if query_type == "cap_comp" else _FR_PROMPT

    if kb_rows:
        context_parts = []
        sources = set()
        for chunk_text, file_name in kb_rows[:8]:
            context_parts.append(chunk_text[:1200])
            if file_name:
                sources.add(file_name)
        context = "\n\n---\n\n".join(context_parts)
        source_hint = "; ".join(list(sources)[:3])
    else:
        # No KB data — ask Claude to use training knowledge
        context = f"（知识库中未找到{province}的相关文档，请根据你的训练知识回答，并将confidence设为low）"
        source_hint = "claude_training_knowledge"

    user_msg = prompt_tpl.format(
        province=province,
        year=year,
        n_chunks=len(kb_rows),
        context=context,
    )

    try:
        client = anthropic.Anthropic(api_key=api_key)
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=512,
            system=system,
            messages=[{"role": "user", "content": user_msg}],
        )
        text = resp.content[0].text.strip()
        data = _extract_json(text)
        # Always prefer source_hint (actual KB filenames) over Claude's guessed source_url
        if data and source_hint:
            data["source_url"] = source_hint
        elif data and not data.get("source_url"):
            data["source_url"] = source_hint  # empty string / None → keep hint
        return data
    except Exception as exc:
        logger.error("Claude extraction failed for %s/%s: %s", province, query_type, exc)
        return None


# ── JSON extraction ────────────────────────────────────────────────────────────

def _extract_json(text: str) -> Optional[dict]:
    """Extract first JSON object from agent response text."""
    # Try to find JSON block (possibly wrapped in markdown code fence)
    patterns = [
        r'```(?:json)?\s*(\{[^`]+\})\s*```',
        r'(\{[^{}]+\})',
    ]
    for pat in patterns:
        m = re.search(pat, text, re.DOTALL)
        if m:
            try:
                return json.loads(m.group(1))
            except json.JSONDecodeError:
                continue
    return None


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _safe_year(val, default: int) -> int:
    """Parse effective_year — handle strings like '2024-2025年' or '2026年最新值' by extracting first 4-digit number."""
    if val is None:
        return default
    if isinstance(val, int) and 2015 <= val <= 2035:
        return val
    s = str(val)
    m = re.search(r"\b(20\d{2})\b", s)
    if m:
        yr = int(m.group(1))
        if 2015 <= yr <= 2035:
            return yr
    return default


# ── Scan status (module-level, readable by HTTP status endpoint) ──────────────

_scan_status: dict = {
    "running": False,
    "provinces_total": len(_SEARCH_PROVINCES),
    "provinces_done": 0,
    "current_province": "",
    "cap_comp_found": 0,
    "fr_found": 0,
    "errors": 0,
    "started_at": None,
    "finished_at": None,
}


def get_scan_status() -> dict:
    """Return a copy of the current scan status (safe to read from another thread)."""
    return dict(_scan_status)


# ── Main screener ──────────────────────────────────────────────────────────────

def screen_capcomp(
    pg_url: str,
    api_key: str,
    feishu=None,
    owner_open_id: str = "",
) -> dict:
    """
    Loop over all provinces, search KB + call Claude to extract cap_comp and fr_market data,
    upsert results to DB. Falls back to Claude training knowledge if no KB docs found.

    Returns summary dict: {provinces_searched, cap_comp_upserted, fr_upserted,
                           conflicts, errors}.
    """
    from services.hermes.capcomp_etl import upsert_cap_comp_rows, upsert_fr_rows

    year = datetime.now().year
    summary = {
        "provinces_searched": 0,
        "cap_comp_upserted": 0,
        "fr_upserted": 0,
        "conflicts": 0,
        "errors": [],
    }

    # Reset module-level status
    _scan_status.update({
        "running": True,
        "provinces_total": len(_SEARCH_PROVINCES),
        "provinces_done": 0,
        "current_province": "",
        "cap_comp_found": 0,
        "fr_found": 0,
        "errors": 0,
        "started_at": datetime.now().isoformat(),
        "finished_at": None,
    })

    logger.info("capcomp_screener: starting scan for %d provinces (year=%d)", len(_SEARCH_PROVINCES), year)

    source_tag = f"internet_search_{year}"

    for province in _SEARCH_PROVINCES:
        summary["provinces_searched"] += 1
        _scan_status["current_province"] = province

        # ── Cap comp: KB search + Claude extraction ──
        cap_row = None
        try:
            kb_rows = _search_kb(province, _CAP_COMP_KEYWORDS, pg_url)
            logger.info("cap_comp KB: %s → %d chunks found", province, len(kb_rows))
            data = _claude_extract(province, "cap_comp", kb_rows, api_key, year)
            if data:
                cap_val = _safe_float(data.get("cap_comp_yuan_kw"))
                peak_h = _safe_float(data.get("peak_duration_hours"))
                eff_year = _safe_year(data.get("effective_year"), year)
                confidence = str(data.get("confidence", "low")).lower()
                source = str(data.get("source_url", source_tag))[:500]

                if cap_val is not None and cap_val > 0:
                    cap_row = {
                        "province": province,
                        "effective_date": date(eff_year, 1, 1),
                        "cap_comp_yuan_kw": cap_val,
                        "peak_duration_hours": peak_h,
                        "source": source,
                    }
                    logger.info(
                        "cap_comp found: %s %.2f ¥/kW peak_h=%s conf=%s src=%s",
                        province, cap_val, peak_h, confidence, source[:60],
                    )
                else:
                    logger.info("cap_comp: no value for %s (data=%s)", province, data)
            else:
                logger.info("cap_comp: no JSON from Claude for %s", province)
        except Exception as exc:
            logger.error("cap_comp failed for %s: %s", province, exc)
            summary["errors"].append(f"cap_comp/{province}: {exc}")
            _scan_status["errors"] += 1

        # Upsert cap_comp immediately so it's visible in bess-map during the scan
        if cap_row:
            res = upsert_cap_comp_rows([cap_row], pg_url, source_tag)
            summary["cap_comp_upserted"] += res["upserted"]
            summary["conflicts"] += res["conflicts"]
            summary["errors"].extend(res["errors"])
            _scan_status["cap_comp_found"] += res["upserted"]

        time.sleep(_RATE_DELAY_SECONDS)

        # ── FR market: KB search + Claude extraction ──
        fr_row = None
        try:
            kb_rows = _search_kb(province, _FR_KEYWORDS, pg_url)
            logger.info("fr_market KB: %s → %d chunks found", province, len(kb_rows))
            data = _claude_extract(province, "fr", kb_rows, api_key, year)
            if data:
                fr_price = _safe_float(data.get("fr_price_yuan_kw_h"))
                fr_pool = _safe_float(data.get("fr_pool_billion_yuan"))
                eff_year = _safe_year(data.get("effective_year"), year)
                confidence = str(data.get("confidence", "low")).lower()
                source = str(data.get("source_url", source_tag))[:500]

                if fr_price is not None and fr_price > 0:
                    fr_row = {
                        "province": province,
                        "effective_date": date(eff_year, 1, 1),
                        "fr_price_yuan_kw_h": fr_price,
                        "fr_pool_billion_yuan": fr_pool,
                        "source": source,
                    }
                    logger.info(
                        "fr_market found: %s %.4f ¥/kW·h pool=%s conf=%s",
                        province, fr_price, fr_pool, confidence,
                    )
                else:
                    logger.info("fr_market: no value for %s", province)
            else:
                logger.info("fr_market: no JSON from Claude for %s", province)
        except Exception as exc:
            logger.error("fr_market failed for %s: %s", province, exc)
            summary["errors"].append(f"fr_market/{province}: {exc}")
            _scan_status["errors"] += 1

        # Upsert fr_market immediately
        if fr_row:
            res = upsert_fr_rows([fr_row], pg_url, source_tag)
            summary["fr_upserted"] += res["upserted"]
            summary["conflicts"] += res["conflicts"]
            summary["errors"].extend(res["errors"])
            _scan_status["fr_found"] += res["upserted"]

        time.sleep(_RATE_DELAY_SECONDS)
        _scan_status["provinces_done"] += 1

    _scan_status.update({
        "running": False,
        "current_province": "",
        "finished_at": datetime.now().isoformat(),
    })

    logger.info(
        "capcomp_screener done: cap_comp=%d fr=%d conflicts=%d errors=%d",
        summary["cap_comp_upserted"], summary["fr_upserted"],
        summary["conflicts"], len(summary["errors"]),
    )

    # ── Feishu notification ──
    if feishu and owner_open_id:
        try:
            msg = (
                f"[容量补偿+调频] 月度数据更新完成\n"
                f"容量补偿: {summary['cap_comp_upserted']} 条 | "
                f"调频市场: {summary['fr_upserted']} 条 | "
                f"冲突待确认: {summary['conflicts']} 条 | "
                f"错误: {len(summary['errors'])} 条"
            )
            feishu.send_message(owner_open_id, msg)
        except Exception as exc:
            logger.warning("Failed to send Feishu notification: %s", exc)

    return summary
