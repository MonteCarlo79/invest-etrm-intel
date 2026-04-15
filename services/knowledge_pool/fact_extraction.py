"""
Structured fact extraction from spot report page text.

Extracts from already-stored page text in staging.spot_report_pages:
  - DA/RT price facts (reusing data already in public.spot_daily where available)
  - Market driver / cause phrases (原因为 sentences)
  - Interprovincial transaction mentions
  - Province-level price anomaly signals
  - Section markers (实时/日前 section headers)
  - High/low province price statements

Each fact row carries source_method to distinguish:
  pdf_regex          — extracted directly from PDF text by regex
  spot_daily_bridge  — copied from public.spot_daily (structured DB table)
"""
from __future__ import annotations

import datetime as dt
import re
from typing import List, Optional

from .db import get_conn

# ── Regex patterns ────────────────────────────────────────────────────────────

# Province allowlist for driver sentence matching
# Kept here (not imported) so fact_extraction.py stays self-contained.
_KNOWN_PROVINCES = frozenset([
    "山东", "山西", "蒙西", "内蒙古", "甘肃", "广东", "四川", "云南",
    "贵州", "广西", "湖南", "湖北", "安徽", "浙江", "江苏", "福建",
    "河南", "陕西", "宁夏", "新疆", "辽宁", "吉林", "黑龙江", "河北",
    "蒙东", "青海", "西藏", "海南", "重庆", "江西", "冀北", "冀南",
    "北京", "天津", "上海", "广东", "甘肃",
])

# Province names pattern (pipe-joined, longest first to avoid substring match)
_PROV_PAT = "|".join(
    re.escape(p) for p in sorted(_KNOWN_PROVINCES, key=len, reverse=True)
)

# Cause/driver sentences: "X省...均价...原因为...；"
# Group 1: province name (from allowlist only)
_RE_REASON = re.compile(
    rf"({_PROV_PAT})"                           # province name (allowlist)
    r".{0,30}(?:实时|日前)?.{0,15}"
    r"(?:均价|价格|出清价格)"
    r".{0,80}?原因为"
    r"(.{1,300}?)"                               # reason body
    r"(?:；|。|\n\n|$)",
    re.DOTALL,
)

# Province high/low signal: "XX省均价最高/最低"
_RE_EXTREMES = re.compile(
    r"([\u4e00-\u9fff]{1,6}(?:省|区)?)"
    r".{0,10}(?:均价|价格)"
    r".{0,20}(?:最高|最低|偏高|偏低)",
)

# Interprovincial: "省间现货交易" or "跨省" mentions
_RE_INTERPROV = re.compile(r"省间现货交易|跨省.{0,20}(送|受|出力|购|售)")

# Province high/low explicit: "XX省均价最高" or "均价最低的为XX省"
_RE_RANK_STMT = re.compile(
    r"(?:"
    r"([\u4e00-\u9fff]{1,6}(?:省|区)?)(?:.{0,10}(?:均价|价格).{0,20}(?:最高|最低|偏高|偏低))"
    r"|"
    r"(?:均价|价格)(?:最高|最低)(?:.{0,10}为|的).{0,5}([\u4e00-\u9fff]{1,6}(?:省|区)?)"
    r")",
)

# DA/RT price with numeric value extraction
_RE_PRICE_VAL = re.compile(
    r"([\u4e00-\u9fff]{1,6}(?:省|区)?)"
    r".{0,20}(?:日前|实时).{0,10}(?:均价|出清均价)"
    r".{0,10}(\d{1,4}(?:\.\d{1,2})?)"
    r"\s*元/[Mm][Ww][Hh]",
)


def _extract_reasons(text: str) -> List[dict]:
    """Return list of {province_cn, fact_text} driver phrases."""
    results = []
    for m in _RE_REASON.finditer(text):
        prov = m.group(1).strip()
        reason = m.group(2).strip()
        results.append({"province_cn": prov, "fact_text": reason})
    return results


def _extract_interprovincial(text: str) -> Optional[str]:
    """Return first interprovincial mention sentence, or None."""
    m = _RE_INTERPROV.search(text)
    if m:
        start = max(0, m.start() - 20)
        end = min(len(text), m.end() + 80)
        return text[start:end].strip()
    return None


def _extract_price_inline(text: str) -> List[dict]:
    """Extract inline price mentions with province and value."""
    results = []
    seen = set()
    for m in _RE_PRICE_VAL.finditer(text):
        prov = m.group(1).strip()
        val_str = m.group(2)
        key = (prov, val_str)
        if key in seen:
            continue
        seen.add(key)
        # Determine DA vs RT from context
        snippet = text[max(0, m.start() - 5): m.end()]
        ftype = "price_da" if "日前" in snippet else "price_rt"
        try:
            val = float(val_str)
        except ValueError:
            continue
        results.append({
            "province_cn": prov,
            "fact_type": ftype,
            "metric_name": "inline_avg",
            "metric_value": val,
            "fact_text": m.group(0)[:200],
        })
    return results


def _extract_rank_statements(text: str) -> List[dict]:
    """Extract highest/lowest price province statements."""
    results = []
    for m in _RE_RANK_STMT.finditer(text):
        prov = (m.group(1) or m.group(2) or "").strip()
        if not prov:
            continue
        context = text[max(0, m.start() - 10): m.end() + 30]
        signal = "highest" if "最高" in context or "偏高" in context else "lowest"
        results.append({
            "province_cn": prov,
            "fact_type": "price_rank",
            "metric_name": signal,
            "fact_text": context[:200],
        })
    return results


def extract_facts_for_document(
    doc_id: int,
    provinces_map: dict,  # {cn: en}
) -> int:
    """
    Read pages for doc_id, extract facts, store in staging.spot_report_facts.
    Returns count of fact rows written.

    source_method = 'pdf_regex' for all rows inserted here.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT page_no, page_date, extracted_text "
                "FROM staging.spot_report_pages "
                "WHERE document_id = %s ORDER BY page_no",
                (doc_id,),
            )
            pages = cur.fetchall()

    inserts = []

    for page_no, page_date, text in pages:
        if not text or not page_date:
            continue

        # 1. Driver / cause phrases — only store if province is in known map
        for reason in _extract_reasons(text):
            prov_cn = reason["province_cn"]
            prov_en = provinces_map.get(prov_cn)
            if prov_en is None:
                continue  # Discard false-positive regex matches
            inserts.append((
                doc_id, page_date, prov_cn, prov_en,
                "driver", "reason_text", None, None,
                reason["fact_text"][:500], page_no, "medium", "pdf_regex",
            ))

        # 2. Interprovincial transaction mentions
        interp = _extract_interprovincial(text)
        if interp:
            inserts.append((
                doc_id, page_date, None, None,
                "interprovincial", "mention", None, None,
                interp[:500], page_no, "low", "pdf_regex",
            ))

        # 3. Section-level price mode signal
        if "现货实时市场" in text:
            inserts.append((
                doc_id, page_date, None, None,
                "section_marker", "rt_section", None, None,
                "实时市场运行情况", page_no, "high", "pdf_regex",
            ))
        if "现货日前市场" in text:
            inserts.append((
                doc_id, page_date, None, None,
                "section_marker", "da_section", None, None,
                "日前市场运行情况", page_no, "high", "pdf_regex",
            ))

        # 4. Inline price mentions — only store if province is in known map
        for price in _extract_price_inline(text):
            prov_cn = price["province_cn"]
            prov_en = provinces_map.get(prov_cn)
            if prov_en is None:
                continue
            inserts.append((
                doc_id, page_date, prov_cn, prov_en,
                price["fact_type"], price["metric_name"],
                price["metric_value"], "yuan/MWh",
                price["fact_text"], page_no, "medium", "pdf_regex",
            ))

        # 5. High/low rank statements — only store if province is in known map
        for rank in _extract_rank_statements(text):
            prov_cn = rank["province_cn"]
            prov_en = provinces_map.get(prov_cn)
            if prov_en is None:
                continue
            inserts.append((
                doc_id, page_date, prov_cn, prov_en,
                rank["fact_type"], rank["metric_name"],
                None, None,
                rank["fact_text"], page_no, "low", "pdf_regex",
            ))

    if not inserts:
        return 0

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO staging.spot_report_facts
                    (document_id, report_date, province_cn, province_en,
                     fact_type, metric_name, metric_value, metric_unit,
                     fact_text, page_no, confidence, source_method)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (document_id, report_date, province_cn, fact_type, metric_name)
                DO UPDATE SET
                    fact_text     = EXCLUDED.fact_text,
                    metric_value  = EXCLUDED.metric_value,
                    source_method = EXCLUDED.source_method
                """,
                inserts,
            )
        conn.commit()

    return len(inserts)


def pull_price_facts_from_spot_daily(doc_id: int, report_dates: List[dt.date]) -> int:
    """
    Copy DA/RT price rows already in public.spot_daily into staging.spot_report_facts.
    Bridges the existing pipeline output into the knowledge pool.

    source_method = 'spot_daily_bridge' for all rows inserted here.
    These rows have confidence='high' since they come from the structured parsed DB.
    """
    if not report_dates:
        return 0

    inserts = []
    with get_conn() as conn:
        with conn.cursor() as cur:
            for d in report_dates:
                cur.execute(
                    """
                    SELECT report_date, province_cn, province_en,
                           da_avg, da_max, da_min, rt_avg, rt_max, rt_min
                    FROM public.spot_daily
                    WHERE report_date = %s
                    """,
                    (d,),
                )
                for row in cur.fetchall():
                    rd, pcn, pen, da_avg, da_max, da_min, rt_avg, rt_max, rt_min = row
                    for metric, val in [
                        ("da_avg", da_avg), ("da_max", da_max), ("da_min", da_min),
                        ("rt_avg", rt_avg), ("rt_max", rt_max), ("rt_min", rt_min),
                    ]:
                        if val is not None:
                            ftype = "price_da" if metric.startswith("da") else "price_rt"
                            inserts.append((
                                doc_id, rd, pcn, pen,
                                ftype, metric, val, "yuan/MWh",
                                None, None, "high", "spot_daily_bridge",
                            ))

    if not inserts:
        return 0

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO staging.spot_report_facts
                    (document_id, report_date, province_cn, province_en,
                     fact_type, metric_name, metric_value, metric_unit,
                     fact_text, page_no, confidence, source_method)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (document_id, report_date, province_cn, fact_type, metric_name)
                DO UPDATE SET
                    metric_value  = EXCLUDED.metric_value,
                    source_method = EXCLUDED.source_method
                """,
                inserts,
            )
        conn.commit()

    return len(inserts)
