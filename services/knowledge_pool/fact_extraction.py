"""
Structured fact extraction from spot report page text.

Extracts from already-stored page text in staging.spot_report_pages:
  - DA/RT price facts (reusing data already in public.spot_daily where available)
  - Market driver / cause phrases (原因为 sentences)
  - Interprovincial transaction mentions
  - Province-level price anomaly signals

Stores results in staging.spot_report_facts.
"""
from __future__ import annotations

import datetime as dt
import re
from typing import List, Optional

from .db import get_conn

# ── Regex patterns ────────────────────────────────────────────────────────────

# Cause/driver sentences: "X省...均价...原因为...；"
_RE_REASON = re.compile(
    r"([\u4e00-\u9fff]{1,6}(?:省|区)?)"        # province name
    r".{0,20}(?:实时|日前)?.{0,10}"
    r"(?:均价|价格|出清价格)"
    r".{0,60}?原因为"
    r"(.{1,200}?)"                               # reason body
    r"(?:；|。|$)",
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

# Numeric price extraction (yuan/MWh)
_RE_PRICE = re.compile(r"(\d{2,4}(?:\.\d{1,2})?)\s*元/[Mm][Ww][Hh]")


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


def extract_facts_for_document(
    doc_id: int,
    provinces_map: dict,  # {cn: en}
) -> int:
    """
    Read pages for doc_id, extract facts, store in staging.spot_report_facts.
    Returns count of fact rows written.
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

        # 1. Driver / cause phrases
        for reason in _extract_reasons(text):
            prov_cn = reason["province_cn"]
            prov_en = provinces_map.get(prov_cn, prov_cn)
            inserts.append(
                (
                    doc_id, page_date, prov_cn, prov_en,
                    "driver", "reason_text", None, None,
                    reason["fact_text"][:500], page_no, "medium",
                )
            )

        # 2. Interprovincial transaction mentions
        interp = _extract_interprovincial(text)
        if interp:
            inserts.append(
                (
                    doc_id, page_date, None, None,
                    "interprovincial", "mention", None, None,
                    interp[:500], page_no, "low",
                )
            )

        # 3. Section-level price mode signal
        if "现货实时市场" in text:
            inserts.append(
                (
                    doc_id, page_date, None, None,
                    "section_marker", "rt_section", None, None,
                    "实时市场运行情况", page_no, "high",
                )
            )
        if "现货日前市场" in text:
            inserts.append(
                (
                    doc_id, page_date, None, None,
                    "section_marker", "da_section", None, None,
                    "日前市场运行情况", page_no, "high",
                )
            )

    if not inserts:
        return 0

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO staging.spot_report_facts
                    (document_id, report_date, province_cn, province_en,
                     fact_type, metric_name, metric_value, metric_unit,
                     fact_text, page_no, confidence)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (document_id, report_date, province_cn, fact_type, metric_name)
                DO UPDATE SET fact_text = EXCLUDED.fact_text
                """,
                inserts,
            )
        conn.commit()

    return len(inserts)


def pull_price_facts_from_spot_daily(doc_id: int, report_dates: List[dt.date]) -> int:
    """
    Copy DA/RT price rows already in public.spot_daily into staging.spot_report_facts.
    Bridges the existing pipeline output into the knowledge pool.
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
                            inserts.append(
                                (
                                    doc_id, rd, pcn, pen,
                                    ftype, metric, val, "yuan/MWh",
                                    None, None, "high",
                                )
                            )

    if not inserts:
        return 0

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO staging.spot_report_facts
                    (document_id, report_date, province_cn, province_en,
                     fact_type, metric_name, metric_value, metric_unit,
                     fact_text, page_no, confidence)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (document_id, report_date, province_cn, fact_type, metric_name)
                DO UPDATE SET metric_value = EXCLUDED.metric_value
                """,
                inserts,
            )
        conn.commit()

    return len(inserts)
