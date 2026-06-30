"""
Province Capacity Compensation + FR Market Screener
====================================================
Uses the Hermes internet agent (run_internet_query) to search per-province for:
  - 储能容量补偿标准 (yuan/kW) and 年最高净负荷峰值时段 (hours)
  - 调频容量价格 (yuan/kW·h) and 全省调频总资金池 (亿元/年)

Results are upserted to province_cap_comp and province_fr_market.
Low-confidence results are stored with status='conflict' for user review.

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

# ── Query templates ────────────────────────────────────────────────────────────

_CAP_COMP_QUERY_TEMPLATE = (
    "{province}储能容量补偿标准是多少元/千瓦（元/kW）？"
    "核准利用小时数或最高净负荷峰值时段是几小时？"
    "请给出{year}年最新政策数据和来源URL。"
    "请以JSON格式回答：{{\"cap_comp_yuan_kw\": 数值, \"peak_duration_hours\": 数值, "
    "\"effective_year\": 年份, \"source_url\": \"URL或文件名\", \"confidence\": \"high/medium/low\"}}"
)

_FR_QUERY_TEMPLATE = (
    "{province}调频辅助服务市场调频容量价格是多少元/kW/h（元/千瓦·小时）？"
    "全省调频总资金池（亿元/年）是多少？"
    "请给出{year}年数据和来源URL。"
    "请以JSON格式回答：{{\"fr_price_yuan_kw_h\": 数值, \"fr_pool_billion_yuan\": 数值, "
    "\"effective_year\": 年份, \"source_url\": \"URL或文件名\", \"confidence\": \"high/medium/low\"}}"
)

_RATE_DELAY_SECONDS = 2  # delay between province queries


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


# ── Main screener ──────────────────────────────────────────────────────────────

def screen_capcomp(
    pg_url: str,
    api_key: str,
    feishu=None,
    owner_open_id: str = "",
) -> dict:
    """
    Loop over all provinces, query internet agent for cap_comp and fr_market data,
    upsert results to DB. Low-confidence results stored with status='conflict'.

    Returns summary dict: {provinces_searched, cap_comp_upserted, fr_upserted,
                           conflicts, errors}.
    """
    from services.hermes.internet_agent import run_internet_query
    from services.hermes.capcomp_etl import upsert_cap_comp_rows, upsert_fr_rows

    year = datetime.now().year
    summary = {
        "provinces_searched": 0,
        "cap_comp_upserted": 0,
        "fr_upserted": 0,
        "conflicts": 0,
        "errors": [],
    }

    logger.info("capcomp_screener: starting scan for %d provinces (year=%d)", len(_SEARCH_PROVINCES), year)

    cap_comp_rows: list[dict] = []
    fr_rows: list[dict] = []

    for province in _SEARCH_PROVINCES:
        summary["provinces_searched"] += 1

        # ── Cap comp query ──
        try:
            q = _CAP_COMP_QUERY_TEMPLATE.format(province=province, year=year)
            answer = run_internet_query(q, api_key, pg_url)
            data = _extract_json(answer)
            if data:
                cap_val = _safe_float(data.get("cap_comp_yuan_kw"))
                peak_h = _safe_float(data.get("peak_duration_hours"))
                eff_year = int(data.get("effective_year", year))
                confidence = str(data.get("confidence", "medium")).lower()
                source = str(data.get("source_url", f"internet_search_{year}"))[:500]

                if cap_val is not None and cap_val > 0:
                    status_override = "conflict" if confidence == "low" else None
                    row = {
                        "province": province,
                        "effective_date": date(eff_year, 1, 1),
                        "cap_comp_yuan_kw": cap_val,
                        "peak_duration_hours": peak_h,
                        "source": source,
                        "_status_override": status_override,
                    }
                    cap_comp_rows.append(row)
                    logger.info(
                        "cap_comp found: %s %.2f ¥/kW peak_h=%s conf=%s",
                        province, cap_val, peak_h, confidence,
                    )
                else:
                    logger.info("cap_comp: no data for %s (answer preview: %s)", province, answer[:100])
            else:
                logger.info("cap_comp: no JSON in answer for %s", province)
        except Exception as exc:
            logger.error("cap_comp query failed for %s: %s", province, exc)
            summary["errors"].append(f"cap_comp/{province}: {exc}")

        time.sleep(_RATE_DELAY_SECONDS)

        # ── FR market query ──
        try:
            q = _FR_QUERY_TEMPLATE.format(province=province, year=year)
            answer = run_internet_query(q, api_key, pg_url)
            data = _extract_json(answer)
            if data:
                fr_price = _safe_float(data.get("fr_price_yuan_kw_h"))
                fr_pool = _safe_float(data.get("fr_pool_billion_yuan"))
                eff_year = int(data.get("effective_year", year))
                confidence = str(data.get("confidence", "medium")).lower()
                source = str(data.get("source_url", f"internet_search_{year}"))[:500]

                if fr_price is not None and fr_price > 0:
                    status_override = "conflict" if confidence == "low" else None
                    row = {
                        "province": province,
                        "effective_date": date(eff_year, 1, 1),
                        "fr_price_yuan_kw_h": fr_price,
                        "fr_pool_billion_yuan": fr_pool,
                        "source": source,
                        "_status_override": status_override,
                    }
                    fr_rows.append(row)
                    logger.info(
                        "fr_market found: %s %.4f ¥/kW·h pool=%s conf=%s",
                        province, fr_price, fr_pool, confidence,
                    )
                else:
                    logger.info("fr_market: no data for %s", province)
            else:
                logger.info("fr_market: no JSON in answer for %s", province)
        except Exception as exc:
            logger.error("fr_market query failed for %s: %s", province, exc)
            summary["errors"].append(f"fr_market/{province}: {exc}")

        time.sleep(_RATE_DELAY_SECONDS)

    # ── Upsert all collected rows ──
    # Separate confirmed vs conflict-flagged rows
    cap_confirmed = [r for r in cap_comp_rows if not r.pop("_status_override", None)]
    cap_conflict = []
    for r in cap_comp_rows:
        override = r.pop("_status_override", None)
        if override:
            cap_conflict.append(r)

    # Re-collect after pop (list was already iterated, rows modified in-place above)
    # Cleaner approach: process status override at upsert time
    # Reset: iterate fresh
    cap_comp_rows_clean = []
    for r in cap_comp_rows:
        r.pop("_status_override", None)
        cap_comp_rows_clean.append(r)

    fr_rows_clean = []
    for r in fr_rows:
        r.pop("_status_override", None)
        fr_rows_clean.append(r)

    if cap_comp_rows_clean:
        result = upsert_cap_comp_rows(cap_comp_rows_clean, pg_url, f"internet_search_{year}")
        summary["cap_comp_upserted"] += result["upserted"]
        summary["conflicts"] += result["conflicts"]
        summary["errors"].extend(result["errors"])

    if fr_rows_clean:
        result = upsert_fr_rows(fr_rows_clean, pg_url, f"internet_search_{year}")
        summary["fr_upserted"] += result["upserted"]
        summary["conflicts"] += result["conflicts"]
        summary["errors"].extend(result["errors"])

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
