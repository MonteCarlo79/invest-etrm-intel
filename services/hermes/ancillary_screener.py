"""
Ancillary (调频) BESS Revenue Screener
=======================================
Searches the knowledge base for 独立储能 frequency-regulation compensation
disclosures (exchange reports, e.g. 新疆 省内辅助服务市场运行情况), then uses
Claude to extract (province, month, amount 万元, metric).

Rows land as status='draft' in marketdata.province_ancillary_revenue pending
human confirmation in bess-map Data Management — the chart stacks confirmed
rows only. Idempotent on (province, month, metric, source_file).

Entry point:
    screen_ancillary_revenue(pg_url, api_key, feishu, owner_open_id, provinces)
"""
from __future__ import annotations

import logging
import re
import time
from typing import Optional

from services.ancillary_revenue.extract_ancillary import AncillaryRow, upsert_rows
from services.hermes.fuel_fleet_screener import _extract_json, _search_kb

logger = logging.getLogger(__name__)

_SEARCH_PROVINCES = [
    "山东", "山西", "蒙西", "广东", "甘肃", "江苏",
    "浙江", "河北南网", "冀北", "河南", "新疆",
    "吉林", "辽宁", "青海", "陕西", "湖北", "宁夏",
]

_ANCILLARY_KEYWORDS = ["独立储能补偿", "调频补偿", "辅助服务补偿", "调频里程"]

_RATE_DELAY_SECONDS = 1

EXTRACTION_PROMPT = """你是电力市场数据提取助手。目标省份：{province}。

从下面的资料中提取该省【独立储能/新型储能】的调频或辅助服务补偿收入（注意：必须是储能对应的补偿，不要火电/水电机组的数据，也不要只有全省合计而无储能细项的数据）。每一条记录给出：
- month（YYYY-MM-01，结算月份）
- amount_wanyuan（万元，数字；原文若以"元"为单位请除以10000）
- metric（原文指标名，如 调频补偿费用_独立储能 / 调频里程补偿_储能）

只输出一个 JSON 对象（不要 markdown）：
{{"rows": [{{"month": "YYYY-MM-01", "amount_wanyuan": float, "metric": "..."}}],
  "confidence": "high|medium|low"}}

资料（{n_chunks} 段）：
{context}"""

_EXTRACTION_SYSTEM = (
    "你是中国电力市场数据提取专家。从提供的文本中提取独立储能调频/辅助服务补偿金额。"
    "只提取明确出现在文本中的数据，不要猜测，不要全省合计数据。若文本无储能相关补偿数据，"
    "返回空 rows 数组。Respond ONLY with valid JSON, no other text."
)


def _claude_extract(province: str, kb_rows: list, api_key: str) -> Optional[dict]:
    if not kb_rows:
        return None
    context_parts, sources = [], set()
    for chunk_text, file_name in kb_rows[:8]:
        context_parts.append(chunk_text[:1200])
        if file_name:
            sources.add(file_name)
    context = "\n\n---\n\n".join(context_parts)
    source_hint = "; ".join(list(sources)[:3])

    user_msg = EXTRACTION_PROMPT.format(
        province=province, n_chunks=len(kb_rows), context=context,
    )
    try:
        from shared.anthropic_client import make_client as _make_anthropic_client
        client = _make_anthropic_client(api_key)
        resp = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            system=_EXTRACTION_SYSTEM,
            messages=[{"role": "user", "content": user_msg}],
        )
        data = _extract_json(resp.content[0].text.strip())
        if data is not None:
            data["_source_hint"] = source_hint
        return data
    except Exception as exc:
        logger.error("Claude extraction failed for %s: %s", province, exc)
        return None


_MONTH_RE = re.compile(r"^\d{4}-\d{2}(-\d{2})?$")


def _to_ancillary_rows(province: str, data: dict) -> list[AncillaryRow]:
    """Validate + convert Claude's extraction to AncillaryRow objects."""
    out: list[AncillaryRow] = []
    data = data or {}
    source = str(data.get("_source_hint", ""))[:200]
    for item in data.get("rows") or []:
        month = str(item.get("month", ""))[:10]
        if not _MONTH_RE.match(month):
            continue
        month = f"{month[:7]}-01"
        try:
            amt = float(item.get("amount_wanyuan"))
        except (TypeError, ValueError):
            continue
        if amt <= 0 or amt > 1e5:      # sanity: >10亿/月 for BESS FR is implausible
            continue
        metric = str(item.get("metric") or "调频补偿费用_独立储能")[:60]
        out.append(AncillaryRow(province=province, month=month, metric=metric,
                                amount_yuan=amt * 1e4, source_file=source))
    return out


def screen_ancillary_revenue(
    pg_url: str,
    api_key: str,
    feishu=None,
    owner_open_id: str = "",
    provinces: Optional[list] = None,
) -> dict:
    """Loop provinces: KB search → Claude extraction → draft upsert.

    Returns summary dict: {scanned, extracted, upserted, errors}."""
    provinces = provinces or _SEARCH_PROVINCES
    summary = {"scanned": 0, "extracted": 0, "upserted": 0, "errors": []}

    for province in provinces:
        summary["scanned"] += 1
        try:
            kb_rows = _search_kb(province, _ANCILLARY_KEYWORDS, pg_url)
            logger.info("ancillary KB: %s → %d chunks", province, len(kb_rows))
            data = _claude_extract(province, kb_rows, api_key)
            rows = _to_ancillary_rows(province, data)
            if rows:
                summary["extracted"] += 1
                summary["upserted"] += upsert_rows(rows, pg_url)
                logger.info("ancillary upserted: %s rows=%d conf=%s",
                            province, len(rows), (data or {}).get("confidence"))
            else:
                logger.info("ancillary: no usable extraction for %s", province)
        except Exception as exc:
            logger.error("ancillary failed for %s: %s", province, exc)
            summary["errors"].append(f"ancillary/{province}: {exc}")
        time.sleep(_RATE_DELAY_SECONDS)

    logger.info("ancillary_screener done: %s", summary)
    if feishu and owner_open_id:
        try:
            feishu.send_text(
                open_id=owner_open_id,
                text=f"🔋 调频收入扫描完成：{summary['extracted']}/{summary['scanned']} 省提取，"
                     f"{summary['upserted']} 条入库（draft，待确认）",
            )
        except Exception as exc:
            logger.warning("Feishu notify failed: %s", exc)
    return summary
