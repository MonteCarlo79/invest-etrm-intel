"""
Province System Operation Fee Web Screener
==========================================
Monthly scan of public Chinese sources for 系统运行费 (grid system operation fee)
data per province.

Extracted data is upserted into province_sysopfee_monthly via sysopfee_etl.

Entry point: screen_sysopfee(pg_url, api_key, feishu, owner_open_id)
Schedule:    1st of each month at 11:00 UTC (19:00 Beijing)
Manual:      /sysopfee command in Feishu / Telegram
HTTP:        POST /hermes/sysopfee/scan
"""
from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime, timezone
from typing import Optional

import requests

from services.hermes.sysopfee_etl import upsert_sysopfee_rows

logger = logging.getLogger(__name__)

_SOURCES: list[dict] = [
    {
        "name": "北极星电力网-系统运行费",
        "index_url": "https://www.bjx.com.cn/news/",
        "link_keywords": ["系统运行费", "辅助服务费", "各省", "省级", "元/kWh", "运行费"],
        "fallback_url": None,
        "description": "Bjx power news portal for system operation fee announcements",
    },
    {
        "name": "中国电力企业联合会",
        "index_url": "https://www.cec.org.cn/detail/index.html?3-328052",
        "link_keywords": ["系统运行费", "辅助服务", "各省", "统计快报"],
        "fallback_url": "https://www.cec.org.cn/category/electric/power-industry/",
        "description": "CEC electricity statistics including auxiliary service fees",
    },
    {
        "name": "南方能源监管局",
        "index_url": "http://nfj.nea.gov.cn/",
        "link_keywords": ["系统运行费", "辅助服务", "各省", "月报", "公告"],
        "fallback_url": None,
        "description": "Southern energy regulatory bureau — auxiliary service fee announcements",
    },
    {
        "name": "华北能源监管局",
        "index_url": "http://hbj.nea.gov.cn/",
        "link_keywords": ["系统运行费", "辅助服务", "各省", "月报"],
        "fallback_url": None,
        "description": "North China energy regulatory bureau — system operation fee data",
    },
]

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}
_FETCH_TIMEOUT = 20


def _fetch(url: str) -> Optional[str]:
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=_FETCH_TIMEOUT, allow_redirects=True)
        resp.raise_for_status()
        resp.encoding = resp.apparent_encoding or "utf-8"
        text = resp.text
        text = re.sub(r'<script[^>]*>.*?</script>', ' ', text, flags=re.DOTALL | re.I)
        text = re.sub(r'<style[^>]*>.*?</style>', ' ', text, flags=re.DOTALL | re.I)
        text = re.sub(r'<!--.*?-->', ' ', text, flags=re.DOTALL)
        text = re.sub(r'<[^>]+>', ' ', text)
        text = re.sub(r'\s{3,}', '\n', text)
        return text[:8000]
    except Exception as exc:
        logger.warning("Fetch failed for %s: %s", url, exc)
        return None


def _find_best_link(index_html: str, base_url: str, keywords: list[str]) -> Optional[str]:
    pattern = re.compile(r'href=["\']([^"\']+)["\'][^>]*>([^<]{0,120})', re.I)
    candidates: list[tuple[int, str]] = []
    for m in pattern.finditer(index_html):
        href, anchor = m.group(1), m.group(2)
        score = sum(1 for kw in keywords if kw in anchor or kw in href)
        if score > 0:
            if href.startswith("http"):
                full_url = href
            elif href.startswith("/"):
                from urllib.parse import urlparse
                parsed = urlparse(base_url)
                full_url = f"{parsed.scheme}://{parsed.netloc}{href}"
            else:
                full_url = base_url.rstrip("/") + "/" + href
            candidates.append((score, full_url))
    if not candidates:
        return None
    candidates.sort(key=lambda x: -x[0])
    return candidates[0][1]


_EXTRACT_PROMPT = """\
You are a data extraction assistant for China's electricity market.

Extract provincial 系统运行费 (grid system operation fee / 辅助服务费) data from the text.
This is a monthly fee published by provincial grid companies in yuan/kWh (元/千瓦时).

Return ONLY a valid JSON object:
{
  "year_month": "YYYY-MM-01",   // data period; null if unknown
  "rows": [
    {"province": "省份名称", "fee_yuan_kwh": 数值}
  ]
}

Rules:
- province: Chinese name exactly as in the text (e.g. 广东, 山西, 内蒙古)
- fee_yuan_kwh: numeric value in yuan/kWh (typical range 0.001–0.20)
- Skip national totals (全国合计, 全国), averages (均值), only keep province/region rows
- If no provincial breakdown found, return {"year_month": null, "rows": []}
- Do NOT invent numbers — only extract what is explicitly stated
"""


def _extract_fees(
    text: str, source_name: str, api_key: str
) -> tuple[Optional[date], list[dict]]:
    from anthropic import Anthropic
    client = Anthropic(api_key=api_key)
    try:
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1000,
            system=_EXTRACT_PROMPT,
            messages=[{
                "role": "user",
                "content": f"Source: {source_name}\n\nText:\n{text[:6000]}",
            }],
        )
        raw = resp.content[0].text.strip()
        m = re.search(r'\{.*\}', raw, re.DOTALL)
        if not m:
            return None, []
        data = json.loads(m.group(0))
        rows = data.get("rows", [])
        ym_str = data.get("year_month")
        year_month: Optional[date] = None
        if ym_str:
            try:
                year_month = date.fromisoformat(ym_str[:10])
            except ValueError:
                pass
        return year_month, rows
    except Exception as exc:
        logger.warning("LLM extraction failed for %s: %s", source_name, exc)
        return None, []


def screen_sysopfee(
    pg_url: str,
    api_key: str,
    feishu=None,
    owner_open_id: str = "",
) -> dict:
    """Scan all sources for provincial system operation fees, upsert to DB."""
    if not pg_url or not api_key:
        return {"error": "pg_url or api_key missing"}

    today = datetime.now(timezone.utc).date()
    default_ym = date(today.year, today.month, 1)

    results: list[dict] = []
    total_upserted = 0

    for src in _SOURCES:
        src_name = src["name"]
        logger.info("SysOpFee screener: scanning %s", src_name)

        index_text = _fetch(src["index_url"])
        target_url = src["index_url"]

        if index_text and src.get("link_keywords"):
            deeper = _find_best_link(index_text, src["index_url"], src["link_keywords"])
            if deeper and deeper != src["index_url"]:
                logger.info("  → following link: %s", deeper)
                page_text = _fetch(deeper)
                if page_text:
                    index_text = page_text
                    target_url = deeper

        if not index_text and src.get("fallback_url"):
            index_text = _fetch(src["fallback_url"])
            target_url = src["fallback_url"] or target_url

        if not index_text:
            results.append({"source": src_name, "status": "fetch_failed", "upserted": 0})
            continue

        year_month, rows = _extract_fees(index_text, src_name, api_key)
        if not rows:
            results.append({
                "source": src_name, "status": "no_data", "upserted": 0, "url": target_url,
            })
            logger.info("  → no provincial data extracted from %s", src_name)
            continue

        ym = year_month or default_ym
        for r in rows:
            r["year_month"] = ym

        result = upsert_sysopfee_rows(rows, pg_url, src_name)
        n = result["upserted"]
        total_upserted += n
        results.append({
            "source":     src_name,
            "status":     "ok",
            "upserted":   n,
            "year_month": str(ym),
            "errors":     result["errors"],
            "url":        target_url,
        })
        logger.info("  → upserted %d province-months from %s (%s)", n, src_name, ym)

    if feishu and owner_open_id:
        _send_digest(feishu, owner_open_id, results, total_upserted, default_ym)

    return {"sources_scanned": len(_SOURCES), "total_upserted": total_upserted, "results": results}


def _send_digest(
    feishu, owner_open_id: str, results: list[dict], total: int, ym: date
) -> None:
    lines = [
        f"📊 系统运行费月度扫描完成 ({ym.strftime('%Y年%m月')})",
        f"共更新 {total} 条省级数据\n",
    ]
    for r in results:
        icon = "✅" if r.get("status") == "ok" else ("⚠️" if r.get("status") == "no_data" else "❌")
        src = r.get("source", "?")
        if r.get("status") == "ok":
            lines.append(f"{icon} {src}：{r.get('upserted', 0)}条")
        elif r.get("status") == "no_data":
            lines.append(f"{icon} {src}：未找到省级数据")
        else:
            lines.append(f"{icon} {src}：获取失败")

    if total == 0:
        lines.append(
            "\n💡 提示：如需手动上传，请发送命名为 各省市电网系统运行费用_YYYYMM.xlsx 的文件"
        )
    try:
        feishu.send_text(open_id=owner_open_id, text="\n".join(lines))
    except Exception as exc:
        logger.error("sysopfee screener digest send failed: %s", exc)
