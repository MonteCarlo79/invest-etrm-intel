"""
BESS Installed Capacity Web Screener
=====================================
Monthly scan of public Chinese sources (NEA, CEC, CNESA, 北极星储能) for
provincial BESS (electrochemical) and pumped-hydro installed capacity data.

Extracted data is upserted into province_installed_monthly via capacity_etl.

Entry point: screen_installed_capacity(pg_url, api_key, feishu, owner_open_id)
Schedule:    1st of each month at 10:00 UTC (18:00 Beijing)
Manual:      /capacity command in Feishu / Telegram
HTTP:        POST /hermes/capacity/scan
"""
from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime, timezone
from typing import Optional

import requests

from services.hermes.capacity_etl import upsert_capacity_rows

logger = logging.getLogger(__name__)

# ── Known public sources ───────────────────────────────────────────────────────
# Each entry: index_url is the listing/landing page; link_keywords used to find
# the most relevant recent link on that page; fallback_url is tried directly if
# link discovery yields nothing.

_SOURCES: list[dict] = [
    {
        "name": "国家能源局-电力工业统计快报",
        "index_url": "http://www.nea.gov.cn/jjyytj/index.htm",
        "link_keywords": ["电力工业统计快报", "电力统计快报", "月全国电力"],
        "fallback_url": None,
        "description": "NEA monthly electricity industry statistics (national + provincial capacity)",
    },
    {
        "name": "中国电力企业联合会-统计快报",
        "index_url": "https://www.cec.org.cn/detail/index.html?3-328052",
        "link_keywords": ["统计快报", "全国电力装机", "新能源装机", "储能装机"],
        "fallback_url": "https://www.cec.org.cn/category/electric/power-industry/",
        "description": "CEC monthly electricity statistics bulletin",
    },
    {
        "name": "CNESA-中国储能网",
        "index_url": "https://www.cnesa.org/storage-market",
        "link_keywords": ["装机", "省", "储能", "GW", "MW", "全国"],
        "fallback_url": "https://www.cnesa.org/news",
        "description": "CNESA China Energy Storage Alliance market reports",
    },
    {
        "name": "北极星储能网",
        "index_url": "https://chuneng.bjx.com.cn/news/",
        "link_keywords": ["装机", "储能容量", "各省", "省级", "GW", "MW"],
        "fallback_url": None,
        "description": "Bjx energy storage capacity news portal",
    },
    {
        "name": "国家能源局-新能源消纳监测",
        "index_url": "http://www.nrec.guodian.com.cn/xnyxnzl/index.html",
        "link_keywords": ["储能", "装机", "省", "月报", "季报"],
        "fallback_url": None,
        "description": "National renewable energy accommodation monitoring center",
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

_FETCH_TIMEOUT = 20  # seconds


# ── Fetch helpers ─────────────────────────────────────────────────────────────

def _fetch(url: str) -> Optional[str]:
    """Fetch URL, return cleaned text. Returns None on failure."""
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=_FETCH_TIMEOUT, allow_redirects=True)
        resp.raise_for_status()
        resp.encoding = resp.apparent_encoding or "utf-8"
        text = resp.text
        # Strip scripts, styles, comments for cleaner LLM input
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
    """Find the most relevant link on an index page matching any keyword."""
    # Extract all href links with their surrounding text
    pattern = re.compile(r'href=["\']([^"\']+)["\'][^>]*>([^<]{0,120})', re.I)
    candidates: list[tuple[int, str]] = []  # (score, url)
    for m in pattern.finditer(index_html):
        href, anchor = m.group(1), m.group(2)
        score = sum(1 for kw in keywords if kw in anchor or kw in href)
        if score > 0:
            # Make absolute URL
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


# ── LLM extraction ────────────────────────────────────────────────────────────

_EXTRACT_PROMPT = """\
You are a data extraction assistant for China's electricity market.

Extract ALL provincial/regional BESS and pumped-hydro storage installed capacity data from the text below.

Sources may report:
- 电化学储能 (electrochemical BESS) → use field "bess_mw"
- 抽水蓄能 (pumped hydro storage) → use field "hydro_mw"
- 新型储能 (new-type storage, mostly electrochemical) → use field "bess_mw"

Unit conversion rules:
- 万千瓦 or 万kW → multiply by 10 to get MW
- GW → multiply by 1000 to get MW
- MW → use directly

Return ONLY a valid JSON object:
{
  "year_month": "YYYY-MM-01",   // best guess at the data period; null if unknown
  "rows": [
    {"province": "省份名称", "bess_mw": 数值或null, "hydro_mw": 数值或null}
  ]
}

Rules:
- province: Chinese name exactly as in the text (e.g. 新疆, 内蒙古, 广东)
- Skip national totals (全国合计, 全国), only keep province/region rows
- If only one storage type is mentioned, set the other to null
- If no provincial breakdown found, return {"year_month": null, "rows": []}
- Do NOT invent numbers — only extract what is explicitly stated
"""


def _extract_capacity(text: str, source_name: str, api_key: str) -> tuple[Optional[date], list[dict]]:
    """Use Claude Haiku to extract provincial capacity rows from page text.

    Returns (year_month, rows).
    """
    from anthropic import Anthropic
    client = Anthropic(api_key=api_key)

    try:
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1500,
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


# ── Main screener ─────────────────────────────────────────────────────────────

def screen_installed_capacity(
    pg_url: str,
    api_key: str,
    feishu=None,
    owner_open_id: str = "",
) -> dict:
    """
    Scan all known sources, extract provincial BESS + hydro capacity, upsert to DB.

    Returns summary dict with per-source results.
    """
    if not pg_url or not api_key:
        logger.error("screen_installed_capacity: pg_url or api_key missing")
        return {"error": "pg_url or api_key missing"}

    # Default year_month: current month
    today = datetime.now(timezone.utc).date()
    default_ym = date(today.year, today.month, 1)

    results: list[dict] = []
    total_upserted = 0

    for src in _SOURCES:
        src_name = src["name"]
        logger.info("Capacity screener: scanning %s", src_name)

        # Step 1: fetch index page
        index_text = _fetch(src["index_url"])

        # Step 2: try to find a better/deeper link
        target_url = src["index_url"]
        if index_text and src.get("link_keywords"):
            deeper = _find_best_link(index_text, src["index_url"], src["link_keywords"])
            if deeper and deeper != src["index_url"]:
                logger.info("  → following link: %s", deeper)
                page_text = _fetch(deeper)
                if page_text:
                    index_text = page_text
                    target_url = deeper

        if not index_text:
            if src.get("fallback_url"):
                index_text = _fetch(src["fallback_url"])
                target_url = src["fallback_url"] or target_url
        if not index_text:
            results.append({"source": src_name, "status": "fetch_failed", "upserted": 0})
            continue

        # Step 3: extract capacity data with Claude
        year_month, rows = _extract_capacity(index_text, src_name, api_key)
        if not rows:
            results.append({"source": src_name, "status": "no_data", "upserted": 0, "url": target_url})
            logger.info("  → no provincial data extracted from %s", src_name)
            continue

        # Step 4: upsert
        ym = year_month or default_ym
        upsert_result = upsert_capacity_rows(rows, pg_url, src_name, ym)
        n = upsert_result["upserted"]
        total_upserted += n
        results.append({
            "source":    src_name,
            "status":    "ok",
            "upserted":  n,
            "provinces": upsert_result["provinces"],
            "year_month": str(ym),
            "errors":    upsert_result["errors"],
            "url":       target_url,
        })
        logger.info("  → upserted %d provinces from %s (%s)", n, src_name, ym)

    # ── Build Feishu digest ───────────────────────────────────────────────────
    if feishu and owner_open_id:
        _send_digest(feishu, owner_open_id, results, total_upserted, default_ym)

    return {"sources_scanned": len(_SOURCES), "total_upserted": total_upserted, "results": results}


def _send_digest(feishu, owner_open_id: str, results: list[dict], total: int, ym: date) -> None:
    lines = [f"📊 装机容量月度扫描完成 ({ym.strftime('%Y年%m月')})", f"共更新 {total} 条省级装机数据\n"]
    for r in results:
        status_icon = "✅" if r.get("status") == "ok" else ("⚠️" if r.get("status") == "no_data" else "❌")
        n = r.get("upserted", 0)
        src = r.get("source", "?")
        if r.get("status") == "ok":
            provs = "、".join(r.get("provinces", [])[:6])
            if len(r.get("provinces", [])) > 6:
                provs += f"等{len(r['provinces'])}省"
            lines.append(f"{status_icon} {src}：{n}省 — {provs}")
        elif r.get("status") == "no_data":
            lines.append(f"{status_icon} {src}：未找到省级数据")
        else:
            lines.append(f"{status_icon} {src}：获取失败")

    if total == 0:
        lines.append("\n💡 提示：如需手动上传装机数据，请发送命名为 各省储能装机容量_YYYYMM.xlsx 的文件")

    try:
        feishu.send_text(open_id=owner_open_id, text="\n".join(lines))
    except Exception as exc:
        logger.error("capacity screener digest send failed: %s", exc)
