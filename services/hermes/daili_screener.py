"""
Province 代理购电信息 Monthly Screener
========================================
Scrapes provincial grid company websites for new 代理购电价格公示 data each month.
Downloads Excel files and runs daili_etl to upsert into province_sysopfee_monthly.

Saves files to:
  {LOCAL_DATA_DIR}/{province}/{year}年/{filename}

Entry point: screen_daili(pg_url, feishu, owner_open_id)
Schedule:    5th of each month at 10:00 UTC (18:00 Beijing)
Manual:      /daili command in Feishu / Telegram
HTTP:        POST /hermes/daili/scan
"""
from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, urljoin

import requests

from services.hermes.daili_etl import parse_daili_file, upsert_sysopfee_rows

logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

# Local data directory (OneDrive path on the host)
_LOCAL_DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "data", "raw", "各省电网购电信息",
)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9",
}
_FETCH_TIMEOUT = 25
_DOWNLOAD_TIMEOUT = 60

# Keywords to identify 代理购电 announcement pages
_PAGE_KEYWORDS = ["代理购电", "购电价格", "购电公示", "代购电价格", "代理采购"]
_FILE_KEYWORDS = ["代理购电", "购电价格", "购电数据", "购电月度", "购电公示"]

# ── Province → grid company website mapping ───────────────────────────────────
# Format: {province_name: (index_url, announcement_path_hint)}
# 国网 provinces: {abbrev}.sgcc.com.cn
# 南方电网 provinces: {abbrev}.csg.cn

_PROVINCE_SOURCES: dict[str, list[dict]] = {
    # ── 国网 (SGCC) provinces ──
    "北京": [{"url": "https://www.bjdl.com.cn/ywzl/dlszywzx/", "name": "国网北京"}],
    "天津": [{"url": "https://www.tj.sgcc.com.cn/html/main/col916/column_916_1.html", "name": "国网天津"}],
    "冀北": [{"url": "https://www.jibei.sgcc.com.cn/ywzl/dlszywzx/", "name": "国网冀北"}],
    "河北": [{"url": "https://www.hb.sgcc.com.cn/html/main/col2476/column_2476_1.html", "name": "国网河北"}],
    "山西": [{"url": "https://www.shanxi.sgcc.com.cn/html/main/col875/column_875_1.html", "name": "国网山西"}],
    "蒙西": [{"url": "https://www.nm.sgcc.com.cn/html/main/col916/column_916_1.html", "name": "国网内蒙古"}],
    "辽宁": [{"url": "https://www.ln.sgcc.com.cn/html/main/col875/column_875_1.html", "name": "国网辽宁"}],
    "吉林": [{"url": "https://www.jl.sgcc.com.cn/html/main/col875/column_875_1.html", "name": "国网吉林"}],
    "黑龙江": [{"url": "https://www.hlj.sgcc.com.cn/html/main/col875/column_875_1.html", "name": "国网黑龙江"}],
    "上海": [{"url": "https://www.sh.sgcc.com.cn/html/main/col2511/column_2511_1.html", "name": "国网上海"}],
    "江苏": [{"url": "https://www.js.sgcc.com.cn/html/main/col875/column_875_1.html", "name": "国网江苏"}],
    "浙江": [{"url": "https://www.zj.sgcc.com.cn/html/main/col875/column_875_1.html", "name": "国网浙江"}],
    "安徽": [{"url": "https://www.ah.sgcc.com.cn/html/main/col875/column_875_1.html", "name": "国网安徽"}],
    "福建": [{"url": "https://www.fj.sgcc.com.cn/html/main/col875/column_875_1.html", "name": "国网福建"}],
    "江西": [{"url": "https://www.jx.sgcc.com.cn/html/main/col875/column_875_1.html", "name": "国网江西"}],
    "山东": [{"url": "https://www.sd.sgcc.com.cn/html/main/col875/column_875_1.html", "name": "国网山东"}],
    "河南": [{"url": "https://www.ha.sgcc.com.cn/html/main/col875/column_875_1.html", "name": "国网河南"}],
    "湖北": [{"url": "https://www.hb.sgcc.com.cn/html/main/col875/column_875_1.html", "name": "国网湖北"}],
    "湖南": [{"url": "https://www.hn.sgcc.com.cn/html/main/col875/column_875_1.html", "name": "国网湖南"}],
    "四川": [{"url": "https://www.sc.sgcc.com.cn/html/main/col875/column_875_1.html", "name": "国网四川"}],
    "重庆": [{"url": "https://www.cq.sgcc.com.cn/html/main/col875/column_875_1.html", "name": "国网重庆"}],
    "陕西": [{"url": "https://www.sn.sgcc.com.cn/html/main/col875/column_875_1.html", "name": "国网陕西"}],
    "甘肃": [{"url": "https://www.gs.sgcc.com.cn/html/main/col875/column_875_1.html", "name": "国网甘肃"}],
    "青海": [{"url": "https://www.qh.sgcc.com.cn/html/main/col875/column_875_1.html", "name": "国网青海"}],
    "宁夏": [{"url": "https://www.nx.sgcc.com.cn/html/main/col875/column_875_1.html", "name": "国网宁夏"}],
    "新疆": [{"url": "https://www.xj.sgcc.com.cn/html/main/col875/column_875_1.html", "name": "国网新疆"}],
    # ── 南方电网 (CSG) provinces ──
    "广东": [{"url": "https://www.gd.csg.cn/dlszyw/dlsz/", "name": "南网广东"}],
    "广西": [{"url": "https://www.gx.csg.cn/dlszyw/", "name": "南网广西"}],
    "贵州": [{"url": "https://www.gz.csg.cn/dlszyw/", "name": "南网贵州"}],
    "云南": [{"url": "https://www.yn.csg.cn/dlszyw/", "name": "南网云南"}],
    "海南": [{"url": "https://www.hi.csg.cn/dlszyw/", "name": "南网海南"}],
}


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def _fetch_html(url: str) -> Optional[str]:
    """Fetch URL, strip scripts/styles, return plain text (up to 10KB)."""
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=_FETCH_TIMEOUT, allow_redirects=True)
        resp.raise_for_status()
        resp.encoding = resp.apparent_encoding or "utf-8"
        text = resp.text
        text = re.sub(r'<script[^>]*>.*?</script>', ' ', text, flags=re.DOTALL | re.I)
        text = re.sub(r'<style[^>]*>.*?</style>', ' ', text, flags=re.DOTALL | re.I)
        text = re.sub(r'<!--.*?-->', ' ', text, flags=re.DOTALL)
        # Keep hrefs + anchor text
        return text[:10000]
    except Exception as exc:
        logger.debug("Fetch failed %s: %s", url, exc)
        return None


def _find_links(html: str, base_url: str, keywords: list[str]) -> list[tuple[int, str, str]]:
    """
    Extract links from html matching any keyword.
    Returns sorted list of (score, url, anchor_text).
    """
    href_pat = re.compile(r'href=["\']([^"\']+)["\'][^>]*>([^<]{0,200})', re.I)
    results: list[tuple[int, str, str]] = []
    parsed_base = urlparse(base_url)
    for m in href_pat.finditer(html):
        href, anchor = m.group(1).strip(), m.group(2).strip()
        if not href or href.startswith('#') or href.startswith('javascript'):
            continue
        score = sum(1 for kw in keywords if kw in anchor or kw in href)
        if score == 0:
            continue
        if href.startswith("http"):
            full_url = href
        elif href.startswith("//"):
            full_url = f"{parsed_base.scheme}:{href}"
        elif href.startswith("/"):
            full_url = f"{parsed_base.scheme}://{parsed_base.netloc}{href}"
        else:
            full_url = urljoin(base_url, href)
        results.append((score, full_url, anchor))
    results.sort(key=lambda x: -x[0])
    return results


def _download_file(url: str) -> Optional[bytes]:
    """Download binary file, return bytes or None."""
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=_DOWNLOAD_TIMEOUT, allow_redirects=True)
        resp.raise_for_status()
        return resp.content
    except Exception as exc:
        logger.debug("Download failed %s: %s", url, exc)
        return None


# ── File save / ETL ───────────────────────────────────────────────────────────

def _save_and_upsert(
    file_bytes: bytes,
    filename: str,
    province: str,
    pg_url: str,
) -> dict:
    """Save file to local data dir and run daili_etl."""
    today = datetime.now(timezone.utc)
    year = today.year

    # Determine year subfolder: try to extract from filename first
    m = re.search(r'(20\d{2})', filename)
    if m:
        year = int(m.group(1))

    save_dir = Path(_LOCAL_DATA_DIR) / province / f"{year}年"
    save_dir.mkdir(parents=True, exist_ok=True)
    save_path = save_dir / filename

    # Avoid overwrite if identical
    if save_path.exists():
        existing = save_path.read_bytes()
        if existing == file_bytes:
            logger.info("  → %s already exists and is identical, skipping save", filename)
        else:
            save_path.write_bytes(file_bytes)
            logger.info("  → updated %s", save_path)
    else:
        save_path.write_bytes(file_bytes)
        logger.info("  → saved %s", save_path)

    # Run ETL on the saved file
    rows = parse_daili_file(str(save_path))
    if not rows:
        return {"upserted": 0, "errors": [f"No data extracted from {filename}"]}

    result = upsert_sysopfee_rows(rows, pg_url, filename)
    return result


# ── Province scan ─────────────────────────────────────────────────────────────

def _scan_province(province: str, source: dict, pg_url: str) -> dict:
    """Scan one provincial website for new 代理购电 Excel files."""
    base_url = source["url"]
    src_name = source["name"]
    logger.info("daili screener: scanning %s (%s)", province, base_url)

    html = _fetch_html(base_url)
    if not html:
        return {"province": province, "status": "fetch_failed", "upserted": 0}

    # Look for links to 代理购电 announcement pages or directly to Excel files
    links = _find_links(html, base_url, _PAGE_KEYWORDS)

    if not links:
        return {"province": province, "status": "no_links", "upserted": 0}

    total_upserted = 0
    downloaded = []

    for score, url, anchor in links[:5]:  # Check top 5 candidate links
        # Check if it's directly an Excel file
        if url.lower().endswith((".xlsx", ".xls")):
            filename = url.split("/")[-1] or f"{province}_daili.xlsx"
            logger.info("  → direct Excel link: %s", url)
            file_bytes = _download_file(url)
            if file_bytes:
                result = _save_and_upsert(file_bytes, filename, province, pg_url)
                total_upserted += result["upserted"]
                downloaded.append(filename)
            continue

        # Follow link to a sub-page and look for Excel files there
        sub_html = _fetch_html(url)
        if not sub_html:
            continue

        sub_links = _find_links(sub_html, url, _FILE_KEYWORDS)
        for _, file_url, file_anchor in sub_links[:10]:
            if not file_url.lower().endswith((".xlsx", ".xls")):
                continue
            filename = file_url.split("/")[-1].split("?")[0]
            if not filename:
                filename = f"{province}_daili.xlsx"
            # Decode URL-encoded Chinese characters
            try:
                from urllib.parse import unquote
                filename = unquote(filename)
            except Exception:
                pass
            logger.info("  → found Excel: %s", file_url)
            file_bytes = _download_file(file_url)
            if file_bytes and len(file_bytes) > 5000:  # Skip tiny/empty downloads
                result = _save_and_upsert(file_bytes, filename, province, pg_url)
                total_upserted += result["upserted"]
                downloaded.append(filename)
                break  # Take first valid Excel per sub-page

        if downloaded:
            break  # Stop once we've found at least one file

    if not downloaded:
        return {
            "province": province,
            "status": "no_files_found",
            "upserted": 0,
            "url": base_url,
        }

    return {
        "province": province,
        "status": "ok",
        "upserted": total_upserted,
        "files": downloaded,
        "url": base_url,
    }


# ── Main entry point ──────────────────────────────────────────────────────────

def screen_daili(
    pg_url: str,
    feishu=None,
    owner_open_id: str = "",
) -> dict:
    """
    Scan all provincial grid company websites for new 代理购电 data.
    Downloads Excel files, runs ETL, upserts to province_sysopfee_monthly.
    """
    if not pg_url:
        return {"error": "pg_url missing"}

    today = datetime.now(timezone.utc)
    results: list[dict] = []
    total_upserted = 0

    for province, sources in _PROVINCE_SOURCES.items():
        for source in sources:
            r = _scan_province(province, source, pg_url)
            results.append(r)
            total_upserted += r.get("upserted", 0)

    logger.info("daili screener complete: %d provinces, %d rows upserted",
                len(_PROVINCE_SOURCES), total_upserted)

    if feishu and owner_open_id:
        _send_digest(feishu, owner_open_id, results, total_upserted, today)

    return {
        "provinces_scanned": len(_PROVINCE_SOURCES),
        "total_upserted": total_upserted,
        "results": results,
    }


def _send_digest(feishu, owner_open_id: str, results: list[dict], total: int, dt: datetime) -> None:
    ym_str = dt.strftime("%Y年%m月")
    lines = [
        f"📋 代理购电月度数据扫描完成 ({ym_str})",
        f"共更新 {total} 条省级月度数据\n",
    ]
    ok = [r for r in results if r.get("status") == "ok"]
    no_file = [r for r in results if r.get("status") == "no_files_found"]
    failed = [r for r in results if r.get("status") in ("fetch_failed", "no_links")]

    for r in ok:
        lines.append(f"✅ {r['province']}：{r.get('upserted', 0)}条")
    for r in no_file:
        lines.append(f"⚠️ {r['province']}：未找到Excel文件")
    for r in failed:
        lines.append(f"❌ {r['province']}：网站访问失败")

    if total == 0:
        lines.append(
            "\n💡 提示：如需手动更新，请上传命名含 '代理购电' 的Excel文件"
        )
    try:
        feishu.send_text(open_id=owner_open_id, text="\n".join(lines))
    except Exception as exc:
        logger.error("daili screener digest send failed: %s", exc)
