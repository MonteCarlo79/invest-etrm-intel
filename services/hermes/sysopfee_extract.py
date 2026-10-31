"""系统运行费 extraction from 代购价格 sources (backfill + article automation).

Two source shapes, one writer:
- extract_from_image_bytes(img_bytes) -> float|None : Claude vision reads the
  per-province 代理购电价格表 and returns 系统运行费用折合度电水平 (元/kWh).
- extract_from_pdf_bytes(pdf_bytes) -> float|None : text-layer first (the row is
  plain text in official PDFs), vision fallback via pdfplumber page render.
- upsert_sysopfee(province, year_month, fee_yuan_kwh, source_file) : idempotent
  upsert into province_sysopfee_monthly (the 系统运行费 tab's table).

Runs on ECS (hermes) — LLM calls are ECS-only by design (Anthropic geo-block
from China Mac). The vision model is claude-sonnet-4-6 via shared.anthropic_client.
"""
from __future__ import annotations

import base64
import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)

_PROMPT = (
    "这是一张中国省级电网代理购电价格公告的表格图片。请找到价格明细表中"
    "「系统运行费用折合度电水平」这一行（也可能写作「系统运行费折合度电水平」"
    "或「系统运行费」），返回其数值。\n"
    "注意单位：有的省份用「元/千瓦时」，有的用「分/千瓦时」。"
    "如果是分/千瓦时，请除以100换算成元/千瓦时后再输出。\n"
    "只输出一个数字（如 0.0357），不要任何其他文字。找不到就输出 NONE。"
)

# Text-layer patterns for PDFs (no vision needed when text exists)
_TEXT_PATTERNS = [
    re.compile(r"系统运行费[用]?折合度电水平[^\d\-]*(-?\d+\.?\d*)"),
    re.compile(r"系统运行费[用]?[^\d\-]{0,10}(-?\d+\.\d{3,6})"),
]


_MAX_IMAGE_BYTES = 4_500_000  # API hard limit is 5 MB; leave headroom


def _sniff_media_type(data: bytes) -> str:
    """File extensions lie (a '.jpg' may be WebP). Sniff magic bytes."""
    if data[:4] == b"\x89PNG":
        return "image/png"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    if data[:2] == b"\xff\xd8":
        return "image/jpeg"
    if data[:6] in (b"GIF87a", b"GIF89a"):
        return "image/gif"
    return "image/jpeg"


def _shrink_image(image_bytes: bytes) -> bytes:
    """Re-encode to fit under the vision API's 5 MB limit (high-res phone photos
    of price tables arrive at 6-10 MB). Returns input unchanged if small enough."""
    if len(image_bytes) <= _MAX_IMAGE_BYTES:
        return image_bytes
    import io

    from PIL import Image
    img = Image.open(io.BytesIO(image_bytes))
    if max(img.size) > 2400:
        img.thumbnail((2400, 2400))
    buf = io.BytesIO()
    img.convert("RGB").save(buf, format="JPEG", quality=85)
    return buf.getvalue()


def _vision_extract(image_bytes: bytes, media_type: str, api_key: str) -> Optional[float]:
    from shared.anthropic_client import make_client
    image_bytes = _shrink_image(image_bytes)
    media_type = _sniff_media_type(image_bytes)  # extensions lie; magic bytes don't
    client = make_client(api_key)
    resp = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=64,
        messages=[{
            "role": "user",
            "content": [
                {"type": "image",
                 "source": {"type": "base64", "media_type": media_type,
                            "data": base64.standard_b64encode(image_bytes).decode()}},
                {"type": "text", "text": _PROMPT},
            ],
        }],
    )
    text = resp.content[0].text.strip()
    if text == "NONE":
        return None
    m = re.search(r"-?\d+\.?\d*", text)
    return _normalize_fee(float(m.group(0))) if m else None


def _normalize_fee(fee: float) -> float:
    """Guard against 分/千瓦时 tables: no province's 系统运行费 exceeds 1 元/kWh
    (observed range 0.02–0.15); a value > 1 means the table was in 分."""
    return fee / 100.0 if fee > 1.0 else fee


def extract_from_image_bytes(image_bytes: bytes, *, media_type: str = "image/jpeg",
                             api_key: str) -> Optional[float]:
    """Vision-extract 系统运行费用折合度电水平 (元/kWh) from a price-table image."""
    return _vision_extract(image_bytes, media_type, api_key)


def extract_from_pdf_bytes(pdf_bytes: bytes, *, api_key: str) -> Optional[float]:
    """Text-layer first; vision fallback via first-page render."""
    import io
    import pdfplumber
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        full = "\n".join((p.extract_text() or "") for p in pdf.pages)
    for pat in _TEXT_PATTERNS:
        m = pat.search(full)
        if m:
            return _normalize_fee(float(m.group(1)))  # 分/千瓦时 tables exist
    # vision fallback: render first page
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        img = pdf.pages[0].to_image(resolution=140).original
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return _vision_extract(buf.getvalue(), "image/png", api_key)


def upsert_sysopfee(cur, province: str, year_month: str, fee_yuan_kwh: float,
                    source_file: str) -> None:
    """Idempotent upsert into province_sysopfee_monthly (the 系统运行费 tab table)."""
    cur.execute(
        """
        INSERT INTO province_sysopfee_monthly
            (province, year_month, fee_yuan_kwh, source_file)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (province, year_month) DO UPDATE SET
            fee_yuan_kwh = EXCLUDED.fee_yuan_kwh,
            source_file = EXCLUDED.source_file,
            ingested_at = NOW()
        """,
        (province, year_month, fee_yuan_kwh, source_file),
    )


# ── Article-driven automation (daily 智慧光伏 scrape) ─────────────────────────
# Monthly 代理购电价格 announcement articles carry one price-table image per
# province. news_screener preserves <img> as [IMG:url] markers in the body
# (mmbiz.qpic.cn CDN — reachable from ECS without the WeChat proxy).
# For each image: vision-extract {province, fee} and upsert the article month.

_PROMPT_PROVINCE_FEE = (
    "这是一张中国省级电网代理购电价格公告的表格图片。请识别：\n"
    "1. 这是哪个省份/地区的公告（表头或标题通常包含省名）。省份用短名回答"
    "（如：山东、山西、蒙西、蒙东、冀北、深圳、新疆、甘肃；北京/上海/天津/重庆不要带「市」字）。\n"
    "2. 价格明细表中「系统运行费用折合度电水平」这一行（也可能写作"
    "「系统运行费折合度电水平」或「系统运行费」）的数值。\n"
    "注意单位：有的省份用「元/千瓦时」，有的用「分/千瓦时」。"
    "如果是分/千瓦时，请除以100换算成元/千瓦时后再输出。\n"
    "只输出 JSON：{\"province\": \"山东\", \"fee\": 0.0357}。"
    "找不到系统运行费数值就输出 {\"province\": \"...\", \"fee\": null}。不要输出任何其他文字。"
)

_IMG_MARKER_RE = re.compile(r"\[IMG:(https?://[^\]\s]+)\]")
_TITLE_MONTH_RE = re.compile(r"(20\d{2})\s*年\s*(\d{1,2})\s*月")
_ARTICLE_GATE_RE = re.compile(r"代理购电|代购电")
_QPIC_HOST_RE = re.compile(r"^https?://mmbiz\.q(pic|logo)\.cn/")

_MAX_IMAGES_PER_ARTICLE = 40
_MIN_IMAGE_BYTES = 15_000  # skip icons / dividers / QR codes

_STRIP_SUFFIXES = (
    "壮族自治区", "回族自治区", "维吾尔自治区", "特别行政区", "自治区", "省", "市",
)


def _canon_province_name(name: str) -> Optional[str]:
    """Normalize a model-returned province name to the platform short name."""
    n = (name or "").strip()
    for suf in _STRIP_SUFFIXES:
        if n.endswith(suf):
            n = n[: -len(suf)]
            break
    if n in {"内蒙古", ""}:  # ambiguous — must be 蒙西/蒙东
        return None
    return n


def _vision_extract_province_fee(image_bytes: bytes, media_type: str,
                                 api_key: str) -> tuple[Optional[str], Optional[float]]:
    """One vision call → (province_short_name, fee) or (None, None)."""
    import json

    from shared.anthropic_client import make_client
    image_bytes = _shrink_image(image_bytes)
    media_type = _sniff_media_type(image_bytes)
    client = make_client(api_key)
    resp = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=128,
        messages=[{
            "role": "user",
            "content": [
                {"type": "image",
                 "source": {"type": "base64", "media_type": media_type,
                            "data": base64.standard_b64encode(image_bytes).decode()}},
                {"type": "text", "text": _PROMPT_PROVINCE_FEE},
            ],
        }],
    )
    text = resp.content[0].text.strip()
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if not m:
        return None, None
    try:
        obj = json.loads(m.group(0))
    except json.JSONDecodeError:
        return None, None
    province = _canon_province_name(str(obj.get("province") or ""))
    fee = obj.get("fee")
    try:
        fee = _normalize_fee(float(fee)) if fee is not None else None
    except (TypeError, ValueError):
        fee = None
    return province, fee


def process_article_images(art: dict, *, pg_url: str, api_key: str) -> int:
    """If `art` is a monthly 代理购电价格 announcement, extract per-province
    系统运行费 from its chart images and upsert. Returns rows written (0 when
    the article is not a price announcement — the common case).
    """
    import psycopg2
    import requests

    title = art.get("title") or ""
    body = art.get("body") or ""
    if not body or not _ARTICLE_GATE_RE.search(title + "\n" + body[:2000]):
        return 0

    m = _TITLE_MONTH_RE.search(title)
    if not m:
        logger.info("sysopfee: %r gated on keywords but no 年月 in title — skip", title[:60])
        return 0
    year_month = f"{m.group(1)}-{int(m.group(2)):02d}-01"

    img_urls = [u for u in _IMG_MARKER_RE.findall(body) if _QPIC_HOST_RE.match(u)]
    if not img_urls:
        return 0
    img_urls = img_urls[:_MAX_IMAGES_PER_ARTICLE]

    session = requests.Session()
    session.trust_env = False  # qpic CDN is direct-reachable; ignore proxy env
    session.headers["User-Agent"] = "Mozilla/5.0"

    source_url = art.get("url", "")[:200]
    written = 0
    for url in img_urls:
        try:
            r = session.get(url, timeout=20)
            r.raise_for_status()
            data = r.content
            if len(data) < _MIN_IMAGE_BYTES:
                continue
            media_type = "image/png" if data[:4] == b"\x89PNG" else "image/jpeg"
            province, fee = _vision_extract_province_fee(data, media_type, api_key)
            if not province or fee is None:
                continue
            with psycopg2.connect(pg_url) as conn, conn.cursor() as cur:
                upsert_sysopfee(cur, province, year_month, fee,
                                source_file=source_url or "wechat-article")
            written += 1
            logger.info("sysopfee: %s %s = %.6f ← %s", province, year_month, fee, url[:60])
        except Exception as e:  # noqa: BLE001 — one bad image must not kill the run
            logger.warning("sysopfee: image failed (%s): %s", url[:60], e)
    if written:
        logger.info("sysopfee: %d provinces upserted for %s from %r",
                    written, year_month, title[:60])
    return written
