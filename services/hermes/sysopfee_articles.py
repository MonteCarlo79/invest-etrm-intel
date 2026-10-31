"""Extract 系统运行费 from 智慧光伏 monthly article URLs (full-resolution images).

The scroll-screenshot path failed validation (394px width, illegible digits).
The same articles embed each province's price table as a FULL-RESOLUTION image
on mmbiz.qpic.cn — which (unlike the article HTML) is not geo/DC blocked.

Flow per article URL:
  1. fetch article HTML (may hit the WeChat risk-control challenge from the
     NAT IP — caller must detect and report, never treat challenge as content)
  2. parse data-src image URLs (mmbiz.qpic.cn) + article month from title
  3. download each image, vision-extract {province, fee} (proven single-table
     path — May per-province files scored fixture-exact)
  4. upsert (province, article_month) into province_sysopfee_monthly

Run on ECS (Anthropic API is geo-blocked from the Mac):
    python -m services.hermes.sysopfee_articles --urls-file /tmp/urls.txt [--dry-run]
"""
from __future__ import annotations

import argparse
import logging
import re
import sys

import psycopg2
import requests

from services.hermes.sysopfee_extract import (
    _canon_province_name,
    _normalize_fee,
    _vision_extract_province_fee,
    upsert_sysopfee,
)

logger = logging.getLogger("sysopfee_articles")

_IMG_RE = re.compile(r'data-src="(https://mmbiz\.qpic\.cn/[^"]+)"')
_TITLE_RE = re.compile(r"<meta[^>]+property=\"og:title\"[^>]+content=\"([^\"]+)\"")
_MONTH_RE = re.compile(r"(20\d{2})\s*年\s*(\d{1,2})\s*月")

_UA = ("Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
       "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1")

_MIN_IMG_BYTES = 20_000  # province tables are big; icons/trackers are tiny


class ChallengePageError(Exception):
    """WeChat served the risk-control challenge instead of the article."""


def fetch_article(url: str) -> str:
    """Fetch article HTML. Raises ChallengePageError on 环境异常 pages."""
    r = requests.get(url, headers={"User-Agent": _UA}, timeout=30)
    r.raise_for_status()
    html = r.text
    if "环境异常" in html or "完成验证后即可继续访问" in html:
        raise ChallengePageError(url)
    return html


def parse_article(html: str) -> tuple[str | None, list[str]]:
    """Return (article_month 'YYYY-MM-01'|None, [image urls])."""
    title_m = _TITLE_RE.search(html)
    month = None
    if title_m:
        m = _MONTH_RE.search(title_m.group(1))
        if m:
            month = f"{m.group(1)}-{int(m.group(2)):02d}-01"
    urls = _IMG_RE.findall(html)
    return month, urls


def process_article(url: str, *, pg_url: str, api_key: str,
                    dry_run: bool = True) -> dict:
    """One article → per-province upserts. Returns a result summary."""
    html = fetch_article(url)
    month, img_urls = parse_article(html)
    if not month:
        logger.warning("no month in article title: %s", url[:80])
        return {"url": url, "month": None, "written": 0, "seen": 0}
    print(f"ARTICLE {url[:70]} → {month}, {len(img_urls)} images")

    session = requests.Session()
    session.trust_env = False  # qpic is direct-reachable; bypass proxy env
    session.headers["User-Agent"] = _UA

    seen = written = 0
    for u in img_urls:
        try:
            img = session.get(u, timeout=30).content
        except Exception as e:  # noqa: BLE001
            logger.warning("image fetch failed %s: %s", u[:60], e)
            continue
        if len(img) < _MIN_IMG_BYTES:
            continue
        try:
            province, fee = _vision_extract_province_fee(img, "image/jpeg", api_key)
        except Exception as e:  # noqa: BLE001
            logger.warning("vision failed %s: %s", u[:60], e)
            continue
        if not province or fee is None:
            continue
        seen += 1
        if dry_run:
            print(f"DRY {province}\t{month}\t{fee}")
        else:
            with psycopg2.connect(pg_url) as conn, conn.cursor() as cur:
                upsert_sysopfee(cur, province, month, fee, source_file=url[:200])
            written += 1
            print(f"OK {province}\t{month}\t{fee}")
    return {"url": url, "month": month, "seen": seen, "written": written}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--urls-file", default="",
                    help="one article URL per line (blank lines/# comments ok)")
    ap.add_argument("--images-dir", default="",
                    help="pre-downloaded images named <YYYY-MM>_<idx> (month from name)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    import os
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    pg_url = os.environ.get("PGURL") or os.environ.get("DATABASE_URL")
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("FATAL: ANTHROPIC_API_KEY not set", file=sys.stderr)
        return 2

    if args.images_dir:
        return _main_images_dir(args.images_dir, pg_url=pg_url, api_key=api_key,
                                dry_run=args.dry_run)

    urls = [l.strip() for l in open(args.urls_file, encoding="utf-8")
            if l.strip() and not l.startswith("#")]
    results = []
    for url in urls:
        try:
            results.append(process_article(url, pg_url=pg_url, api_key=api_key,
                                           dry_run=args.dry_run))
        except ChallengePageError:
            print(f"CHALLENGE {url[:70]} — WeChat risk-control page; "
                  f"need proxy or manual image save")
        except Exception as e:  # noqa: BLE001
            logger.error("FAIL %s: %s", url[:70], e)
    total_w = sum(r["written"] for r in results)
    total_s = sum(r["seen"] for r in results)
    print(f"DONE articles={len(results)} seen={total_s} written={total_w} "
          f"dry_run={args.dry_run}")
    return 0


def _main_images_dir(images_dir: str, *, pg_url: str, api_key: str,
                     dry_run: bool) -> int:
    """Process pre-downloaded article images (name = <YYYY-MM>_<NNN>)."""
    import os
    from pathlib import Path

    files = sorted(
        p for p in Path(images_dir).iterdir()
        if re.match(r"20\d{2}-\d{2}_\d+$", p.name)
    )
    print(f"IMAGES {len(files)} in {images_dir}")
    seen = written = skipped = 0
    for f in files:
        month = f.name[:7] + "-01"
        data = f.read_bytes()
        if len(data) < _MIN_IMG_BYTES:
            skipped += 1
            continue
        try:
            province, fee = _vision_extract_province_fee(data, "image/jpeg", api_key)
        except Exception as e:  # noqa: BLE001
            logger.warning("vision failed %s: %s", f.name, e)
            continue
        if not province or fee is None:
            skipped += 1
            continue
        seen += 1
        if dry_run:
            print(f"DRY {province}\t{month}\t{fee}\t{f.name}")
        else:
            with psycopg2.connect(pg_url) as conn, conn.cursor() as cur:
                upsert_sysopfee(cur, province, month, fee,
                                source_file=f"article-images/{f.name}")
            written += 1
            print(f"OK {province}\t{month}\t{fee}\t{f.name}")
    print(f"DONE images seen={seen} written={written} skipped={skipped} "
          f"dry_run={dry_run}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
