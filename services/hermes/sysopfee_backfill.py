"""Backfill 系统运行费 from downloaded 代理购电价格公告 files.

Walks data/raw/各省电网购电信息/<省份>/<年份>年/<月份>月份.(jpg|jpeg|png|pdf),
extracts 系统运行费用折合度电水平 via services/hermes/sysopfee_extract.py,
and upserts into province_sysopfee_monthly.

Runs on ECS (hermes task-def) — Anthropic API + RDS both unreachable from the
Mac. Transport: S3 tarball, container override.

Usage (on ECS, workdir /app with services/ + shared/ on path):
    python -m services.hermes.sysopfee_backfill --root /data/dg --year 2026 --months 5 [--dry-run]
    python -m services.hermes.sysopfee_backfill --probe        # list existing table contents
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from pathlib import Path

import psycopg2

from services.hermes.sysopfee_extract import (
    extract_from_image_bytes,
    extract_from_pdf_bytes,
    extract_scroll_rows,
    identify_scroll_month,
    upsert_sysopfee,
)

logger = logging.getLogger("sysopfee_backfill")

_IMAGE_EXTS = {".jpg", ".jpeg", ".png"}
_MEDIA_TYPES = {".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png"}

# Folder name → canonical province name used across the platform (spot tables,
# sysopfee tab). Identity for the rest; the folders already use these names.
_PROVINCE_ALIASES = {
    "蒙西": "蒙西",
    "蒙东": "蒙东",
    "冀北": "冀北",
}


def _canon_province(folder: str) -> str:
    return _PROVINCE_ALIASES.get(folder, folder)


def _find_month_file(prov_dir: Path, year: int, month: int) -> Path | None:
    ydir = prov_dir / f"{year}年"
    if not ydir.is_dir():
        return None
    for ext in (".jpg", ".jpeg", ".png", ".pdf"):
        f = ydir / f"{month}月份{ext}"
        if f.is_file():
            return f
    # fall back to any file starting with e.g. "5月份"
    for f in sorted(ydir.glob(f"{month}月份*")):
        if f.suffix.lower() in _IMAGE_EXTS | {".pdf"}:
            return f
    return None


def probe(pg_url: str) -> None:
    with psycopg2.connect(pg_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT province, COUNT(*), MIN(year_month), MAX(year_month) "
            "FROM province_sysopfee_monthly GROUP BY province ORDER BY province"
        )
        rows = cur.fetchall()
        for prov, n, mn, mx in rows:
            print(f"PROBE {prov}\t{n}\t{mn}\t{mx}")
        print(f"PROBE total provinces: {len(rows)}")


def run_scrolls(scrolls_dir: Path, months: set[int], year: int,
                pg_url: str, api_key: str, dry_run: bool) -> int:
    """Process full-article scroll screenshots (IMG_*.JPG): identify the article
    month, extract all province rows, upsert. Duplicate months across images:
    the image with more rows wins (near-duplicate captures of the same article).
    """
    imgs = sorted(
        p for p in scrolls_dir.iterdir()
        if p.is_file() and p.name.upper().startswith("IMG_")
        and p.suffix.lower() in {".jpg", ".jpeg", ".png"}
    )
    if not imgs:
        print("no IMG_* scroll files found")
        return 2

    by_month: dict[str, tuple[str, dict[str, float]]] = {}
    for f in imgs:
        data = f.read_bytes()
        try:
            month = identify_scroll_month(data, api_key)
        except Exception as e:  # noqa: BLE001
            logger.error("FAIL %s: month-id error %s", f.name, e)
            continue
        if not month:
            logger.warning("SKIP %s: no article month identified", f.name)
            continue
        if int(month[5:7]) not in months or int(month[:4]) != year:
            logger.info("SKIP %s: month %s outside requested %s/%s",
                        f.name, month, year, sorted(months))
            continue
        try:
            rows = extract_scroll_rows(data, api_key)
        except Exception as e:  # noqa: BLE001
            logger.error("FAIL %s: extract error %s", f.name, e)
            continue
        print(f"SCROLL {f.name}\t{month}\t{len(rows)} provinces")
        cur = by_month.get(month)
        if cur is None or len(rows) > len(cur[1]):
            if cur is not None:
                logger.info("month %s: %s (%d rows) replaces %s (%d rows)",
                            month, f.name, len(rows), cur[0], len(cur[1]))
            by_month[month] = (f.name, rows)

    ok = fail = 0
    for month, (fname, rows) in sorted(by_month.items()):
        for prov, fee in sorted(rows.items()):
            if dry_run:
                print(f"DRY {prov}\t{month}\t{fee}\t{fname}")
            else:
                try:
                    with psycopg2.connect(pg_url) as conn, conn.cursor() as cur:
                        upsert_sysopfee(cur, prov, month, fee, fname)
                    print(f"OK {prov}\t{month}\t{fee}\t{fname}")
                    ok += 1
                except Exception as e:  # noqa: BLE001
                    logger.error("FAIL upsert %s %s: %s", prov, month, e)
                    fail += 1
    print(f"DONE scrolls ok={ok} failed={fail} dry_run={dry_run}")
    return 1 if fail else 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="data/raw/各省电网购电信息")
    ap.add_argument("--year", type=int, default=2026)
    ap.add_argument("--months", default="5")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--probe", action="store_true")
    ap.add_argument("--provinces", default="",
                    help="comma-separated province folder names; empty = all")
    ap.add_argument("--scrolls", default="",
                    help="directory of full-article scroll screenshots (IMG_*.JPG)")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    pg_url = os.environ.get("PGURL") or os.environ.get("DATABASE_URL")
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not pg_url:
        print("FATAL: PGURL/DATABASE_URL not set", file=sys.stderr)
        return 2

    if args.probe:
        probe(pg_url)
        return 0

    if not api_key:
        print("FATAL: ANTHROPIC_API_KEY not set", file=sys.stderr)
        return 2

    if args.scrolls:
        months = {int(m) for m in args.months.split(",") if m.strip()}
        return run_scrolls(Path(args.scrolls), months, args.year,
                           pg_url, api_key, args.dry_run)

    root = Path(args.root)
    months = [int(m) for m in args.months.split(",") if m.strip()]
    ok = skipped = failed = 0

    prov_dirs = sorted(
        p for p in root.iterdir()
        if p.is_dir() and p.name not in {"电网购电数据处理", "各省市输配电价(2020-2022)"}
    )
    if args.provinces:
        wanted = {x.strip() for x in args.provinces.split(",") if x.strip()}
        prov_dirs = [p for p in prov_dirs if p.name in wanted or _canon_province(p.name) in wanted]
    for prov_dir in prov_dirs:
        province = _canon_province(prov_dir.name)
        for month in months:
            f = _find_month_file(prov_dir, args.year, month)
            if f is None:
                logger.warning("SKIP %s %d-%02d: no file", province, args.year, month)
                skipped += 1
                continue
            year_month = f"{args.year}-{month:02d}-01"
            data = f.read_bytes()
            try:
                if f.suffix.lower() == ".pdf":
                    fee = extract_from_pdf_bytes(data, api_key=api_key)
                else:
                    fee = extract_from_image_bytes(
                        data, media_type=_MEDIA_TYPES[f.suffix.lower()], api_key=api_key
                    )
            except Exception as e:  # noqa: BLE001 — keep looping other provinces
                logger.error("FAIL %s %s: extract error %s", province, year_month, e)
                failed += 1
                continue
            if fee is None:
                logger.error("FAIL %s %s: no value found (%s)", province, year_month, f.name)
                failed += 1
                continue
            rel = f.relative_to(root)
            if args.dry_run:
                print(f"DRY {province}\t{year_month}\t{fee}\t{rel}")
            else:
                with psycopg2.connect(pg_url) as conn, conn.cursor() as cur:
                    upsert_sysopfee(cur, province, year_month, fee, str(rel))
                print(f"OK {province}\t{year_month}\t{fee}\t{rel}")
            ok += 1

    print(f"DONE ok={ok} skipped={skipped} failed={failed}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
