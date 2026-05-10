"""
LingFeng daily data collection + ingestion pipeline.

Downloads Excel data from https://lingfeng-saas.tradingthink.cn, renames it
to <market>.xlsx, runs fundamentals ingestion and (optionally) the RT capture
pipeline for the specified province.

Usage — manual one-shot:
    python services/lingfeng/run_daily.py \\
        --market 山东 --indicator 市场供需数据 \\
        --lookback 30 \\
        --model ols_rt_time_v1

Usage — with explicit date range:
    python services/lingfeng/run_daily.py \\
        --market 山东 --indicator 市场供需数据 \\
        --start-date 2026-01-01 --end-date 2026-05-09

Credentials are read from env vars:
    LINGFENG_USERNAME   account username
    LINGFENG_PASSWORD   account password
    PGURL               Postgres DSN (already in config/.env)

Or pass --username / --password on the command line (not recommended for scheduled use).

Scheduling (Windows Task Scheduler):
    Action: python C:\\...\\services\\lingfeng\\run_daily.py
    Trigger: Daily, 08:00 (data typically published by 07:00)
    Working dir: C:\\...\\bess-platform
    Before first run: set LINGFENG_USERNAME and LINGFENG_PASSWORD in the system
    environment variables (Control Panel → System → Advanced → Environment Variables).
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import date, timedelta
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Repo root (works whether run from project root or directly)
# ---------------------------------------------------------------------------
_HERE = Path(__file__).resolve().parent
_REPO = _HERE.parent.parent   # services/lingfeng → services → repo root

_INGEST_PRICES_SCRIPT       = _REPO / "services" / "bess_map" / "run_all_provinces.py"
_INGEST_FUNDAMENTALS_SCRIPT = _REPO / "services" / "bess_map" / "run_fundamentals_ingest.py"
_CAPTURE_PIPELINE_SCRIPT    = _REPO / "services" / "bess_map" / "run_capture_pipeline.py"

# Province → canonical slug used in capture pipeline --province-list arg
_PROVINCE_SLUG = {
    "山东": "shandong",
    "内蒙古": "inner_mongolia",
    "广东": "guangdong",
    "浙江": "zhejiang",
    "湖北": "hubei",
    "四川": "sichuan",
    "甘肃": "gansu",
    "新疆": "xinjiang",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _run(cmd: list, label: str) -> bool:
    """Run a subprocess, stream stdout, return True on success."""
    logger.info(f"[RUN] {label}")
    logger.info(f"  cmd: {' '.join(str(c) for c in cmd)}")
    proc = subprocess.Popen(
        [str(c) for c in cmd],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        cwd=str(_REPO),
        env={**os.environ, "PYTHONPATH": str(_REPO)},
    )
    for line in proc.stdout:
        sys.stdout.write(line)
    proc.wait()
    if proc.returncode != 0:
        logger.error(f"[FAIL] {label} exited with rc={proc.returncode}")
        return False
    logger.info(f"[OK]   {label}")
    return True


def _province_from_market(market: str) -> str:
    """Extract Chinese province name from market string (strip non-Chinese chars)."""
    return re.sub(r"[^\u4e00-\u9fa5]", "", market)


# ---------------------------------------------------------------------------
# Date chunking helper
# ---------------------------------------------------------------------------

def _date_chunks(start: date, end: date, chunk_days: int):
    """Yield (chunk_start, chunk_end) pairs covering [start, end] in steps of chunk_days."""
    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + timedelta(days=chunk_days - 1), end)
        yield cursor, chunk_end
        cursor = chunk_end + timedelta(days=1)


# ---------------------------------------------------------------------------
# Single-chunk download + ingest
# ---------------------------------------------------------------------------

def _collect_and_ingest_chunk(
    collect_fn,
    username: str,
    password: str,
    market: str,
    indicator: str,
    chunk_start: date,
    chunk_end: date,
    province_cn: str,
    schema: str,
    skip_prices: bool,
    skip_fundamentals: bool,
    headless: bool,
    download_dir: Path,
    keep_files: bool,
) -> bool:
    """Download one date-range chunk and run price + fundamentals ingest. Returns True on success."""

    logger.info(f"  Chunk {chunk_start} → {chunk_end}")

    # Download
    try:
        raw_path = collect_fn(
            username=username,
            password=password,
            market=market,
            indicator=indicator,
            start_date=chunk_start,
            end_date=chunk_end,
            download_dir=download_dir,
            headless=headless,
        )
    except Exception as exc:
        logger.error(f"  [FAIL] Download failed for chunk {chunk_start}–{chunk_end}: {exc}")
        return False

    logger.info(f"  Downloaded: {raw_path}")

    # Rename to <province>.xlsx so ingest scripts resolve province from stem
    target_name = f"{province_cn}.xlsx"
    target_path = raw_path.parent / target_name
    if raw_path.name != target_name:
        shutil.move(str(raw_path), str(target_path))

    # Price ingestion
    if not skip_prices:
        ok = _run(
            [sys.executable, _INGEST_PRICES_SCRIPT,
             "--indir",    str(target_path.parent),
             "--auto-cols", "--upload-db",
             "--env",      "none",
             "--schema",   schema,
             "--continue-on-error"],
            f"Price ingestion ({chunk_start}–{chunk_end})",
        )
        if not ok:
            logger.warning("  Price ingestion failed — continuing.")

    # Fundamentals ingestion
    if not skip_fundamentals:
        ok = _run(
            [sys.executable, _INGEST_FUNDAMENTALS_SCRIPT,
             "--indir",      str(target_path.parent),
             "--env",        "none",
             "--schema",     schema,
             "--start-date", str(chunk_start),
             "--end-date",   str(chunk_end),
             "--continue-on-error"],
            f"Fundamentals ingestion ({chunk_start}–{chunk_end})",
        )
        if not ok:
            logger.warning("  Fundamentals ingestion failed — continuing.")

    # Cleanup
    if not keep_files:
        try:
            target_path.unlink(missing_ok=True)
        except Exception as e:
            logger.warning(f"  Could not clean up {target_path}: {e}")

    return True


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run_pipeline(
    username: str,
    password: str,
    market: str,
    indicator: str,
    start_date: date,
    end_date: date,
    schema: str,
    model: str,
    duration_h: str,
    skip_prices: bool,
    skip_fundamentals: bool,
    skip_capture: bool,
    force_capture: bool,
    headless: bool,
    download_dir: Path,
    keep_files: bool,
    chunk_days: int,
) -> None:

    province_cn   = _province_from_market(market)
    province_slug = _PROVINCE_SLUG.get(province_cn, province_cn)

    total_days = (end_date - start_date).days + 1
    chunks = list(_date_chunks(start_date, end_date, chunk_days))

    logger.info("=" * 60)
    logger.info(f"LingFeng collection — {market} / {indicator}")
    logger.info(f"Date range: {start_date} → {end_date} ({total_days} days)")
    logger.info(f"Chunk size: {chunk_days} days → {len(chunks)} chunk(s)")
    logger.info("=" * 60)

    try:
        from services.lingfeng.collector import collect
    except ImportError:
        sys.path.insert(0, str(_REPO))
        from services.lingfeng.collector import collect

    # ── Steps 1–4: download + ingest each chunk ────────────────────────────
    failed_chunks = []
    for i, (chunk_start, chunk_end) in enumerate(chunks, 1):
        logger.info(f"[CHUNK {i}/{len(chunks)}]")
        ok = _collect_and_ingest_chunk(
            collect_fn=collect,
            username=username,
            password=password,
            market=market,
            indicator=indicator,
            chunk_start=chunk_start,
            chunk_end=chunk_end,
            province_cn=province_cn,
            schema=schema,
            skip_prices=skip_prices,
            skip_fundamentals=skip_fundamentals,
            headless=headless,
            download_dir=download_dir,
            keep_files=keep_files,
        )
        if not ok:
            failed_chunks.append((chunk_start, chunk_end))

    if failed_chunks:
        logger.warning(f"Failed chunks: {failed_chunks}")

    # ── Step 5: Capture pipeline — once for the full date range ───────────
    if not skip_capture:
        durations = ["2", "4"] if duration_h == "both" else [duration_h.replace("h", "")]
        for dur in durations:
            logger.info(f"[CAPTURE] {dur}h, model={model}, province={province_slug} …")
            cmd = [
                sys.executable, _CAPTURE_PIPELINE_SCRIPT,
                "--env",           "none",
                "--schema",        schema,
                "--duration-h",    dur,
                "--model",         model,
                "--province-list", province_slug,
            ]
            if force_capture:
                cmd += ["--force", "--force-theoretical"]
            ok = _run(cmd, f"Capture pipeline ({dur}h)")
            if not ok:
                logger.warning(f"Capture pipeline {dur}h failed.")
    else:
        logger.info("[CAPTURE] Skipped (--skip-capture).")

    logger.info("=" * 60)
    logger.info(f"Collection complete. Chunks ok={len(chunks)-len(failed_chunks)}, "
                f"failed={len(failed_chunks)}")
    logger.info("=" * 60)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="LingFeng daily data collection + DB ingestion + capture pipeline."
    )

    # Credentials (prefer env vars)
    p.add_argument("--username", default=None,
                   help="LingFeng username (default: $LINGFENG_USERNAME)")
    p.add_argument("--password", default=None,
                   help="LingFeng password (default: $LINGFENG_PASSWORD)")

    # Data selection
    p.add_argument("--market",    default="山东",          help="Market to download (default: 山东)")
    p.add_argument("--indicator", default="市场供需数据",  help="Indicator type (default: 市场供需数据)")

    # Date range — mutually exclusive with --lookback
    date_grp = p.add_mutually_exclusive_group()
    date_grp.add_argument("--lookback", type=int, default=30,
                          help="Download the last N days (default: 30)")
    date_grp.add_argument("--start-date", default=None,
                          help="Explicit start date YYYY-MM-DD (use with --end-date)")
    p.add_argument("--end-date", default=None,
                   help="Explicit end date YYYY-MM-DD (default: yesterday)")

    # Pipeline options
    p.add_argument("--schema",    default="marketdata",  help="DB schema (default: marketdata)")
    p.add_argument("--model",     default="ols_rt_time_v1",
                   help="Capture pipeline forecast model (default: ols_rt_time_v1)")
    p.add_argument("--duration-h", default="both", choices=["2", "4", "both"],
                   help="BESS duration for capture pipeline (default: both)")
    p.add_argument("--force-capture", action="store_true",
                   help="Pass --force --force-theoretical to capture pipeline")

    # Skip flags
    p.add_argument("--skip-prices",       action="store_true",
                   help="Skip RT/DA price ingestion (run_all_provinces.py)")
    p.add_argument("--skip-fundamentals", action="store_true",
                   help="Skip fundamentals ingestion")
    p.add_argument("--skip-capture",      action="store_true",
                   help="Skip capture pipeline")

    # Download options
    p.add_argument("--chunk-days", type=int, default=30,
                   help="Max days per download request — splits the full range into "
                        "chunks of this size (default: 30). Set to the platform limit.")
    p.add_argument("--download-dir", default=None,
                   help="Directory to save downloaded files (default: system temp)")
    p.add_argument("--keep-files", action="store_true",
                   help="Do not delete downloaded Excel after ingestion")
    p.add_argument("--show-browser", action="store_true",
                   help="Run browser in visible (non-headless) mode for debugging")

    return p


def main() -> None:
    # Load config/.env if it exists (picks up PGURL etc.)
    _env_file = _REPO / "config" / ".env"
    if _env_file.exists():
        try:
            from dotenv import load_dotenv
            load_dotenv(str(_env_file))
            logger.info(f"Loaded env from {_env_file}")
        except ImportError:
            logger.warning("python-dotenv not installed — skipping .env load.")

    args = _build_parser().parse_args()

    # Resolve credentials
    username = args.username or os.environ.get("LINGFENG_USERNAME")
    password = args.password or os.environ.get("LINGFENG_PASSWORD")
    if not username or not password:
        logger.error(
            "LingFeng credentials not found.\n"
            "Set LINGFENG_USERNAME and LINGFENG_PASSWORD environment variables,\n"
            "or pass --username / --password on the command line."
        )
        sys.exit(1)

    # Resolve date range
    today = date.today()
    if args.start_date:
        start_date = date.fromisoformat(args.start_date)
        end_date   = date.fromisoformat(args.end_date) if args.end_date else today - timedelta(days=1)
    else:
        end_date   = date.fromisoformat(args.end_date) if args.end_date else today - timedelta(days=1)
        start_date = end_date - timedelta(days=args.lookback - 1)

    # Resolve download dir
    if args.download_dir:
        download_dir = Path(args.download_dir)
    else:
        download_dir = Path(tempfile.gettempdir()) / "lingfeng_downloads"

    run_pipeline(
        username=username,
        password=password,
        market=args.market,
        indicator=args.indicator,
        start_date=start_date,
        end_date=end_date,
        schema=args.schema,
        model=args.model,
        duration_h=args.duration_h,
        skip_prices=args.skip_prices,
        skip_fundamentals=args.skip_fundamentals,
        skip_capture=args.skip_capture,
        force_capture=args.force_capture,
        headless=not args.show_browser,
        download_dir=download_dir,
        keep_files=args.keep_files,
        chunk_days=args.chunk_days,
    )


if __name__ == "__main__":
    main()
