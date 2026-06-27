"""
LingFeng daily data collection + ingestion pipeline.

Downloads Excel data from https://lingfeng-saas.tradingthink.cn, renames it
to <market>.xlsx, runs fundamentals ingestion and (optionally) the RT capture
pipeline for the specified provinces.

Usage — all provinces, all models (daily scheduled run):
    python services/lingfeng/run_daily.py --markets all

Usage — single province, manual backfill:
    python services/lingfeng/run_daily.py \\
        --markets 山东 --indicator 市场供需数据 \\
        --lookback 30 \\
        --models ols_rt_time_v1

Usage — explicit date range:
    python services/lingfeng/run_daily.py \\
        --markets 山东,山西 \\
        --start-date 2026-01-01 --end-date 2026-05-09

Credentials are read from env vars:
    LINGFENG_USERNAME        account username
    LINGFENG_PASSWORD        account password
    PGURL                    Postgres DSN (already in config/.env)

Hermes notification (optional — set in config/.env to enable alerts):
    FEISHU_APP_ID            Feishu app ID
    FEISHU_APP_SECRET        Feishu app secret
    FEISHU_OWNER_OPEN_ID     Your Feishu open_id (for proactive alerts)
    TELEGRAM_BOT_TOKEN       Telegram bot token
    TELEGRAM_OWNER_CHAT_ID   Your Telegram chat_id (for proactive alerts)

Or pass --username / --password on the command line (not recommended for scheduled use).

Scheduling (Windows Task Scheduler):
    Action: python C:\\...\\services\\lingfeng\\run_daily.py --markets all
    Trigger: Daily, 06:00
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

# ---------------------------------------------------------------------------
# All 29 LingFeng markets (Chinese province names as shown on the platform)
# Province names in DB are stored as Chinese characters.
# ---------------------------------------------------------------------------
_ALL_MARKETS = [
    "河南", "新疆", "吉林", "海南", "湖北", "四川", "黑龙江", "福建",
    "浙江", "江苏", "广西", "安徽", "陕西", "贵州", "云南", "广东",
    "蒙东", "湖南", "宁夏", "辽宁", "河北南网", "甘肃", "蒙西",
    "山东", "山西", "冀北", "广州", "青海", "江西",
]

# Default capture models for daily runs
_DEFAULT_MODELS = "ols_rt_time_v1,naive_rt_ar17,ols_fundamentals_v1"

# Default indicator for all markets
_DEFAULT_INDICATOR = "市场供需数据"

# Sentinel file written when a CredentialError is detected.
# Its presence causes the pipeline to refuse to run (protecting against lockout).
# Deleted automatically when the user supplies a new password via Hermes.
_CREDENTIAL_HALT_FILE = _HERE / "CREDENTIAL_HALT"


# ---------------------------------------------------------------------------
# Hermes integration helpers
# ---------------------------------------------------------------------------

def _db_get_setting(key: str) -> str | None:
    """Read a value from hermes_settings using PGURL (graceful fallback)."""
    import psycopg2
    pgurl = os.environ.get("PGURL") or os.environ.get("DATABASE_URL")
    if not pgurl:
        return None
    try:
        with psycopg2.connect(pgurl) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT value FROM hermes_settings WHERE key = %s", (key,)
                )
                row = cur.fetchone()
                return row[0] if row else None
    except Exception as e:
        logger.debug(f"_db_get_setting({key!r}) failed: {e}")
        return None


def _db_set_setting(key: str, value: str) -> None:
    """Write a value to hermes_settings using PGURL (graceful fallback)."""
    import psycopg2
    pgurl = os.environ.get("PGURL") or os.environ.get("DATABASE_URL")
    if not pgurl:
        return
    try:
        with psycopg2.connect(pgurl) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO hermes_settings (key, value, updated_at) VALUES (%s, %s, NOW()) "
                    "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()",
                    (key, value),
                )
            conn.commit()
    except Exception as e:
        logger.debug(f"_db_set_setting({key!r}) failed: {e}")


def _send_hermes_alert(text: str) -> None:
    """Send an alert via Feishu and/or Telegram if credentials are configured."""
    feishu_app_id     = os.environ.get("FEISHU_APP_ID", "")
    feishu_app_secret = os.environ.get("FEISHU_APP_SECRET", "")
    feishu_open_id    = os.environ.get("FEISHU_OWNER_OPEN_ID", "")
    if feishu_app_id and feishu_app_secret and feishu_open_id:
        try:
            sys.path.insert(0, str(_REPO))
            from services.hermes.feishu_client import FeishuClient
            FeishuClient(feishu_app_id, feishu_app_secret).send_text(feishu_open_id, text)
            logger.info("Hermes alert sent via Feishu.")
        except Exception as e:
            logger.warning(f"Hermes Feishu alert failed: {e}")

    tg_token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    tg_chat_id = os.environ.get("TELEGRAM_OWNER_CHAT_ID", "")
    if tg_token and tg_chat_id:
        try:
            sys.path.insert(0, str(_REPO))
            from services.hermes.telegram_client import TelegramClient
            TelegramClient(tg_token).send_text(tg_chat_id, text)
            logger.info("Hermes alert sent via Telegram.")
        except Exception as e:
            logger.warning(f"Hermes Telegram alert failed: {e}")


def _find_data_gaps(halt_date: date, end_date: date) -> dict[str, list[str]]:
    """
    Return {province: [missing_date_str, ...]} for dates in [halt_date, end_date]
    for provinces that already have some data in spot_fundamentals_hourly.
    """
    import psycopg2
    pgurl = os.environ.get("PGURL") or os.environ.get("DATABASE_URL")
    if not pgurl:
        return {}
    sql = """
        WITH expected AS (
            SELECT DISTINCT province FROM marketdata.spot_fundamentals_hourly
        ),
        date_series AS (
            SELECT generate_series(%s::date, %s::date, '1 day'::interval)::date AS d
        ),
        present AS (
            SELECT province, datetime::date AS d
            FROM marketdata.spot_fundamentals_hourly
            WHERE load_mw > 0
              AND datetime::date BETWEEN %s::date AND %s::date
            GROUP BY province, datetime::date
        )
        SELECT e.province, ds.d
        FROM expected e
        CROSS JOIN date_series ds
        LEFT JOIN present p ON p.province = e.province AND p.d = ds.d
        WHERE p.d IS NULL
        ORDER BY e.province, ds.d
    """
    try:
        gaps: dict[str, list[str]] = {}
        with psycopg2.connect(pgurl) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (halt_date, end_date, halt_date, end_date))
                for province, d in cur.fetchall():
                    gaps.setdefault(province, []).append(str(d))
        return gaps
    except Exception as e:
        logger.warning(f"Gap check failed: {e}")
        return {}


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

def _ingest_downloaded_chunk(
    raw_path: Path,
    chunk_start: date,
    chunk_end: date,
    province_cn: str,
    schema: str,
    skip_prices: bool,
    skip_fundamentals: bool,
    keep_files: bool,
) -> bool:
    """Run price + fundamentals ingest for an already-downloaded Excel file. Returns True on success."""
    logger.info(f"  Ingesting chunk {chunk_start} → {chunk_end}  ({raw_path.name})")

    # Rename to <province>.xlsx so ingest scripts resolve province from stem
    target_name = f"{province_cn}.xlsx"
    target_path = raw_path.parent / target_name
    if raw_path.resolve() != target_path.resolve():
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

    # Download — let CredentialError propagate immediately (do not catch it here)
    try:
        from services.lingfeng.collector import CredentialError
    except ImportError:
        CredentialError = None  # type: ignore[assignment,misc]

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
        if CredentialError and isinstance(exc, CredentialError):
            raise  # propagate to run_pipeline which writes the halt sentinel
        logger.error(f"  [FAIL] Download failed for chunk {chunk_start}–{chunk_end}: {exc}")
        return False

    return _ingest_downloaded_chunk(
        raw_path=raw_path,
        chunk_start=chunk_start,
        chunk_end=chunk_end,
        province_cn=province_cn,
        schema=schema,
        skip_prices=skip_prices,
        skip_fundamentals=skip_fundamentals,
        keep_files=keep_files,
    )


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run_pipeline(
    username: str,
    password: str,
    markets: list[str],
    indicator: str,
    start_date: date,
    end_date: date,
    schema: str,
    models: list[str],
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

    # ── Credential halt check ──────────────────────────────────────────────────
    if _CREDENTIAL_HALT_FILE.exists():
        msg = _CREDENTIAL_HALT_FILE.read_text(encoding="utf-8").strip()
        logger.error("=" * 60)
        logger.error("CREDENTIAL HALT — pipeline refused to start.")
        logger.error(f"Reason: {msg}")
        logger.error(
            f"Action: update LINGFENG_PASSWORD in config/.env, then delete "
            f"{_CREDENTIAL_HALT_FILE} to re-enable the pipeline."
        )
        logger.error("=" * 60)
        sys.exit(2)

    total_days = (end_date - start_date).days + 1

    logger.info("=" * 60)
    logger.info(f"LingFeng collection — {len(markets)} market(s) / {indicator}")
    logger.info(f"Markets: {', '.join(markets)}")
    logger.info(f"Date range: {start_date} → {end_date} ({total_days} days)")
    logger.info(f"Chunk size: {chunk_days} days")
    logger.info(f"Models: {', '.join(models)}")
    logger.info("=" * 60)

    try:
        from services.lingfeng.collector import collect, collect_province, CredentialError
    except ImportError:
        sys.path.insert(0, str(_REPO))
        from services.lingfeng.collector import collect, collect_province, CredentialError

    # Try to load ops_log; degrade gracefully if unavailable
    try:
        sys.path.insert(0, str(_REPO))
        from services.lingfeng import ops_log as _ops_log
        _ops_log.ensure_table()
        _use_ops_log = True
    except Exception as e:
        logger.warning(f"ops_log unavailable — skipping: {e}")
        _use_ops_log = False

    # ── Phase 1: Download + Ingest all markets ─────────────────────────────
    all_provinces = []
    for market in markets:
        province_cn = _province_from_market(market)
        all_provinces.append(province_cn)
        chunks = list(_date_chunks(start_date, end_date, chunk_days))

        logger.info(f"\n{'─'*50}")
        logger.info(f"MARKET: {market} ({province_cn})  [{len(chunks)} chunk(s)]")
        logger.info(f"{'─'*50}")

        dr_str = f"{start_date}→{end_date}"
        op_id = _ops_log.start_op("lingfeng_ingest", market=market, date_range=dr_str) if _use_ops_log else None

        failed_chunks = []
        try:
            # Download ALL chunks for this province in ONE browser session (one login).
            # collect_province() returns only the successfully downloaded chunks.
            logger.info(f"  Logging in once for {len(chunks)} chunk(s) …")
            downloaded = collect_province(
                username=username,
                password=password,
                market=market,
                indicator=indicator,
                chunks=chunks,
                download_dir=download_dir,
                headless=headless,
            )

            # Identify which chunks failed to download
            downloaded_starts = {cs for cs, _ce2, _p in downloaded}
            failed_chunks = [(cs, ce) for cs, ce in chunks if cs not in downloaded_starts]
            if failed_chunks:
                logger.warning(f"  {market}: {len(failed_chunks)} chunk(s) failed to download: {failed_chunks}")

            # Ingest each successfully downloaded chunk
            for i, (chunk_start, chunk_end, raw_path) in enumerate(downloaded, 1):
                logger.info(f"[CHUNK {i}/{len(downloaded)}]")
                ok = _ingest_downloaded_chunk(
                    raw_path=raw_path,
                    chunk_start=chunk_start,
                    chunk_end=chunk_end,
                    province_cn=province_cn,
                    schema=schema,
                    skip_prices=skip_prices,
                    skip_fundamentals=skip_fundamentals,
                    keep_files=keep_files,
                )
                if not ok:
                    failed_chunks.append((chunk_start, chunk_end))

        except CredentialError as _ce:
            # Write sentinel to prevent any further login attempts
            _halt_msg = str(_ce)
            try:
                _CREDENTIAL_HALT_FILE.write_text(_halt_msg, encoding="utf-8")
            except Exception:
                pass
            if _use_ops_log and op_id is not None:
                _ops_log.finish_op(op_id, False, f"CREDENTIAL HALT: {_halt_msg}")
            logger.error("=" * 60)
            logger.error("CREDENTIAL HALT — wrong password detected, pipeline stopped.")
            logger.error(_halt_msg)
            logger.error(
                f"Sentinel written to {_CREDENTIAL_HALT_FILE}. "
                f"Send new password via Hermes (Feishu/Telegram): 'lingfeng password: NEW_PW'"
            )
            logger.error("=" * 60)
            # Store halt date so backfill knows the gap start
            _db_set_setting("lingfeng_halt_date", str(start_date))
            # Notify user via Feishu / Telegram
            _alert = (
                f"🔴 LingFeng数据采集已停止 — 密码错误\n\n"
                f"时间：{date.today()}\n"
                f"最后采集起始日期：{start_date}\n\n"
                f"请通过飞书或Telegram发送新密码恢复采集：\n"
                f"  lingfeng password: 新密码\n\n"
                f"收到新密码后，下次定时运行（凌晨4点）将自动恢复并补填缺失数据。"
            )
            _send_hermes_alert(_alert)
            sys.exit(2)

        market_ok = len(failed_chunks) == 0
        msg = "" if market_ok else f"Failed chunks: {failed_chunks}"
        if failed_chunks:
            logger.warning(f"  {market}: {msg}")

        if _use_ops_log and op_id is not None:
            _ops_log.finish_op(op_id, market_ok, msg)

    # ── Phase 2: Capture pipeline — once per model for all provinces ───────
    if not skip_capture:
        province_list = ",".join(all_provinces)
        durations = ["2", "4"] if duration_h == "both" else [duration_h.replace("h", "")]

        for model in models:
            logger.info(f"\n[CAPTURE] model={model}, provinces={province_list}")
            op_id = _ops_log.start_op("capture", market="all", date_range=model) if _use_ops_log else None

            model_ok = True
            for dur in durations:
                cmd = [
                    sys.executable, _CAPTURE_PIPELINE_SCRIPT,
                    "--env",           "none",
                    "--schema",        schema,
                    "--duration-h",    dur,
                    "--model",         model,
                    "--province-list", province_list,
                ]
                if force_capture:
                    cmd += ["--force", "--force-theoretical"]
                ok = _run(cmd, f"Capture ({dur}h, {model})")
                if not ok:
                    logger.warning(f"Capture pipeline {dur}h {model} failed.")
                    model_ok = False

            if _use_ops_log and op_id is not None:
                _ops_log.finish_op(op_id, model_ok, "" if model_ok else "One or more durations failed")
    else:
        logger.info("[CAPTURE] Skipped (--skip-capture).")

    logger.info("=" * 60)
    logger.info("Pipeline complete.")
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
    p.add_argument("--markets", default="all",
                   help="Comma-separated market names or 'all' for all 29 markets (default: all)")
    p.add_argument("--indicator", default=_DEFAULT_INDICATOR,
                   help=f"Indicator type (default: {_DEFAULT_INDICATOR})")

    # Date range — mutually exclusive with --lookback
    date_grp = p.add_mutually_exclusive_group()
    date_grp.add_argument("--lookback", type=int, default=2,
                          help="Download the last N days (default: 2)")
    date_grp.add_argument("--start-date", default=None,
                          help="Explicit start date YYYY-MM-DD (use with --end-date)")
    p.add_argument("--end-date", default=None,
                   help="Explicit end date YYYY-MM-DD (default: yesterday)")

    # Pipeline options
    p.add_argument("--schema",    default="marketdata",  help="DB schema (default: marketdata)")
    p.add_argument("--models",    default=_DEFAULT_MODELS,
                   help=f"Comma-separated capture models (default: {_DEFAULT_MODELS})")
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
    p.add_argument("--reset-credential-halt", action="store_true",
                   help=(
                       "Delete the CREDENTIAL_HALT sentinel file and exit. "
                       "Run this after updating LINGFENG_PASSWORD in config/.env "
                       "to re-enable the pipeline."
                   ))
    p.add_argument("--check-trigger", action="store_true",
                   help=(
                       "Check hermes_settings.lingfeng_trigger_run in DB. "
                       "Exit silently if no trigger is set; otherwise run pipeline "
                       "with the requested date range. Intended for the 15-min "
                       "Task Scheduler task."
                   ))

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

    # Handle --reset-credential-halt before anything else
    if args.reset_credential_halt:
        if _CREDENTIAL_HALT_FILE.exists():
            _CREDENTIAL_HALT_FILE.unlink()
            logger.info(f"Credential halt cleared: {_CREDENTIAL_HALT_FILE}")
            logger.info("Pipeline will resume on next run. Make sure LINGFENG_PASSWORD is updated.")
        else:
            logger.info("No credential halt sentinel found — nothing to reset.")
        sys.exit(0)

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

    # ── Check DB for manual trigger sent via Hermes ───────────────────────────
    if args.check_trigger:
        _trigger_val = _db_get_setting("lingfeng_trigger_run")
        if not _trigger_val:
            sys.exit(0)   # nothing pending — silent no-op
        _db_set_setting("lingfeng_trigger_run", "")   # consume immediately
        logger.info(f"Manual trigger received via Hermes: {_trigger_val!r}")
        # Override date args based on trigger value
        # Formats: "auto" | "YYYY-MM-DD" | "YYYY-MM-DD:YYYY-MM-DD"
        if _trigger_val != "auto":
            if ":" in _trigger_val:
                _t_start, _t_end = _trigger_val.split(":", 1)
                args.start_date = _t_start.strip()
                args.end_date   = _t_end.strip()
            else:
                args.start_date = _trigger_val.strip()
                args.end_date   = None
        # If account is halted, inform user and exit rather than re-locking the account
        if _CREDENTIAL_HALT_FILE.exists():
            _halt_msg = (
                "⚠️ LingFeng手动补填被阻止 — 账号处于密码锁定状态。\n"
                "请先发送新密码：lingfeng password: 新密码\n\n"
                "⚠️ LingFeng manual backfill blocked — credential halt is active.\n"
                "Send new password first: lingfeng password: NEW_PASSWORD"
            )
            logger.warning(_halt_msg)
            _send_hermes_alert(_halt_msg)
            sys.exit(0)

    # ── Check DB for new password sent via Hermes ─────────────────────────────
    _new_pw_via_hermes = _db_get_setting("lingfeng_new_password")
    _resuming_from_halt = False
    _halt_start: date | None = None
    if _new_pw_via_hermes:
        logger.info("New LingFeng password received via Hermes — resuming pipeline.")
        password = _new_pw_via_hermes
        _db_set_setting("lingfeng_new_password", "")   # consume; don't reuse on next run
        _resuming_from_halt = True
        # Clear sentinel so run_pipeline() doesn't refuse to start
        if _CREDENTIAL_HALT_FILE.exists():
            _CREDENTIAL_HALT_FILE.unlink()
            logger.info("Credential halt sentinel cleared.")
        # Extend backfill range to cover the gap since halt
        _halt_date_str = _db_get_setting("lingfeng_halt_date")
        if _halt_date_str:
            try:
                _halt_start = date.fromisoformat(_halt_date_str)
            except ValueError:
                pass

    # Resolve markets
    if args.markets.strip().lower() == "all":
        markets = _ALL_MARKETS
    else:
        markets = [m.strip() for m in args.markets.split(",") if m.strip()]
    if not markets:
        logger.error("No markets specified.")
        sys.exit(1)

    # Resolve models
    models = [m.strip() for m in args.models.split(",") if m.strip()]
    if not models:
        logger.error("No models specified.")
        sys.exit(1)

    # Resolve date range
    today = date.today()
    if args.start_date:
        start_date = date.fromisoformat(args.start_date)
        end_date   = date.fromisoformat(args.end_date) if args.end_date else today - timedelta(days=1)
    else:
        end_date   = date.fromisoformat(args.end_date) if args.end_date else today - timedelta(days=1)
        start_date = end_date - timedelta(days=args.lookback - 1)

    # If resuming from a credential halt, extend start_date to cover the full gap
    if _resuming_from_halt and _halt_start and _halt_start < start_date:
        logger.info(f"Backfill: extending start_date {start_date} → {_halt_start} to cover gap.")
        start_date = _halt_start

    # Resolve download dir
    if args.download_dir:
        download_dir = Path(args.download_dir)
    else:
        download_dir = Path(tempfile.gettempdir()) / "lingfeng_downloads"

    run_pipeline(
        username=username,
        password=password,
        markets=markets,
        indicator=args.indicator,
        start_date=start_date,
        end_date=end_date,
        schema=args.schema,
        models=models,
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

    # ── Post-resume gap check ─────────────────────────────────────────────────
    if _resuming_from_halt:
        _db_set_setting("lingfeng_halt_date", "")   # clear halt marker
        _gap_start = _halt_start or start_date
        _yesterday = today - timedelta(days=1)
        logger.info(f"Checking for remaining data gaps {_gap_start} → {_yesterday} …")
        _gaps = _find_data_gaps(_gap_start, _yesterday)
        if not _gaps:
            _ok_msg = (
                f"✅ LingFeng采集已恢复\n"
                f"补填区间：{_gap_start} → {_yesterday}\n"
                f"所有省份数据完整，无缺口。"
            )
            logger.info(_ok_msg)
            _send_hermes_alert(_ok_msg)
        else:
            _gap_lines = []
            for _prov, _dates in sorted(_gaps.items()):
                if len(_dates) <= 5:
                    _gap_lines.append(f"  {_prov}: {', '.join(_dates)}")
                else:
                    _gap_lines.append(
                        f"  {_prov}: {_dates[0]} ~ {_dates[-1]} ({len(_dates)}天)"
                    )
            _gap_msg = (
                f"⚠️ LingFeng采集已恢复，但仍有缺口：\n\n"
                + "\n".join(_gap_lines[:30])
                + ("\n  ..." if len(_gap_lines) > 30 else "")
                + f"\n\n共 {len(_gaps)} 省有缺口。"
                f"请运行：py services/lingfeng/run_daily.py --markets <省份> "
                f"--start-date {_gap_start} --end-date {_yesterday}"
            )
            logger.warning(_gap_msg)
            _send_hermes_alert(_gap_msg)


if __name__ == "__main__":
    main()
