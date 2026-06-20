"""
LingFeng ECS entrypoint.

Runs two jobs:
  1. Daily ingestion  — 04:00 CST (20:00 UTC) every day
  2. Trigger check    — every 15 minutes (on-demand backfill via Feishu/Telegram)

Env vars required:
    LINGFENG_USERNAME   LingFeng account username
    LINGFENG_PASSWORD   LingFeng account password
    PGURL               Postgres DSN

Optional (for Hermes alerts):
    FEISHU_APP_ID / FEISHU_APP_SECRET / FEISHU_OWNER_OPEN_ID
    TELEGRAM_BOT_TOKEN / TELEGRAM_OWNER_CHAT_ID
"""
from __future__ import annotations

import logging
import subprocess
import sys
import time
from pathlib import Path

import schedule

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

_REPO   = Path(__file__).resolve().parents[2]
_SCRIPT = _REPO / "services" / "lingfeng" / "run_daily.py"
_PYTHON = sys.executable


def _run(args: list[str]) -> None:
    logger.info("Running: %s", " ".join(args))
    subprocess.run([_PYTHON, str(_SCRIPT)] + args, check=False)


def run_daily() -> None:
    logger.info("=== Daily LingFeng ingestion starting ===")
    _run(["--markets", "all"])


def run_trigger_check() -> None:
    _run(["--markets", "all",
          "--models", "ols_rt_time_v1,naive_rt_ar17,ols_fundamentals_v1",
          "--check-trigger"])


# Daily at 20:00 UTC = 04:00 CST
schedule.every().day.at("20:00").do(run_daily)

# Trigger check every 15 minutes
schedule.every(15).minutes.do(run_trigger_check)

logger.info("LingFeng scheduler started.")
logger.info("  Daily ingestion : 20:00 UTC (04:00 CST)")
logger.info("  Trigger check   : every 15 min")

# Run a trigger check immediately on startup
run_trigger_check()

while True:
    schedule.run_pending()
    time.sleep(30)
