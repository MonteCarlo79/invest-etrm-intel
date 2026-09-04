"""
Province clearing price ingestion via LingFeng REST API.

SCOPE: Only the two province-level clearing price tables:
  hist_<province>_provincerealtimeclearprice_15min   (RT)
  hist_<province>_provincedayaheadclearprice_15min   (DA)

NOTE: The four fundamentals tables written by province_misc_to_db_v2.py
(hist_*_newenergyreal_15min, hist_*_windpowerreal_15min,
 hist_*_solarpowerreal_15min, hist_*_loadregulationreal_15min,
 hist_*_biddingspacereal_15min) come from the ENOS market/clear/data
endpoint and require APP_KEY / APP_SECRET — those are NOT covered here.

LingFeng API endpoint:
  POST https://lingfeng-saas.tradingthink.cn/api/open/v1/metrics/data/query
  Metrics: quansheng_real_clearing_price  (RT)
           quansheng_dayahead_clearing_price  (DA)
  Auth: X-API-KEY-SECRET  (set LINGFENG_API_KEY in config/.env)
  Doc: data/nodal/聆风开放平台数据接口文档_V1.1(1).pdf

Table schema:
  time  TIMESTAMP PRIMARY KEY
  price DOUBLE PRECISION

Usage — daily (from scheduled run or Hermes trigger):
  python services/lingfeng/ingest_province_clearing.py --markets 蒙西 --lookback 3

Usage — backfill:
  python services/lingfeng/ingest_province_clearing.py \\
      --markets 蒙西,山东,山西 \\
      --start-date 2026-07-21 --end-date 2026-07-29

Env vars required:
  LINGFENG_API_KEY   API key from LingFeng (X-API-KEY-SECRET)
  PGURL              Postgres DSN

Supported market names (Chinese):
  蒙西, 山东, 山西, 安徽, 江苏, 广东, 广西, 云南, 贵州, 海南,
  河南, 湖南, 湖北, 四川, 重庆, 辽宁, 吉林, 黑龙江, 陕西, 甘肃,
  宁夏, 新疆, 青海, 河北南网, 冀北, 蒙东, 江西, 福建, 浙江

Province code mapping (for -15min shift logic — North China non-South-Grid):
  South Grid (NO shift): 云南=53, 广西=45, 贵州=52, 广东=44
  All others: shift already applied in api_client (time_order_96→start-of-interval)
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date, timedelta
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

_REPO = Path(__file__).resolve().parents[2]


# ── Helpers ──────────────────────────────────────────────────────────────────

def _get_engine():
    from sqlalchemy import create_engine
    pgurl = os.environ.get("PGURL") or os.environ.get("DB_DSN")
    if not pgurl:
        raise RuntimeError("PGURL environment variable not set.")
    return create_engine(pgurl, pool_pre_ping=True)


def _province_table_prefix(market_name: str) -> str:
    """
    Convert a Chinese market name to its lowercase DB table prefix.
    Mirrors the sanitisation in province_misc_to_db_v2.py.
    """
    _MAP = {
        "蒙西":  "mengxi",
        "蒙东":  "mengdong",
        "山东":  "shandong",
        "山西":  "shanxi",
        "安徽":  "anhui",
        "江苏":  "jiangsu",
        "广东":  "guangdong",
        "广西":  "guangxi",
        "云南":  "yunnan",
        "贵州":  "guizhou",
        "海南":  "hainan",
        "河南":  "henan",
        "湖南":  "hunan",
        "湖北":  "hubei",
        "四川":  "sichuan",
        "重庆":  "chongqing",
        "辽宁":  "liaoning",
        "吉林":  "jilin",
        "黑龙江": "heilongjiang",
        "陕西":  "shaanxi",
        "甘肃":  "gansu",
        "宁夏":  "ningxia",
        "新疆":  "xinjiang",
        "青海":  "qinghai",
        "河北南网": "hebei_south",
        "冀北":  "jibei",
        "江西":  "jiangxi",
        "福建":  "fujian",
        "浙江":  "zhejiang",
        "北京":  "beijing",
        "天津":  "tianjin",
    }
    return _MAP.get(market_name, market_name.lower())


def _ensure_15min_table(engine, table_name: str) -> None:
    from sqlalchemy import text
    ddl = f"""
    CREATE TABLE IF NOT EXISTS "{table_name}" (
        time  TIMESTAMP PRIMARY KEY,
        price DOUBLE PRECISION
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def _upsert_15min(engine, table_name: str, df) -> int:
    """Delete-then-insert for the fetched time range."""
    import pandas as pd
    from sqlalchemy import text

    if df.empty:
        return 0

    sub = df[["time", "price"]].dropna().drop_duplicates(subset=["time"]).sort_values("time")
    if sub.empty:
        return 0

    _ensure_15min_table(engine, table_name)

    times = sub["time"].tolist()
    chunk = 1000
    with engine.begin() as conn:
        for i in range(0, len(times), chunk):
            batch = times[i: i + chunk]
            placeholders = ",".join([f":t{j}" for j in range(len(batch))])
            params = {f"t{j}": t for j, t in enumerate(batch)}
            conn.execute(text(f'DELETE FROM "{table_name}" WHERE time IN ({placeholders})'), params)
        sub.to_sql(table_name, con=conn, if_exists="append", index=False, method="multi", chunksize=5000)

    return len(sub)


# ── Main ingestion logic ──────────────────────────────────────────────────────

def ingest_market(
    client,
    engine,
    market_name: str,
    start_date: date,
    end_date: date,
    metrics: list[str],
) -> dict[str, int]:
    """
    Ingest one market for the given date range.

    Returns dict mapping table_name → rows_written.
    """
    prefix = _province_table_prefix(market_name)
    results: dict[str, int] = {}

    metric_to_table = {
        "rt": f"hist_{prefix}_provincerealtimeclearprice_15min",
        "da": f"hist_{prefix}_provincedayaheadclearprice_15min",
    }

    for metric in metrics:
        table_name = metric_to_table.get(metric)
        if not table_name:
            logger.warning("Unknown metric %r — skipped", metric)
            continue

        logger.info("[%s] Fetching %s: %s → %s", market_name, metric, start_date, end_date)
        try:
            df = client.fetch_province_clearing_as_df(market_name, metric, start_date, end_date)
            if df.empty:
                logger.info("[%s] %s: no data returned", market_name, metric)
                results[table_name] = 0
                continue

            n = _upsert_15min(engine, table_name, df)
            logger.info("[%s] %s → %s: %d rows written", market_name, metric, table_name, n)
            results[table_name] = n

        except Exception as exc:
            logger.error("[%s] %s failed: %s", market_name, metric, exc)
            results[table_name] = -1

    return results


def run(
    markets: list[str],
    start_date: date,
    end_date: date,
    metrics: list[str],
    api_key: str | None = None,
) -> None:
    from services.lingfeng.api_client import LingFengAPIClient

    client = LingFengAPIClient(api_key=api_key)
    engine = _get_engine()

    total_written = 0
    for market in markets:
        res = ingest_market(client, engine, market, start_date, end_date, metrics)
        for tbl, n in res.items():
            if n > 0:
                total_written += n

    logger.info("Done. Total rows written: %d", total_written)


# ── CLI ───────────────────────────────────────────────────────────────────────

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Ingest province clearing prices from LingFeng API into hist_* tables."
    )
    p.add_argument(
        "--markets", default="蒙西",
        help="Comma-separated Chinese market names (default: 蒙西)"
    )
    p.add_argument(
        "--metrics", default="rt,da",
        help="Comma-separated metrics: rt, da (default: rt,da)"
    )
    date_grp = p.add_mutually_exclusive_group()
    date_grp.add_argument(
        "--lookback", type=int, default=3,
        help="Ingest the last N days (default: 3)"
    )
    date_grp.add_argument(
        "--start-date", default=None,
        help="Explicit start date YYYY-MM-DD"
    )
    p.add_argument(
        "--end-date", default=None,
        help="Explicit end date YYYY-MM-DD (default: yesterday)"
    )
    p.add_argument(
        "--api-key", default=None,
        help="LingFeng API key (default: LINGFENG_API_KEY env var)"
    )
    return p


def main() -> None:
    _env_file = _REPO / "config" / ".env"
    if _env_file.exists():
        try:
            from dotenv import load_dotenv
            load_dotenv(str(_env_file))
        except ImportError:
            pass

    sys.path.insert(0, str(_REPO))

    args = _build_parser().parse_args()

    markets = [m.strip() for m in args.markets.split(",") if m.strip()]
    metrics = [m.strip() for m in args.metrics.split(",") if m.strip()]

    today = date.today()
    if args.start_date:
        start_date = date.fromisoformat(args.start_date)
        end_date = date.fromisoformat(args.end_date) if args.end_date else today - timedelta(days=1)
    else:
        end_date = date.fromisoformat(args.end_date) if args.end_date else today - timedelta(days=1)
        start_date = end_date - timedelta(days=args.lookback - 1)

    logger.info("Markets: %s", markets)
    logger.info("Metrics: %s", metrics)
    logger.info("Date range: %s → %s", start_date, end_date)

    run(
        markets=markets,
        start_date=start_date,
        end_date=end_date,
        metrics=metrics,
        api_key=args.api_key,
    )


if __name__ == "__main__":
    main()
