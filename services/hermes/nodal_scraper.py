"""
Daily provincial nodal price scraper.

Runs once per day (scheduled in Hermes) to fetch yesterday's 15-min nodal
prices for each configured province from the Fengxing API and persist them as
monthly CSV files on OneDrive.

OneDrive layout:
    etrm/bess-platform/data/nodal/<province>_<YYYY-MM>.csv

Each monthly CSV accumulates rows day-by-day.  On each run the file is
downloaded, the new day's rows are appended (dedup by metric_time + node_name),
and the file is re-uploaded.  This means the OneDrive files are always the
authoritative local store — no ECS disk writes needed.

Optionally upserts into RDS when pg_url is supplied (for the combined
CSV→RDS ingest path).

Province list: FENGXING_PROVINCES env var, comma-separated.
Default: 蒙西,山西,山东,陕西,湖南,浙江,云南,贵州,广东,广西,海南,甘肃

Usage (called from Hermes scheduler / chat command):
    from services.hermes.nodal_scraper import scrape_daily, scrape_date
    results = scrape_daily(onedrive_client, api_key)          # yesterday
    results = scrape_date(target_date, onedrive_client, api_key)  # specific date
"""
from __future__ import annotations

import csv
import io
import logging
import os
from datetime import date, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

_ONEDRIVE_NODAL_DIR = "etrm/bess-platform/data/nodal"

_DEFAULT_PROVINCES = [
    "蒙西", "山西", "山东", "陕西", "湖南",
    "浙江", "云南", "贵州", "广东", "广西", "海南", "甘肃",
]

_CSV_FIELDNAMES = ["node_name", "metric_time", "time_order_96", "market_name", "avg_node_price"]


def _get_provinces() -> list[str]:
    raw = os.environ.get("FENGXING_PROVINCES", "")
    if raw.strip():
        return [p.strip() for p in raw.split(",") if p.strip()]
    return list(_DEFAULT_PROVINCES)


def _onedrive_path(province: str, target_date: date) -> tuple[str, str]:
    """Return (folder_path, filename) for the monthly CSV on OneDrive."""
    filename = f"{province}_{target_date.strftime('%Y-%m')}.csv"
    return _ONEDRIVE_NODAL_DIR, filename


def _download_existing_csv(onedrive, folder: str, filename: str) -> list[dict]:
    """Download and parse an existing monthly CSV from OneDrive.

    Returns list of row dicts, or [] if the file doesn't exist yet.
    """
    try:
        raw = onedrive.read_file_by_path(f"{folder}/{filename}")
        text = raw.decode("utf-8-sig")
        reader = csv.DictReader(io.StringIO(text))
        return list(reader)
    except Exception as exc:
        # File not found or other read error — treat as empty
        logger.debug("No existing CSV %s/%s: %s", folder, filename, exc)
        return []


def _rows_to_csv_bytes(rows: list[dict]) -> bytes:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=_CSV_FIELDNAMES, extrasaction="ignore",
                            lineterminator="\r\n")
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8-sig")


def _merge_rows(existing: list[dict], new_rows: list[dict]) -> list[dict]:
    """Append new_rows to existing, deduplicating by (node_name, metric_time)."""
    seen: set[tuple] = {(r.get("node_name"), r.get("metric_time")) for r in existing}
    merged = list(existing)
    for r in new_rows:
        key = (r.get("node_name"), r.get("metric_time"))
        if key not in seen:
            seen.add(key)
            merged.append(r)
    return merged


def _upsert_to_rds(rows: list[dict], pg_url: str) -> int:
    """Upsert rows into marketdata.md_shanxi_nodal_price_96 via psycopg2."""
    if not rows or not pg_url:
        return 0
    import psycopg2
    _UPSERT = """
        INSERT INTO marketdata.md_shanxi_nodal_price_96
            (node_name, metric_time, time_order_96, market_name, avg_node_price)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (node_name, metric_time, time_order_96) DO UPDATE SET
            market_name    = EXCLUDED.market_name,
            avg_node_price = EXCLUDED.avg_node_price,
            inserted_at    = NOW()
    """
    conn = psycopg2.connect(pg_url, options="-c statement_timeout=60000")
    try:
        with conn.cursor() as cur:
            for r in rows:
                cur.execute(_UPSERT, (
                    r.get("node_name"), r.get("metric_time"),
                    r.get("time_order_96"), r.get("market_name"),
                    r.get("avg_node_price"),
                ))
        conn.commit()
        return len(rows)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Core scrape function
# ---------------------------------------------------------------------------

ProvinceResult = dict  # {province, date, status, rows, msg}


def scrape_date(
    target_date: date,
    onedrive,
    api_key: str,
    provinces: Optional[list[str]] = None,
    pg_url: Optional[str] = None,
) -> list[ProvinceResult]:
    """Fetch nodal prices for *target_date* for each province and save to OneDrive.

    Returns list of per-province result dicts.
    """
    from services.fengxing.nodal_price import _fetch_day

    if provinces is None:
        provinces = _get_provinces()

    results: list[ProvinceResult] = []

    for province in provinces:
        try:
            # 1. Fetch from API
            new_rows = _fetch_day(target_date, api_key, province)
            if not new_rows:
                results.append({
                    "province": province, "date": target_date,
                    "status": "empty", "rows": 0,
                    "msg": "API returned 0 rows",
                })
                continue

            # 2. Download existing monthly CSV from OneDrive
            folder, filename = _onedrive_path(province, target_date)
            existing = _download_existing_csv(onedrive, folder, filename)

            # 3. Merge and re-upload
            merged = _merge_rows(existing, new_rows)
            csv_bytes = _rows_to_csv_bytes(merged)
            onedrive.upload_file(folder_path=folder, filename=filename, content=csv_bytes)

            n_new = len(merged) - len(existing)

            # 4. Optionally upsert to RDS
            rds_note = ""
            if pg_url:
                try:
                    n_upserted = _upsert_to_rds(new_rows, pg_url)
                    rds_note = f" +{n_upserted} RDS"
                except Exception as db_exc:
                    logger.warning("RDS upsert failed for %s %s: %s", province, target_date, db_exc)
                    rds_note = " (RDS failed)"

            results.append({
                "province": province, "date": target_date,
                "status": "ok", "rows": n_new,
                "msg": f"{n_new} new rows → {filename}{rds_note}",
            })
            logger.info("Nodal scrape %s %s: %d new rows (total %d in file)",
                        province, target_date, n_new, len(merged))

        except Exception as exc:
            logger.error("Nodal scrape failed %s %s: %s", province, target_date, exc)
            results.append({
                "province": province, "date": target_date,
                "status": "error", "rows": 0, "msg": str(exc),
            })

    return results


def scrape_daily(
    onedrive,
    api_key: str,
    provinces: Optional[list[str]] = None,
    pg_url: Optional[str] = None,
    days_back: int = 1,
) -> list[ProvinceResult]:
    """Scrape the last *days_back* days (default: yesterday only).

    Called from the Hermes daily scheduler.
    """
    all_results: list[ProvinceResult] = []
    for i in range(days_back, 0, -1):
        target = date.today() - timedelta(days=i)
        day_results = scrape_date(target, onedrive, api_key,
                                  provinces=provinces, pg_url=pg_url)
        all_results.extend(day_results)
    return all_results


def format_summary(results: list[ProvinceResult]) -> str:
    """Return a compact Feishu-friendly summary string."""
    ok    = [r for r in results if r["status"] == "ok"]
    empty = [r for r in results if r["status"] == "empty"]
    err   = [r for r in results if r["status"] == "error"]

    lines = []
    if ok:
        total_rows = sum(r["rows"] for r in ok)
        lines.append(f"✅ {len(ok)} province(s) saved — {total_rows:,} new rows")
    if empty:
        names = ", ".join(r["province"] for r in empty)
        lines.append(f"⚠️ {len(empty)} empty: {names}")
    if err:
        for r in err:
            lines.append(f"❌ {r['province']}: {r['msg']}")
    return "\n".join(lines) if lines else "No provinces configured."
