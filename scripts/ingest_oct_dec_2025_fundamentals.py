"""
Ingest Oct–Dec 2025 spot fundamentals from the manually-collected Excel file
  data/lingfeng/2025年10-12月各省边界及现货价格.xlsx

The file has 15-minute (96-slot) granularity; this script aggregates to hourly
and upserts into marketdata.spot_fundamentals_hourly.

Column mapping (by positional index, headers are Chinese):
  0  省份             → province
  1  全省             (sub-region label — kept for reference, not stored)
  2  日期             → date portion of datetime
  3  24时段  (1–24)  → hour portion of datetime  (hour = slot - 1)
  4  96时段  (1–96)  (15-min slot — used for grouping only)
  6  新能源出力       → renewable_total_mw
  14 系统负荷         → load_mw
  21 省间交换         → net_export_mw  (negative = net export)
  26 竞价空间         → bidding_space_mw
  28 实时价格         → clearing_price_mwh  (yuan/MWh)

Province aliases (file name → DB name):
  冀南     → 河北南网   (file likely uses 冀南; DB stores 河北南网)

Run:
    python scripts/ingest_oct_dec_2025_fundamentals.py [--dry-run] [--dsn DSN]
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import sys
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

_REPO = Path(__file__).resolve().parents[1]
_FILE = _REPO / "data" / "lingfeng" / "2025年10-12月各省边界及现货价格.xlsx"

# Province name aliases: file value → DB value
_ALIASES: dict[str, str] = {
    "冀南":   "河北南网",
    "河北南网": "河北南网",  # already correct
}

# Positional column indices (0-based)
_COL_PROVINCE  = 0
_COL_DATE      = 2
_COL_HOUR_24   = 3   # 1–24
_COL_RENEWABLE = 6   # 新能源出力 (MW)
_COL_LOAD      = 14  # 系统负荷 (MW)
_COL_WIND      = 18  # 风电 (MW)
_COL_SOLAR     = 19  # 光伏 (MW)
_COL_NET_EXP   = 21  # 省间交换 (MW)  negative = net export
_COL_BIDDING   = 26  # 竞价空间 (MW)
# Note: price data (cols 27/28) goes to spot_prices_hourly, not spot_fundamentals_hourly

_UPSERT_SQL = """
INSERT INTO marketdata.spot_fundamentals_hourly
    (province, datetime, load_mw, renewable_total_mw, net_export_mw,
     bidding_space_mw, wind_mw, solar_mw)
VALUES %s
ON CONFLICT (province, datetime) DO UPDATE SET
    load_mw            = EXCLUDED.load_mw,
    renewable_total_mw = EXCLUDED.renewable_total_mw,
    net_export_mw      = EXCLUDED.net_export_mw,
    bidding_space_mw   = EXCLUDED.bidding_space_mw,
    wind_mw            = EXCLUDED.wind_mw,
    solar_mw           = EXCLUDED.solar_mw
"""


def _safe_float(v) -> float | None:
    try:
        f = float(v)
        return None if f != f else f  # NaN → None
    except (TypeError, ValueError):
        return None


def load_and_aggregate(path: Path) -> pd.DataFrame:
    """Read the Excel, aggregate 15-min → hourly, return clean DataFrame."""
    log.info("Reading %s …", path.name)
    raw = pd.read_excel(path, sheet_name=0, header=0)
    log.info("  Raw rows: %d", len(raw))

    # Rename by position so we don't depend on garbled column headers
    cols = raw.columns.tolist()
    renames = {
        cols[_COL_PROVINCE]:  "province",
        cols[_COL_DATE]:      "date",
        cols[_COL_HOUR_24]:   "hour24",
        cols[_COL_RENEWABLE]: "renewable_mw",
        cols[_COL_LOAD]:      "load_mw",
        cols[_COL_WIND]:      "wind_mw",
        cols[_COL_SOLAR]:     "solar_mw",
        cols[_COL_NET_EXP]:   "net_export_mw",
        cols[_COL_BIDDING]:   "bidding_space_mw",
    }
    df = raw.rename(columns=renames)[list(renames.values())].copy()

    # Drop rows without province or hour
    df = df.dropna(subset=["province", "hour24"])
    df["hour24"] = df["hour24"].astype(int)

    # Apply province aliases
    df["province"] = df["province"].map(lambda p: _ALIASES.get(p, p))

    # Parse date
    df["date"] = pd.to_datetime(df["date"]).dt.date

    # Build datetime: 24时段 1 = 00:00, 2 = 01:00, ...
    df["datetime"] = df.apply(
        lambda r: dt.datetime.combine(r["date"], dt.time(r["hour24"] - 1, 0)),
        axis=1,
    )

    # Numeric coercion
    for col in ["load_mw", "renewable_mw", "wind_mw", "solar_mw", "net_export_mw", "bidding_space_mw"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Aggregate 15-min → hourly (mean of up to 4 slots)
    hourly = (
        df.groupby(["province", "datetime"], sort=False)
        .agg(
            load_mw=("load_mw", "mean"),
            renewable_total_mw=("renewable_mw", "mean"),
            wind_mw=("wind_mw", "mean"),
            solar_mw=("solar_mw", "mean"),
            net_export_mw=("net_export_mw", "mean"),
            bidding_space_mw=("bidding_space_mw", "mean"),
        )
        .reset_index()
    )

    log.info("  Hourly rows after aggregation: %d", len(hourly))
    log.info("  Date range: %s → %s", hourly["datetime"].min(), hourly["datetime"].max())
    log.info("  Provinces: %s", sorted(hourly["province"].unique().tolist()))
    return hourly


def upsert(df: pd.DataFrame, dsn: str, dry_run: bool = False) -> int:
    """Upsert hourly rows into spot_fundamentals_hourly."""
    records = []
    for _, row in df.iterrows():
        records.append((
            row["province"],
            row["datetime"],
            _safe_float(row["load_mw"]),
            _safe_float(row["renewable_total_mw"]),
            _safe_float(row["net_export_mw"]),
            _safe_float(row["bidding_space_mw"]),
            _safe_float(row["wind_mw"]),
            _safe_float(row["solar_mw"]),
        ))

    if dry_run:
        log.info("DRY-RUN: would upsert %d rows", len(records))
        # Show per-province counts
        from collections import Counter
        cnt = Counter(r[0] for r in records)
        for prov, n in sorted(cnt.items()):
            log.info("  %-20s  %d rows", prov, n)
        return len(records)

    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            execute_values(cur, _UPSERT_SQL, records, page_size=2000)
        conn.commit()
        log.info("Upserted %d rows", len(records))
        return len(records)
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--dsn", default=None)
    parser.add_argument("--file", default=str(_FILE))
    args = parser.parse_args()

    # Load DSN from .env if not provided
    dsn = args.dsn
    if not dsn:
        import os
        env_file = _REPO / "config" / ".env"
        if env_file.exists():
            for line in env_file.read_text(encoding="utf-8", errors="ignore").splitlines():
                if line.startswith("PGURL="):
                    dsn = line.split("=", 1)[1].strip()
                    break
        dsn = dsn or os.environ.get("PGURL") or os.environ.get("DATABASE_URL")

    if not dsn and not args.dry_run:
        log.error("No DSN found. Set PGURL in config/.env or pass --dsn")
        sys.exit(1)

    path = Path(args.file)
    if not path.exists():
        log.error("File not found: %s", path)
        sys.exit(1)

    df = load_and_aggregate(path)
    upsert(df, dsn or "", dry_run=args.dry_run)


if __name__ == "__main__":
    main()
