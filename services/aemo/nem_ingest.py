"""AEMO (NEM) data ingestion → intl_market.au_* tables.

Sources:
  - AEMO MMSDM monthly archive — TRADING_PRICE, DISPATCHREGIONSUM,
    TRADINGLOAD, DISPATCH_UNIT_SOLUTION
  - AEMO Generators and Scheduled Loads Excel — battery asset list

Tables populated:
  au_spot_price        — 30-min NEM spot prices by region
  au_ancillary_results — daily FCAS clearing prices by service/region
  au_bess_assets       — BESS asset register (power, capacity, region, owner)
  au_bess_leaderboard  — daily revenue per asset per market
  au_bess_daily_index  — fleet-average revenue index per market
  au_bess_monthly_index — monthly aggregate of daily index

Notes:
  MMSDM monthly files are published ~2 weeks after month-end.
  The daily scheduler at 03:00 SGT will log 0 rows for the current
  month until that file is published.  Run the backfill commands below
  to populate historical data.

Usage:
    python -m services.aemo.nem_ingest                              # yesterday
    python -m services.aemo.nem_ingest --start 2025-01-01 --end 2026-04-30
    python -m services.aemo.nem_ingest --only assets
    python -m services.aemo.nem_ingest --start 2025-01-01 --end 2026-04-30 \\
        --only spot_price,fcas_price,bess_wholesale,bess_fcas,daily_index
"""
import argparse
import io
import logging
import os
import sys
import zipfile
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterator

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

logger = logging.getLogger(__name__)

_MMSDM_BASE = (
    "https://www.nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM"
)
_GENERATORS_URL = (
    "https://aemo.com.au/-/media/files/electricity/nem/participant_information"
    "/nem-registration-and-exemption-list.xlsx"
)
_REQUEST_HEADERS = {"User-Agent": "BESS-Platform/1.0 (research; contact: ops@pjh-etrm.ai)"}


# ---------------------------------------------------------------------------
# AEMO MMSDM CSV parser
# ---------------------------------------------------------------------------

def _parse_aemo_csv(
    file_obj,
    filter_col: str | None = None,
    filter_vals: set | None = None,
) -> pd.DataFrame:
    """Stream-parse AEMO MMSDM CSV format.

    Format:
      I,CAT,TABLE,VER,col1,col2,...   <- column headers
      D,CAT,TABLE,VER,val1,val2,...   <- data rows
      C,... / END                     <- ignored

    filter_col/filter_vals: keep only D rows where filter_col is in filter_vals.
    Applied before building row list — critical for 200 MB DISPATCH_UNIT_SOLUTION.
    """
    headers: list[str] | None = None
    filter_idx: int | None = None
    rows: list[list] = []

    for raw in file_obj:
        line = (
            raw.decode("utf-8", errors="ignore")
            if isinstance(raw, bytes)
            else raw
        )
        line = line.rstrip("\r\n")
        if not line:
            continue
        parts = line.split(",")
        row_type = parts[0].strip('"')

        if row_type == "I":
            headers = [p.strip('" ') for p in parts[4:]]
            if filter_col and filter_col in headers:
                filter_idx = headers.index(filter_col)

        elif row_type == "D" and headers is not None:
            data = [p.strip('" ') for p in parts[4:]]
            # Fast filter before allocating the full row
            if filter_vals and filter_idx is not None:
                if len(data) <= filter_idx or data[filter_idx] not in filter_vals:
                    continue
            n = len(headers)
            if len(data) < n:
                data += [""] * (n - len(data))
            rows.append(data[:n])

    if not rows or headers is None:
        return pd.DataFrame()
    return pd.DataFrame(rows, columns=headers)


# ---------------------------------------------------------------------------
# MMSDM download helpers
# ---------------------------------------------------------------------------

def _mmsdm_url(yyyy: int, mm: int, table: str) -> str:
    return (
        f"{_MMSDM_BASE}/{yyyy}/MMSDM_{yyyy}_{mm:02d}"
        f"/MMSDM_Historical_Data_SQLServer/DATA"
        f"/PUBLIC_DVD_{table}_{yyyy}{mm:02d}010000.zip"
    )


def _download_zip(url: str, timeout: int = 300) -> io.BytesIO | None:
    try:
        resp = requests.get(
            url, headers=_REQUEST_HEADERS, timeout=timeout, stream=True
        )
        resp.raise_for_status()
        buf = io.BytesIO()
        for chunk in resp.iter_content(65536):
            buf.write(chunk)
        buf.seek(0)
        return buf
    except Exception as exc:
        logger.warning("Download failed %s: %s", url, exc)
        return None


def _read_mmsdm_table(
    yyyy: int,
    mm: int,
    table: str,
    filter_col: str | None = None,
    filter_vals: set | None = None,
) -> pd.DataFrame:
    url = _mmsdm_url(yyyy, mm, table)
    logger.info("Fetching %s", url)
    buf = _download_zip(url)
    if buf is None:
        return pd.DataFrame()
    try:
        with zipfile.ZipFile(buf) as zf:
            csv_name = next(
                (n for n in zf.namelist() if n.upper().endswith(".CSV")), None
            )
            if csv_name is None:
                logger.warning("No CSV in ZIP: %s", url)
                return pd.DataFrame()
            with zf.open(csv_name) as f:
                return _parse_aemo_csv(
                    f, filter_col=filter_col, filter_vals=filter_vals
                )
    except Exception as exc:
        logger.warning("ZIP parse failed %s: %s", url, exc)
        return pd.DataFrame()


def _month_range(start: date, end: date) -> Iterator[tuple[int, int]]:
    """Yield (yyyy, mm) tuples for every month overlapping [start, end]."""
    d = start.replace(day=1)
    while d <= end:
        yield d.year, d.month
        if d.month == 12:
            d = d.replace(year=d.year + 1, month=1)
        else:
            d = d.replace(month=d.month + 1)


# ---------------------------------------------------------------------------
# Settlement date/period helpers
# ---------------------------------------------------------------------------

def _parse_settlement(dt_str: str) -> tuple[date, int]:
    """Map AEMO SETTLEMENTDATE string → (settlement_date, settlement_period).

    NEM convention: period 1 = 00:00–00:30 (end-time 00:30),
    period 47 = 23:00–23:30 (end-time 23:30),
    period 48 = 23:30–00:00 (end-time 00:00 of the *next* calendar day).
    """
    s = dt_str.strip()
    try:
        dt = datetime.strptime(s, "%Y/%m/%d %H:%M:%S")
    except ValueError:
        try:
            dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return date.today(), 1

    if dt.hour == 0 and dt.minute == 0:
        return dt.date() - timedelta(days=1), 48

    period = dt.hour * 2 + (1 if dt.minute == 30 else 0)
    return dt.date(), period


# ---------------------------------------------------------------------------
# DB upsert helper
# ---------------------------------------------------------------------------

def _upsert(
    engine,
    table: str,
    df: pd.DataFrame,
    conflict_cols: list[str],
    batch_size: int = 2000,
) -> int:
    from psycopg2.extras import execute_values

    if df.empty:
        return 0
    df = df.where(df.notna(), other=None)
    df = df.drop_duplicates(subset=conflict_cols, keep="last")
    cols = list(df.columns)
    update_cols = [c for c in cols if c not in conflict_cols]
    on_conflict = (
        f"DO UPDATE SET {', '.join(f'{c} = EXCLUDED.{c}' for c in update_cols)}"
        if update_cols
        else "DO NOTHING"
    )
    sql = (
        f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s "
        f"ON CONFLICT ({', '.join(conflict_cols)}) {on_conflict}"
    )
    rows_data = [tuple(row[c] for c in cols) for row in df.to_dict("records")]
    for attempt in range(3):
        try:
            with engine.begin() as conn:
                with conn.connection.cursor() as cur:
                    execute_values(cur, sql, rows_data, page_size=batch_size)
            return len(rows_data)
        except Exception as exc:
            if attempt == 2:
                raise
            import time
            logger.warning("Upsert attempt %d failed: %s", attempt + 1, exc)
            engine.dispose()
            time.sleep(10 * (attempt + 1))
    return 0


# ---------------------------------------------------------------------------
# 1. Asset register
# ---------------------------------------------------------------------------

def ingest_assets(engine) -> int:
    """Fetch AEMO generator registration, extract battery assets → au_bess_assets."""
    logger.info("Fetching AEMO generator registration from %s", _GENERATORS_URL)
    try:
        resp = requests.get(_GENERATORS_URL, headers=_REQUEST_HEADERS, timeout=60)
        resp.raise_for_status()
    except Exception as exc:
        logger.error("Generator registration fetch failed: %s", exc)
        return 0

    try:
        all_sheets = pd.read_excel(io.BytesIO(resp.content), sheet_name=None)
    except Exception as exc:
        logger.error("Excel parse failed: %s", exc)
        return 0

    # Find the "PU and Scheduled Loads" sheet (preferred) or any sheet with DUID
    gen_df = None
    for preferred in ("PU and Scheduled Loads", "Generators and Scheduled Loads"):
        if preferred in all_sheets:
            gen_df = all_sheets[preferred].copy()
            gen_df.columns = [str(c).strip() for c in gen_df.columns]
            break
    if gen_df is None:
        for _sname, sdf in all_sheets.items():
            cols_upper = [str(c).upper() for c in sdf.columns]
            if "DUID" in cols_upper and any("FUEL" in c for c in cols_upper):
                gen_df = sdf.copy()
                gen_df.columns = [str(c).strip() for c in gen_df.columns]
                break

    if gen_df is None:
        logger.warning("No generator sheet found in AEMO registration file")
        return 0

    # Filter for battery/storage technology
    battery_mask = pd.Series(False, index=gen_df.index)
    for col in gen_df.columns:
        col_up = col.upper()
        if any(k in col_up for k in ("FUEL", "TECH", "TYPE", "SOURCE")):
            mask = gen_df[col].astype(str).str.upper().str.contains(
                r"BATTER|STORAGE", na=False, regex=True
            )
            battery_mask |= mask

    batteries = gen_df[battery_mask].copy()
    if batteries.empty:
        logger.warning("No battery assets found — trying broader search")
        # Some registrations use "BATTERY" in station name
        for col in gen_df.columns:
            if "STATION" in col.upper() or "NAME" in col.upper():
                mask = gen_df[col].astype(str).str.upper().str.contains(
                    r"BATTER|STORAGE|GRID STORE", na=False, regex=True
                )
                if mask.any():
                    batteries = gen_df[mask].copy()
                    break

    if batteries.empty:
        logger.warning("No battery assets found in AEMO registration")
        return 0

    def _find_col(partial: str) -> str | None:
        for c in batteries.columns:
            if partial.upper() in c.upper():
                return c
        return None

    duid_col      = _find_col("DUID") or _find_col("Dispatch Unit")
    station_col   = _find_col("Station Name") or _find_col("Station")
    power_col     = _find_col("Reg Cap generation") or _find_col("Registered Capacity") or _find_col("Max Cap generation")
    energy_col    = _find_col("Maximum storage capacity") or _find_col("Energy Storage") or _find_col("Energy Capacity") or _find_col("MWh")
    region_col    = _find_col("NEM Region") or _find_col("Region")
    operator_col  = _find_col("Participant") or _find_col("Company")

    if not duid_col:
        logger.warning("Cannot identify DUID column in %s", list(batteries.columns))
        return 0

    rows = []
    for _, row in batteries.iterrows():
        duid = str(row.get(duid_col, "")).strip()
        if not duid or duid.lower() in ("nan", "none", ""):
            continue

        def _add(ht: str, raw_val):
            v = str(raw_val).strip()
            if v and v.lower() not in ("nan", "none", ""):
                rows.append({
                    "asset": duid,
                    "history_table": ht,
                    "date_from": date(2000, 1, 1),
                    "date_to": None,
                    "value": v,
                })

        if power_col:
            _add("rated_power", row.get(power_col, ""))
        if energy_col:
            _add("energy_capacity", row.get(energy_col, ""))
        if station_col:
            _add("owner", row.get(station_col, ""))
        if operator_col and operator_col != station_col:
            _add("operator", row.get(operator_col, ""))
        if region_col:
            _add("region", row.get(region_col, ""))

    if not rows:
        logger.warning("Parsed 0 asset rows from %d battery records", len(batteries))
        return 0

    n = _upsert(
        engine,
        "intl_market.au_bess_assets",
        pd.DataFrame(rows),
        ["asset", "history_table", "date_from"],
    )
    logger.info("Assets: %d rows upserted for %d DUIDs", n, len(batteries))
    return n


# ---------------------------------------------------------------------------
# 2. Spot prices
# ---------------------------------------------------------------------------

def ingest_spot_prices(engine, start: date, end: date) -> int:
    """Fetch AEMO MMSDM TRADING_PRICE → au_spot_price."""
    total = 0
    for yyyy, mm in _month_range(start, end):
        df = _read_mmsdm_table(yyyy, mm, "TRADING_PRICE")
        if df.empty:
            logger.warning("No TRADING_PRICE data for %04d-%02d", yyyy, mm)
            continue

        # Keep energy market only
        if "PERIODTYPE" in df.columns:
            df = df[df["PERIODTYPE"].str.upper().str.strip() == "ENERGY"]

        if "SETTLEMENTDATE" not in df.columns or "REGIONID" not in df.columns:
            logger.warning(
                "TRADING_PRICE missing expected columns for %04d-%02d: %s",
                yyyy, mm, list(df.columns),
            )
            continue

        records = []
        for _, row in df.iterrows():
            sd, sp = _parse_settlement(row["SETTLEMENTDATE"])
            if not (start <= sd <= end):
                continue
            try:
                rrp = float(row.get("RRP") or 0)
            except (ValueError, TypeError):
                continue
            records.append({
                "settlement_date": sd,
                "settlement_period": sp,
                "region": str(row["REGIONID"]).strip(),
                "spot_price": rrp,
            })

        if records:
            n = _upsert(
                engine,
                "intl_market.au_spot_price",
                pd.DataFrame(records),
                ["settlement_date", "settlement_period", "region"],
            )
            total += n
            logger.info("Spot prices %04d-%02d: %d rows", yyyy, mm, n)

    return total


# ---------------------------------------------------------------------------
# 3. FCAS clearing prices
# ---------------------------------------------------------------------------

_FCAS_PRICE_MAP: dict[str, tuple[str, str]] = {
    "RAISEREGRRP":   ("raise_reg",   "RAISEREGACTUALAVAILABILITY"),
    "LOWERREGRRP":   ("lower_reg",   "LOWERREGACTUALAVAILABILITY"),
    "RAISE6SECRRP":  ("raise_6s",    "RAISE6SECACTUALAVAILABILITY"),
    "RAISE60SECRRP": ("raise_60s",   "RAISE60SECACTUALAVAILABILITY"),
    "RAISE5MINRRP":  ("raise_5min",  "RAISE5MINACTUALAVAILABILITY"),
    "LOWER6SECRRP":  ("lower_6s",    "LOWER6SECACTUALAVAILABILITY"),
    "LOWER60SECRRP": ("lower_60s",   "LOWER60SECACTUALAVAILABILITY"),
    "LOWER5MINRRP":  ("lower_5min",  "LOWER5MINACTUALAVAILABILITY"),
}


def ingest_fcas_prices(engine, start: date, end: date) -> int:
    """Fetch AEMO MMSDM DISPATCHREGIONSUM → au_ancillary_results (daily avg)."""
    total = 0
    for yyyy, mm in _month_range(start, end):
        df = _read_mmsdm_table(yyyy, mm, "DISPATCHREGIONSUM")
        if df.empty:
            logger.warning("No DISPATCHREGIONSUM data for %04d-%02d", yyyy, mm)
            continue

        if "SETTLEMENTDATE" not in df.columns:
            logger.warning(
                "DISPATCHREGIONSUM missing SETTLEMENTDATE for %04d-%02d", yyyy, mm
            )
            continue

        df["_date"] = df["SETTLEMENTDATE"].apply(lambda x: _parse_settlement(x)[0])
        df = df[(df["_date"] >= start) & (df["_date"] <= end)]
        if df.empty:
            continue

        region_col = "REGIONID" if "REGIONID" in df.columns else None
        records = []

        for price_col, (service_name, avail_col) in _FCAS_PRICE_MAP.items():
            if price_col not in df.columns:
                continue
            df[price_col] = pd.to_numeric(df[price_col], errors="coerce")

            group_cols = ["_date"] + ([region_col] if region_col else [])
            agg_dict: dict = {"clearing_price": (price_col, "mean")}
            if avail_col in df.columns:
                df[avail_col] = pd.to_numeric(df[avail_col], errors="coerce")
                agg_dict["volume_mw"] = (avail_col, "mean")

            agg = df[group_cols + [price_col] + ([avail_col] if avail_col in df.columns else [])].groupby(
                group_cols
            ).agg(**agg_dict).reset_index()

            for _, row in agg.iterrows():
                records.append({
                    "settlement_date": row["_date"],
                    "service": service_name,
                    "region": str(row[region_col]).strip() if region_col else "NEM",
                    "clearing_price": (
                        float(row["clearing_price"])
                        if pd.notna(row.get("clearing_price"))
                        else None
                    ),
                    "volume_mw": (
                        float(row["volume_mw"])
                        if "volume_mw" in row.index and pd.notna(row.get("volume_mw"))
                        else None
                    ),
                })

        if records:
            n = _upsert(
                engine,
                "intl_market.au_ancillary_results",
                pd.DataFrame(records),
                ["settlement_date", "service", "region"],
            )
            total += n
            logger.info("FCAS prices %04d-%02d: %d rows", yyyy, mm, n)

    return total


# ---------------------------------------------------------------------------
# 4a. BESS wholesale revenue
# ---------------------------------------------------------------------------

def ingest_bess_wholesale(
    engine,
    start: date,
    end: date,
    battery_duids: set,
    asset_region: dict,
    asset_power: dict,
    asset_capacity: dict,
) -> int:
    """Compute BESS wholesale revenue: TRADINGLOAD × TRADING_PRICE → au_bess_leaderboard."""
    from sqlalchemy import text as sql_text

    total = 0
    for yyyy, mm in _month_range(start, end):
        tl_df = _read_mmsdm_table(
            yyyy, mm, "TRADINGLOAD",
            filter_col="DUID", filter_vals=battery_duids,
        )
        if tl_df.empty:
            logger.warning("No TRADINGLOAD battery rows for %04d-%02d", yyyy, mm)
            continue

        required = {"DUID", "SETTLEMENTDATE", "TOTALCLEARED"}
        if not required.issubset(set(tl_df.columns)):
            logger.warning(
                "TRADINGLOAD missing columns %04d-%02d: found %s",
                yyyy, mm, list(tl_df.columns),
            )
            continue

        tl_df["TOTALCLEARED"] = pd.to_numeric(tl_df["TOTALCLEARED"], errors="coerce").fillna(0.0)
        tl_df["_sd"] = tl_df["SETTLEMENTDATE"].apply(lambda x: _parse_settlement(x)[0])
        tl_df["_sp"] = tl_df["SETTLEMENTDATE"].apply(lambda x: _parse_settlement(x)[1])
        tl_df = tl_df[(tl_df["_sd"] >= start) & (tl_df["_sd"] <= end)]
        if tl_df.empty:
            continue

        # Map DUID → NEM region for price join
        tl_df["_region"] = tl_df["DUID"].map(asset_region)

        # Load spot prices from DB for this month
        m_start = date(yyyy, mm, 1)
        m_end_raw = date(yyyy, mm, 28) + timedelta(days=4)
        m_end = min(m_end_raw, end)
        try:
            with engine.connect() as conn:
                sp_df = pd.read_sql(
                    sql_text(
                        "SELECT settlement_date, settlement_period, region, spot_price "
                        "FROM intl_market.au_spot_price "
                        "WHERE settlement_date BETWEEN :s AND :e"
                    ),
                    conn,
                    params={"s": m_start, "e": m_end},
                )
        except Exception as exc:
            logger.warning(
                "Spot price DB query failed %04d-%02d: %s — run ingest_spot_prices first",
                yyyy, mm, exc,
            )
            continue

        if sp_df.empty:
            logger.warning(
                "No spot prices in DB for %04d-%02d — skipping wholesale revenue",
                yyyy, mm,
            )
            continue

        sp_df["settlement_period"] = sp_df["settlement_period"].astype(int)
        sp_df = sp_df.rename(columns={
            "settlement_date": "_sd",
            "settlement_period": "_sp",
            "region": "_region",
        })

        merged = tl_df.merge(sp_df, on=["_sd", "_sp", "_region"], how="left")
        merged["spot_price"] = pd.to_numeric(merged["spot_price"], errors="coerce").fillna(0.0)
        # revenue = MW × 0.5h × $/MWh  (positive = discharge earns, negative = charge pays)
        merged["_revenue"] = merged["TOTALCLEARED"] * 0.5 * merged["spot_price"]

        daily = (
            merged
            .groupby(["DUID", "_sd"])["_revenue"]
            .sum()
            .reset_index()
            .rename(columns={"DUID": "asset", "_sd": "settlement_date", "_revenue": "revenue"})
        )
        daily["market"] = "wholesale"
        daily["rated_power"] = daily["asset"].map(asset_power)
        daily["energy_capacity"] = daily["asset"].map(asset_capacity)

        n = _upsert(
            engine,
            "intl_market.au_bess_leaderboard",
            daily[["asset", "settlement_date", "market", "revenue", "rated_power", "energy_capacity"]],
            ["asset", "settlement_date", "market"],
        )
        total += n
        logger.info("Wholesale revenue %04d-%02d: %d rows", yyyy, mm, n)

    return total


# ---------------------------------------------------------------------------
# 4b. BESS FCAS revenue
# ---------------------------------------------------------------------------

_FCAS_ENABLEMENT_MAP: dict[str, str] = {
    "RAISEREGMW":  "RAISEREGRRP",
    "LOWERREGMW":  "LOWERREGRRP",
    "RAISE6SECMW": "RAISE6SECRRP",
    "RAISE60SECMW":"RAISE60SECRRP",
    "RAISE5MINMW": "RAISE5MINRRP",
    "LOWER6SECMW": "LOWER6SECRRP",
    "LOWER60SECMW":"LOWER60SECRRP",
    "LOWER5MINMW": "LOWER5MINRRP",
}

# Map each enablement col to the market label we store in leaderboard
_ENABLEMENT_TO_MARKET: dict[str, str] = {
    "RAISEREGMW":  "raise_reg",
    "LOWERREGMW":  "lower_reg",
    "RAISE6SECMW": "raise_contingency",
    "RAISE60SECMW":"raise_contingency",
    "RAISE5MINMW": "raise_contingency",
    "LOWER6SECMW": "lower_contingency",
    "LOWER60SECMW":"lower_contingency",
    "LOWER5MINMW": "lower_contingency",
}


def ingest_bess_fcas(
    engine,
    start: date,
    end: date,
    battery_duids: set,
    asset_region: dict,
    asset_power: dict,
    asset_capacity: dict,
) -> int:
    """Compute BESS FCAS revenue: DISPATCH_UNIT_SOLUTION × DISPATCHREGIONSUM."""
    total = 0
    for yyyy, mm in _month_range(start, end):
        # DISPATCH_UNIT_SOLUTION — stream-filter to battery DUIDs only
        # (reduces ~200 MB download to a few MB of relevant rows)
        dus_df = _read_mmsdm_table(
            yyyy, mm, "DISPATCH_UNIT_SOLUTION",
            filter_col="DUID", filter_vals=battery_duids,
        )
        if dus_df.empty:
            logger.warning("No DISPATCH_UNIT_SOLUTION battery rows for %04d-%02d", yyyy, mm)
            continue

        if "SETTLEMENTDATE" not in dus_df.columns:
            logger.warning(
                "DISPATCH_UNIT_SOLUTION missing SETTLEMENTDATE for %04d-%02d", yyyy, mm
            )
            continue

        # Parse 5-min settlement date
        dus_df["_date"] = dus_df["SETTLEMENTDATE"].apply(lambda x: _parse_settlement(x)[0])
        dus_df = dus_df[(dus_df["_date"] >= start) & (dus_df["_date"] <= end)]
        if dus_df.empty:
            continue

        dus_df["_region"] = dus_df["DUID"].map(asset_region)

        # Convert enablement columns to numeric
        enablement_cols = [c for c in _FCAS_ENABLEMENT_MAP if c in dus_df.columns]
        for col in enablement_cols:
            dus_df[col] = pd.to_numeric(dus_df[col], errors="coerce").fillna(0.0)

        # DISPATCHREGIONSUM — for FCAS prices (5-min)
        drs_df = _read_mmsdm_table(yyyy, mm, "DISPATCHREGIONSUM")
        if drs_df.empty:
            logger.warning("No DISPATCHREGIONSUM for %04d-%02d", yyyy, mm)
            continue

        drs_df["_date"] = drs_df["SETTLEMENTDATE"].apply(lambda x: _parse_settlement(x)[0])
        drs_df = drs_df[(drs_df["_date"] >= start) & (drs_df["_date"] <= end)]

        price_cols = [c for c in _FCAS_ENABLEMENT_MAP.values() if c in drs_df.columns]
        for col in price_cols:
            drs_df[col] = pd.to_numeric(drs_df[col], errors="coerce").fillna(0.0)

        # Merge on SETTLEMENTDATE + region
        region_col = "REGIONID" if "REGIONID" in drs_df.columns else None
        if region_col:
            drs_df = drs_df.rename(columns={region_col: "_region"})
        drs_keep = (
            ["SETTLEMENTDATE", "_region"] + price_cols
            if region_col
            else ["SETTLEMENTDATE"] + price_cols
        )
        drs_df = drs_df[[c for c in drs_keep if c in drs_df.columns]]

        merged = dus_df.merge(
            drs_df,
            on=["SETTLEMENTDATE"] + (["_region"] if region_col else []),
            how="left",
        )

        # Compute per-interval per-service revenue (5/60 h per interval)
        market_revenue: dict[tuple, float] = {}  # (DUID, date, market) → total revenue
        for enab_col in enablement_cols:
            price_col = _FCAS_ENABLEMENT_MAP[enab_col]
            mkt = _ENABLEMENT_TO_MARKET[enab_col]
            if price_col not in merged.columns:
                continue
            rev = merged[enab_col] * pd.to_numeric(merged[price_col], errors="coerce").fillna(0.0) * (5.0 / 60.0)
            merged[f"_rev_{enab_col}"] = rev

        for _, row in merged.iterrows():
            duid = row["DUID"]
            sd = row["_date"]
            for enab_col in enablement_cols:
                mkt = _ENABLEMENT_TO_MARKET[enab_col]
                key = (duid, sd, mkt)
                market_revenue[key] = market_revenue.get(key, 0.0) + float(
                    row.get(f"_rev_{enab_col}", 0.0) or 0.0
                )

        if not market_revenue:
            continue

        lb_rows = []
        for (duid, sd, mkt), rev in market_revenue.items():
            lb_rows.append({
                "asset": duid,
                "settlement_date": sd,
                "market": mkt,
                "revenue": rev,
                "rated_power": asset_power.get(duid),
                "energy_capacity": asset_capacity.get(duid),
            })

        n = _upsert(
            engine,
            "intl_market.au_bess_leaderboard",
            pd.DataFrame(lb_rows),
            ["asset", "settlement_date", "market"],
        )
        total += n
        logger.info("FCAS revenue %04d-%02d: %d rows (%d asset-days)", yyyy, mm, n, len(market_revenue))

    return total


# ---------------------------------------------------------------------------
# 5. Daily and monthly index
# ---------------------------------------------------------------------------

def compute_daily_index(engine, start: date, end: date) -> int:
    """Derive au_bess_daily_index + au_bess_monthly_index from au_bess_leaderboard."""
    from sqlalchemy import text as sql_text

    try:
        with engine.connect() as conn:
            lb = pd.read_sql(
                sql_text(
                    "SELECT asset, settlement_date, market, revenue, "
                    "rated_power, energy_capacity "
                    "FROM intl_market.au_bess_leaderboard "
                    "WHERE settlement_date BETWEEN :s AND :e"
                ),
                conn,
                params={"s": start, "e": end},
            )
    except Exception as exc:
        logger.error("Leaderboard query for index failed: %s", exc)
        return 0

    if lb.empty:
        return 0

    lb["revenue"] = pd.to_numeric(lb["revenue"], errors="coerce").fillna(0.0)
    lb["rated_power"] = pd.to_numeric(lb["rated_power"], errors="coerce")
    lb["energy_capacity"] = pd.to_numeric(lb["energy_capacity"], errors="coerce")

    daily_rows = []
    for (sd, mkt), grp in lb.groupby(["settlement_date", "market"]):
        valid = grp.dropna(subset=["rated_power"])
        if valid.empty:
            continue
        total_rev = valid["revenue"].sum()
        total_mw = valid["rated_power"].sum()
        total_mwh = (
            valid["energy_capacity"].dropna().sum()
            if valid["energy_capacity"].notna().any()
            else total_mw * 2.0
        )
        if total_mw <= 0:
            continue
        daily_rows.append({
            "settlement_date": sd,
            "market": mkt,
            "revenue_permw": float(total_rev / total_mw),
            "revenue_permwh": float(total_rev / total_mwh) if total_mwh > 0 else None,
            "duration": "*",
        })

    if not daily_rows:
        return 0

    n_daily = _upsert(
        engine,
        "intl_market.au_bess_daily_index",
        pd.DataFrame(daily_rows),
        ["settlement_date", "market"],
    )

    # Monthly index: average of daily revenue_permw per month per market
    daily_df = pd.DataFrame(daily_rows)
    daily_df["year_month"] = daily_df["settlement_date"].apply(
        lambda d: d.strftime("%Y-%m")
    )
    monthly_rows = []
    for (ym, mkt), grp in daily_df.groupby(["year_month", "market"]):
        monthly_rows.append({
            "year_month": ym,
            "market": mkt,
            "revenue_permw": float(grp["revenue_permw"].mean()),
            "revenue_permwh": (
                float(grp["revenue_permwh"].dropna().mean())
                if grp["revenue_permwh"].notna().any()
                else None
            ),
            "duration": "*",
        })

    n_monthly = _upsert(
        engine,
        "intl_market.au_bess_monthly_index",
        pd.DataFrame(monthly_rows),
        ["year_month", "market"],
    )

    logger.info(
        "Daily index: %d rows; Monthly index: %d rows", n_daily, n_monthly
    )
    return n_daily + n_monthly


# ---------------------------------------------------------------------------
# Table DDL
# ---------------------------------------------------------------------------

def _ensure_tables(engine) -> None:
    from sqlalchemy import text as sql_text

    ddls = [
        """CREATE TABLE IF NOT EXISTS intl_market.au_bess_assets (
            asset         TEXT NOT NULL,
            history_table TEXT NOT NULL,
            date_from     DATE,
            date_to       DATE,
            value         TEXT,
            UNIQUE (asset, history_table, date_from)
        )""",
        """CREATE TABLE IF NOT EXISTS intl_market.au_bess_daily_index (
            settlement_date DATE NOT NULL,
            market          TEXT NOT NULL,
            revenue_permw   NUMERIC,
            revenue_permwh  NUMERIC,
            duration        TEXT,
            PRIMARY KEY (settlement_date, market)
        )""",
        """CREATE TABLE IF NOT EXISTS intl_market.au_bess_monthly_index (
            year_month     TEXT NOT NULL,
            market         TEXT NOT NULL,
            revenue_permw  NUMERIC,
            revenue_permwh NUMERIC,
            duration       TEXT,
            PRIMARY KEY (year_month, market)
        )""",
        """CREATE TABLE IF NOT EXISTS intl_market.au_bess_leaderboard (
            asset            TEXT NOT NULL,
            settlement_date  DATE NOT NULL,
            market           TEXT NOT NULL,
            revenue          NUMERIC,
            rated_power      NUMERIC,
            energy_capacity  NUMERIC,
            PRIMARY KEY (asset, settlement_date, market)
        )""",
        """CREATE TABLE IF NOT EXISTS intl_market.au_spot_price (
            settlement_date   DATE NOT NULL,
            settlement_period INT  NOT NULL,
            region            TEXT NOT NULL DEFAULT 'NEM',
            spot_price        NUMERIC,
            PRIMARY KEY (settlement_date, settlement_period, region)
        )""",
        """CREATE TABLE IF NOT EXISTS intl_market.au_ancillary_results (
            settlement_date DATE NOT NULL,
            service         TEXT NOT NULL,
            region          TEXT NOT NULL DEFAULT 'NEM',
            clearing_price  NUMERIC,
            volume_mw       NUMERIC,
            PRIMARY KEY (settlement_date, service, region)
        )""",
    ]
    with engine.begin() as conn:
        for ddl in ddls:
            conn.execute(sql_text(ddl))


# ---------------------------------------------------------------------------
# Asset metadata loader (shared across ingest functions)
# ---------------------------------------------------------------------------

def _load_asset_metadata(engine) -> tuple[set, dict, dict, dict]:
    """Return (battery_duids, asset_region, asset_power, asset_capacity)."""
    from sqlalchemy import text as sql_text

    with engine.connect() as conn:
        rp = pd.read_sql(
            sql_text(
                "SELECT DISTINCT ON (asset) asset, value::numeric AS v "
                "FROM intl_market.au_bess_assets WHERE history_table='rated_power' "
                "ORDER BY asset, date_from DESC NULLS LAST"
            ),
            conn,
        )
        ec = pd.read_sql(
            sql_text(
                "SELECT DISTINCT ON (asset) asset, value::numeric AS v "
                "FROM intl_market.au_bess_assets WHERE history_table='energy_capacity' "
                "ORDER BY asset, date_from DESC NULLS LAST"
            ),
            conn,
        )
        reg = pd.read_sql(
            sql_text(
                "SELECT DISTINCT ON (asset) asset, value AS v "
                "FROM intl_market.au_bess_assets WHERE history_table='region' "
                "ORDER BY asset, date_from DESC NULLS LAST"
            ),
            conn,
        )

    battery_duids = set(rp["asset"].tolist())
    asset_power   = dict(zip(rp["asset"], rp["v"]))
    asset_capacity = dict(zip(ec["asset"], ec["v"]))
    asset_region  = dict(zip(reg["asset"], reg["v"]))
    return battery_duids, asset_region, asset_power, asset_capacity


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

def run_ingestion(
    start: date,
    end: date,
    only: list[str] | None = None,
) -> dict[str, int]:
    from services.common.db_utils import get_engine

    engine = get_engine()
    _ensure_tables(engine)

    results: dict[str, int] = {}

    def _run(key: str, fn):
        if only and key not in only:
            return
        logger.info("[aemo/%s] starting…", key)
        try:
            n = fn()
            results[key] = n
            logger.info("[aemo/%s] done: %d rows", key, n)
        except Exception as exc:
            results[key] = 0
            logger.error("[aemo/%s] ERROR: %s", key, exc, exc_info=True)

    _run("assets", lambda: ingest_assets(engine))

    _run("spot_price", lambda: ingest_spot_prices(engine, start, end))
    _run("fcas_price", lambda: ingest_fcas_prices(engine, start, end))

    # Load asset metadata once for revenue functions
    battery_duids: set = set()
    asset_region: dict = {}
    asset_power: dict = {}
    asset_capacity: dict = {}
    if not only or any(k in only for k in ("bess_wholesale", "bess_fcas", "daily_index")):
        try:
            battery_duids, asset_region, asset_power, asset_capacity = _load_asset_metadata(engine)
            logger.info("Asset metadata loaded: %d battery DUIDs", len(battery_duids))
        except Exception as exc:
            logger.error("Asset metadata load failed: %s", exc)

    if battery_duids:
        _run(
            "bess_wholesale",
            lambda: ingest_bess_wholesale(
                engine, start, end,
                battery_duids, asset_region, asset_power, asset_capacity,
            ),
        )
        _run(
            "bess_fcas",
            lambda: ingest_bess_fcas(
                engine, start, end,
                battery_duids, asset_region, asset_power, asset_capacity,
            ),
        )

    _run("daily_index", lambda: compute_daily_index(engine, start, end))

    return results


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="AEMO NEM data ingestion")
    parser.add_argument(
        "--start",
        default=(date.today() - timedelta(days=1)).isoformat(),
        help="Start date YYYY-MM-DD",
    )
    parser.add_argument(
        "--end",
        default=(date.today() - timedelta(days=1)).isoformat(),
        help="End date YYYY-MM-DD",
    )
    parser.add_argument(
        "--only",
        help="Comma-separated tasks: assets,spot_price,fcas_price,bess_wholesale,bess_fcas,daily_index",
    )
    args = parser.parse_args()
    only = args.only.split(",") if args.only else None

    from dotenv import load_dotenv
    load_dotenv(str(Path(__file__).parent.parent.parent / "config" / ".env"))

    results = run_ingestion(
        date.fromisoformat(args.start),
        date.fromisoformat(args.end),
        only=only,
    )
    print("Results:", results)
