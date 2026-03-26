# -*- coding: utf-8 -*-
"""
Province-or-Node downloader for market data
==========================================

What this script adds (vs your original):
- **Province alias mode**: `Column_to_Matrix("", "Anhui")` will fetch using a default
  node under Anhui (to get the full column set) but **only write non-node fields**
  into 15-minute tables named like `hist_anhui_<field>_15min`.
- **Node mode** (unchanged): `Column_to_Matrix("", "Anhui_DingYuan")` writes node
  prices (actual/forecast + day-ahead) in both 15-min and hourly-matrix tables, and
  also writes all other non-node fields as separate 15-min tables for that market
  (e.g., `hist_anhui_dingyuan_<field>_15min`).
- **Time alignment**: Non-Southern Grid provinces return 00:15→24:00 and are shifted
  by −15 minutes to become 00:00→23:45. Southern Grid (Yunnan=53, Guangxi=45,
  Guizhou=52, Guangdong=44) are not shifted.
- **Forecast name synonyms**: accepts both `...PriceForecast` and `...ForecastPrice`.
- **ECS-friendly entrypoint**: use `main()` plus `MARKET_LIST` / `PROVINCE_MARKETS` /
  `NODE_MARKETS` env vars instead of hardcoded desktop execution.
- **RDS-friendly incremental window**: when not running full history, the script can
  infer a safe overlap window from existing Postgres `hist_*` tables instead of local
  desktop state.

Env vars required:
  APP_KEY, APP_SECRET
  DB_DSN  (preferred)   or
  DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME
Optional:
  FULL_HISTORY, HIST_EARLIEST, HIST_CHUNK_DAYS, HIST_STOP_EMPTY, HIST_SLEEP_SEC
  HIST_START_DATE, HIST_END_DATE
  MARKET_LIST / MARKETS / PROVINCE_MARKETS / NODE_MARKETS
  DB_LOOKBACK_DAYS
  RUN_INHOUSE_WIND
  <PROVINCE>_DEFAULT_ASSET_ID   for province aliases without a default node
  <PROVINCE>_DEFAULT_ASSET_NAME for province aliases without a default node

Usage examples:
  Column_to_Matrix("", "Anhui")            # province alias → write only non-node 15-min tables
  Column_to_Matrix("", "Anhui_DingYuan")   # node mode → node + DA + misc

  # ECS / CLI entrypoint
  MARKET_LIST=Mengxi,Anhui,Shandong python province_misc_to_db_v2.py
"""
from __future__ import annotations

import ast
import json
import logging
import os
import re
import time
from typing import Iterable

import pandas as pd
from sqlalchemy import create_engine, text

try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

try:
    from poseidon import poseidon
except Exception as e:
    raise SystemExit("Poseidon SDK not found. Install: pip install enos-poseidon==0.1.6\n" + str(e))

logger = logging.getLogger(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

# ---- API / DB config ----
APIM_URL = "https://app-portal-cn-ft.enos-iot.com/tt-daas-service/v1/market/clear/data"
PROVINCE_CODE = "15"  # default (Mengxi)


def env_flag(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return str(val).strip().strip('"').strip("'").lower() in ("1", "true", "yes", "on")


def env_csv(name: str) -> list[str]:
    raw = os.getenv(name, "")
    if not raw:
        return []
    return [x.strip() for x in raw.split(",") if x.strip()]


DB_OVERWRITE_ALL = env_flag("DB_OVERWRITE_ALL", False)
RUN_INHOUSE_WIND = env_flag("RUN_INHOUSE_WIND", True)
DB_LOOKBACK_DAYS = int(os.getenv("DB_LOOKBACK_DAYS", "2"))
DEFAULT_INCREMENTAL_DAYS = int(os.getenv("DEFAULT_INCREMENTAL_DAYS", "7"))

DB_DSN = os.getenv("DB_DSN") or os.getenv("PGURL")

DB_DEFAULTS = {
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "root"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5433"),
    "name": os.getenv("DB_NAME", "marketdata"),
}

# Southern Grid provinces: do NOT shift by −15 min
SOUTH_GRID_PROV = {"53", "45", "52", "44"}  # Yunnan, Guangxi, Guizhou, Guangdong

# --- In-house wind APIM (env-overridable) ---
INHOUSE_APIM_HOST = os.getenv("INHOUSE_APIM_HOST", "https://ag-cn5.envisioniot.com")
INHOUSE_APP_KEY = os.getenv("INHOUSE_APP_KEY")
INHOUSE_APP_SECRET = os.getenv("INHOUSE_APP_SECRET")
INHOUSE_PATH = "/grid-forecast-api/v1.0.0/joint-result"  # GET

# Crawl toggles
HIST_START_DATE = os.getenv("HIST_START_DATE")
HIST_END_DATE = os.getenv("HIST_END_DATE")

FULL_HISTORY = env_flag("FULL_HISTORY", True)
HIST_EARLIEST = os.getenv("HIST_EARLIEST", "2020-01-01")
HIST_CHUNK_DAYS = int(os.getenv("HIST_CHUNK_DAYS", "7"))
HIST_STOP_EMPTY = int(os.getenv("HIST_STOP_EMPTY", "8"))
HIST_SLEEP_SEC = float(os.getenv("HIST_SLEEP_SEC", "0.3"))

# -------------------- MARKET MAPS --------------------
MARKET_MAP = {
    # Mengxi (15)
    "Mengxi_SuYou": {"asset_name": "SuYou", "asset_id": "zTqaBeNI", "tbl_clear": "hist_mengxi_suyou_clear", "tbl_fcst": "hist_mengxi_suyou_forecast", "province_code": "15"},
    "Mengxi_WuLaTe": {"asset_name": "WuLaTe", "asset_id": "wHqvXeTB", "tbl_clear": "hist_mengxi_wulate_clear", "tbl_fcst": "hist_mengxi_wulate_forecast", "province_code": "15"},
    "Mengxi_WuHai": {"asset_name": "WuHai", "asset_id": "aQeaYeJK", "tbl_clear": "hist_mengxi_wuhai_clear", "tbl_fcst": "hist_mengxi_wuhai_forecast", "province_code": "15"},
    "Mengxi_WuLanChaBu": {"asset_name": "WuLanChaBu", "asset_id": "LJAJkAeU", "tbl_clear": "hist_mengxi_wulanchabu_clear", "tbl_fcst": "hist_mengxi_wulanchabu_forecast", "province_code": "15"},

    # Shandong (37)
    "Shandong_BinZhou": {"asset_name": "BinZhou", "asset_id": "I3j1Fnsm", "tbl_clear": "hist_shandong_binzhou_clear", "tbl_fcst": "hist_shandong_binzhou_forecast", "province_code": "37"},

    # Anhui (34)
    "Anhui_DingYuan": {"asset_name": "DingYuan", "asset_id": "eKqaXeEN", "tbl_clear": "hist_anhui_dingyuan_clear", "tbl_fcst": "hist_anhui_dingyuan_forecast", "province_code": "34"},

    # Jiangsu (32)
    "Jiangsu_SheYang": {"asset_name": "SheYang", "asset_id": "IXwfDSsY", "tbl_clear": "hist_jiangsu_sheyang_clear", "tbl_fcst": "hist_jiangsu_sheyang_forecast", "province_code": "32"},
}

# Province alias → delegate node & province code
PROVINCE_ALIAS_MAP = {
    "Anhui": {"province_code": "34", "default_market_key": "Anhui_DingYuan"},
    "Shandong": {"province_code": "37", "default_market_key": "Shandong_BinZhou"},
    "Jiangsu": {"province_code": "32", "default_market_key": "Jiangsu_SheYang"},
    "Mengxi": {"province_code": "15", "default_market_key": "Mengxi_SuYou"},
    "Guangxi": {"province_code": "45", "default_market_key": None},
}

# 省调网格（in-house接口要求的grid）
PROVINCE_TO_GRID = {
    "Mengxi": "NEI_MENG_WEST",
    "Shandong": "SHAN_DONG",
    "Guangxi": "GUANG_XI",
    "Yunnan": "YUN_NAN",
    "Gansu": "GAN_SU",
    "Shanxi": "SHAN_XI",
}

# 每省默认策略
PROVINCE_DEFAULT_STRATEGY = {
    "Mengxi": "ACTUAL_FORECAST",
    "Shandong": "ACTUAL_FORECAST",
    "Guangxi": "ACTUAL_FORECAST",
    "Yunnan": "ACTUAL_FORECAST",
    "Gansu": "AFTER_CLEANING_FORECAST",
    "Shanxi": "AFTER_CLEANING_FORECAST",
}


def _match_province_key(user_key: str):
    if not user_key:
        return None
    normalized = re.sub(r"[^a-zA-Z0-9]+", "", user_key).lower()
    for name in PROVINCE_ALIAS_MAP.keys():
        if re.sub(r"[^a-zA-Z0-9]+", "", name).lower() == normalized:
            return name
    return None


# Prefer explicit API keys per asset; fall back to generic names if needed
ASSET_API_KEYS = {
    "SuYou": {"actual": "realTimeClearPrice_zTqaBeNI", "forecast": "realTimePriceForecast_zTqaBeNI"},
    "WuLaTe": {"actual": "realTimeClearPrice_wHqvXeTB", "forecast": "realTimePriceForecast_wHqvXeTB"},
    "WuHai": {"actual": "realTimeClearPrice_aQeaYeJK", "forecast": "realTimePriceForecast_aQeaYeJK"},
    "WuLanChaBu": {"actual": "realTimeClearPrice_LJAJkAeU", "forecast": "realTimePriceForecast_LJAJkAeU"},
    "BinZhou": {"actual": "realTimeClearPrice_I3j1Fnsm", "forecast": "realTimePriceForecast_I3j1Fnsm", "actual_DA": "dayAheadClearPrice_I3j1Fnsm", "forecast_DA": "dayAheadPriceForecast_I3j1Fnsm"},
    "DingYuan": {"actual": "realTimeClearPrice_eKqaXeEN", "forecast": "realTimePriceForecast_eKqaXeEN", "actual_DA": "dayAheadClearPrice_eKqaXeEN", "forecast_DA": "dayAheadPriceForecast_eKqaXeEN"},
    "SheYang": {"actual": "realTimeClearPrice_jiangBei_IXwfDSsY", "forecast": "realTimePriceForecast_jiangBei_IXwfDSsY", "actual_DA": "dayAheadClearPrice_jiangBei_IXwfDSsY", "forecast_DA": "dayAheadPriceForecast_jiangBei_IXwfDSsY"},
}
GENERIC_KEYS = {"actual": "realTimeClearPrice", "forecast": "realTimePriceForecast"}


# -------------------- helpers --------------------
def _truncate_all_hist_tables(engine):
    sql = """
    DO $$
    DECLARE
        r RECORD;
    BEGIN
        FOR r IN
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = 'public'
              AND tablename LIKE 'hist_%'
        LOOP
            EXECUTE format('TRUNCATE TABLE %I CASCADE', r.tablename);
        END LOOP;
    END $$;
    """
    with engine.begin() as conn:
        conn.execute(text(sql))


def _list_hist_tables(engine, namespace_prefix: str) -> list[str]:
    pattern = f"hist_{namespace_prefix.lower()}%"
    sql = text("""
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'
          AND tablename ILIKE :pattern
        ORDER BY tablename
    """)
    with engine.begin() as conn:
        rows = conn.execute(sql, {"pattern": pattern}).fetchall()
    return [r[0] for r in rows]


def _get_existing_max_timestamp(engine, namespace_prefix: str):
    max_seen = None
    for table_name in _list_hist_tables(engine, namespace_prefix):
        col_sql = text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = :table_name
              AND column_name IN ('time', 'date')
            ORDER BY CASE WHEN column_name = 'time' THEN 0 ELSE 1 END
        """)
        with engine.begin() as conn:
            col_rows = conn.execute(col_sql, {"table_name": table_name}).fetchall()

        if not col_rows:
            continue

        col_name = col_rows[0][0]
        with engine.begin() as conn:
            value = conn.execute(text(f'SELECT MAX("{col_name}") FROM "{table_name}"')).scalar()

        if value is None:
            continue

        ts = pd.to_datetime(value, errors="coerce")
        if pd.isna(ts):
            continue

        if max_seen is None or ts > max_seen:
            max_seen = ts

    return max_seen


def _resolve_time_window(engine=None, namespace_prefix: str | None = None):
    """
    Priority:
    1) Explicit HIST_START_DATE / HIST_END_DATE
    2) FULL_HISTORY + HIST_EARLIEST
    3) DB-backed incremental window from existing hist_* tables
    4) Default incremental (last N days)
    """
    today = pd.Timestamp.today().normalize().date()

    if HIST_START_DATE and HIST_END_DATE:
        return (
            pd.to_datetime(HIST_START_DATE).date(),
            pd.to_datetime(HIST_END_DATE).date(),
        )

    if FULL_HISTORY:
        return (
            pd.to_datetime(HIST_EARLIEST).date(),
            today,
        )

    if engine is not None and namespace_prefix:
        max_ts = _get_existing_max_timestamp(engine, namespace_prefix)
        if max_ts is not None:
            start = (pd.to_datetime(max_ts).normalize() - pd.Timedelta(days=DB_LOOKBACK_DAYS)).date()
            floor = pd.to_datetime(HIST_EARLIEST).date()
            start = max(start, floor)
            logger.info(
                "Resolved incremental window from DB for %s: %s -> %s (max existing ts=%s)",
                namespace_prefix,
                start,
                today,
                max_ts,
            )
            return start, today

    return (
        today - pd.Timedelta(days=DEFAULT_INCREMENTAL_DAYS),
        today,
    )


def _need_shift_minus_15(province_code: str) -> bool:
    return str(province_code) not in SOUTH_GRID_PROV


def _sanitize_ident(name: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9_]+", "_", str(name)).lower()
    return s[:60]


def _make_df15(df_long: pd.DataFrame, province_code: str) -> pd.DataFrame:
    out = df_long.copy()
    if _need_shift_minus_15(province_code):
        out["time"] = out["time"] - pd.Timedelta(minutes=15)
    return out


def _db_engine():
    if DB_DSN:
        return create_engine(DB_DSN, pool_pre_ping=True)

    url = (
        f"postgresql+psycopg2://{DB_DEFAULTS['user']}:{DB_DEFAULTS['password']}"
        f"@{DB_DEFAULTS['host']}:{DB_DEFAULTS['port']}/{DB_DEFAULTS['name']}"
    )
    return create_engine(url, pool_pre_ping=True)


def _ensure_15min_table(engine, table_name: str):
    ddl = f'''
    CREATE TABLE IF NOT EXISTS "{table_name}" (
        time TIMESTAMP PRIMARY KEY,
        price DOUBLE PRECISION
    );
    '''
    with engine.begin() as conn:
        conn.execute(text(ddl))


def _chunked_delete_time(conn, table_name: str, times: list):
    if not times:
        return
    chunk = 1000
    for i in range(0, len(times), chunk):
        batch = times[i:i + chunk]
        placeholders = ",".join([f":t{i + j}" for j in range(len(batch))])
        params = {f"t{i + j}": t for j, t in enumerate(batch)}
        conn.execute(text(f'DELETE FROM "{table_name}" WHERE time IN ({placeholders})'), params)


def _replace_15min_timevalue(engine, table_name: str, df_pair: pd.DataFrame) -> int:
    if df_pair.empty:
        return 0
    sub = df_pair[["time", "price"]].dropna(subset=["time", "price"]).copy()
    if sub.empty:
        return 0
    sub["time"] = pd.to_datetime(sub["time"], errors="coerce")
    sub = (
        sub.dropna(subset=["time", "price"])
        .drop_duplicates(subset=["time"], keep="last")
        .sort_values("time")
    )

    _ensure_15min_table(engine, table_name)
    times = sub["time"].tolist()
    with engine.begin() as conn:
        _chunked_delete_time(conn, table_name, times)
        sub.to_sql(table_name, con=conn, if_exists="append", index=False, method="multi", chunksize=10000)
    return len(sub)


def _ensure_matrix_table(engine, table_name: str):
    cols = ",\n".join([f'"Hour_{str(i).zfill(2)}" DOUBLE PRECISION' for i in range(24)])
    ddl = f'''
    CREATE TABLE IF NOT EXISTS "{table_name}" (
        date DATE PRIMARY KEY,
        {cols}
    );
    '''
    with engine.begin() as conn:
        conn.execute(text(ddl))


def _chunked_delete(conn, table_name: str, dates: list):
    if not dates:
        return
    chunk = 100
    for i in range(0, len(dates), chunk):
        batch = dates[i:i + chunk]
        placeholders = ",".join([f":d{i + j}" for j in range(len(batch))])
        params = {f"d{i + j}": d for j, d in enumerate(batch)}
        conn.execute(text(f'DELETE FROM "{table_name}" WHERE date IN ({placeholders})'), params)


def _replace_matrix(engine, tbl: str, mat_df: pd.DataFrame, metric_name: str):
    subset = mat_df[mat_df["metric"] == metric_name].copy()
    if subset.empty:
        return 0
    keep = ["date"] + [c for c in subset.columns if c.startswith("Hour_")]
    out = subset[keep].drop_duplicates(subset=["date"]).sort_values("date")
    _ensure_matrix_table(engine, tbl)
    out["date"] = pd.to_datetime(out["date"]).dt.date
    dates = out["date"].tolist()
    with engine.begin() as conn:
        _chunked_delete(conn, tbl, dates)
        out.to_sql(tbl, con=conn, if_exists="append", index=False, method="multi", chunksize=1000)
    return len(out)


def _shift15_pivot_hour(df_long: pd.DataFrame) -> pd.DataFrame:
    w = df_long.copy()
    w["shifted_time"] = w["time"] - pd.Timedelta(minutes=15)
    w["date"] = w["shifted_time"].dt.date
    w["hour"] = w["shifted_time"].dt.hour
    hourly = w.groupby(["metric", "date", "hour"], as_index=False)["price"].mean()
    mat = hourly.pivot_table(index=["metric", "date"], columns="hour", values="price")
    mat = mat.sort_index()
    mat.columns = [f"Hour_{str(c).zfill(2)}" for c in mat.columns]
    return mat.reset_index()


# -------------------- API helpers --------------------
def _parse_resp(resp_obj):
    if resp_obj is None:
        raise RuntimeError("API returned no body (None). Check APP_KEY/APP_SECRET, assetId, provinceCode, and network.")
    text_ = resp_obj.decode("utf-8", errors="replace") if isinstance(resp_obj, (bytes, bytearray)) else str(resp_obj)
    try:
        return json.loads(text_)
    except json.JSONDecodeError:
        pass
    try:
        obj = ast.literal_eval(text_)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass
    try:
        fixed = text_.replace("'", '"')
        return json.loads(fixed)
    except Exception as e:
        raise RuntimeError(f"Unexpected response (not JSON-like): {text_[:600]} ...") from e


def _fetch(app_key, app_secret, asset_id, start_day, end_day, province_code=PROVINCE_CODE, timeout=30.0):
    payload = {
        "provinceCode": str(province_code),
        "assetId": str(asset_id),
        "startDay": str(start_day),
        "endDay": str(end_day),
    }
    headers = {"Content-Type": "application/json"}
    try:
        resp = poseidon.urlopen(
            app_key,
            app_secret,
            APIM_URL,
            payload,
            headers,
            method="POST",
            timeout=timeout,
            content_type="application/json",
        )
    except TypeError:
        resp = poseidon.urlopen(
            app_key,
            app_secret,
            APIM_URL,
            payload,
            headers,
            method="POST",
            timeout=timeout,
        )
    return _parse_resp(resp)


def _normalize(resp_dict) -> pd.DataFrame:
    rows = resp_dict.get("data") or []
    if not isinstance(rows, list):
        return pd.DataFrame(columns=["time"])
    df = pd.DataFrame.from_records(rows)
    if "time" in df.columns:
        if df["time"].dtype == object:
            mask_24 = df["time"].astype(str).str.endswith("24:00:00")
            if mask_24.any():
                def fix_24(s: str) -> str:
                    if s.endswith("24:00:00"):
                        d = pd.to_datetime(s[:10], errors="coerce")
                        if pd.notna(d):
                            return (d + pd.Timedelta(days=1)).strftime("%Y-%m-%d") + " 00:00:00"
                    return s
                df["time"] = df["time"].astype(str).map(fix_24)
        df["time"] = pd.to_datetime(df["time"], errors="coerce")
        df = df.sort_values("time").reset_index(drop=True)
    return df


def _fetch_inhouse_wind(grid: str, start_date: str, length: int, strategy: str = "ACTUAL_FORECAST") -> pd.DataFrame:
    """
    Calls /grid-forecast-api/v1.0.0/joint-result using GET + query-string.
    Returns a 15-min DataFrame with Asia/Shanghai naive timestamps.
    """
    if not INHOUSE_APP_KEY or not INHOUSE_APP_SECRET:
        raise RuntimeError("INHOUSE_APP_KEY/INHOUSE_APP_SECRET not set for in-house wind fetch.")

    from urllib.parse import urlencode

    qs = {
        "grid": grid,
        "strategy": strategy,
        "startDate": start_date,
        "length": str(int(length)),
        "domain": "WIND",
    }
    base = INHOUSE_APIM_HOST.rstrip("/")
    url = f"{base}{INHOUSE_PATH}?{urlencode(qs)}"

    raw = poseidon.urlopen(INHOUSE_APP_KEY, INHOUSE_APP_SECRET, url, None, {}, method="GET", timeout=30.0)
    text_resp = raw.decode("utf-8", "replace") if isinstance(raw, (bytes, bytearray)) else str(raw)
    text_resp = text_resp.strip()

    def _parse_relaxed(s: str):
        try:
            return json.loads(s)
        except json.JSONDecodeError:
            pass
        if s.startswith("{'") or s.startswith("['") or s.replace(" ", "").startswith("{'startTime'"):
            try:
                return json.loads(s.replace("'", '"'))
            except Exception:
                pass
        try:
            obj = ast.literal_eval(s)
            if isinstance(obj, (dict, list)):
                return obj
        except Exception:
            pass
        return None

    obj = _parse_relaxed(text_resp)

    if not isinstance(obj, dict) or ("powers" not in obj and "startTime" not in obj):
        qs2 = {
            "grid": grid,
            "strategy": strategy,
            "startDate": start_date,
            "length": str(int(length)),
        }
        url2 = f"{INHOUSE_APIM_HOST.rstrip('/')}{INHOUSE_PATH}?{urlencode(qs2)}"
        raw2 = poseidon.urlopen(INHOUSE_APP_KEY, INHOUSE_APP_SECRET, url2, None, {}, method="GET", timeout=30.0)
        text2 = raw2.decode("utf-8", "replace") if isinstance(raw2, (bytes, bytearray)) else str(raw2)
        obj = _parse_relaxed(text2.strip())

    if not isinstance(obj, dict):
        raise ValueError(f"in-house response not JSON-like; head={text_resp[:120]!r}")

    if "code" in obj and "message" in obj and "powers" not in obj:
        raise ValueError(f"in-house API error: code={obj.get('code')}, message={obj.get('message')}")

    powers = obj.get("powers") or []
    if not powers:
        return pd.DataFrame(columns=["time", "price"])

    start_time_utc = pd.to_datetime(obj["startTime"], utc=True, errors="raise")
    rng_utc = pd.date_range(start_time_utc, periods=len(powers), freq="15min")
    rng_local = rng_utc.tz_convert("Asia/Shanghai").tz_localize(None)
    df = pd.DataFrame({"time": rng_local, "price": pd.to_numeric(powers, errors="coerce")})
    return df.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)


def _write_inhouse_wind(engine, province_name: str, start_date: pd.Timestamp, end_date: pd.Timestamp) -> int:
    """
    Download in-house wind forecast for [start_date, end_date) and write into:
      hist_<province>_inhouse_windforecast_15min
    Returns total rows inserted.
    """
    prov = province_name
    grid = PROVINCE_TO_GRID.get(prov)
    if not grid:
        logger.warning("In-house wind: province '%s' is not mapped to a grid; skipped.", prov)
        return 0

    strategy = PROVINCE_DEFAULT_STRATEGY.get(prov, "ACTUAL_FORECAST")
    start_d = pd.to_datetime(start_date).normalize().date()
    end_d = pd.to_datetime(end_date).normalize().date()
    cursor = start_d
    total = 0
    tbl = f"hist_{prov.lower()}_inhouse_windforecast_15min"

    while cursor < end_d:
        span_days = min((end_d - cursor).days, 31)
        if span_days <= 0:
            break
        length = span_days * 96
        df15 = _fetch_inhouse_wind(grid, cursor.isoformat(), length, strategy=strategy)
        n = _replace_15min_timevalue(engine, tbl, df15)
        total += n
        cursor = (pd.to_datetime(cursor) + pd.Timedelta(days=span_days)).date()

    logger.info("%s: in-house wind forecast rows -> %s=%s", prov, tbl, total)
    return total


# -------------------- series pickers --------------------
def _pick_series(df: pd.DataFrame, asset_name: str, which: str, province_code: str):
    """
    Choose the best series with real data.
    Accepts both ...PriceForecast and ...ForecastPrice.
    Returns: (series, used_key, candidates_tried)
    """
    keys = ASSET_API_KEYS.get(asset_name, {}) or {}
    region_generics_actual = ["hubaoxiRealTimeClearPrice", "hubaoxiDayAheadClearPrice"] if province_code == "15" else []
    region_generics_fcst = [
        "hubaoxiRealTimePriceForecast", "hubaoxiDayAheadPriceForecast",
        "hubaoxiRealTimeForecastPrice", "hubaoxiDayAheadForecastPrice",
    ] if province_code == "15" else []

    generic_actual = ["realTimeClearPrice", "dayAheadClearPrice"]
    generic_forecast = [
        "realTimePriceForecast", "dayAheadPriceForecast",
        "realTimeForecastPrice", "dayAheadForecastPrice",
    ]

    if which == "actual":
        candidates = [keys.get("actual"), keys.get("actual_DA")] + generic_actual + region_generics_actual
    else:
        candidates = [keys.get("forecast"), keys.get("forecast_DA")] + generic_forecast + region_generics_fcst

    def has_data(s: pd.Series) -> bool:
        if s is None:
            return False
        t = s
        if t.dtype == object:
            t = t.replace(r"^\s*$", pd.NA, regex=True)
        return t.notna().sum() > 0

    for k in candidates:
        if k and k in df.columns and has_data(df[k]):
            return df[k], k, candidates

    cols = list(df.columns)
    if which == "actual":
        picks = [c for c in cols if c.lower().endswith("realtimeclearprice") or c.lower().endswith("dayaheadclearprice")]
    else:
        picks = [c for c in cols if c.lower().endswith("realtimepriceforecast") or c.lower().endswith("dayaheadpriceforecast")
                                or c.lower().endswith("realtimeforecastprice") or c.lower().endswith("dayaheadforecastprice")]
    for p in picks:
        s = df[p]
        if has_data(s):
            return s, p, candidates + ["<pattern>"]

    return pd.Series([None] * len(df), index=df.index, dtype="float64"), None, candidates


def _pick_series_by_names(df: pd.DataFrame, names: list[str]):
    def has_data(s: pd.Series) -> bool:
        if s is None:
            return False
        t = s
        if t.dtype == object:
            t = t.replace(r"^\s*$", pd.NA, regex=True)
        return t.notna().sum() > 0

    for k in names:
        if k and k in df.columns and has_data(df[k]):
            return df[k], k

    return pd.Series([None] * len(df), index=df.index, dtype="float64"), None


def _resolve_delegate_asset(province_name: str):
    entry = PROVINCE_ALIAS_MAP[province_name]
    delegate_key = entry.get("default_market_key")

    if delegate_key:
        market_info = MARKET_MAP[delegate_key]
        return market_info["asset_name"], market_info["asset_id"]

    env_prefix = re.sub(r"[^A-Za-z0-9]+", "_", province_name).upper()
    asset_id = os.getenv(f"{env_prefix}_DEFAULT_ASSET_ID") or os.getenv("DEFAULT_PROVINCE_ASSET_ID")
    asset_name = os.getenv(f"{env_prefix}_DEFAULT_ASSET_NAME") or province_name

    if not asset_id:
        raise ValueError(
            f"Province alias '{province_name}' has no default delegate node. "
            f"Set {env_prefix}_DEFAULT_ASSET_ID (and optionally {env_prefix}_DEFAULT_ASSET_NAME) in ECS env vars."
        )

    return asset_name, asset_id


def Column_to_Matrix(pricefile: str, market: str):
    # Province alias mode?
    province_name = _match_province_key(market)
    province_only = False

    if province_name:
        province_only = True
        prov_entry = PROVINCE_ALIAS_MAP[province_name]
        prov_code = prov_entry["province_code"]
        asset_name, asset_id = _resolve_delegate_asset(province_name)
        tables_namespace = province_name
    else:
        if market not in MARKET_MAP and market.lower() in [k.lower() for k in MARKET_MAP.keys()]:
            market = [k for k in MARKET_MAP.keys() if k.lower() == market.lower()][0]
        if market not in MARKET_MAP:
            raise ValueError(f"Unsupported market: {market}")

        m = MARKET_MAP[market]
        asset_name = m["asset_name"]
        asset_id = m["asset_id"]
        prov_code = m.get("province_code", PROVINCE_CODE)
        tables_namespace = market
        tbl_clear = m["tbl_clear"]
        tbl_fcst = m["tbl_fcst"]

    app_key = os.getenv("APP_KEY")
    app_secret = os.getenv("APP_SECRET")
    if not app_key or not app_secret:
        logger.error("APP_KEY / APP_SECRET environment variables are required.")
        return

    engine = _db_engine()

    if DB_OVERWRITE_ALL:
        logger.warning("Truncating ALL hist_* tables before load")
        _truncate_all_hist_tables(engine)

    start_day, end_day = _resolve_time_window(engine=engine, namespace_prefix=tables_namespace.lower())

    def _fetch_range(start_day, end_day):
        resp = _fetch(app_key, app_secret, asset_id, start_day, end_day, province_code=prov_code)
        return _normalize(resp)

    all_chunks = []
    cur = start_day
    empty_streak = 0

    while cur <= end_day:
        chunk_end = min(
            end_day,
            (pd.to_datetime(cur) + pd.Timedelta(days=HIST_CHUNK_DAYS - 1)).date(),
        )

        df_chunk = _fetch_range(cur.isoformat(), chunk_end.isoformat())

        if not df_chunk.empty and not df_chunk["time"].isna().all():
            all_chunks.append(df_chunk)
            empty_streak = 0
        else:
            empty_streak += 1

        cur = (pd.to_datetime(chunk_end) + pd.Timedelta(days=1)).date()

        if HIST_STOP_EMPTY > 0 and empty_streak >= HIST_STOP_EMPTY and not FULL_HISTORY:
            logger.warning(
                "%s: stopping early after %s consecutive empty chunks in incremental mode.",
                tables_namespace,
                empty_streak,
            )
            break

        if HIST_SLEEP_SEC > 0:
            time.sleep(HIST_SLEEP_SEC)

    if not all_chunks:
        logger.warning("No data returned for %s in %s -> %s", tables_namespace, start_day, end_day)
        return

    df = (
        pd.concat(all_chunks, ignore_index=True)
        .drop_duplicates(subset=["time"])
        .sort_values("time")
        .reset_index(drop=True)
    )

    tmin, tmax = pd.to_datetime(df["time"]).min(), pd.to_datetime(df["time"]).max()
    logger.info("%s: parsed time window %s -> %s (rows=%s)", tables_namespace, tmin, tmax, len(df))

    s_actual, used_actual, _ = _pick_series(df, asset_name, "actual", province_code=prov_code)
    s_fcst, used_fcst, _ = _pick_series(df, asset_name, "forecast", province_code=prov_code)

    keys = ASSET_API_KEYS.get(asset_name, {})
    da_act_candidates = [keys.get("actual_DA"), "dayAheadClearPrice", "hubaoxiDayAheadClearPrice"]
    da_fc_candidates = [keys.get("forecast_DA"), "dayAheadPriceForecast", "dayAheadForecastPrice", "hubaoxiDayAheadPriceForecast", "hubaoxiDayAheadForecastPrice"]
    s_da_act, used_da_act = _pick_series_by_names(df, da_act_candidates)
    s_da_fc, used_da_fc = _pick_series_by_names(df, da_fc_candidates)

    logger.info(
        "%s: picked actual=%s (non-NA=%s), forecast=%s (non-NA=%s), DA actual=%s, DA forecast=%s",
        tables_namespace,
        used_actual,
        int(pd.Series(s_actual).notna().sum()),
        used_fcst,
        int(pd.Series(s_fcst).notna().sum()),
        used_da_act,
        used_da_fc,
    )

    def write_misc_15min(namespace_prefix: str):
        exclude_cols = set(filter(None, [used_actual, used_fcst, used_da_act, used_da_fc]))
        exclude_cols.add("time")
        misc_cols = [c for c in df.columns if c not in exclude_cols]
        total = 0
        for col in misc_cols:
            series = pd.to_numeric(df[col], errors="coerce")
            if series.notna().sum() == 0:
                continue
            df_misc_long = pd.DataFrame({"time": df["time"], "metric": "value", "price": series}).dropna(subset=["price"])
            if df_misc_long.empty:
                continue
            df_misc15 = _make_df15(df_misc_long, prov_code)
            col_slug = _sanitize_ident(col)
            tbl_misc_15 = f"hist_{namespace_prefix}_{col_slug}_15min"
            n = _replace_15min_timevalue(engine, tbl_misc_15, df_misc15[["time", "price"]])
            if n > 0:
                total += n
                logger.info("%s: 15-min misc -> %s=%s", tables_namespace, tbl_misc_15, n)
        logger.info("%s: 15-min misc total rows = %s", tables_namespace, total)

    if province_only:
        ns = tables_namespace.lower()
        write_misc_15min(ns)
        if RUN_INHOUSE_WIND:
            try:
                _write_inhouse_wind(engine, tables_namespace, tmin, tmax + pd.Timedelta(days=1))
            except Exception as e:
                logger.warning("%s: in-house wind write failed: %s", tables_namespace, e)
        return

    if s_actual.notna().sum() == 0 and s_fcst.notna().sum() == 0:
        logger.warning("%s: No usable price columns; will still write misc fields.", tables_namespace)
    else:
        logger.info("%s: using columns -> actual=%s, forecast=%s", tables_namespace, used_actual, used_fcst)

    df_long = (
        pd.DataFrame({"time": df["time"], "actual": s_actual, "forecast": s_fcst})
        .melt(id_vars=["time"], value_vars=["actual", "forecast"], var_name="metric", value_name="price")
        .dropna(subset=["price"])
    )
    if not df_long.empty:
        df15 = _make_df15(df_long, prov_code)
        n_15_clear = _replace_15min_timevalue(engine, f"{tbl_clear}_15min", df15[df15["metric"] == "actual"][["time", "price"]])
        n_15_fcst = _replace_15min_timevalue(engine, f"{tbl_fcst}_15min", df15[df15["metric"] == "forecast"][["time", "price"]])
        logger.info("%s: 15-min rows written -> %s_15min=%s, %s_15min=%s", tables_namespace, tbl_clear, n_15_clear, tbl_fcst, n_15_fcst)

        mat_df = _shift15_pivot_hour(df_long)
        n_clear = _replace_matrix(engine, tbl_clear, mat_df, "actual")
        n_fcst = _replace_matrix(engine, tbl_fcst, mat_df, "forecast")
        n_single = _replace_matrix(engine, f"hist_{tables_namespace.lower()}", mat_df, "actual")
        logger.info("%s: matrix rows -> %s=%s, %s=%s, hist_%s=%s", tables_namespace, tbl_clear, n_clear, tbl_fcst, n_fcst, tables_namespace.lower(), n_single)

    df_da_long = (
        pd.DataFrame({"time": df["time"], "actual_DA": s_da_act, "forecast_DA": s_da_fc})
        .melt(id_vars=["time"], value_vars=["actual_DA", "forecast_DA"], var_name="metric", value_name="price")
        .dropna(subset=["price"])
    )
    if not df_da_long.empty:
        df_da15 = _make_df15(df_da_long, prov_code)
        n_da15_clear = _replace_15min_timevalue(engine, f"{tbl_clear}_dayahead_15min", df_da15[df_da15["metric"] == "actual_DA"][["time", "price"]])
        n_da15_fcst = _replace_15min_timevalue(engine, f"{tbl_fcst}_dayahead_15min", df_da15[df_da15["metric"] == "forecast_DA"][["time", "price"]])
        logger.info("%s: 15-min DA rows -> %s_dayahead_15min=%s, %s_dayahead_15min=%s", tables_namespace, tbl_clear, n_da15_clear, tbl_fcst, n_da15_fcst)

        mat_da = _shift15_pivot_hour(df_da_long)
        n_da_clear_hour = _replace_matrix(engine, f"{tbl_clear}_dayahead", mat_da, "actual_DA")
        n_da_fcst_hour = _replace_matrix(engine, f"{tbl_fcst}_dayahead", mat_da, "forecast_DA")
        logger.info("%s: DA matrix rows -> %s_dayahead=%s, %s_dayahead=%s", tables_namespace, tbl_clear, n_da_clear_hour, tbl_fcst, n_da_fcst_hour)

    write_misc_15min(tables_namespace.lower())


def _default_markets() -> list[str]:
    return ["Mengxi", "Anhui", "Shandong", "Jiangsu"]


def _selected_markets() -> list[str]:
    direct = env_csv("MARKET_LIST") or env_csv("MARKETS")
    if direct:
        return direct

    provinces = env_csv("PROVINCE_MARKETS")
    nodes = env_csv("NODE_MARKETS")
    combined = provinces + nodes
    if combined:
        seen = set()
        ordered = []
        for item in combined:
            key = item.lower()
            if key not in seen:
                ordered.append(item)
                seen.add(key)
        return ordered

    return _default_markets()


def main(markets: Iterable[str] | None = None):
    selected = list(markets) if markets is not None else _selected_markets()
    logger.info("Starting province/node market loader for %s", selected)

    failures: dict[str, str] = {}
    for market in selected:
        try:
            Column_to_Matrix("", market)
        except Exception as e:
            failures[market] = str(e)
            logger.exception("Loader failed for market=%s", market)

    if failures:
        raise RuntimeError(f"province_misc_to_db_v2.py completed with failures: {failures}")

    logger.info("Province/node market loader completed successfully for %s", selected)


if __name__ == "__main__":
    main()
