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

Env vars required:
  APP_KEY, APP_SECRET
  DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME
Optional:
  FULL_HISTORY, HIST_EARLIEST, HIST_CHUNK_DAYS, HIST_STOP_EMPTY, HIST_SLEEP_SEC

Usage examples:
  Column_to_Matrix("", "Anhui")            # province alias → write only non-node 15-min tables
  Column_to_Matrix("", "Anhui_DingYuan")   # node mode → node + DA + misc
"""
import requests
import pytz

import os
import re
import ast
import json
import time
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from poseidon import poseidon


# Optional: auto-load .env
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

# ---- API / DB config ----
APIM_URL = "https://app-portal-cn-ft.enos-iot.com/tt-daas-service/v1/market/clear/data"
PROVINCE_CODE = "15"  # default (Mengxi)

def env_flag(name: str, default=False):
    val = os.getenv(name)
    if val is None:
        return default
    return str(val).strip().strip('"').strip("'").lower() in ("1", "true", "yes", "on")


DB_OVERWRITE_ALL = env_flag("DB_OVERWRITE_ALL", False)

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
INHOUSE_APP_KEY   = os.getenv("INHOUSE_APP_KEY")     # use same key/secret by default
INHOUSE_APP_SECRET= os.getenv("INHOUSE_APP_SECRET")
INHOUSE_PATH      = "/grid-forecast-api/v1.0.0/joint-result"  # GET





# Crawl toggles
HIST_START_DATE = os.getenv("HIST_START_DATE")
HIST_END_DATE   = os.getenv("HIST_END_DATE")



FULL_HISTORY     = env_flag("FULL_HISTORY", True)
HIST_EARLIEST    = os.getenv("HIST_EARLIEST", "2020-01-01")
HIST_CHUNK_DAYS  = int(os.getenv("HIST_CHUNK_DAYS", "7"))
HIST_STOP_EMPTY  = int(os.getenv("HIST_STOP_EMPTY", "8"))
HIST_SLEEP_SEC   = float(os.getenv("HIST_SLEEP_SEC", "0.3"))

# -------------------- MARKET MAPS --------------------
MARKET_MAP = {
    # Mengxi (15)
    "Mengxi_SuYou":     {"asset_name": "SuYou",      "asset_id": "zTqaBeNI", "tbl_clear": "hist_mengxi_suyou_clear",      "tbl_fcst": "hist_mengxi_suyou_forecast",      "province_code": "15"},
    "Mengxi_WuLaTe":    {"asset_name": "WuLaTe",     "asset_id": "wHqvXeTB", "tbl_clear": "hist_mengxi_wulate_clear",     "tbl_fcst": "hist_mengxi_wulate_forecast",     "province_code": "15"},
    "Mengxi_WuHai":     {"asset_name": "WuHai",      "asset_id": "aQeaYeJK", "tbl_clear": "hist_mengxi_wuhai_clear",      "tbl_fcst": "hist_mengxi_wuhai_forecast",      "province_code": "15"},
    "Mengxi_WuLanChaBu":{"asset_name": "WuLanChaBu", "asset_id": "LJAJkAeU", "tbl_clear": "hist_mengxi_wulanchabu_clear",  "tbl_fcst": "hist_mengxi_wulanchabu_forecast",  "province_code": "15"},

    # Shandong (37)
    "Shandong_BinZhou": {"asset_name": "BinZhou",    "asset_id": "I3j1Fnsm", "tbl_clear": "hist_shandong_binzhou_clear",  "tbl_fcst": "hist_shandong_binzhou_forecast",  "province_code": "37"},

    # Anhui (34)
    "Anhui_DingYuan":  {"asset_name": "DingYuan",   "asset_id": "eKqaXeEN", "tbl_clear": "hist_anhui_dingyuan_clear",   "tbl_fcst": "hist_anhui_dingyuan_forecast",   "province_code": "34"},

    # Jiangsu (32)
    "Jiangsu_SheYang": {"asset_name": "SheYang",    "asset_id": "IXwfDSsY", "tbl_clear": "hist_jiangsu_sheyang_clear",  "tbl_fcst": "hist_jiangsu_sheyang_forecast",  "province_code": "32"},
}

# Province alias → delegate node & province code
PROVINCE_ALIAS_MAP = {
    "Anhui":    {"province_code": "34", "default_market_key": "Anhui_DingYuan"},
    "Shandong": {"province_code": "37", "default_market_key": "Shandong_BinZhou"},
    "Jiangsu":  {"province_code": "32", "default_market_key": "Jiangsu_SheYang"},
    "Mengxi":   {"province_code": "15", "default_market_key": "Mengxi_SuYou"},
    "Guangxi": {"province_code": "45", "default_market_key": None}, # 南网省，默认无节点：用环境变量提供 assetId
    # Add more as needed: Guangxi(45), Guangdong(44), Guizhou(52), Yunnan(53), ...
}


# 省调网格（in-house接口要求的grid）
PROVINCE_TO_GRID = {
    "Mengxi":   "NEI_MENG_WEST",
    "Shandong": "SHAN_DONG",
    "Guangxi":  "GUANG_XI",
    "Yunnan":   "YUN_NAN",
    "Gansu":    "GAN_SU",
    "Shanxi":   "SHAN_XI",
    # Add more if/when supported by the interface: "Guangxi": "GUANG_XI", "Yunnan": "YUN_NAN", ...
}

# 每省默认策略（可按需改成 BEFORE_CLEANING_FORECAST / THEORETICAL_FORECAST 等）
PROVINCE_DEFAULT_STRATEGY = {
    "Mengxi":   "ACTUAL_FORECAST",
    "Shandong": "ACTUAL_FORECAST",
    "Guangxi":  "ACTUAL_FORECAST",
    "Yunnan":   "ACTUAL_FORECAST",
    "Gansu":    "AFTER_CLEANING_FORECAST",  # adjust to what your API supports
    "Shanxi":   "AFTER_CLEANING_FORECAST",
    # "Guangxi":  "ACTUAL_FORECAST",
}


def _match_province_key(user_key: str):
    if not user_key:
        return None
    k = user_key.strip().replace("_", "").lower()
    for name in PROVINCE_ALIAS_MAP.keys():
        if name.lower() == k:
            return name
    return None

# Prefer explicit API keys per asset; fall back to generic names if needed
ASSET_API_KEYS = {
    "SuYou":     {"actual": "realTimeClearPrice_zTqaBeNI",        "forecast": "realTimePriceForecast_zTqaBeNI"},
    "WuLaTe":    {"actual": "realTimeClearPrice_wHqvXeTB",        "forecast": "realTimePriceForecast_wHqvXeTB"},
    "WuHai":     {"actual": "realTimeClearPrice_aQeaYeJK",        "forecast": "realTimePriceForecast_aQeaYeJK"},
    "WuLanChaBu":{"actual": "realTimeClearPrice_LJAJkAeU",        "forecast": "realTimePriceForecast_LJAJkAeU"},
    "BinZhou":   {"actual": "realTimeClearPrice_I3j1Fnsm",        "forecast": "realTimePriceForecast_I3j1Fnsm",
                   "actual_DA": "dayAheadClearPrice_I3j1Fnsm",      "forecast_DA": "dayAheadPriceForecast_I3j1Fnsm"},
    "DingYuan":  {"actual": "realTimeClearPrice_eKqaXeEN",        "forecast": "realTimePriceForecast_eKqaXeEN",
                   "actual_DA": "dayAheadClearPrice_eKqaXeEN",      "forecast_DA": "dayAheadPriceForecast_eKqaXeEN"},
    "SheYang":   {"actual": "realTimeClearPrice_jiangBei_IXwfDSsY","forecast": "realTimePriceForecast_jiangBei_IXwfDSsY",
                   "actual_DA": "dayAheadClearPrice_jiangBei_IXwfDSsY", "forecast_DA": "dayAheadPriceForecast_jiangBei_IXwfDSsY"},
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

def _resolve_time_window():
    """
    Priority:
    1) Explicit HIST_START_DATE / HIST_END_DATE
    2) FULL_HISTORY + HIST_EARLIEST
    3) Default incremental (last 7 days)
    """
    today = pd.Timestamp.today().normalize().date()

    if HIST_START_DATE and HIST_END_DATE:
        return (
            pd.to_datetime(HIST_START_DATE).date(),
            pd.to_datetime(HIST_END_DATE).date()
        )

    if FULL_HISTORY:
        return (
            pd.to_datetime(HIST_EARLIEST).date(),
            today
        )

    return (
        today - pd.Timedelta(days=7),
        today
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
    url = f"postgresql+psycopg2://{DB_DEFAULTS['user']}:{DB_DEFAULTS['password']}@" \
          f"{DB_DEFAULTS['host']}:{DB_DEFAULTS['port']}/{DB_DEFAULTS['name']}"
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
    CHUNK = 1000
    for i in range(0, len(times), CHUNK):
        batch = times[i:i+CHUNK]
        placeholders = ",".join([f":t{i+j}" for j in range(len(batch))])
        params = {f"t{i+j}": t for j, t in enumerate(batch)}
        conn.execute(text(f'DELETE FROM "{table_name}" WHERE time IN ({placeholders})'), params)


def _replace_15min_timevalue(engine, table_name: str, df_pair: pd.DataFrame) -> int:
    if df_pair.empty:
        return 0
    sub = df_pair[["time", "price"]].dropna(subset=["time", "price"]).copy()
    if sub.empty:
        return 0
    sub["time"] = pd.to_datetime(sub["time"], errors="coerce")
    sub = sub.dropna(subset=["time", "price"]).drop_duplicates(subset=["time"], keep="last").sort_values("time")

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
    CHUNK = 100
    for i in range(0, len(dates), CHUNK):
        batch = dates[i:i+CHUNK]
        placeholders = ",".join([f":d{i+j}" for j in range(len(batch))])
        params = {f"d{i+j}": d for j, d in enumerate(batch)}
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
try:
    from poseidon import poseidon
except Exception as e:
    raise SystemExit("Poseidon SDK not found. Install: pip install enos-poseidon==0.1.6\n" + str(e))


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
        resp = poseidon.urlopen(app_key, app_secret, APIM_URL, payload, headers,
                                method="POST", timeout=timeout, content_type="application/json")
    except TypeError:
        resp = poseidon.urlopen(app_key, app_secret, APIM_URL, payload, headers,
                                method="POST", timeout=timeout)
    return _parse_resp(resp)


def _normalize(resp_dict) -> pd.DataFrame:
    rows = resp_dict.get("data") or []
    if not isinstance(rows, list):
        return pd.DataFrame(columns=["time"])  # treat as empty
    df = pd.DataFrame.from_records(rows)
    if "time" in df.columns:
        # fix 24:00 → next day 00:00
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
    Calls /grid-forecast-api/v1.0.0/joint-result using GET + query-string
    (the pattern you just confirmed works). Returns a 15-min DataFrame with
    Asia/Shanghai naive timestamps.
    """
    if not INHOUSE_APP_KEY or not INHOUSE_APP_SECRET:
        raise RuntimeError("INHOUSE_APP_KEY/INHOUSE_APP_SECRET not set for in-house wind fetch.")

    # Build query string exactly as per the working probe
    from urllib.parse import urlencode
    qs = {
        "grid": grid,
        "strategy": strategy,
        "startDate": start_date,         # yyyy-MM-dd
        "length": str(int(length)),      # 96..5952
        "domain": "WIND",                # omit if you want the sum; keep WIND here for wind
    }
    base = INHOUSE_APIM_HOST.rstrip("/")
    url = f"{base}{INHOUSE_PATH}?{urlencode(qs)}"

    # IMPORTANT: GET with querystring; payload=None; no content_type kwarg
    # IMPORTANT: GET with querystring; payload=None; no content_type kwarg
    raw = poseidon.urlopen(INHOUSE_APP_KEY, INHOUSE_APP_SECRET, url,
                           None, {}, method="GET", timeout=30.0)
    
    # ---- tolerant parsing ----
    text = raw.decode("utf-8", "replace") if isinstance(raw, (bytes, bytearray)) else str(raw)
    text = text.strip()
    
    def _parse_relaxed(s: str):
        import json, re
        # fast path
        try:
            return json.loads(s)
        except json.JSONDecodeError:
            pass
        # common case: single quotes → double quotes
        # only if it looks like a Python dict
        if s.startswith("{'") or s.startswith("['") or s.replace(" ", "").startswith("{'startTime'"):
            try:
                return json.loads(s.replace("'", '"'))
            except Exception:
                pass
        # try literal_eval as a last resort (API sometimes returns python-literal-like)
        try:
            import ast
            obj = ast.literal_eval(s)
            if isinstance(obj, (dict, list)):
                return obj
        except Exception:
            pass
        return None
    
    obj = _parse_relaxed(text)
    
    # Fallback: some gateways require omitting domain (sum)
    if not isinstance(obj, dict) or ("powers" not in obj and "startTime" not in obj):
        from urllib.parse import urlencode
        qs2 = {
            "grid": grid,
            "strategy": strategy,
            "startDate": start_date,
            "length": str(int(length)),
            # domain omitted
        }
        url2 = f"{INHOUSE_APIM_HOST.rstrip('/')}{INHOUSE_PATH}?{urlencode(qs2)}"
        raw2 = poseidon.urlopen(INHOUSE_APP_KEY, INHOUSE_APP_SECRET, url2,
                                None, {}, method="GET", timeout=30.0)
        text2 = raw2.decode("utf-8", "replace") if isinstance(raw2, (bytes, bytearray)) else str(raw2)
        obj = _parse_relaxed(text2.strip())
    
    if not isinstance(obj, dict):
        raise ValueError(f"in-house response not JSON-like; head={text[:120]!r}")
    
    # Handle API error envelope gracefully
    if "code" in obj and "message" in obj and "powers" not in obj:
        raise ValueError(f"in-house API error: code={obj.get('code')}, message={obj.get('message')}")
    
    # Expect: {"startTime": "...Z", "powers": [...]}
    powers = obj.get("powers") or []
    if not powers:
        # No data is not a hard error; write nothing
        return pd.DataFrame(columns=["time", "price"])
    
    start_time_utc = pd.to_datetime(obj["startTime"], utc=True, errors="raise")
    rng_utc   = pd.date_range(start_time_utc, periods=len(powers), freq="15min")
    rng_local = rng_utc.tz_convert("Asia/Shanghai").tz_localize(None)
    df = pd.DataFrame({"time": rng_local, "price": pd.to_numeric(powers, errors="coerce")})
    return df.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)



def _write_inhouse_wind(engine, province_name: str, start_date: pd.Timestamp, end_date: pd.Timestamp) -> int:
    """
    Download in-house wind forecast for [start_date, end_date) and write into:
      hist_<province>_inhouse_windforecast_15min
    Returns total rows inserted.
    """
    prov = province_name  # expected keys like "Mengxi", "Shandong"
    grid = PROVINCE_TO_GRID.get(prov)
    if not grid:
        print(f"[WARN] In-house wind: province '{prov}' is not mapped to a grid; skipped.")
        return 0
    strategy = PROVINCE_DEFAULT_STRATEGY.get(prov, "ACTUAL_FORECAST")

    # limit per call: 5952 points (≈62 days). Split into month-ish chunks to be safe.
    start_d = pd.to_datetime(start_date).normalize().date()
    end_d = pd.to_datetime(end_date).normalize().date()
    cursor = start_d
    total = 0
    tbl = f"hist_{prov.lower()}_inhouse_windforecast_15min"
    while cursor < end_d:
        # request chunk up to API max (here pick <= 3000 points ≈ 31 days to be safe)
        span_days = min((end_d - cursor).days, 31)
        if span_days <= 0:
            break
        length = span_days * 96
        df15 = _fetch_inhouse_wind(grid, cursor.isoformat(), length, strategy=strategy)
        n = _replace_15min_timevalue(engine, tbl, df15)
        total += n
        cursor = (pd.to_datetime(cursor) + pd.Timedelta(days=span_days)).date()
    print(f"[OK] {prov}: in-house wind forecast rows → {tbl}={total}")
    return total


# -------------------- series pickers --------------------

def _pick_series(df: pd.DataFrame, asset_name: str, which: str, province_code: str):
    """
    Choose the best series with real data.
    Accepts both ...PriceForecast and ...ForecastPrice.
    Returns: (series, used_key, candidates_tried)
    """
    keys = ASSET_API_KEYS.get(asset_name, {}) or {}

    # Region candidates only for Mengxi (15)
    region_generics_actual = ["hubaoxiRealTimeClearPrice", "hubaoxiDayAheadClearPrice"] if province_code == "15" else []
    region_generics_fcst   = [
        "hubaoxiRealTimePriceForecast", "hubaoxiDayAheadPriceForecast",
        "hubaoxiRealTimeForecastPrice", "hubaoxiDayAheadForecastPrice",
    ] if province_code == "15" else []

    generic_actual   = ["realTimeClearPrice", "dayAheadClearPrice"]
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

    # liberal pattern scan
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

    return pd.Series([None]*len(df), index=df.index, dtype="float64"), None, candidates


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
    return pd.Series([None]*len(df), index=df.index, dtype="float64"), None


# -------------------- top-level --------------------

def Column_to_Matrix(pricefile: str, market: str):
    # Province alias mode?
    province_name = _match_province_key(market)
    province_only = False
    tables_namespace = None

    if province_name:
        province_only = True
        prov_entry = PROVINCE_ALIAS_MAP[province_name]
        prov_code  = prov_entry["province_code"]
        delegate_key = prov_entry["default_market_key"]
        asset_name = MARKET_MAP[delegate_key]["asset_name"]
        asset_id   = MARKET_MAP[delegate_key]["asset_id"]
        tables_namespace = province_name  # table prefix uses province name
        
    else:
        # Node mode (original behavior)
        if market not in MARKET_MAP and market.lower() in [k.lower() for k in MARKET_MAP.keys()]:
            # case-insensitive match
            market = [k for k in MARKET_MAP.keys() if k.lower() == market.lower()][0]
        if market not in MARKET_MAP:
            raise ValueError(f"Unsupported market: {market}")
        m = MARKET_MAP[market]
        asset_name = m["asset_name"]
        asset_id   = m["asset_id"]
        prov_code  = m.get("province_code", PROVINCE_CODE)
        tables_namespace = market
        tbl_clear = m["tbl_clear"]
        tbl_fcst  = m["tbl_fcst"]
        tbl_single= f"hist_{market}".lower()
    # Decide the working window once, regardless of legacy API
    start_day, end_day = _resolve_time_window()


    app_key = os.getenv("APP_KEY")
    app_secret = os.getenv("APP_SECRET")
    if not app_key or not app_secret:
        # raise SystemExit("APP_KEY / APP_SECRET environment variables are required.")
        print("[FATAL] APP_KEY / APP_SECRET environment variables are required.")
        return



    # -------- decide window & fetch --------
    def _fetch_range(start_day, end_day):
        resp = _fetch(app_key, app_secret, asset_id, start_day, end_day, province_code=prov_code)
        return _normalize(resp)

    all_chunks = []
    cur = start_day
    
    while cur <= end_day:
        chunk_end = min(
            end_day,
            (pd.to_datetime(cur) + pd.Timedelta(days=HIST_CHUNK_DAYS - 1)).date()
        )
    
        df_chunk = _fetch_range(cur.isoformat(), chunk_end.isoformat())
    
        if not df_chunk.empty and not df_chunk["time"].isna().all():
            all_chunks.append(df_chunk)
    
        cur = (pd.to_datetime(chunk_end) + pd.Timedelta(days=1)).date()
    
        if HIST_SLEEP_SEC > 0:
            time.sleep(HIST_SLEEP_SEC)
    
    if not all_chunks:
        print(f"[WARN] No data returned for {tables_namespace} in {start_day} → {end_day}")
        return
    
    df = (
        pd.concat(all_chunks, ignore_index=True)
          .drop_duplicates(subset=["time"])
          .sort_values("time")
          .reset_index(drop=True)
    )


    tmin, tmax = pd.to_datetime(df["time"]).min(), pd.to_datetime(df["time"]).max()
    print(f"[INFO] {tables_namespace}: parsed time window {tmin} → {tmax} (rows={len(df)})")

    # -------- pick node series (only to know which columns to exclude in misc) --------
    s_actual, used_actual, _ = _pick_series(df, asset_name, "actual", province_code=prov_code)
    s_fcst,   used_fcst,   _ = _pick_series(df, asset_name, "forecast", province_code=prov_code)

    # Day-ahead (for exclusion only; we might not write them in province mode)
    keys = ASSET_API_KEYS.get(asset_name, {})
    da_act_candidates = [keys.get("actual_DA"), "dayAheadClearPrice", "hubaoxiDayAheadClearPrice"]
    da_fc_candidates  = [keys.get("forecast_DA"), "dayAheadPriceForecast", "dayAheadForecastPrice",
                         "hubaoxiDayAheadPriceForecast", "hubaoxiDayAheadForecastPrice"]
    s_da_act, used_da_act = _pick_series_by_names(df, da_act_candidates)
    s_da_fc,  used_da_fc  = _pick_series_by_names(df, da_fc_candidates)

    print(f"[DEBUG] {tables_namespace}: picked actual={used_actual} (non-NA={int(pd.Series(s_actual).notna().sum())}), "
          f"forecast={used_fcst} (non-NA={int(pd.Series(s_fcst).notna().sum())}), "
          f"DA actual={used_da_act}, DA forecast={used_da_fc}")

    # -------- province-only? skip node/DA tables, write only misc 15-min --------
    engine = _db_engine()

    if DB_OVERWRITE_ALL:
        print("[WARN] Truncating ALL hist_* tables before load")
        _truncate_all_hist_tables(engine)


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
                print(f"[OK] {tables_namespace}: 15-min misc → {tbl_misc_15}={n}")
        print(f"[OK] {tables_namespace}: 15-min misc total rows = {total}")

    if province_only:
        ns = tables_namespace.lower()
        write_misc_15min(ns)
        # write in-house wind forecast for the same parsed window
        try:
            # use parsed df window tmin..tmax already computed
            _write_inhouse_wind(engine, tables_namespace, tmin, tmax + pd.Timedelta(days=1))
        except Exception as e:
            print(f"[WARN] {tables_namespace}: in-house wind write failed: {e}")
        return


    # -------- node mode: write node & DA + misc --------
    if s_actual.notna().sum() == 0 and s_fcst.notna().sum() == 0:
        print(f"[WARN] {tables_namespace}: No usable price columns; will still write misc fields.")
    else:
        print(f"[INFO] {tables_namespace}: using columns → actual={used_actual}, forecast={used_fcst}")

    # node RT/forecast 15-min
    df_long = (
        pd.DataFrame({"time": df["time"], "actual": s_actual, "forecast": s_fcst})
          .melt(id_vars=["time"], value_vars=["actual", "forecast"], var_name="metric", value_name="price")
          .dropna(subset=["price"])
    )
    if not df_long.empty:
        df15 = _make_df15(df_long, prov_code)
        n_15_clear = _replace_15min_timevalue(engine, f"{tbl_clear}_15min", df15[df15["metric"]=="actual"][ ["time","price"] ])
        n_15_fcst  = _replace_15min_timevalue(engine, f"{tbl_fcst}_15min",  df15[df15["metric"]=="forecast"][ ["time","price"] ])
        print(f"[OK] {tables_namespace}: 15-min rows written → {tbl_clear}_15min={n_15_clear}, {tbl_fcst}_15min={n_15_fcst}")

        # hourly matrix (node RT/forecast)
        mat_df = _shift15_pivot_hour(df_long)
        n_clear = _replace_matrix(engine, tbl_clear, mat_df, "actual")
        n_fcst  = _replace_matrix(engine, tbl_fcst,  mat_df, "forecast")
        n_single= _replace_matrix(engine, f"hist_{tables_namespace.lower()}", mat_df, "actual")
        print(f"[OK] {tables_namespace}: matrix rows → {tbl_clear}={n_clear}, {tbl_fcst}={n_fcst}, hist_{tables_namespace.lower()}={n_single}")

    # day-ahead 15-min + hourly
    df_da_long = (
        pd.DataFrame({"time": df["time"], "actual_DA": s_da_act, "forecast_DA": s_da_fc})
          .melt(id_vars=["time"], value_vars=["actual_DA", "forecast_DA"], var_name="metric", value_name="price")
          .dropna(subset=["price"])
    )
    if not df_da_long.empty:
        df_da15 = _make_df15(df_da_long, prov_code)
        n_da15_clear = _replace_15min_timevalue(engine, f"{tbl_clear}_dayahead_15min", df_da15[df_da15["metric"]=="actual_DA"][ ["time","price"] ])
        n_da15_fcst  = _replace_15min_timevalue(engine, f"{tbl_fcst}_dayahead_15min",  df_da15[df_da15["metric"]=="forecast_DA"][ ["time","price"] ])
        print(f"[OK] {tables_namespace}: 15-min DA rows → {tbl_clear}_dayahead_15min={n_da15_clear}, {tbl_fcst}_dayahead_15min={n_da15_fcst}")

        mat_da = _shift15_pivot_hour(df_da_long)
        n_da_clear_hour = _replace_matrix(engine, f"{tbl_clear}_dayahead",  mat_da, "actual_DA")
        n_da_fcst_hour  = _replace_matrix(engine, f"{tbl_fcst}_dayahead",   mat_da, "forecast_DA")
        print(f"[OK] {tables_namespace}: DA matrix rows → {tbl_clear}_dayahead={n_da_clear_hour}, {tbl_fcst}_dayahead={n_da_fcst_hour}")

    # finally: always write misc 15-min for the node market too
    write_misc_15min(tables_namespace.lower())


if __name__ == "__main__":
    # Examples:
        
    Column_to_Matrix("", "Mengxi")
    Column_to_Matrix("", "Anhui")
    Column_to_Matrix("", "Shandong")
    Column_to_Matrix("", "Jiangsu")

    Column_to_Matrix("", "Guangxi")


