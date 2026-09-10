# -*- coding: utf-8 -*-
"""
API-backed Column_to_Matrix with:
- FULL_HISTORY + incremental CSV append
- Incremental API fetch (only new days when hist_<market>.csv exists)
- DB writes to:
    * hist_<market>_clear (actual)   — as before
    * hist_<market>_forecast         — as before
    * hist_<market>                  — NEW, matches Excel version (delete+append dates)

Env vars required:
  APP_KEY, APP_SECRET
  DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME
Optional:
  FULL_HISTORY, HIST_EARLIEST, HIST_CHUNK_DAYS, HIST_STOP_EMPTY, HIST_SLEEP_SEC
"""

import os
import ast
import json
import time
import pandas as pd
from sqlalchemy import create_engine, text
import numpy as np  # near the top of the file

# Optional: auto-load .env
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

# ---- API / DB config ----
MARKET_APIM_HOST = os.getenv("MARKET_APIM_HOST", "https://app-portal-cn-ft.enos-iot.com").rstrip("/")
APIM_URL = f"{MARKET_APIM_HOST}/tt-daas-service/v1/market/clear/data"
# APIM_URL = "https://app-portal-cn-ft.enos-iot.com/tt-daas-service/v1/market/clear/data"
PROVINCE_CODE = "15"  # 蒙西

DB_DEFAULTS = {
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "root"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5433"),
    "name": os.getenv("DB_NAME", "marketdata"),
}

def env_flag(name: str, default=False):
    """Parse boolean-ish env flag robustly (1/"1"/true/yes/on)."""
    val = os.getenv(name)
    if val is None:
        return default
    return str(val).strip().strip('"').strip("'").lower() in ("1", "true", "yes", "on")

# Crawl toggles
FULL_HISTORY     = env_flag("FULL_HISTORY", False)
HIST_EARLIEST    = os.getenv("HIST_EARLIEST", "2020-01-01")
HIST_CHUNK_DAYS  = int(os.getenv("HIST_CHUNK_DAYS", "7"))
HIST_STOP_EMPTY  = int(os.getenv("HIST_STOP_EMPTY", "8"))
HIST_SLEEP_SEC   = float(os.getenv("HIST_SLEEP_SEC", "0.3"))

# Market -> asset_id mapping
MARKET_MAP = {
    "Mengxi_SuYou":  {"asset_name": "SuYou",  "asset_id": "zTqaBeNI",
                      "tbl_clear": "hist_mengxi_suyou_clear", "tbl_fcst": "hist_mengxi_suyou_forecast"},
    "Mengxi_WuLaTe": {"asset_name": "WuLaTe", "asset_id": "wHqvXeTB",
                      "tbl_clear": "hist_mengxi_wulate_clear", "tbl_fcst": "hist_mengxi_wulate_forecast"},

    "Mengxi_WuHai": {"asset_name": "WuHai", "asset_id": "aQeaYeJK",
                      "tbl_clear": "hist_mengxi_wuhai_clear", "tbl_fcst": "hist_mengxi_wuhai_forecast"},

    "Mengxi_WuLanChaBu": {"asset_name": "WuLanChaBu", "asset_id": "LJAJkAeU",
                      "tbl_clear": "hist_mengxi_wulanchabu_clear", "tbl_fcst": "hist_mengxi_wulanchabu_forecast"},

    "Shandong_BinZhou": {"asset_name": "BinZhou", "asset_id": "I3j1Fnsm",
                      "tbl_clear": "hist_shandong_binzhou_clear", "tbl_fcst": "hist_shandong_binzhou_forecast"},

    "Anhui_DingYuan": {"asset_name": "DingYuan", "asset_id": "eKqaXeEN",
                      "tbl_clear": "hist_anhui_dingyuan_clear", "tbl_fcst": "hist_anhui_dingyuan_forecast"},
    
    "Jiangsu_SheYang": {"asset_name": "SheYang", "asset_id": "IXwfDSsY",
                      "tbl_clear": "hist_jiangsu_sheyang_clear", "tbl_fcst": "hist_jiangsu_sheyang_forecast"},    


}


# --- Ensure MARKET_MAP exists, then add entries ---
try:
    MARKET_MAP
except NameError:
    MARKET_MAP = {}

MARKET_MAP.update({
    # Existing Inner Mongolia nodes
    "Mengxi_SuYou": {
        "asset_name": "SuYou",
        "asset_id":   "zTqaBeNI",  # <-- keep as your API expects (ok to be same as name)
        "tbl_clear":  "hist_mengxi_suyou_clear",
        "tbl_fcst":   "hist_mengxi_suyou_forecast",
        "timezone":   "Asia/Shanghai",
        "province_code": "15",
    },
    "Mengxi_WuLaTe": {
        "asset_name": "WuLaTe",
        "asset_id":   "wHqvXeTB",
        "tbl_clear":  "hist_mengxi_wulate_clear",
        "tbl_fcst":   "hist_mengxi_wulate_forecast",
        "timezone":   "Asia/Shanghai",
        "province_code": "15",
    },

    # NEW: this is the one your run is failing on
    # Inner Mongolia (Mengxi = 15)
    "Mengxi_WuHai": {
        "asset_name": "WuHai",
        "asset_id":   "aQeaYeJK",
        "tbl_clear":  "hist_mengxi_wuhai_clear",
        "tbl_fcst":   "hist_mengxi_wuhai_forecast",
        "timezone":   "Asia/Shanghai",
        "province_code": "15",
    },
    "Mengxi_WuLanChaBu": {
        "asset_name": "WuLanChaBu",
        "asset_id":   "LJAJkAeU",
        "tbl_clear":  "hist_mengxi_wulanchabu_clear",
        "tbl_fcst":   "hist_mengxi_wulanchabu_forecast",
        "timezone":   "Asia/Shanghai",
        "province_code": "15",
    },
    
    # Shandong (37)
    "Shandong_BinZhou": {
        "asset_name": "BinZhou",
        "asset_id":   "I3j1Fnsm",
        "tbl_clear":  "hist_shandong_binzhou_clear",
        "tbl_fcst":   "hist_shandong_binzhou_forecast",
        "timezone":   "Asia/Shanghai",
        "province_code": "37",
    },
    
    # Anhui (34)
    "Anhui_DingYuan": {
        "asset_name": "DingYuan",
        "asset_id":   "eKqaXeEN",
        "tbl_clear":  "hist_anhui_dingyuan_clear",
        "tbl_fcst":   "hist_anhui_dingyuan_forecast",
        "timezone":   "Asia/Shanghai",
        "province_code": "34",
    },
    
    # Jiangsu (32)
    "Jiangsu_SheYang": {
        "asset_name": "SheYang",
        "asset_id":   "IXwfDSsY",
        "tbl_clear":  "hist_jiangsu_sheyang_clear",
        "tbl_fcst":   "hist_jiangsu_sheyang_forecast",
        "timezone":   "Asia/Shanghai",
        "province_code": "32",
    },

})


# Prefer explicit API keys per asset; fall back to generic names if needed
ASSET_API_KEYS = {
    "SuYou":  {"actual": "realTimeClearPrice_zTqaBeNI",   "forecast": "realTimePriceForecast_zTqaBeNI"},
    "WuLaTe": {"actual": "realTimeClearPrice_wHqvXeTB",   "forecast": "realTimePriceForecast_wHqvXeTB"},
    "WuHai": {"actual": "realTimeClearPrice_aQeaYeJK",   "forecast": "realTimePriceForecast_aQeaYeJK"},
    "WuLanChaBu": {"actual": "realTimeClearPrice_LJAJkAeU",   "forecast": "realTimePriceForecast_LJAJkAeU"},
    "BinZhou": {"actual": "realTimeClearPrice_I3j1Fnsm",   "forecast": "realTimePriceForecast_I3j1Fnsm","actual_DA": "dayAheadClearPrice_I3j1Fnsm",   "forecast_DA": "dayAheadPriceForecast_I3j1Fnsm"},
    "DingYuan": {"actual": "realTimeClearPrice_eKqaXeEN",   "forecast": "realTimePriceForecast_eKqaXeEN","actual_DA": "dayAheadClearPrice_eKqaXeEN",   "forecast_DA": "dayAheadPriceForecast_eKqaXeEN"},
    "SheYang": {"actual": "realTimeClearPrice_jiangBei_IXwfDSsY",   "forecast": "realTimePriceForecast_jiangBei_IXwfDSsY","actual_DA": "dayAheadClearPrice_jiangBei_IXwfDSsY",   "forecast_DA": "dayAheadPriceForecast_jiangBei_IXwfDSsY"},


}
GENERIC_KEYS = {"actual": "realTimeClearPrice", "forecast": "realTimePriceForecast"}


def _pick_series(df: pd.DataFrame, asset_name: str, which: str, province_code):
    """
    Choose the best price series with real data:
    1) asset-specific RT
    2) asset-specific DA
    3) generic RT/DA
    4) region-generic (e.g., hubaoxi*)  [only when province_code == "15"]
    5) pattern scan (accepts both ...PriceForecast and ...ForecastPrice)
    Returns: (series, used_key, candidates_tried)
    """
    keys = ASSET_API_KEYS.get(asset_name, {}) or {}

    # Region candidates only for Mengxi (15)
    region_generics_actual = ["hubaoxiRealTimeClearPrice", "hubaoxiDayAheadClearPrice"] if province_code == "15" else []
    region_generics_fcst   = [
        "hubaoxiRealTimePriceForecast", "hubaoxiDayAheadPriceForecast",
        "hubaoxiRealTimeForecastPrice", "hubaoxiDayAheadForecastPrice",
    ] if province_code == "15" else []

    # Generic + synonyms (accept both orders)
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

    # 1–4: direct names in order, only if they have data
    for k in candidates:
        if k and k in df.columns and has_data(df[k]):
            return df[k], k, candidates

    # 5: liberal pattern scan (also matches ...ForecastPrice)
    cols = list(df.columns)
    if which == "actual":
        picks = [c for c in cols if c.lower().endswith("realtimeclearprice")
                               or c.lower().endswith("dayaheadclearprice")]
    else:
        picks = [c for c in cols if c.lower().endswith("realtimepriceforecast")
                               or c.lower().endswith("dayaheadpriceforecast")
                               or c.lower().endswith("realtimeforecastprice")
                               or c.lower().endswith("dayaheadforecastprice")]
    for p in picks:
        s = df[p]
        if has_data(s):
            return s, p, candidates + ["<pattern>"]

    # nothing usable
    return pd.Series([None]*len(df), index=df.index, dtype="float64"), None, candidates


def _pick_series_by_names(df: pd.DataFrame, names: list[str]):
    """
    Return (Series, used_key) for the first name that exists AND has non-empty data.
    """
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


# Poseidon SDK
try:
    from poseidon import poseidon
except Exception as e:
    raise SystemExit("Poseidon SDK not found. Install: pip install enos-poseidon==0.1.6\n" + str(e))

# ---------- DB & CSV helpers ----------
def _db_engine():
    url = f"postgresql+psycopg2://{DB_DEFAULTS['user']}:{DB_DEFAULTS['password']}@" \
          f"{DB_DEFAULTS['host']}:{DB_DEFAULTS['port']}/{DB_DEFAULTS['name']}"
    return create_engine(url, pool_pre_ping=True)

def _hist_path(market: str) -> str:
    return f"hist_{market}.csv"

def _last_hist_date(market: str):
    """Return the last Date in existing hist_<market>.csv or None."""
    fn = _hist_path(market)
    try:
        df = pd.read_csv(fn, usecols=["Date"])
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        if df["Date"].notna().any():
            return df["Date"].max().date()
    except Exception:
        pass
    return None

# ---------- API helpers ----------
def _parse_resp(resp_obj):
    if resp_obj is None:
        raise RuntimeError(
            "API returned no body (None). Check APP_KEY/APP_SECRET, assetId, provinceCode, and network."
        )
    text_ = resp_obj.decode("utf-8", errors="replace") if isinstance(resp_obj, (bytes, bytearray)) else str(resp_obj)

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
        # fix "24:00:00" => next day "00:00:00"
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

def _series_for(df: pd.DataFrame, asset_name: str, which: str) -> pd.Series:
    """
    Return the best-available price series for (asset, which).
    Tries asset-specific RT first, then DA, then generic key.
    """
    keys = ASSET_API_KEYS.get(asset_name, {})
    candidates = []
    if which == "actual":
        candidates = [keys.get("actual"), keys.get("actual_DA"), GENERIC_KEYS["actual"]]
    else:
        candidates = [keys.get("forecast"), keys.get("forecast_DA"), GENERIC_KEYS["forecast"]]

    for k in candidates:
        if k and k in df.columns:
            return df[k]
    # No matching column: return empty series of right length
    return pd.Series([None] * len(df), index=df.index, dtype="float64")

def _shift15_pivot_hour(df_long: pd.DataFrame) -> pd.DataFrame:
    # shift timestamps so 00:15 -> 00:00 hour, etc.
    w = df_long.copy()
    w["shifted_time"] = w["time"] - pd.Timedelta(minutes=15)
    w["date"] = w["shifted_time"].dt.date
    w["hour"] = w["shifted_time"].dt.hour
    hourly = w.groupby(["metric", "date", "hour"], as_index=False)["price"].mean()
    mat = hourly.pivot_table(index=["metric", "date"], columns="hour", values="price")
    mat = mat.sort_index()
    mat.columns = [f"Hour_{str(c).zfill(2)}" for c in mat.columns]
    return mat.reset_index()

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
    """DELETE ... WHERE date IN (...) in safe chunks to avoid driver quirks with arrays/ANY."""
    if not dates:
        return
    CHUNK = 100
    for i in range(0, len(dates), CHUNK):
        batch = dates[i:i+CHUNK]
        placeholders = ",".join([f":d{i+j}" for j in range(len(batch))])
        params = {f"d{i+j}": d for j, d in enumerate(batch)}
        conn.execute(text(f'DELETE FROM "{table_name}" WHERE date IN ({placeholders})'), params)


def _replace_matrix(engine, tbl: str, mat_df: pd.DataFrame, metric_name: str):
    """Delete then append for the split tables (actual/forecast)."""
    subset = mat_df[mat_df["metric"] == metric_name].copy()
    if subset.empty:
        return 0
    keep = ["date"] + [c for c in subset.columns if c.startswith("Hour_")]
    out = subset[keep].drop_duplicates(subset=["date"]).sort_values("date")
    _ensure_matrix_table(engine, tbl)
    # normalize dtype for driver consistency
    out["date"] = pd.to_datetime(out["date"]).dt.date
    dates = out["date"].tolist()
    with engine.begin() as conn:
        _chunked_delete(conn, tbl, dates)
        out.to_sql(tbl, con=conn, if_exists="append", index=False, method="multi", chunksize=1000)
    return len(out)


def _replace_hist_single(engine, table_name: str, mat_df: pd.DataFrame):
    """
    Delete+append into a single table hist_<market> (actual only),
    mirroring the Excel version's pattern.
    """
    subset = mat_df[mat_df["metric"] == "actual"].copy()
    if subset.empty:
        return 0
    keep = ["date"] + [c for c in subset.columns if c.startswith("Hour_")]
    out = subset[keep].drop_duplicates(subset=["date"]).sort_values("date")
    _ensure_matrix_table(engine, table_name)
    out["date"] = pd.to_datetime(out["date"]).dt.date
    dates = out["date"].tolist()
    with engine.begin() as conn:
        _chunked_delete(conn, table_name, dates)
        out.to_sql(table_name, con=conn, if_exists="append", index=False, method="multi", chunksize=1000)
    return len(out)

# ---------- CSV export (incremental merge) ----------
def _export_hist_csv_incremental(market: str, mat_df: pd.DataFrame, tbl_label: str = "actual") -> str:
    """
    Append/merge new days into hist_<market>.csv instead of overwriting.
    Keeps one row per Date; new data overwrites same-day old rows.
    """
    sl = mat_df[mat_df["metric"] == ("actual" if tbl_label == "actual" else "forecast")].copy()
    if sl.empty:
        return ""
    keep = ["date"] + [c for c in sl.columns if c.startswith("Hour_")]
    out = sl[keep].sort_values("date").reset_index(drop=True)
    out.rename(columns={"date": "Date"}, inplace=True)
    out["Date"] = pd.to_datetime(out["Date"]).dt.date

    fn = _hist_path(market)
    try:
        old = pd.read_csv(fn)
        old["Date"] = pd.to_datetime(old["Date"], errors="coerce").dt.date
    except Exception:
        old = pd.DataFrame(columns=out.columns)

    merged = pd.concat([old, out], ignore_index=True)
    merged = merged.drop_duplicates(subset=["Date"], keep="last").sort_values("Date").reset_index(drop=True)
    merged.to_csv(fn, index=False, encoding="utf-8-sig")
    return fn

# ---------- Full-history crawl ----------
def _crawl_all_history(app_key: str, app_secret: str, asset_id: str, province_code: str) -> pd.DataFrame:
    """
    Crawl ALL history from HIST_EARLIEST to today in HIST_CHUNK_DAYS-day chunks.
    Stop after HIST_STOP_EMPTY consecutive empty windows (API lower bound).
    """
    earliest = pd.to_datetime(HIST_EARLIEST).date()
    today = pd.Timestamp.today().date()
    all_chunks = []
    empty_streak = 0
    cur = earliest
    while cur <= today:
        end = min(today, (pd.to_datetime(cur) + pd.Timedelta(days=HIST_CHUNK_DAYS - 1)).date())
        resp = _fetch(app_key, app_secret, asset_id, cur.isoformat(), end.isoformat(), province_code=province_code)
        df = _normalize(resp)
        # print(f"[DEBUG] {cur}→{end} forecast-like cols:",
        #       [c for c in df.columns if "Forecast" in c or "forecast" in c])


        
        if df.empty or len(df.dropna(how="all")) == 0:
            empty_streak += 1
        else:
            empty_streak = 0
            all_chunks.append(df)
        if empty_streak >= HIST_STOP_EMPTY:
            break

     
        cur = (pd.to_datetime(end) + pd.Timedelta(days=1)).date()
        if HIST_SLEEP_SEC > 0:
            time.sleep(HIST_SLEEP_SEC)

    
    all_chunks = [d for d in all_chunks if isinstance(d, pd.DataFrame) and not d.empty]
    if not all_chunks:
        return pd.DataFrame(columns=["time"])
    out = pd.concat(all_chunks, ignore_index=True)
    out = out.drop_duplicates(subset=["time"]).sort_values("time").reset_index(drop=True)
    return out



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
    """DELETE ... WHERE time IN (...) in chunks to avoid param limits."""
    if not times:
        return
    CHUNK = 1000
    for i in range(0, len(times), CHUNK):
        batch = times[i:i+CHUNK]
        placeholders = ",".join([f":t{i+j}" for j in range(len(batch))])
        params = {f"t{i+j}": t for j, t in enumerate(batch)}
        conn.execute(text(f'DELETE FROM "{table_name}" WHERE time IN ({placeholders})'), params)

def _replace_15min_split(engine, table_name: str, df_long: pd.DataFrame, metric_value: str) -> int:
    """
    Delete+append original 15-min rows (time, price) for one metric ('actual' or 'forecast')
    into the given table.
    """
    if df_long.empty:
        return 0
    subset = df_long[df_long["metric"] == metric_value][["time", "price"]].dropna(subset=["time", "price"]).copy()
    if subset.empty:
        return 0

    # normalize types; drop dupes on timestamp (keep newest)
    subset["time"] = pd.to_datetime(subset["time"], errors="coerce")
    subset = subset.dropna(subset=["time", "price"]).drop_duplicates(subset=["time"], keep="last").sort_values("time")

    _ensure_15min_table(engine, table_name)
    times = subset["time"].tolist()

    with engine.begin() as conn:
        _chunked_delete_time(conn, table_name, times)
        subset.to_sql(table_name, con=conn, if_exists="append", index=False, method="multi", chunksize=10000)
    return len(subset)


# ---------- Top-level ----------
def Column_to_Matrix(pricefile: str, market: str):
    """
    - FULL_HISTORY=1: crawl all history; else fetch only new days if hist_<market>.csv exists.
    - Writes DB:
        * hist_<market>_clear (actual)
        * hist_<market>_forecast (forecast)
        * hist_<market>  (actual)  <-- single-table Excel-style
    - Exports hist_<market>.csv via incremental merge (actual).
    """
    # -------- tolerant market resolution (case-insensitive) --------
    if market in MARKET_MAP:
        market_key = market
    else:
        lowered = (market or "").lower()
        matches = [k for k in MARKET_MAP.keys() if k.lower() == lowered]
        if matches:
            market_key = matches[0]
        else:
            raise ValueError(f"Unsupported market: {market}. Add mapping in MARKET_MAP.")

    app_key = os.getenv("APP_KEY")
    app_secret = os.getenv("APP_SECRET")
    if not app_key or not app_secret:
        raise SystemExit("APP_KEY / APP_SECRET environment variables are required.")

    asset_name = MARKET_MAP[market_key]["asset_name"]
    asset_id   = MARKET_MAP[market_key]["asset_id"]
    tbl_clear  = MARKET_MAP[market_key]["tbl_clear"]
    tbl_fcst   = MARKET_MAP[market_key]["tbl_fcst"]
    tbl_single = f"hist_{market_key}".lower()  # Excel-style single table
    # NEW: pick per-market province (fallback to global)
    prov_code  = MARKET_MAP[market_key].get("province_code", PROVINCE_CODE)
    # -------- decide window & fetch --------
    # -------- decide window & fetch --------
    HIST_START = os.getenv("HIST_START_DATE")
    HIST_END   = os.getenv("HIST_END_DATE")
    
    if HIST_START and HIST_END:
        resp = _fetch(app_key, app_secret, asset_id, HIST_START, HIST_END, province_code=prov_code)
        df = _normalize(resp)
    
    elif FULL_HISTORY:
        df = _crawl_all_history(app_key, app_secret, asset_id, prov_code)
            
        if df.empty:
            print(f"[WARN] No historical data available for {market_key}")
            return
    else:
        last_hist = _last_hist_date(market_key)
        today = pd.Timestamp.today().date()

        if last_hist is not None:
            start_day = (pd.to_datetime(last_hist) + pd.Timedelta(days=1)).date().isoformat()
            end_day = today.isoformat()
            if pd.to_datetime(start_day) > pd.to_datetime(end_day):
                print(f"[OK] {market_key}: no new days after {last_hist}. Nothing to update.")
                return
        else:
            # try to infer from a CSV hint; fallback to last 7 days
            start_day = end_day = None
            try:
                df_hint = pd.read_csv(pricefile, nrows=10000, encoding="utf-8")
                for col in ["time", "Time", "datetime", "Date", "date"]:
                    if col in df_hint.columns:
                        dt = pd.to_datetime(df_hint[col], errors="coerce")
                        if dt.notna().any():
                            start_day = dt.min().date().isoformat()
                            end_day   = dt.max().date().isoformat()
                            break
            except Exception:
                pass
            if not start_day or not end_day:
                end_day = today.isoformat()
                start_day = (today - pd.Timedelta(days=7)).isoformat()

        resp = _fetch(app_key, app_secret, asset_id, start_day, end_day, province_code=prov_code)
        df = _normalize(resp)
        # print("[DEBUG] forecast-like cols:", [c for c in df.columns if "Forecast" in c or "forecast" in c])

        if df.empty or df["time"].isna().all():
            print(f"[WARN] {market_key}: API returned no parsable rows in {start_day}→{end_day}.")
            return

    tmin, tmax = pd.to_datetime(df["time"]).min(), pd.to_datetime(df["time"]).max()
    print(f"[INFO] {market_key}: parsed time window {tmin} → {tmax} (rows={len(df)})")

    # -------- pick price series (with logging of which column matched) --------
    s_actual, used_actual, cand_actual = _pick_series(df, asset_name, "actual", province_code=prov_code)
    s_fcst,   used_fcst,   cand_fcst   = _pick_series(df, asset_name, "forecast", province_code=prov_code)

    print(f"[DEBUG] {market_key}: picked actual={used_actual} (non-NA={pd.Series(s_actual).notna().sum()}), "
          f"forecast={used_fcst} (non-NA={pd.Series(s_fcst).notna().sum()})")


    if s_actual.notna().sum() == 0 and s_fcst.notna().sum() == 0:
        print(f"[WARN] {market_key}: No usable price columns for asset '{asset_name}'. "
              f"Tried actual={cand_actual}, forecast={cand_fcst}. "
              f"First columns seen={list(df.columns)[:10]}")
        return

    if used_actual or used_fcst:
        print(f"[INFO] {market_key}: using columns → actual={used_actual}, forecast={used_fcst}")

    # -------- long -> matrix (15-min shift) --------
    df_long = (
        pd.DataFrame({
            "time": df["time"],
            "actual":   s_actual,
            "forecast": s_fcst,
        })
        .melt(id_vars=["time"], value_vars=["actual", "forecast"],
              var_name="metric", value_name="price")
        .dropna(subset=["price"])
    )

    if df_long.empty:
        print(f"[WARN] {market_key}: Price series exist, but melted frame is empty after dropna.")
        return
    # --- NEW: shift raw 15-min timestamps back by 15 min (00:15 -> 00:00, …, 00:00(next day) -> 23:45)
    df15 = df_long.copy()
    df15["time"] = df15["time"] - pd.Timedelta(minutes=15)
    # optional: enforce exact 15-min grid
    # df15["time"] = df15["time"].dt.floor("15min")

    # -------- write original 15-min series (unaltered timestamps) --------
    engine = _db_engine()
    tbl_clear_15 = f"{tbl_clear}_15min"
    tbl_fcst_15  = f"{tbl_fcst}_15min"
    
    n_15_clear = _replace_15min_split(engine, tbl_clear_15, df15, "actual")
    n_15_fcst  = _replace_15min_split(engine, tbl_fcst_15,  df15, "forecast")
    print(f"[OK] {market_key}: 15-min rows written → {tbl_clear_15}={n_15_clear}, {tbl_fcst_15}={n_15_fcst}")

    # -------- ALSO extract & write Day-Ahead series (15-min + hourly) --------
    keys = ASSET_API_KEYS.get(asset_name, {})
    
    # explicit day-ahead candidates (prefer asset-specific, then generic, then region-generic)
    da_act_candidates = [keys.get("actual_DA"), "dayAheadClearPrice", "hubaoxiDayAheadClearPrice"]
    da_fc_candidates = [keys.get("forecast_DA"),
    "dayAheadPriceForecast", "dayAheadForecastPrice",           # <— added
    "hubaoxiDayAheadPriceForecast", "hubaoxiDayAheadForecastPrice",  # <— added
    ]
    s_da_act, used_da_act = _pick_series_by_names(df, da_act_candidates)
    s_da_fc,  used_da_fc  = _pick_series_by_names(df, da_fc_candidates)
    
    print(f"[DEBUG] {market_key}: picked DA actual={used_da_act} (non-NA={pd.Series(s_da_act).notna().sum()}), "
          f"DA forecast={used_da_fc} (non-NA={pd.Series(s_da_fc).notna().sum()})")
    
    # build long frames for DA; shift by 15 minutes to align to 00:00~23:45
    df_da_long = (
        pd.DataFrame({"time": df["time"], "actual_DA": s_da_act, "forecast_DA": s_da_fc})
        .melt(id_vars=["time"], value_vars=["actual_DA", "forecast_DA"],
              var_name="metric", value_name="price")
        .dropna(subset=["price"])
    )
    df_da15 = df_da_long.copy()
    df_da15["time"] = df_da15["time"] - pd.Timedelta(minutes=15)
    
    # 15-min DA tables
    tbl_da_clear_15 = f"{tbl_clear}_dayahead_15min"      # e.g., hist_anhui_dingyuan_clear_dayahead_15min
    tbl_da_fcst_15  = f"{tbl_fcst}_dayahead_15min"       # e.g., hist_anhui_dingyuan_forecast_dayahead_15min
    
    n_da15_clear = _replace_15min_split(engine, tbl_da_clear_15, df_da15, "actual_DA")
    n_da15_fcst  = _replace_15min_split(engine, tbl_da_fcst_15,  df_da15, "forecast_DA")
    print(f"[OK] {market_key}: 15-min DA rows → {tbl_da_clear_15}={n_da15_clear}, {tbl_da_fcst_15}={n_da15_fcst}")
    
    # Hourly DA matrix (keeps your split tables pattern)
    if not df_da_long.empty:
        mat_da_df = _shift15_pivot_hour(df_da_long)
        n_da_clear_hour = _replace_matrix(engine, f"{tbl_clear}_dayahead",  mat_da_df, "actual_DA")
        n_da_fcst_hour  = _replace_matrix(engine, f"{tbl_fcst}_dayahead",   mat_da_df, "forecast_DA")
        print(f"[OK] {market_key}: DA matrix rows → {tbl_clear}_dayahead={n_da_clear_hour}, {tbl_fcst}_dayahead={n_da_fcst_hour}")

    mat_df = _shift15_pivot_hour(df_long)

    # -------- write to DB (split + single) --------
    engine = _db_engine()
    n_clear = _replace_matrix(engine, tbl_clear, mat_df, "actual")
    n_fcst  = _replace_matrix(engine, tbl_fcst,  mat_df, "forecast")
    n_single= _replace_hist_single(engine, tbl_single, mat_df)

    print(f"[OK] {market_key}: matrix rows written → {tbl_clear}={n_clear}, {tbl_fcst}={n_fcst}, {tbl_single}={n_single}")

    # -------- export incremental CSV (actual) --------
    csv_path = _export_hist_csv_incremental(market_key, mat_df, tbl_label="actual")
    if csv_path:
        print(f"[OK] Exported {csv_path}")
        

def smoke_test_market_api(market_key="Mengxi_SuYou", days=2):
    m = MARKET_MAP[market_key]
    app_key = os.getenv("APP_KEY"); app_secret = os.getenv("APP_SECRET")
    if not app_key or not app_secret:
        print("APP_KEY/APP_SECRET not set"); return
    today = pd.Timestamp.today().date()
    start = (today - pd.Timedelta(days=days)).isoformat()
    end   = today.isoformat()
    print(f"[TEST] host={APIM_URL.split('/tt-')[0]} assetId={m['asset_id']} province={m.get('province_code','?')} window={start}->{end}")
    try:
        resp = _fetch(app_key, app_secret, m["asset_id"], start, end, province_code=m.get("province_code","15"))
        df = _normalize(resp)
        print(f"[TEST] rows={len(df)} time=[{df['time'].min()}..{df['time'].max()}]" if not df.empty else "[TEST] empty dataframe")
    except Exception as e:
        print(f"[TEST] error: {e}")



if __name__=="__main__":
    Column_to_Matrix(pricefile="", market="Mengxi_SuYou")
    Column_to_Matrix(pricefile="", market="Mengxi_WuLaTe")
    Column_to_Matrix(pricefile="", market="Mengxi_WuHai")
    Column_to_Matrix(pricefile="", market="Mengxi_WuLanChaBu")
    Column_to_Matrix(pricefile="", market="Shandong_BinZhou")
    Column_to_Matrix(pricefile="", market="Anhui_DingYuan")
    Column_to_Matrix(pricefile="", market="Jiangsu_SheYang")
    