import os
import re
from datetime import datetime, date, timedelta, timezone

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

TABLE_PREFIX = "md_"
LOAD_LOG_TABLE = f"{TABLE_PREFIX}load_log"

# ----------------------------
# Sheet → target table mapping
# ----------------------------
SHEET_TABLE_MAP = {
    "现货市场平均申报电价": f"{TABLE_PREFIX}avg_bid_price",
    "日前各类电源电量和台数及平均出清电价": f"{TABLE_PREFIX}da_fuel_summary",
    "日前各时段出清电量": f"{TABLE_PREFIX}da_cleared_energy",
    "日内各类电源电量和台数及平均出清电价": f"{TABLE_PREFIX}id_fuel_summary",
    "日内各时段出清电量": f"{TABLE_PREFIX}id_cleared_energy",
    "发电侧现货实时出清总电量": f"{TABLE_PREFIX}rt_total_cleared_energy",
    "实时节点电价": f"{TABLE_PREFIX}rt_nodal_price",
    "统一结算参考点价格": f"{TABLE_PREFIX}settlement_ref_price",
}

# ================= TIME SEMANTICS BY TABLE =================
# Rules interpret how to convert the Excel "time" column (often 00:15..24:00 without date)
# into a proper timestamp stored in DB.
#
# Definitions:
# - anchor_plus_days: how many days to add to the file date to form the *anchor date*
# - shift_minutes: minutes to add after parsing (negative means shift earlier)
#
# Notes:
# - For 15-minute series where Excel uses "24:00" as the last point, using shift_minutes=-15
#   ensures the last point becomes 23:45 of the anchor date.
TIME_RULES = {
    # RT total cleared energy: timestamp belongs to file date, then shift -15 min
    "md_rt_total_cleared_energy": {"anchor_plus_days": 0, "shift_minutes": -15},

    # Intraday: use file date, shift -15 min
    "md_id_cleared_energy":       {"anchor_plus_days": 0, "shift_minutes": -15},
    "md_id_fuel_summary":         {"anchor_plus_days": 0, "shift_minutes": -15},

    # Day-ahead: delivery is next day (file_date+1), shift -15 min
    "md_da_cleared_energy":       {"anchor_plus_days": 1, "shift_minutes": -15},
    "md_da_fuel_summary":         {"anchor_plus_days": 1, "shift_minutes": -15},

    # Settlement reference price: hourly series, anchor=file date, shift -60 min
    "md_settlement_ref_price":    {"anchor_plus_days": 0, "shift_minutes": -60},

    # RT nodal price: anchor=file date, shift -15 min
    "md_rt_nodal_price":          {"anchor_plus_days": 0, "shift_minutes": -15},
}
# ============================================================



# DA sheets represent next-day delivery (per your earlier rule)
DA_NEXT_DAY_SHEETS = {
    "日前各类电源电量和台数及平均出清电价",
    "日前各时段出清电量",
}

# ----------------------------
# Column normalization mapping
# ----------------------------
COL_MAP = {
    "类型": "type",
    "价格": "price",
    "时刻": "datetime",
    "时间": "datetime",
    "交易时刻": "datetime",
    "时段": "datetime",
    "电量": "energy_mwh",
    "台数": "unit_count",
    "调度电厂名称": "plant_name",
    "电厂名称": "plant_name",
    "调度机组名称": "dispatch_unit_name",
    "机组名称": "dispatch_unit_name",
    "调度单元名称": "dispatch_unit_name",
    "中标电量": "cleared_energy_mwh",
    "中标电价": "cleared_price",
    "节点名称": "node_name",
    "节点电价(元/MWh)": "node_price",
    "现货市场实时总出清电量(MWh)": "rt_total_cleared_energy_mwh",
    "全网统一结算点价格（元/MWh）": "system_settlement_price",
    "电能价格(元/MWh)": "energy_price",
    "阻塞价格(元/MWh)": "congestion_price",
}

# ----------------------------
# “Preferred” conflict keys (we will dynamically pick those that exist)
# ----------------------------
PREFERRED_KEYS = {
    f"{TABLE_PREFIX}avg_bid_price": ["data_date", "type"],
    f"{TABLE_PREFIX}da_fuel_summary": ["data_date", "datetime", "type", "plant_name", "dispatch_unit_name"],
    f"{TABLE_PREFIX}da_cleared_energy": ["data_date", "datetime", "plant_name", "dispatch_unit_name"],
    f"{TABLE_PREFIX}id_fuel_summary": ["data_date", "datetime", "type", "plant_name", "dispatch_unit_name"],
    f"{TABLE_PREFIX}id_cleared_energy": ["data_date", "datetime", "plant_name", "dispatch_unit_name"],
    f"{TABLE_PREFIX}rt_nodal_price": ["data_date", "datetime", "node_name"],
    f"{TABLE_PREFIX}rt_total_cleared_energy": ["data_date", "datetime"],
    f"{TABLE_PREFIX}settlement_ref_price": ["data_date", "datetime"],
}

# For dedup scoring, prioritize rows with non-null values in these columns (if present)
VALUE_COL_HINTS = {
    f"{TABLE_PREFIX}da_cleared_energy": ["energy_mwh", "cleared_energy_mwh", "cleared_price", "price"],
    f"{TABLE_PREFIX}id_cleared_energy": ["energy_mwh", "cleared_energy_mwh", "cleared_price", "price"],
    f"{TABLE_PREFIX}rt_total_cleared_energy": ["rt_total_cleared_energy_mwh"],
    f"{TABLE_PREFIX}rt_nodal_price": ["node_price"],
    f"{TABLE_PREFIX}settlement_ref_price": ["system_settlement_price"],
}

# Sheets where time is interval-end and must be shifted to interval-start
SHIFT_15MIN_TABLES = {
    f"{TABLE_PREFIX}da_cleared_energy",
    f"{TABLE_PREFIX}id_cleared_energy",
    f"{TABLE_PREFIX}rt_total_cleared_energy",
    f"{TABLE_PREFIX}rt_nodal_price",
    f"{TABLE_PREFIX}settlement_ref_price",
}

FILENAME_RE = re.compile(r"^data_(\d{4}-\d{2}-\d{2})\.xlsx$", re.I)


def parse_file_date(fn: str) -> date:
    m = FILENAME_RE.match(fn)
    if not m:
        raise ValueError(f"Bad filename: {fn}")
    return datetime.strptime(m.group(1), "%Y-%m-%d").date()


def safe_col(name: str) -> str:
    name = name.lower().strip()
    name = re.sub(r"[^\w]+", "_", name)   # replace non-alphanumeric with _
    name = re.sub(r"_+", "_", name).strip("_")
    return name


def translate_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    def norm_col(x):
        if x is None:
            return ""
        x = str(x).strip().replace("\n", "").replace("\r", "")
        if x in COL_MAP:
            return COL_MAP[x]
        return safe_col(x)   # <--- NEW SAFETY

    df.columns = [norm_col(c) for c in df.columns]
    return df


def get_last_loaded_date(engine: Engine, schema: str):
    with engine.begin() as c:
        return c.execute(text(
            f"SELECT max(file_date) FROM {schema}.{LOAD_LOG_TABLE} WHERE status='success'"
        )).scalar()


def already_loaded_success(engine: Engine, schema: str, file_date: date) -> bool:
    sql = f"""
        SELECT 1
        FROM {schema}.{LOAD_LOG_TABLE}
        WHERE file_date = :d
          AND status = 'success'
        LIMIT 1
    """
    with engine.begin() as c:
        return c.execute(text(sql), {"d": file_date}).scalar() is not None


def is_hierarchical_time(df: pd.DataFrame) -> bool:
    # many blank datetime cells -> merged/hierarchical
    if "datetime" not in df.columns:
        return False
    # treat empty strings as missing too
    s = df["datetime"]
    missing = s.isna() | (s.astype(str).str.strip() == "")
    return missing.sum() > len(df) * 0.2


def parse_time_to_minutes(v):
    """
    Accept:
      - 00:15 / 00:15:00
      - 24:00 / 24:00:00
    Return minutes since 00:00 where 24:00 => 1440
    """
    s = str(v).strip()
    if s == "" or s.lower() in {"nan", "none"}:
        return None
    m = re.match(r"^(\d{1,2}):(\d{2})(?::(\d{2}))?$", s)
    if not m:
        return None
    hh = int(m.group(1))
    mm = int(m.group(2))
    if hh == 24 and mm == 0:
        return 1440
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        return None
    return hh * 60 + mm


def has_date_component(s: str) -> bool:
    # detects 2025-01-02 or 2025/1/2 etc
    return re.search(r"\d{4}[-/]\d{1,2}[-/]\d{1,2}", s) is not None


def normalize_datetime_series(raw: pd.Series, table_name: str, file_date: date) -> pd.Series:
    """Normalize a datetime-like series using per-table semantics.

    Inputs commonly observed in the source Excels:
    - Excel serial datetime numbers
    - time-only strings like '00:15', '23:45', '24:00'
    - full timestamps (sometimes with date)

    Rule application order:
    1) If value includes an explicit date -> parse it (special-case 24:00).
    2) Else treat as time-only -> anchor to (file_date + anchor_plus_days), with 24:00 -> next day 00:00.
    3) Apply shift_minutes (can be negative, e.g., -15 or -60).

    TIME_RULES (configured at top of file) is the source of truth.
    """
    s = raw.copy()

    # keep blanks as NA
    s = s.astype(str).str.strip()
    s = s.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA, "NaT": pd.NA})

    rule = TIME_RULES.get(table_name, {"anchor_plus_days": 0, "shift_minutes": 0})
    anchor_plus_days = int(rule.get("anchor_plus_days", 0) or 0)
    shift_minutes = int(rule.get("shift_minutes", 0) or 0)
    anchor_date = pd.Timestamp(file_date) + pd.Timedelta(days=anchor_plus_days)

    out = []
    for v in s.tolist():
        if v is pd.NA or v is None:
            out.append(pd.NaT)
            continue
        v_str = str(v).strip()

        if has_date_component(v_str):
            # explicit date-time; handle "... 24:00"
            if re.search(r"\s24:00(:00)?$", v_str):
                # convert "YYYY-MM-DD 24:00" -> next day 00:00
                d_part = re.split(r"\s+", v_str)[0]
                d0 = pd.to_datetime(d_part, errors="coerce")
                if pd.isna(d0):
                    out.append(pd.NaT)
                else:
                    out.append(d0 + pd.Timedelta(days=1))
            else:
                ts = pd.to_datetime(v_str, errors="coerce")
                out.append(ts if pd.notna(ts) else pd.NaT)
        else:
            # time-only
            mins = parse_time_to_minutes(v_str)
            if mins is None:
                # last resort parse
                ts = pd.to_datetime(v_str, errors="coerce")
                out.append(ts if pd.notna(ts) else pd.NaT)
            else:
                base = pd.Timestamp(anchor_date)
                if mins == 1440:
                    ts = base + pd.Timedelta(days=1)  # 24:00 -> next day 00:00
                else:
                    ts = base + pd.Timedelta(minutes=mins)
                out.append(ts)

    dt = pd.to_datetime(pd.Series(out), errors="coerce")

    if shift_minutes:
        dt = dt + pd.Timedelta(minutes=shift_minutes)
    return dt


def pick_conflict_keys(table: str, df: pd.DataFrame) -> list[str]:
    """
    Dynamically choose conflict keys from preferred list.
    Must include datetime if present, and data_date if present, to avoid cross-day collisions.
    """
    pref = PREFERRED_KEYS.get(table, [])
    keys = [k for k in pref if k in df.columns]

    # Safety: ensure at least one deterministic key
    if not keys:
        # fall back: best effort
        if "data_date" in df.columns and "datetime" in df.columns:
            keys = ["data_date", "datetime"]
        elif "datetime" in df.columns:
            keys = ["datetime"]
        elif "data_date" in df.columns:
            keys = ["data_date"]
        else:
            raise ValueError(f"No usable conflict keys for {table}; columns={list(df.columns)}")

    return keys


def ensure_table(engine, schema, table, df):
    cols = []
    for c, t in df.dtypes.items():

        # datatype detection
        if c == "datetime":
            sql_t = "timestamp"
        elif c == "data_date":
            sql_t = "date"
        else:
            ts = str(t).lower()
            if "int" in ts or "float" in ts or "number" in ts:
                sql_t = "numeric(18,3)"
            else:
                sql_t = "text"

        # add NOT NULL BEFORE append
        if c in ("datetime", "data_date"):
            sql_t += " NOT NULL"

        cols.append(f"{c} {sql_t}")

    ddl = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        {", ".join(cols)}
    );
    """
    with engine.begin() as c:
        c.execute(text(ddl))



def create_unique_index(engine, schema, table, keys):
    if not keys:
        return

    idx_name = f"ux_{table}_" + "_".join(keys)

    with engine.begin() as c:
        c.execute(text(f"""
            DROP INDEX IF EXISTS {schema}.{idx_name};
            CREATE UNIQUE INDEX {idx_name}
            ON {schema}.{table} ({",".join(keys)});
        """))




def dedup_keep_best(df: pd.DataFrame, key_cols: list[str], value_cols: list[str]) -> pd.DataFrame:
    """
    For duplicated keys, keep the row with the highest count of non-null value cols.
    If no value cols exist, keep last.
    """
    if not key_cols:
        return df

    vcols = [c for c in value_cols if c in df.columns]
    if not vcols:
        return df.drop_duplicates(subset=key_cols, keep="last")

    d = df.copy()
    d["_score"] = d[vcols].notna().sum(axis=1)
    d = d.sort_values(key_cols + ["_score"], ascending=[True] * len(key_cols) + [False])
    d = d.drop_duplicates(subset=key_cols, keep="first").drop(columns=["_score"])
    return d


def ensure_rt_nodal_price_table(engine, schema):
    with engine.begin() as c:
        c.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {schema}.md_rt_nodal_price (
                datetime TIMESTAMP NOT NULL,
                node_name TEXT NOT NULL,
                node_price NUMERIC(18,3),
                energy_price NUMERIC(18,3),
                congestion_price NUMERIC(18,3),
                data_date DATE NOT NULL,
                source_file TEXT,
                PRIMARY KEY (data_date, datetime, node_name)
            );
        """))

def ensure_schema_and_log(engine: Engine, schema: str):
    with engine.begin() as c:
        c.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))
        c.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {schema}.{LOAD_LOG_TABLE} (
                id BIGSERIAL PRIMARY KEY,
                file_date DATE,
                file_name TEXT,
                loaded_at TIMESTAMP DEFAULT now(),
                status TEXT,
                message TEXT
            );
        """))
        # Ensure the nodal price table is created
        ensure_rt_nodal_price_table(engine, schema)

def upsert(engine, schema, table, df):
    keys = pick_conflict_keys(table, df)

    # Deduplicate first (important for "日内各时段出清电量" style duplicates)
    value_cols = VALUE_COL_HINTS.get(table, [])
    df = dedup_keep_best(df, keys, value_cols)

    stage = f"_stg_{table}_{int(datetime.now(timezone.utc).timestamp())}"
    df.to_sql(stage, engine, schema=schema, if_exists="replace", index=False, method="multi")

    cols = list(df.columns)
    col_list = ",".join(cols)

    # do not update key columns
    update_cols = [c for c in cols if c not in keys]
    if update_cols:
        updates = ",".join([f"{c}=EXCLUDED.{c}" for c in update_cols])
        conflict = f"ON CONFLICT ({','.join(keys)}) DO UPDATE SET {updates}"
    else:
        conflict = f"ON CONFLICT ({','.join(keys)}) DO NOTHING"

    sql = f"""
        INSERT INTO {schema}.{table} ({col_list})
        SELECT {col_list} FROM {schema}.{stage}
        {conflict};
    """

    with engine.begin() as c:
        c.execute(text(sql))
        c.execute(text(f"DROP TABLE {schema}.{stage};"))






def main(output_dir):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    load_dotenv(os.path.join(base_dir, ".env"))

    pgurl = os.environ.get("PGURL") or os.environ.get("DATABASE_URL") or os.environ.get("PG_URI")
    if not pgurl:
        raise RuntimeError("Missing PGURL (or DATABASE_URL/PG_URI) environment variable.")

    engine = create_engine(pgurl)
    schema = os.environ.get("DB_SCHEMA", "marketdata")

    ensure_schema_and_log(engine, schema)  # Ensure schema and log tables

    files = [fn for fn in os.listdir(output_dir) if FILENAME_RE.match(fn)]
    if not files:
        print(f"No files matched in {output_dir} (pattern data_YYYY-MM-DD.xlsx).")
        return

    force_reload = os.getenv("FORCE_RELOAD", "false").lower() == "true"

    for fn in sorted(files):
        file_date = parse_file_date(fn)
    
        if already_loaded_success(engine, schema, file_date) and not force_reload:
            print(f"[SKIP LOADED] {fn} already loaded successfully")
            continue

        try:
            xls = pd.ExcelFile(os.path.join(output_dir, fn))

            file_had_success = False
            file_errors = []
            
            for sheet in xls.sheet_names:
                if sheet not in SHEET_TABLE_MAP:
                    continue
            
                try:
                    table = SHEET_TABLE_MAP[sheet]
                    df = translate_columns(pd.read_excel(xls, sheet, header=0))
                    df = df.loc[:, ~df.columns.str.contains("^unnamed", case=False)]
            
                    # ---- ADD METADATA ----
                    _rule = TIME_RULES.get(table, {"anchor_plus_days": 0, "shift_minutes": 0})
                    delivery_date = file_date + timedelta(days=int(_rule.get("anchor_plus_days", 0) or 0))
                    df["data_date"] = delivery_date
                    df["source_file"] = fn
            
                    # ---- TIME NORMALIZATION ----
                    if "datetime" in df.columns:
                        raw = df["datetime"]
                        df["datetime"] = normalize_datetime_series(raw, table_name=table, file_date=file_date)
            
                    # ---- FILTER RT NODAL PRICE TO FILE DATE ----
                    if table == "md_rt_nodal_price" and "datetime" in df.columns:
                        df = df[df["datetime"].dt.date == file_date]
            
                    # ---- EMPTY CHECK ----
                    non_meta_cols = [c for c in df.columns if c not in ("data_date", "source_file", "datetime")]
                    if df[non_meta_cols].dropna(how="all").empty:
                        print(f"[SKIP EMPTY] {fn} sheet {sheet}")
                        continue
            
                    # ---- TABLE ENSURE ----
                    if table == "md_rt_nodal_price":
                        ensure_rt_nodal_price_table(engine, schema)
                    else:
                        ensure_table(engine, schema, table, df)
            
                    keys = pick_conflict_keys(table, df)
                    create_unique_index(engine, schema, table, keys)
            
                    # ---- UPSERT ----
                    upsert(engine, schema, table, df)
            
                    print(f"[OK] {fn} | {sheet}")
                    file_had_success = True
            
                except Exception as sheet_error:
                    msg = f"[SHEET FAIL] {fn} | {sheet}: {repr(sheet_error)}"
                    print(msg)
                    file_errors.append(msg)

            notes_combined = "\n".join(file_errors) if file_errors else None

            status = "success" if file_had_success else "failed"
            
            with engine.begin() as c:
                c.execute(
                    text(f"""
                        INSERT INTO {schema}.{LOAD_LOG_TABLE}
                        (file_date, file_name, status, message)
                        VALUES (:d, :f, :s, :m)
                    """),
                    {
                        "d": file_date,
                        "f": fn,
                        "s": status,
                        "m": notes_combined
                    },
                )
            print(f"[OK] {fn}")

        except Exception as e:
            with engine.begin() as c:
                if file_had_success:
                    status = "success"
                    message = "\n".join(file_errors) if file_errors else None
                else:
                    status = "failed"
                    message = "\n".join(file_errors)
            
                c.execute(
                    text(f"""
                        INSERT INTO {schema}.{LOAD_LOG_TABLE}
                        (file_date, file_name, status, message)
                        VALUES (:d, :f, :s, :m)
                    """),
                    {"d": file_date, "f": fn, "s": status, "m": message},
                )
            
            print(f"[FILE {status.upper()}] {fn}")

            print(f"[FAIL] {fn}: {e}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python load_excel_to_marketdata.py <output_dir>")
    main(sys.argv[1])