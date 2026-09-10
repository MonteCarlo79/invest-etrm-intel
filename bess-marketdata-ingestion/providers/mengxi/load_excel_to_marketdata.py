import os
import re
from datetime import datetime, date, timedelta, timezone
import io
from uuid import uuid4

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

TABLE_PREFIX = "md_"
LOAD_LOG_TABLE = f"{TABLE_PREFIX}load_log"

# ----------------------------
# Runtime controls
# ----------------------------
MIN_FILE_SIZE_MB = 3
MIN_FILE_SIZE_BYTES = MIN_FILE_SIZE_MB * 1024 * 1024
DEFAULT_PROVINCE = "mengxi"

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
TIME_RULES = {
    "md_rt_total_cleared_energy": {"anchor_plus_days": 0, "shift_minutes": -15},
    "md_id_cleared_energy": {"anchor_plus_days": 0, "shift_minutes": -15},
    "md_id_fuel_summary": {"anchor_plus_days": 0, "shift_minutes": -15},
    "md_da_cleared_energy": {"anchor_plus_days": 1, "shift_minutes": -15},
    "md_da_fuel_summary": {"anchor_plus_days": 1, "shift_minutes": -15},
    "md_settlement_ref_price": {"anchor_plus_days": 0, "shift_minutes": -60},
    "md_rt_nodal_price": {"anchor_plus_days": 0, "shift_minutes": -15},
}
# ============================================================

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
    "中标价格": "cleared_price", 
    "出清电价": "cleared_price",
    "出清价格": "cleared_price",
    "节点名称": "node_name",
    "节点电价(元/MWh)": "node_price",
    "现货市场实时总出清电量(MWh)": "rt_total_cleared_energy_mwh",
    "全网统一结算点价格（元/MWh）": "system_settlement_price",
    "电能价格(元/MWh)": "energy_price",
    "阻塞价格(元/MWh)": "congestion_price",
}

# ----------------------------
# “Preferred” conflict keys
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

VALUE_COL_HINTS = {
    f"{TABLE_PREFIX}da_cleared_energy": ["energy_mwh", "cleared_energy_mwh", "cleared_price", "price"],
    f"{TABLE_PREFIX}id_cleared_energy": ["energy_mwh", "cleared_energy_mwh", "cleared_price", "price"],
    f"{TABLE_PREFIX}rt_total_cleared_energy": ["rt_total_cleared_energy_mwh"],
    f"{TABLE_PREFIX}rt_nodal_price": ["node_price"],
    f"{TABLE_PREFIX}settlement_ref_price": ["system_settlement_price"],
}

SHIFT_15MIN_TABLES = {
    f"{TABLE_PREFIX}da_cleared_energy",
    f"{TABLE_PREFIX}id_cleared_energy",
    f"{TABLE_PREFIX}rt_total_cleared_energy",
    f"{TABLE_PREFIX}rt_nodal_price",
    f"{TABLE_PREFIX}settlement_ref_price",
}

FILENAME_RE = re.compile(r"^data_(\d{4}-\d{2}-\d{2})\.xlsx$", re.I)


def quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def copy_dataframe_to_postgres(df, table_name, engine, schema="marketdata"):
    if df.empty:
        return

    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False, na_rep="", lineterminator="\n")
    buffer.seek(0)

    raw_conn = engine.raw_connection()
    try:
        cursor = raw_conn.cursor()

        quoted_schema = quote_ident(schema)
        quoted_table = quote_ident(table_name)
        quoted_cols = ",".join(quote_ident(c) for c in df.columns)

        sql = (
            f"COPY {quoted_schema}.{quoted_table} ({quoted_cols}) "
            f"FROM STDIN WITH (FORMAT CSV, NULL '')"
        )

        cursor.copy_expert(sql, buffer)
        raw_conn.commit()
    except Exception:
        raw_conn.rollback()
        raise
    finally:
        raw_conn.close()


def update_data_quality(
    engine: Engine,
    schema: str,
    province: str,
    file_date: date,
    filepath: str,
    interval_count: int,
    has_failures: bool = False,
    notes: str | None = None,
):
    file_size_mb = os.path.getsize(filepath) / (1024 * 1024)

    expected = 96
    coverage = (interval_count / expected) if expected else 0.0

    is_complete = (
        file_size_mb >= MIN_FILE_SIZE_MB
        and interval_count >= expected
        and not has_failures
    )

    sql = f"""
    INSERT INTO {schema}.data_quality_status
    (province, data_date, source_file, file_size_mb,
     expected_intervals, actual_intervals, interval_coverage, is_complete, notes)
    VALUES
    (:province, :data_date, :source_file, :file_size_mb,
     :expected, :actual, :coverage, :is_complete, :notes)

    ON CONFLICT (province, data_date)
    DO UPDATE SET
      source_file = EXCLUDED.source_file,
      file_size_mb = EXCLUDED.file_size_mb,
      expected_intervals = EXCLUDED.expected_intervals,
      actual_intervals = EXCLUDED.actual_intervals,
      interval_coverage = EXCLUDED.interval_coverage,
      is_complete = EXCLUDED.is_complete,
      notes = EXCLUDED.notes,
      check_time = now();
    """

    with engine.begin() as conn:
        conn.execute(text(sql), {
            "province": province,
            "data_date": file_date,
            "source_file": os.path.basename(filepath),
            "file_size_mb": round(file_size_mb, 2),
            "expected": expected,
            "actual": int(interval_count),
            "coverage": round(coverage, 4),
            "is_complete": is_complete,
            "notes": notes,
        })


def parse_file_date(fn: str) -> date:
    m = FILENAME_RE.match(fn)
    if not m:
        raise ValueError(f"Bad filename: {fn}")
    return datetime.strptime(m.group(1), "%Y-%m-%d").date()


def parse_optional_date(s: str | None) -> date | None:
    if not s:
        return None
    return datetime.strptime(s, "%Y-%m-%d").date()


def env_bool(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "y"}


def safe_col(name: str) -> str:
    name = name.lower().strip()
    name = re.sub(r"[^\w]+", "_", name)
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
        return safe_col(x)

    df.columns = [norm_col(c) for c in df.columns]
    return df


def get_last_loaded_date(engine: Engine, schema: str):
    with engine.begin() as c:
        return c.execute(
            text(
                f"SELECT max(file_date) FROM {schema}.{LOAD_LOG_TABLE} WHERE status='success'"
            )
        ).scalar()


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


def log_load_status(engine: Engine, schema: str, file_date: date, file_name: str, status: str, message: str | None = None):
    with engine.begin() as c:
        c.execute(
            text(f"""
                INSERT INTO {schema}.{LOAD_LOG_TABLE}
                (file_date, file_name, status, message)
                VALUES (:d, :f, :s, :m)
            """),
            {"d": file_date, "f": file_name, "s": status, "m": message},
        )


def is_hierarchical_time(df: pd.DataFrame) -> bool:
    if "datetime" not in df.columns:
        return False
    s = df["datetime"]
    missing = s.isna() | (s.astype(str).str.strip() == "")
    return missing.sum() > len(df) * 0.2


def parse_time_to_minutes(v):
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
    return re.search(r"\d{4}[-/]\d{1,2}[-/]\d{1,2}", s) is not None


def normalize_datetime_series(raw: pd.Series, table_name: str, file_date: date) -> pd.Series:
    s = raw.copy()
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
            if re.search(r"\s24:00(:00)?$", v_str):
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
            mins = parse_time_to_minutes(v_str)
            if mins is None:
                ts = pd.to_datetime(v_str, errors="coerce")
                out.append(ts if pd.notna(ts) else pd.NaT)
            else:
                base = pd.Timestamp(anchor_date)
                if mins == 1440:
                    ts = base + pd.Timedelta(days=1)
                else:
                    ts = base + pd.Timedelta(minutes=mins)
                out.append(ts)

    dt = pd.to_datetime(pd.Series(out), errors="coerce")

    if shift_minutes:
        dt = dt + pd.Timedelta(minutes=shift_minutes)
    return dt


def pick_conflict_keys(table: str, df: pd.DataFrame) -> list[str]:
    pref = PREFERRED_KEYS.get(table, [])
    keys = [k for k in pref if k in df.columns]

    if not keys:
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
        if c == "datetime":
            sql_t = "timestamp"
        elif c == "data_date":
            sql_t = "date"
        else:
            ts = str(t).lower()
            if "int" in ts or "float" in ts or "number" in ts:
                sql_t = "numeric(18,3)"
            elif "bool" in ts:
                sql_t = "boolean"
            else:
                sql_t = "text"

        if c in ("datetime", "data_date"):
            sql_t += " NOT NULL"

        cols.append(f"{quote_ident(c)} {sql_t}")

    ddl = f"""
    CREATE TABLE IF NOT EXISTS {quote_ident(schema)}.{quote_ident(table)} (
        {", ".join(cols)}
    );
    """
    with engine.begin() as c:
        c.execute(text(ddl))


def create_unique_index(engine, schema, table, keys):
    if not keys:
        return

    idx_name = f"ux_{table}_" + "_".join(keys)

    quoted_idx_name = quote_ident(idx_name)
    quoted_schema = quote_ident(schema)
    quoted_table = quote_ident(table)
    quoted_keys = ",".join(quote_ident(k) for k in keys)

    with engine.begin() as c:
        c.execute(text(f"DROP INDEX IF EXISTS {quoted_schema}.{quoted_idx_name};"))
        c.execute(text(f"""
            CREATE UNIQUE INDEX {quoted_idx_name}
            ON {quoted_schema}.{quoted_table} ({quoted_keys});
        """))


def dedup_keep_best(df: pd.DataFrame, key_cols: list[str], value_cols: list[str]) -> pd.DataFrame:
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


def ensure_data_quality_table(engine: Engine, schema: str):
    with engine.begin() as c:
        c.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {schema}.data_quality_status (
                id BIGSERIAL PRIMARY KEY,
                province TEXT NOT NULL,
                data_date DATE NOT NULL,
                source_file TEXT,
                file_size_mb NUMERIC(10,2),
                expected_intervals INTEGER,
                actual_intervals INTEGER,
                interval_coverage NUMERIC(10,4),
                is_complete BOOLEAN DEFAULT FALSE,
                check_time TIMESTAMP DEFAULT now(),
                notes TEXT
            );
        """))
        c.execute(text(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_data_quality_status_province_date
            ON {schema}.data_quality_status (province, data_date);
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
    ensure_rt_nodal_price_table(engine, schema)
    ensure_data_quality_table(engine, schema)


def upsert(engine, schema, table, df):
    keys = pick_conflict_keys(table, df)
    value_cols = VALUE_COL_HINTS.get(table, [])
    df = dedup_keep_best(df, keys, value_cols)

    if df.empty:
        return

    stage = f"_stg_{table}_{uuid4().hex}"

    ensure_table(engine, schema, stage, df)
    copy_dataframe_to_postgres(df, stage, engine, schema=schema)

    cols = list(df.columns)
    quoted_cols = ",".join(quote_ident(c) for c in cols)

    update_cols = [c for c in cols if c not in keys]
    if update_cols:
        updates = ",".join([f"{quote_ident(c)}=EXCLUDED.{quote_ident(c)}" for c in update_cols])
        conflict = f"ON CONFLICT ({','.join(quote_ident(k) for k in keys)}) DO UPDATE SET {updates}"
    else:
        conflict = f"ON CONFLICT ({','.join(quote_ident(k) for k in keys)}) DO NOTHING"

    sql = f"""
        INSERT INTO {quote_ident(schema)}.{quote_ident(table)} ({quoted_cols})
        SELECT {quoted_cols} FROM {quote_ident(schema)}.{quote_ident(stage)}
        {conflict};
    """

    with engine.begin() as c:
        c.execute(text(sql))
        c.execute(text(f"DROP TABLE IF EXISTS {quote_ident(schema)}.{quote_ident(stage)};"))


def delete_existing_rows_for_file_date(engine: Engine, schema: str, file_date: date):
    """
    Used for repair/backfill runs.
    Deletes existing rows tied to the file date/delivery date so patched files can fully replace
    previously incomplete data instead of relying only on upsert overlaps.
    """
    with engine.begin() as c:
        for table in SHEET_TABLE_MAP.values():
            rule = TIME_RULES.get(table, {"anchor_plus_days": 0})
            delivery_date = file_date + timedelta(days=int(rule.get("anchor_plus_days", 0) or 0))
            c.execute(
                text(f"DELETE FROM {quote_ident(schema)}.{quote_ident(table)} WHERE data_date = :d"),
                {"d": delivery_date},
            )


def main(output_dir):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    load_dotenv(os.path.join(base_dir, ".env"))

    pgurl = os.environ.get("PGURL") or os.environ.get("DATABASE_URL") or os.environ.get("PG_URI")
    if not pgurl:
        raise RuntimeError("Missing PGURL (or DATABASE_URL/PG_URI) environment variable.")

    engine = create_engine(pgurl, pool_pre_ping=True)
    schema = os.environ.get("DB_SCHEMA", "marketdata")
    province = os.environ.get("PROVINCE", DEFAULT_PROVINCE)

    force_reload = env_bool("FORCE_RELOAD", "false")
    start_date = parse_optional_date(os.getenv("START_DATE"))
    end_date = parse_optional_date(os.getenv("END_DATE"))

    ensure_schema_and_log(engine, schema)

    files = [fn for fn in os.listdir(output_dir) if FILENAME_RE.match(fn)]
    if not files:
        print(f"No files matched in {output_dir} (pattern data_YYYY-MM-DD.xlsx).")
        return

    for fn in sorted(files):
        file_date = parse_file_date(fn)
        filepath = os.path.join(output_dir, fn)
        interval_count_for_quality = 0

        if start_date and file_date < start_date:
            print(f"[SKIP DATE] {fn} before START_DATE={start_date}")
            continue

        if end_date and file_date > end_date:
            print(f"[SKIP DATE] {fn} after END_DATE={end_date}")
            continue

        file_size = os.path.getsize(filepath)
        if file_size < MIN_FILE_SIZE_BYTES:
            msg = f"Skipped: file too small ({file_size / 1024 / 1024:.2f} MB < {MIN_FILE_SIZE_MB} MB)"
            print(f"[SKIP SIZE] {fn} | {msg}")
            log_load_status(engine, schema, file_date, fn, "skipped", msg)
            update_data_quality(
                engine=engine,
                schema=schema,
                province=province,
                file_date=file_date,
                filepath=filepath,
                interval_count=0,
                has_failures=True,
                notes=msg,
            )
            continue

        if already_loaded_success(engine, schema, file_date) and not force_reload:
            print(f"[SKIP LOADED] {fn} already loaded successfully")
            continue

        file_had_success = False
        file_errors = []

        try:
            if force_reload:
                print(f"[FORCE RELOAD] deleting existing rows for {fn}")
                delete_existing_rows_for_file_date(engine, schema, file_date)

            xls = pd.ExcelFile(filepath)

            for sheet in xls.sheet_names:
                if sheet not in SHEET_TABLE_MAP:
                    continue

                try:
                    table = SHEET_TABLE_MAP[sheet]
                    
                    df = translate_columns(pd.read_excel(xls, sheet, header=0))
                    
                    numeric_cols = [
                        "energy_mwh",
                        "cleared_energy_mwh",
                        "cleared_price",
                        "price",
                        "node_price",
                        "energy_price",
                        "congestion_price",
                        "system_settlement_price"
                    ]
                    
                    for c in numeric_cols:
                        if c in df.columns:
                            df[c] = (
                                df[c]
                                .astype(str)
                                .str.replace(",", "", regex=False)
                                .str.replace("--", "", regex=False)
                                .str.replace("—", "", regex=False)
                                .str.replace("－", "", regex=False)
                                .str.strip()
                            )
                            df[c] = pd.to_numeric(df[c], errors="coerce")
                    
                    
                    df = df.loc[:, ~df.columns.str.contains("^unnamed", case=False)]
                    df = df.replace({"--": None, "—": None, "－": None})
                    _rule = TIME_RULES.get(table, {"anchor_plus_days": 0, "shift_minutes": 0})
                    delivery_date = file_date + timedelta(days=int(_rule.get("anchor_plus_days", 0) or 0))
                    df["data_date"] = delivery_date
                    df["source_file"] = fn

                    if "datetime" in df.columns:
                        raw = df["datetime"]
                        df["datetime"] = normalize_datetime_series(raw, table_name=table, file_date=file_date)

                    if table == "md_rt_nodal_price" and "datetime" in df.columns:
                        df = df[df["datetime"].dt.date == file_date]

                    if sheet == "实时节点电价" and "datetime" in df.columns and not df.empty:
                        interval_count_for_quality = int(df["datetime"].nunique())

                    non_meta_cols = [c for c in df.columns if c not in ("data_date", "source_file", "datetime")]
                    if not non_meta_cols or df[non_meta_cols].dropna(how="all").empty:
                        print(f"[SKIP EMPTY] {fn} sheet {sheet}")
                        continue

                    if table == "md_rt_nodal_price":
                        ensure_rt_nodal_price_table(engine, schema)
                    else:
                        ensure_table(engine, schema, table, df)

                    keys = pick_conflict_keys(table, df)
                    create_unique_index(engine, schema, table, keys)

                    upsert(engine, schema, table, df)

                    print(f"[OK] {fn} | {sheet}")
                    file_had_success = True

                except Exception as sheet_error:
                    msg = f"[SHEET FAIL] {fn} | {sheet}: {repr(sheet_error)}"
                    print(msg)
                    file_errors.append(msg)

            notes = None
            if file_errors:
                notes = "\n".join(file_errors)

            update_data_quality(
                engine=engine,
                schema=schema,
                province=province,
                file_date=file_date,
                filepath=filepath,
                interval_count=interval_count_for_quality,
                has_failures=bool(file_errors),
                notes=notes,
            )

            if file_had_success:
                status = "partial_success" if file_errors else "success"
                message_bits = []
                if force_reload:
                    message_bits.append("force_reload=true")
                if file_errors:
                    message_bits.append("\n".join(file_errors))
                log_load_status(
                    engine,
                    schema,
                    file_date,
                    fn,
                    status,
                    "\n".join(message_bits) if message_bits else None,
                )
                if file_errors:
                    print(f"[PARTIAL] {fn}")
                else:
                    print(f"[OK] {fn}")
            else:
                msg = "\n".join(file_errors) if file_errors else "No recognized sheets were loaded successfully"
                log_load_status(engine, schema, file_date, fn, "failed", msg)
                print(f"[FILE FAILED] {fn}")

        except Exception as e:
            if file_had_success:
                status = "success"
                message = "\n".join(file_errors) if file_errors else f"Partial success; outer exception: {repr(e)}"
            else:
                status = "failed"
                message = "\n".join(file_errors) if file_errors else repr(e)

            try:
                update_data_quality(
                    engine=engine,
                    schema=schema,
                    province=province,
                    file_date=file_date,
                    filepath=filepath,
                    interval_count=interval_count_for_quality,
                    has_failures=True,
                    notes=message,
                )
            except Exception as dq_error:
                print(f"[WARN] failed to update data_quality_status for {fn}: {dq_error}")

            log_load_status(engine, schema, file_date, fn, status, message)

            print(f"[FILE {status.upper()}] {fn}")
            print(f"[FAIL] {fn}: {e}")


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python load_excel_to_marketdata.py <output_dir>")
    main(sys.argv[1])
