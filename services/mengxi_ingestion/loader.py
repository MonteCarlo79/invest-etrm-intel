"""
Reusable loader for Mengxi BESS market data Excel files.

Extracted from bess-marketdata-ingestion/providers/mengxi/load_excel_to_marketdata.py
for use by the mengxi-dashboard Data Management tab (manual upload ingestion).

Public API:
    load_excel_file(file_bytes, filename, engine, schema, province, force_reload) -> dict
    ensure_schema_and_log(engine, schema)
"""
from __future__ import annotations

import io
import re
from datetime import date, datetime, timedelta
from uuid import uuid4

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TABLE_PREFIX = "md_"
LOAD_LOG_TABLE = f"{TABLE_PREFIX}load_log"
MIN_FILE_SIZE_BYTES = 3 * 1024 * 1024  # 3 MB

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

TIME_RULES = {
    "md_rt_total_cleared_energy": {"anchor_plus_days": 0, "shift_minutes": -15},
    "md_id_cleared_energy":       {"anchor_plus_days": 0, "shift_minutes": -15},
    "md_id_fuel_summary":         {"anchor_plus_days": 0, "shift_minutes": -15},
    "md_da_cleared_energy":       {"anchor_plus_days": 1, "shift_minutes": -15},
    "md_da_fuel_summary":         {"anchor_plus_days": 1, "shift_minutes": -15},
    "md_settlement_ref_price":    {"anchor_plus_days": 0, "shift_minutes": -60},
    "md_rt_nodal_price":          {"anchor_plus_days": 0, "shift_minutes": -15},
}

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

PREFERRED_KEYS = {
    f"{TABLE_PREFIX}avg_bid_price":           ["data_date", "type"],
    f"{TABLE_PREFIX}da_fuel_summary":         ["data_date", "datetime", "type", "plant_name", "dispatch_unit_name"],
    f"{TABLE_PREFIX}da_cleared_energy":       ["data_date", "datetime", "plant_name", "dispatch_unit_name"],
    f"{TABLE_PREFIX}id_fuel_summary":         ["data_date", "datetime", "type", "plant_name", "dispatch_unit_name"],
    f"{TABLE_PREFIX}id_cleared_energy":       ["data_date", "datetime", "plant_name", "dispatch_unit_name"],
    f"{TABLE_PREFIX}rt_nodal_price":          ["data_date", "datetime", "node_name"],
    f"{TABLE_PREFIX}rt_total_cleared_energy": ["data_date", "datetime"],
    f"{TABLE_PREFIX}settlement_ref_price":    ["data_date", "datetime"],
}

VALUE_COL_HINTS = {
    f"{TABLE_PREFIX}da_cleared_energy":       ["energy_mwh", "cleared_energy_mwh", "cleared_price", "price"],
    f"{TABLE_PREFIX}id_cleared_energy":       ["energy_mwh", "cleared_energy_mwh", "cleared_price", "price"],
    f"{TABLE_PREFIX}rt_total_cleared_energy": ["rt_total_cleared_energy_mwh"],
    f"{TABLE_PREFIX}rt_nodal_price":          ["node_price"],
    f"{TABLE_PREFIX}settlement_ref_price":    ["system_settlement_price"],
}

FILENAME_RE = re.compile(r"^data_(\d{4}-\d{2}-\d{2})\.xlsx$", re.I)

NUMERIC_COLS = [
    "energy_mwh", "cleared_energy_mwh", "cleared_price", "price",
    "node_price", "energy_price", "congestion_price", "system_settlement_price",
]

# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------

def _quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _safe_col(name: str) -> str:
    name = name.lower().strip()
    name = re.sub(r"[^\w]+", "_", name)
    return re.sub(r"_+", "_", name).strip("_")


def _translate_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    def norm(x):
        if x is None:
            return ""
        x = str(x).strip().replace("\n", "").replace("\r", "")
        return COL_MAP.get(x, _safe_col(x))

    df.columns = [norm(c) for c in df.columns]
    return df


def _parse_time_to_minutes(v) -> int | None:
    s = str(v).strip()
    if not s or s.lower() in {"nan", "none"}:
        return None
    m = re.match(r"^(\d{1,2}):(\d{2})(?::(\d{2}))?$", s)
    if not m:
        return None
    hh, mm = int(m.group(1)), int(m.group(2))
    if hh == 24 and mm == 0:
        return 1440
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        return None
    return hh * 60 + mm


def _has_date_component(s: str) -> bool:
    return bool(re.search(r"\d{4}[-/]\d{1,2}[-/]\d{1,2}", s))


def _normalize_datetime_series(raw: pd.Series, table_name: str, file_date: date) -> pd.Series:
    rule = TIME_RULES.get(table_name, {"anchor_plus_days": 0, "shift_minutes": 0})
    anchor_plus_days = int(rule.get("anchor_plus_days", 0) or 0)
    shift_minutes = int(rule.get("shift_minutes", 0) or 0)
    anchor_date = pd.Timestamp(file_date) + pd.Timedelta(days=anchor_plus_days)

    s = raw.copy().astype(str).str.strip()
    s = s.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA, "NaT": pd.NA})

    out = []
    for v in s.tolist():
        if v is pd.NA or v is None:
            out.append(pd.NaT)
            continue
        v_str = str(v).strip()
        if _has_date_component(v_str):
            if re.search(r"\s24:00(:00)?$", v_str):
                d_part = re.split(r"\s+", v_str)[0]
                d0 = pd.to_datetime(d_part, errors="coerce")
                out.append(d0 + pd.Timedelta(days=1) if pd.notna(d0) else pd.NaT)
            else:
                out.append(pd.to_datetime(v_str, errors="coerce"))
        else:
            mins = _parse_time_to_minutes(v_str)
            if mins is None:
                out.append(pd.to_datetime(v_str, errors="coerce"))
            elif mins == 1440:
                out.append(pd.Timestamp(anchor_date) + pd.Timedelta(days=1))
            else:
                out.append(pd.Timestamp(anchor_date) + pd.Timedelta(minutes=mins))

    dt = pd.to_datetime(pd.Series(out), errors="coerce")
    if shift_minutes:
        dt = dt + pd.Timedelta(minutes=shift_minutes)
    return dt


def _pick_conflict_keys(table: str, df: pd.DataFrame) -> list[str]:
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


def _dedup_keep_best(df: pd.DataFrame, key_cols: list[str], value_cols: list[str]) -> pd.DataFrame:
    vcols = [c for c in value_cols if c in df.columns]
    if not vcols:
        return df.drop_duplicates(subset=key_cols, keep="last")
    d = df.copy()
    d["_score"] = d[vcols].notna().sum(axis=1)
    d = d.sort_values(key_cols + ["_score"], ascending=[True] * len(key_cols) + [False])
    return d.drop_duplicates(subset=key_cols, keep="first").drop(columns=["_score"])


def _copy_df_to_postgres(df: pd.DataFrame, table_name: str, engine: Engine, schema: str):
    if df.empty:
        return
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, na_rep="", lineterminator="\n")
    buf.seek(0)
    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        cols = ",".join(_quote_ident(c) for c in df.columns)
        sql = (
            f"COPY {_quote_ident(schema)}.{_quote_ident(table_name)} ({cols}) "
            f"FROM STDIN WITH (FORMAT CSV, NULL '')"
        )
        cur.copy_expert(sql, buf)
        raw.commit()
    except Exception:
        raw.rollback()
        raise
    finally:
        raw.close()


def _ensure_table(engine: Engine, schema: str, table: str, df: pd.DataFrame):
    cols = []
    for c, t in df.dtypes.items():
        if c == "datetime":
            sql_t = "timestamp NOT NULL"
        elif c == "data_date":
            sql_t = "date NOT NULL"
        elif "int" in str(t) or "float" in str(t):
            sql_t = "numeric(18,3)"
        elif "bool" in str(t):
            sql_t = "boolean"
        else:
            sql_t = "text"
        cols.append(f"{_quote_ident(c)} {sql_t}")
    ddl = f"CREATE TABLE IF NOT EXISTS {_quote_ident(schema)}.{_quote_ident(table)} ({', '.join(cols)});"
    with engine.begin() as c:
        c.execute(text(ddl))


def _create_unique_index(engine: Engine, schema: str, table: str, keys: list[str]):
    if not keys:
        return
    idx = _quote_ident(f"ux_{table}_" + "_".join(keys))
    qs, qt = _quote_ident(schema), _quote_ident(table)
    qkeys = ",".join(_quote_ident(k) for k in keys)
    with engine.begin() as c:
        c.execute(text(f"DROP INDEX IF EXISTS {qs}.{idx};"))
        c.execute(text(f"CREATE UNIQUE INDEX {idx} ON {qs}.{qt} ({qkeys});"))


def _upsert(engine: Engine, schema: str, table: str, df: pd.DataFrame):
    keys = _pick_conflict_keys(table, df)
    df = _dedup_keep_best(df, keys, VALUE_COL_HINTS.get(table, []))
    if df.empty:
        return
    stage = f"_stg_{table}_{uuid4().hex}"
    _ensure_table(engine, schema, stage, df)
    _copy_df_to_postgres(df, stage, engine, schema)
    cols = list(df.columns)
    qcols = ",".join(_quote_ident(c) for c in cols)
    update_cols = [c for c in cols if c not in keys]
    if update_cols:
        updates = ",".join(f"{_quote_ident(c)}=EXCLUDED.{_quote_ident(c)}" for c in update_cols)
        conflict = f"ON CONFLICT ({','.join(_quote_ident(k) for k in keys)}) DO UPDATE SET {updates}"
    else:
        conflict = f"ON CONFLICT ({','.join(_quote_ident(k) for k in keys)}) DO NOTHING"
    with engine.begin() as c:
        c.execute(text(f"INSERT INTO {_quote_ident(schema)}.{_quote_ident(table)} ({qcols}) SELECT {qcols} FROM {_quote_ident(schema)}.{_quote_ident(stage)} {conflict};"))
        c.execute(text(f"DROP TABLE IF EXISTS {_quote_ident(schema)}.{_quote_ident(stage)};"))


def _delete_existing_rows(engine: Engine, schema: str, file_date: date):
    with engine.begin() as c:
        for table in SHEET_TABLE_MAP.values():
            rule = TIME_RULES.get(table, {"anchor_plus_days": 0})
            delivery_date = file_date + timedelta(days=int(rule.get("anchor_plus_days", 0) or 0))
            c.execute(
                text(f"DELETE FROM {_quote_ident(schema)}.{_quote_ident(table)} WHERE data_date = :d"),
                {"d": delivery_date},
            )


def _log_load_status(engine: Engine, schema: str, file_date: date, file_name: str, status: str, message: str | None = None):
    with engine.begin() as c:
        c.execute(
            text(f"INSERT INTO {schema}.{LOAD_LOG_TABLE} (file_date, file_name, status, message) VALUES (:d, :f, :s, :m)"),
            {"d": file_date, "f": file_name, "s": status, "m": message},
        )


def _update_data_quality(engine: Engine, schema: str, province: str, file_date: date,
                          file_name: str, file_size_bytes: int, interval_count: int,
                          has_failures: bool = False, notes: str | None = None):
    file_size_mb = file_size_bytes / (1024 * 1024)
    expected = 96
    coverage = interval_count / expected if expected else 0.0
    is_complete = file_size_mb >= 3.0 and interval_count >= expected and not has_failures
    sql = f"""
    INSERT INTO {schema}.data_quality_status
    (province, data_date, source_file, file_size_mb, expected_intervals, actual_intervals,
     interval_coverage, is_complete, notes)
    VALUES (:province, :data_date, :source_file, :file_size_mb, :expected, :actual,
            :coverage, :is_complete, :notes)
    ON CONFLICT (province, data_date) DO UPDATE SET
      source_file = EXCLUDED.source_file,
      file_size_mb = EXCLUDED.file_size_mb,
      expected_intervals = EXCLUDED.expected_intervals,
      actual_intervals = EXCLUDED.actual_intervals,
      interval_coverage = EXCLUDED.interval_coverage,
      is_complete = EXCLUDED.is_complete,
      notes = EXCLUDED.notes,
      check_time = now();
    """
    with engine.begin() as c:
        c.execute(text(sql), {
            "province": province, "data_date": file_date, "source_file": file_name,
            "file_size_mb": round(file_size_mb, 2), "expected": expected,
            "actual": int(interval_count), "coverage": round(coverage, 4),
            "is_complete": is_complete, "notes": notes,
        })


def _ensure_rt_nodal_price_table(engine: Engine, schema: str):
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


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def ensure_schema_and_log(engine: Engine, schema: str = "marketdata"):
    """Idempotent: create schema, load log table, and quality status table if they don't exist."""
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
    _ensure_rt_nodal_price_table(engine, schema)


def parse_file_date(filename: str) -> date:
    m = FILENAME_RE.match(filename)
    if not m:
        raise ValueError(f"Filename must be data_YYYY-MM-DD.xlsx, got: {filename}")
    return datetime.strptime(m.group(1), "%Y-%m-%d").date()


def load_excel_file(
    file_bytes: bytes,
    filename: str,
    engine: Engine,
    schema: str = "marketdata",
    province: str = "mengxi",
    force_reload: bool = True,
) -> dict:
    """
    Load a single Mengxi Excel file (as bytes) into the database.

    Returns a dict:
        filename       : str
        file_date      : date
        status         : "success" | "partial_success" | "failed" | "skipped"
        sheets_ok      : list[str]   — sheet names loaded successfully
        sheets_failed  : list[str]   — "sheet: error" strings
        message        : str | None
    """
    result: dict = {
        "filename": filename,
        "file_date": None,
        "status": "failed",
        "sheets_ok": [],
        "sheets_failed": [],
        "message": None,
    }

    # Validate filename
    try:
        file_date = parse_file_date(filename)
        result["file_date"] = file_date
    except ValueError as e:
        result["status"] = "skipped"
        result["message"] = str(e)
        return result

    file_size = len(file_bytes)

    # Reject files that are too small (likely corrupted / empty API response)
    if file_size < MIN_FILE_SIZE_BYTES:
        msg = f"File too small ({file_size / 1024 / 1024:.2f} MB < 3 MB) — likely corrupted"
        result["status"] = "skipped"
        result["message"] = msg
        _log_load_status(engine, schema, file_date, filename, "skipped", msg)
        _update_data_quality(engine, schema, province, file_date, filename,
                              file_size, 0, has_failures=True, notes=msg)
        return result

    if force_reload:
        _delete_existing_rows(engine, schema, file_date)

    interval_count = 0
    sheets_ok: list[str] = []
    sheets_failed: list[str] = []

    try:
        xls = pd.ExcelFile(io.BytesIO(file_bytes))

        for sheet in xls.sheet_names:
            if sheet not in SHEET_TABLE_MAP:
                continue
            try:
                table = SHEET_TABLE_MAP[sheet]
                rule = TIME_RULES.get(table, {"anchor_plus_days": 0, "shift_minutes": 0})
                delivery_date = file_date + timedelta(days=int(rule.get("anchor_plus_days", 0) or 0))

                df = _translate_columns(pd.read_excel(xls, sheet, header=0))

                for col in NUMERIC_COLS:
                    if col in df.columns:
                        df[col] = (
                            df[col].astype(str)
                            .str.replace(",", "", regex=False)
                            .str.replace("--", "", regex=False)
                            .str.replace("—", "", regex=False)
                            .str.replace("－", "", regex=False)
                            .str.strip()
                        )
                        df[col] = pd.to_numeric(df[col], errors="coerce")

                df = df.loc[:, ~df.columns.str.contains("^unnamed", case=False)]
                df = df.replace({"--": None, "—": None, "－": None})
                df["data_date"] = delivery_date
                df["source_file"] = filename

                if "datetime" in df.columns:
                    df["datetime"] = _normalize_datetime_series(df["datetime"], table, file_date)

                if table == "md_rt_nodal_price" and "datetime" in df.columns:
                    df = df[df["datetime"].dt.date == file_date]

                if sheet == "实时节点电价" and "datetime" in df.columns and not df.empty:
                    interval_count = int(df["datetime"].nunique())

                non_meta = [c for c in df.columns if c not in ("data_date", "source_file", "datetime")]
                if not non_meta or df[non_meta].dropna(how="all").empty:
                    continue  # sheet is empty/header-only

                if table == "md_rt_nodal_price":
                    _ensure_rt_nodal_price_table(engine, schema)
                else:
                    _ensure_table(engine, schema, table, df)

                _create_unique_index(engine, schema, table, _pick_conflict_keys(table, df))
                _upsert(engine, schema, table, df)
                sheets_ok.append(sheet)

            except Exception as sheet_err:
                sheets_failed.append(f"{sheet}: {repr(sheet_err)}")

        notes = "\n".join(sheets_failed) if sheets_failed else None
        _update_data_quality(engine, schema, province, file_date, filename,
                              file_size, interval_count,
                              has_failures=bool(sheets_failed), notes=notes)

        if sheets_ok:
            status = "partial_success" if sheets_failed else "success"
            _log_load_status(engine, schema, file_date, filename, status, notes)
        else:
            msg = "\n".join(sheets_failed) if sheets_failed else "No recognized sheets found"
            _log_load_status(engine, schema, file_date, filename, "failed", msg)
            status = "failed"
            notes = msg

        result["status"] = status
        result["sheets_ok"] = sheets_ok
        result["sheets_failed"] = sheets_failed
        result["message"] = notes

    except Exception as e:
        msg = repr(e)
        result["status"] = "failed"
        result["message"] = msg
        try:
            _log_load_status(engine, schema, file_date, filename, "failed", msg)
            _update_data_quality(engine, schema, province, file_date, filename,
                                  file_size, interval_count, has_failures=True, notes=msg)
        except Exception:
            pass

    return result
