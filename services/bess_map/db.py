from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from urllib.parse import urlparse


@dataclass
class DBConfig:
    host: str
    port: int
    db: str
    user: str
    password: str
    schema: str = "marketdata"

    @property
    def sqlalchemy_url(self) -> str:
        return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"




def load_db_config(env_path: Optional[str] = None) -> DBConfig:
    if env_path:
        load_dotenv(env_path)
    else:
        load_dotenv()

    pgurl = os.getenv("PGURL")
    if pgurl:
        parsed = urlparse(pgurl)
        return DBConfig(
            host=parsed.hostname,
            port=parsed.port or 5432,
            db=parsed.path.lstrip("/"),
            user=parsed.username,
            password=parsed.password,
            schema=os.getenv("DB_SCHEMA") or "marketdata",
        )

    # fallback to old method
    host = os.getenv("PGHOST") or os.getenv("DB_HOST") or "localhost"
    port = int(os.getenv("PGPORT") or os.getenv("DB_PORT") or "5432")
    db = os.getenv("PGDATABASE") or os.getenv("DB_NAME") or "marketdata"
    user = os.getenv("PGUSER") or os.getenv("DB_USER") or "postgres"
    password = os.getenv("PGPASSWORD") or os.getenv("DB_PASSWORD") or ""
    schema = os.getenv("DB_SCHEMA") or "marketdata"

    return DBConfig(host=host, port=port, db=db, user=user, password=password, schema=schema)


def get_engine(cfg: DBConfig) -> Engine:
    return create_engine(cfg.sqlalchemy_url, pool_pre_ping=True)


def ensure_tables(engine: Engine, schema: str) -> None:
    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS {schema};

    CREATE TABLE IF NOT EXISTS {schema}.raw_timeseries (
        province TEXT NOT NULL,
        ts TIMESTAMP NOT NULL,
        field TEXT NOT NULL,
        value DOUBLE PRECISION NULL,
        source_file TEXT NULL,
        ingested_at TIMESTAMP NOT NULL DEFAULT NOW(),
        PRIMARY KEY (province, ts, field)
    );

    CREATE TABLE IF NOT EXISTS {schema}.bess_daily (
        province TEXT NOT NULL,
        date DATE NOT NULL,
        price_type TEXT NOT NULL,
        duration_h DOUBLE PRECISION NOT NULL,
        power_mw DOUBLE PRECISION NOT NULL,
        roundtrip_eff DOUBLE PRECISION NOT NULL,
        profit DOUBLE PRECISION NOT NULL,
        profit_per_mw_day DOUBLE PRECISION NOT NULL,
        profit_per_mwh_day DOUBLE PRECISION NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
        PRIMARY KEY (province, date, price_type, duration_h, power_mw, roundtrip_eff)
    );

    CREATE TABLE IF NOT EXISTS {schema}.bess_monthly (
        province TEXT NOT NULL,
        month TEXT NOT NULL,
        price_type TEXT NOT NULL,
        duration_h DOUBLE PRECISION NOT NULL,
        power_mw DOUBLE PRECISION NOT NULL,
        roundtrip_eff DOUBLE PRECISION NOT NULL,
        profit DOUBLE PRECISION NOT NULL,
        profit_per_mw_day DOUBLE PRECISION NOT NULL,
        profit_per_mwh_day DOUBLE PRECISION NOT NULL,
        days INTEGER NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
        PRIMARY KEY (province, month, price_type, duration_h, power_mw, roundtrip_eff)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))

def upsert_theoretical_dispatch(conn, theoretical_dispatch_fp, province, duration_h, power_mw, roundtrip_eff):
    """Upsert the theoretical dispatch data into the database"""
    with conn.cursor() as cur:
        # Assuming the data is in the correct format
        df = pd.read_csv(theoretical_dispatch_fp)
        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO marketdata.spot_dispatch_hourly_theoretical (
                    province, datetime, duration_h, power_mw, roundtrip_eff, charge_mw, discharge_mw, dispatch_grid_mw, soc_mwh, solver_status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (province, datetime) DO UPDATE 
                SET charge_mw = EXCLUDED.charge_mw, discharge_mw = EXCLUDED.discharge_mw, 
                    dispatch_grid_mw = EXCLUDED.dispatch_grid_mw, soc_mwh = EXCLUDED.soc_mwh, solver_status = EXCLUDED.solver_status;
            """, (
                province, row['datetime'], duration_h, power_mw, roundtrip_eff, 
                row['charge_mw'], row['discharge_mw'], row['dispatch_grid_mw'], row['soc_mwh'], row['solver_status']
            ))
        conn.commit()


def upsert_raw_timeseries(engine: Engine, schema: str, province: str, df: pd.DataFrame, source_file: str | None = None) -> None:
    long = (
        df.reset_index(names="ts")
        .melt(id_vars=["ts"], var_name="field", value_name="value")
        .assign(province=province, source_file=source_file,
                ingested_at=datetime.now(timezone.utc).replace(tzinfo=None))
    )
    long = long.drop_duplicates(subset=["province", "ts", "field"], keep="last")

    tmp = f"_tmp_raw_{int(datetime.now().timestamp())}"
    with engine.begin() as conn:
        conn.execute(text(f"CREATE TEMP TABLE {tmp} (LIKE {schema}.raw_timeseries INCLUDING ALL) ON COMMIT DROP;"))
    long.to_sql(tmp, engine, if_exists="append", index=False, method="multi", chunksize=5000)

    upsert_sql = f"""
    INSERT INTO {schema}.raw_timeseries (province, ts, field, value, source_file, ingested_at)
    SELECT province, ts, field, value, source_file, ingested_at
    FROM {tmp}
    ON CONFLICT (province, ts, field) DO UPDATE
      SET value = EXCLUDED.value,
          source_file = EXCLUDED.source_file,
          ingested_at = EXCLUDED.ingested_at;
    """
    with engine.begin() as conn:
        conn.execute(text(upsert_sql))


def write_bess_daily(engine: Engine, schema: str, daily: pd.DataFrame, province: str, price_type: str,
                    duration_h: float, power_mw: float, roundtrip_eff: float) -> None:
    out = daily.copy().reset_index().rename(columns={"index": "date"})
    out["province"] = province
    out["price_type"] = price_type
    out["duration_h"] = duration_h
    out["power_mw"] = power_mw
    out["roundtrip_eff"] = roundtrip_eff
    cols = ["province","date","price_type","duration_h","power_mw","roundtrip_eff",
            "profit","profit_per_mw_day","profit_per_mwh_day"]
    out = out[cols]
    num_cols = ["profit", "profit_per_mw_day", "profit_per_mwh_day"]
    out[num_cols] = out[num_cols].apply(pd.to_numeric, errors="coerce").fillna(0.0)

    tmp = f"_tmp_daily_{int(datetime.now().timestamp())}"
    with engine.begin() as conn:
        conn.execute(text(f"CREATE TEMP TABLE {tmp} (LIKE {schema}.bess_daily INCLUDING ALL) ON COMMIT DROP;"))
    out.to_sql(tmp, engine, if_exists="append", index=False, method="multi", chunksize=5000)

    sql = f"""
    INSERT INTO {schema}.bess_daily
    (province, date, price_type, duration_h, power_mw, roundtrip_eff, profit, profit_per_mw_day, profit_per_mwh_day)
    SELECT province, date, price_type, duration_h, power_mw, roundtrip_eff, profit, profit_per_mw_day, profit_per_mwh_day
    FROM {tmp}
    ON CONFLICT (province, date, price_type, duration_h, power_mw, roundtrip_eff)
    DO UPDATE SET
      profit = EXCLUDED.profit,
      profit_per_mw_day = EXCLUDED.profit_per_mw_day,
      profit_per_mwh_day = EXCLUDED.profit_per_mwh_day,
      created_at = NOW();
    """
    with engine.begin() as conn:
        conn.execute(text(sql))


def write_bess_monthly(engine: Engine, schema: str, monthly: pd.DataFrame, province: str, price_type: str,
                      duration_h: float, power_mw: float, roundtrip_eff: float) -> None:
    out = monthly.copy().reset_index(names="month")
    out["province"] = province
    out["price_type"] = price_type
    out["duration_h"] = duration_h
    out["power_mw"] = power_mw
    out["roundtrip_eff"] = roundtrip_eff
    cols = ["province","month","price_type","duration_h","power_mw","roundtrip_eff",
            "profit","profit_per_mw_day","profit_per_mwh_day","days"]
    out = out[cols]

    tmp = f"_tmp_monthly_{int(datetime.now().timestamp())}"
    with engine.begin() as conn:
        conn.execute(text(f"CREATE TEMP TABLE {tmp} (LIKE {schema}.bess_monthly INCLUDING ALL) ON COMMIT DROP;"))
    out.to_sql(tmp, engine, if_exists="append", index=False, method="multi", chunksize=5000)

    sql = f"""
    INSERT INTO {schema}.bess_monthly
    (province, month, price_type, duration_h, power_mw, roundtrip_eff, profit, profit_per_mw_day, profit_per_mwh_day, days)
    SELECT province, month, price_type, duration_h, power_mw, roundtrip_eff, profit, profit_per_mw_day, profit_per_mwh_day, days
    FROM {tmp}
    ON CONFLICT (province, month, price_type, duration_h, power_mw, roundtrip_eff)
    DO UPDATE SET
      profit = EXCLUDED.profit,
      profit_per_mw_day = EXCLUDED.profit_per_mw_day,
      profit_per_mwh_day = EXCLUDED.profit_per_mwh_day,
      days = EXCLUDED.days,
      created_at = NOW();
    """
    with engine.begin() as conn:
        conn.execute(text(sql))




def ensure_hourly_price_table(engine, schema: str):
    sql = f"""
    CREATE TABLE IF NOT EXISTS "{schema}"."spot_prices_hourly" (
        province     text        NOT NULL,
        datetime     timestamp   NOT NULL,
        rt_price     double precision,
        da_price     double precision,
        source_file  text,
        updated_at   timestamp   NOT NULL DEFAULT now(),
        PRIMARY KEY (province, datetime)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(sql))

def upsert_hourly_prices(engine, schema: str, province: str, hourly_prices_df, source_file: str):
    df = hourly_prices_df.reset_index().rename(columns={"datetime": "datetime"})
    df["province"] = province
    df["source_file"] = source_file

    records = df[["province", "datetime", "rt_price", "da_price", "source_file"]].to_dict("records")

    sql = text(f"""
    INSERT INTO "{schema}"."spot_prices_hourly"
        (province, datetime, rt_price, da_price, source_file, updated_at)
    VALUES
        (:province, :datetime, :rt_price, :da_price, :source_file, now())
    ON CONFLICT (province, datetime) DO UPDATE SET
        rt_price    = EXCLUDED.rt_price,
        da_price    = EXCLUDED.da_price,
        source_file = EXCLUDED.source_file,
        updated_at  = now();
    """)
    with engine.begin() as conn:
        conn.execute(sql, records)

def ensure_dispatch_hourly_table(engine: Engine, schema: str) -> None:
    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS {schema};

    CREATE TABLE IF NOT EXISTS {schema}.bess_dispatch_hourly (
        province TEXT NOT NULL,
        ts TIMESTAMP NOT NULL,
        price_type TEXT NOT NULL,
        duration_h DOUBLE PRECISION NOT NULL,
        power_mw DOUBLE PRECISION NOT NULL,
        roundtrip_eff DOUBLE PRECISION NOT NULL,

        -- 你可以按你当前约定：正=充电 或 正=放电，保持一致即可
        dispatch_batt_mw DOUBLE PRECISION NOT NULL,
        dispatch_grid_mw DOUBLE PRECISION NULL,
        soc_mwh DOUBLE PRECISION NULL,

        source_file TEXT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT NOW(),

        PRIMARY KEY (province, ts, price_type, duration_h, power_mw, roundtrip_eff)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def upsert_dispatch_hourly(
    engine: Engine,
    schema: str,
    province: str,
    dispatch_df: pd.DataFrame,
    price_type: str,
    duration_h: float,
    power_mw: float,
    roundtrip_eff: float,
    source_file: str | None = None,
) -> None:
    df = dispatch_df.copy()

    # 允许 index 是 ts，或已有 ts 列
    if "ts" not in df.columns:
        idx_name = df.index.name or "ts"
        df = df.reset_index().rename(columns={idx_name: "ts"})

    required_cols = {"ts", "dispatch_batt_mw"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"dispatch_df missing columns: {missing}")

    if "dispatch_grid_mw" not in df.columns:
        df["dispatch_grid_mw"] = np.nan
    if "soc_mwh" not in df.columns:
        df["soc_mwh"] = np.nan

    df["province"] = province
    df["price_type"] = price_type
    df["duration_h"] = float(duration_h)
    df["power_mw"] = float(power_mw)
    df["roundtrip_eff"] = float(roundtrip_eff)
    df["source_file"] = source_file

    df = df[
        [
            "province", "ts", "price_type", "duration_h", "power_mw", "roundtrip_eff",
            "dispatch_batt_mw", "dispatch_grid_mw", "soc_mwh", "source_file"
        ]
    ].dropna(subset=["ts", "dispatch_batt_mw"])

    if df.empty:
        return

    tmp = f"_tmp_dispatch_{int(pd.Timestamp.utcnow().timestamp())}"

    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {tmp};"))
        df.to_sql(tmp, con=conn, index=False, if_exists="replace")

        sql = f"""
        INSERT INTO {schema}.bess_dispatch_hourly
        (province, ts, price_type, duration_h, power_mw, roundtrip_eff,
         dispatch_batt_mw, dispatch_grid_mw, soc_mwh, source_file)
        SELECT province, ts, price_type, duration_h, power_mw, roundtrip_eff,
               dispatch_batt_mw, dispatch_grid_mw, soc_mwh, source_file
        FROM {tmp}
        ON CONFLICT (province, ts, price_type, duration_h, power_mw, roundtrip_eff)
        DO UPDATE SET
          dispatch_batt_mw = EXCLUDED.dispatch_batt_mw,
          dispatch_grid_mw = EXCLUDED.dispatch_grid_mw,
          soc_mwh = EXCLUDED.soc_mwh,
          source_file = EXCLUDED.source_file,
          created_at = NOW();
        """
        conn.execute(text(sql))
        conn.execute(text(f"DROP TABLE IF EXISTS {tmp};"))
