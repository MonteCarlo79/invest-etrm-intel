# -*- coding: utf-8 -*-
"""
run_capture_pipeline.py (refactored)

Authoritative logic (your intended model):
A) THEORETICAL:
   spot_prices_hourly(rt_price) -> perfect foresight dispatch
   -> spot_dispatch_hourly_theoretical

B) FORECAST:
   spot_prices_hourly(da_price, rt_price) -> rt forecast
   -> spot_prices_hourly_rt_forecast

C) CAPTURED:
   rt_forecast -> dispatch
   -> spot_dispatch_hourly_rt_forecast
   apply dispatch to actual rt_price -> realized profit

D) CAPTURE RATE:
   compare realized vs theoretical -> bess_capture_daily

Key upgrades:
- Correct naming: spot_dispatch_hourly_rt_forecast
- UPSERT with proper conflict keys
- Incremental by province/duration/model unless --force
- Multiple forecast models
- Daily throughput / cycle cap to represent degradation
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import numpy as np
import pandas as pd
import pulp
from sqlalchemy import create_engine, text as sql_text
from psycopg2.extras import execute_values


# =============================================================================
# DB / ENV
# =============================================================================
# def _read_env_file(env_path: str) -> Dict[str, str]:
#     env: Dict[str, str] = {}
#     p = Path(env_path)
#     if not p.exists():
#         raise FileNotFoundError(f".env not found: {env_path}")
#     for raw in p.read_text(encoding="utf-8", errors="ignore").splitlines():
#         s = raw.strip()
#         if not s or s.startswith("#") or "=" not in s:
#             continue
#         k, v = s.split("=", 1)
#         env[k.strip()] = v.strip().strip('"').strip("'")
#     return env

def _read_env_file(env_path: Optional[str]) -> Dict[str, str]:
    # "none" / "" / None => don't read a file
    if not env_path or str(env_path).strip().lower() in {"none", "null", "nil"}:
        return {}

    p = Path(env_path)
    if not p.exists():
        raise FileNotFoundError(f".env not found: {env_path}")

    env: Dict[str, str] = {}
    for raw in p.read_text(encoding="utf-8", errors="ignore").splitlines():
        s = raw.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env
# def make_engine_from_env(env_path: str):
#     env = _read_env_file(env_path)

#     url = (
#         env.get("DATABASE_URL")
#         or env.get("PGURL")
#         or env.get("DB_URL")
#         or env.get("DB_URI")
#         or env.get("PG_URI")
#     )

#     if url:
#         if url.startswith("postgres://"):
#             url = "postgresql://" + url[len("postgres://"):]
#         if url.startswith("postgresql://") and not url.startswith("postgresql+psycopg2://"):
#             url = "postgresql+psycopg2://" + url[len("postgresql://"):]
#         return create_engine(url, pool_pre_ping=True)

#     host = env.get("PGHOST") or env.get("DB_HOST") or "localhost"
#     port = env.get("PGPORT") or env.get("DB_PORT") or "5432"
#     user = env.get("PGUSER") or env.get("DB_USER") or "postgres"
#     password = env.get("PGPASSWORD") or env.get("DB_PASSWORD") or ""
#     dbname = env.get("PGDATABASE") or env.get("DB_NAME") or "marketdata"

#     from urllib.parse import quote_plus
#     pwd = quote_plus(password) if password else ""
#     auth = f"{user}:{pwd}" if pwd else user
#     return create_engine(f"postgresql+psycopg2://{auth}@{host}:{port}/{dbname}", pool_pre_ping=True)

def make_engine_from_env(env_path: Optional[str]):
    file_env = _read_env_file(env_path)

    # Prefer values from file, but fall back to real environment (ECS)
    def get(k: str) -> Optional[str]:
        return file_env.get(k) or os.getenv(k)

    url = (
        get("DATABASE_URL")
        or get("PGURL")
        or get("DB_URL")
        or get("DB_URI")
        or get("PG_URI")
    )

    if url:
        if url.startswith("postgres://"):
            url = "postgresql://" + url[len("postgres://"):]
        if url.startswith("postgresql://") and not url.startswith("postgresql+psycopg2://"):
            url = "postgresql+psycopg2://" + url[len("postgresql://"):]
        return create_engine(url, pool_pre_ping=True)

    host = get("PGHOST") or get("DB_HOST") or "localhost"
    port = get("PGPORT") or get("DB_PORT") or "5432"
    user = get("PGUSER") or get("DB_USER") or "postgres"
    password = get("PGPASSWORD") or get("DB_PASSWORD") or ""
    dbname = get("PGDATABASE") or get("DB_NAME") or "marketdata"

    from urllib.parse import quote_plus
    pwd = quote_plus(password) if password else ""
    auth = f"{user}:{pwd}" if pwd else user
    return create_engine(f"postgresql+psycopg2://{auth}@{host}:{port}/{dbname}", pool_pre_ping=True)

def _execute_values(conn, sql: str, rows: List[Tuple], page_size: int = 5000):
    raw = conn.connection
    with raw.cursor() as cur:
        execute_values(cur, sql, rows, page_size=page_size)
    raw.commit()


# =============================================================================
# Tables (DDL)
# =============================================================================
def ensure_tables(engine, schema: str):
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {schema}.spot_prices_hourly_rt_forecast (
        province TEXT NOT NULL,
        datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL,
        model TEXT NOT NULL,
        rt_pred DOUBLE PRECISION,
        updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
        PRIMARY KEY (province, datetime, model)
    );

    -- CAPTURED dispatch (based on rt forecast)
    CREATE TABLE IF NOT EXISTS {schema}.spot_dispatch_hourly_rt_forecast (
        province TEXT NOT NULL,
        datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL,
        model TEXT NOT NULL,
        duration_h DOUBLE PRECISION NOT NULL,
        power_mw DOUBLE PRECISION NOT NULL,
        roundtrip_eff DOUBLE PRECISION NOT NULL,
        charge_mw DOUBLE PRECISION,
        discharge_mw DOUBLE PRECISION,
        dispatch_grid_mw DOUBLE PRECISION,
        soc_mwh DOUBLE PRECISION,
        solver_status TEXT,
        updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
        PRIMARY KEY (province, datetime, model, duration_h, power_mw, roundtrip_eff)
    );

    -- THEORETICAL dispatch (perfect foresight on actual rt_price)
    CREATE TABLE IF NOT EXISTS {schema}.spot_dispatch_hourly_theoretical (
        province TEXT NOT NULL,
        datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL,
        duration_h DOUBLE PRECISION NOT NULL,
        power_mw DOUBLE PRECISION NOT NULL,
        roundtrip_eff DOUBLE PRECISION NOT NULL,
        charge_mw DOUBLE PRECISION,
        discharge_mw DOUBLE PRECISION,
        dispatch_grid_mw DOUBLE PRECISION,
        soc_mwh DOUBLE PRECISION,
        solver_status TEXT,
        updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
        PRIMARY KEY (province, datetime, duration_h, power_mw, roundtrip_eff)
    );

    CREATE TABLE IF NOT EXISTS {schema}.bess_capture_daily (
        province TEXT NOT NULL,
        date DATE NOT NULL,
        model TEXT NOT NULL,
        duration_h DOUBLE PRECISION NOT NULL,
        power_mw DOUBLE PRECISION NOT NULL,
        roundtrip_eff DOUBLE PRECISION NOT NULL,
        realized_profit DOUBLE PRECISION,
        realized_profit_per_mwh_day DOUBLE PRECISION,
        theoretical_profit DOUBLE PRECISION,
        theoretical_profit_per_mwh_day DOUBLE PRECISION,
        capture_rate DOUBLE PRECISION,
        capturable_profit_per_mwh_day DOUBLE PRECISION,
        created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
        PRIMARY KEY (province, date, model, duration_h, power_mw, roundtrip_eff)
    );
    """
    with engine.begin() as conn:
        conn.execute(sql_text(ddl))


# =============================================================================
# Fetching
# =============================================================================
def fetch_provinces(engine, schema: str) -> List[str]:
    sql = f"SELECT DISTINCT province FROM {schema}.spot_prices_hourly ORDER BY 1"
    return pd.read_sql(sql_text(sql), engine)["province"].dropna().astype(str).tolist()


def fetch_hourly_prices(engine, schema: str, province: str) -> pd.DataFrame:
    sql = f"""
        SELECT datetime, rt_price, da_price
        FROM {schema}.spot_prices_hourly
        WHERE province = :p
        ORDER BY datetime
    """
    df = pd.read_sql(sql_text(sql), engine, params={"p": province})
    if df.empty:
        return df
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.drop_duplicates(subset=["datetime"]).sort_values("datetime").set_index("datetime")
    df["rt_price"] = pd.to_numeric(df["rt_price"], errors="coerce")
    df["da_price"] = pd.to_numeric(df["da_price"], errors="coerce")
    return df


def get_last_capture_day(engine, schema: str, province: str, model: str, duration_h: float, power_mw: float, rte: float) -> Optional[dt.date]:
    sql = f"""
        SELECT MAX(date) AS max_date
        FROM {schema}.bess_capture_daily
        WHERE province=:p AND model=:m AND duration_h=:d AND power_mw=:pw AND roundtrip_eff=:rte
    """
    with engine.connect() as conn:
        r = conn.execute(sql_text(sql), {"p": province, "m": model, "d": duration_h, "pw": power_mw, "rte": rte}).fetchone()
    return r[0]


# =============================================================================
# Forecast Models
# =============================================================================
def _design_matrix(hours: np.ndarray, da: np.ndarray) -> np.ndarray:
    h = hours.astype(float)
    return np.column_stack([
        np.ones_like(h),
        da.astype(float),
        np.sin(2 * np.pi * h / 24.0),
        np.cos(2 * np.pi * h / 24.0),
    ])


def forecast_naive_da(hourly: pd.DataFrame) -> pd.Series:
    """rt_pred = da_price"""
    s = hourly["da_price"].copy()
    s.name = "rt_pred"
    return s


def forecast_ols_da_time_v1(hourly: pd.DataFrame, min_train_days: int = 7, lookback_days: int = 60) -> pd.Series:
    if hourly.empty:
        return pd.Series(dtype=float, name="rt_pred")

    df = hourly[["rt_price", "da_price"]].copy()
    df["hour"] = df.index.hour
    df["date"] = df.index.date

    dates = pd.Index(pd.to_datetime(df["date"]).unique()).sort_values()
    preds: List[pd.DataFrame] = []

    for d in dates:
        day = d.date()
        day_df = df.loc[df["date"] == day]
        if day_df.empty:
            continue

        train_end = pd.Timestamp(day)
        train_start = train_end - pd.Timedelta(days=lookback_days)
        train_df = df.loc[(df.index < train_end) & (df.index >= train_start)].dropna(subset=["rt_price", "da_price"])

        if train_df["date"].nunique() < min_train_days:
            pred = day_df["da_price"].copy()
            pred.name = "rt_pred"
            preds.append(pred.to_frame())
            continue

        X = _design_matrix(train_df["hour"].to_numpy(), train_df["da_price"].to_numpy())
        y = train_df["rt_price"].to_numpy(dtype=float)
        beta, *_ = np.linalg.lstsq(X, y, rcond=None)

        Xp = _design_matrix(day_df["hour"].to_numpy(), day_df["da_price"].to_numpy())
        yhat = Xp @ beta
        preds.append(pd.Series(yhat, index=day_df.index, name="rt_pred").to_frame())

    out = pd.concat(preds).sort_index()
    out = out[~out.index.duplicated(keep="last")]
    return out["rt_pred"].astype(float)


def build_forecast(hourly: pd.DataFrame, model: str, min_train_days: int, lookback_days: int) -> pd.Series:
    model = model.lower().strip()
    if model == "naive_da":
        return forecast_naive_da(hourly)
    if model == "ols_da_time_v1":
        return forecast_ols_da_time_v1(hourly, min_train_days=min_train_days, lookback_days=lookback_days)
    raise ValueError(f"Unknown model: {model}. Use one of: naive_da, ols_da_time_v1")


# =============================================================================
# Optimisation (with degradation / throughput cap)
# =============================================================================
@dataclass
class DispatchResult:
    charge_mw: np.ndarray
    discharge_mw: np.ndarray
    soc_mwh: np.ndarray
    profit: float
    status: str


def optimise_day(
    prices: np.ndarray,
    power_mw: float,
    duration_h: float,
    roundtrip_eff: float,
    max_throughput_mwh: Optional[float] = None,
    max_cycles_per_day: Optional[float] = None,
) -> DispatchResult:
    """
    max_throughput_mwh: optional cap on total discharge energy per day (MWh), a simple degradation proxy.
    max_cycles_per_day: optional cap on equivalent full cycles/day (approx via discharge energy / energy_capacity).
    """
    T = len(prices)
    if T != 24:
        raise ValueError(f"optimise_day expects 24 hourly prices, got {T}")

    eta_c = float(np.sqrt(roundtrip_eff))
    eta_d = float(np.sqrt(roundtrip_eff))
    e_cap = float(power_mw * duration_h)

    prob = pulp.LpProblem("bess_arbitrage", pulp.LpMaximize)

    ch = pulp.LpVariable.dicts("ch", range(T), lowBound=0, upBound=power_mw, cat="Continuous")
    dis = pulp.LpVariable.dicts("dis", range(T), lowBound=0, upBound=power_mw, cat="Continuous")
    soc = pulp.LpVariable.dicts("soc", range(T + 1), lowBound=0, upBound=e_cap, cat="Continuous")

    # binary prevents simultaneous charge/discharge
    y = pulp.LpVariable.dicts("y", range(T), lowBound=0, upBound=1, cat="Binary")
    M = power_mw

    prob += soc[0] == 0

    for t in range(T):
        prob += soc[t + 1] == soc[t] + ch[t] * eta_c - dis[t] * (1.0 / eta_d)
        prob += ch[t] <= M * y[t]
        prob += dis[t] <= M * (1 - y[t])

    # degradation proxies
    if max_throughput_mwh is not None:
        prob += pulp.lpSum(dis[t] for t in range(T)) <= float(max_throughput_mwh)

    if max_cycles_per_day is not None:
        # discharge_energy <= cycles * energy_capacity
        prob += pulp.lpSum(dis[t] for t in range(T)) <= float(max_cycles_per_day) * e_cap

    prob += pulp.lpSum(float(prices[t]) * (dis[t] - ch[t]) for t in range(T))
    prob.solve(pulp.PULP_CBC_CMD(msg=False))

    status = pulp.LpStatus.get(prob.status, str(prob.status))

    chv = np.array([pulp.value(ch[t]) for t in range(T)], dtype=float)
    disv = np.array([pulp.value(dis[t]) for t in range(T)], dtype=float)
    socv = np.array([pulp.value(soc[t + 1]) for t in range(T)], dtype=float)
    profit = float(np.nansum(prices * (disv - chv)))

    return DispatchResult(chv, disv, socv, profit, status)

def get_last_theoretical_ts(conn, province: str, duration_h: float, power_mw: float, roundtrip_eff: float):
    """ Get last theoretical datetime from spot_dispatch_hourly_theoretical """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT MAX(datetime) 
            FROM marketdata.spot_dispatch_hourly_theoretical
            WHERE province = %s
            AND duration_h = %s
            AND power_mw = %s
            AND roundtrip_eff = %s;
        """, (province, duration_h, power_mw, roundtrip_eff))
        return cur.fetchone()[0]




def compute_dispatch_from_hourly_prices(
    hourly_prices: pd.Series,
    power_mw: float,
    duration_h: float,
    roundtrip_eff: float,
    max_throughput_mwh: Optional[float],
    max_cycles_per_day: Optional[float],
) -> Tuple[pd.DataFrame, pd.Series]:
    s = hourly_prices.dropna().copy()
    if s.empty:
        return pd.DataFrame(), pd.Series(dtype=float)

    if not isinstance(s.index, pd.DatetimeIndex):
        s.index = pd.to_datetime(s.index)

    df = s.to_frame("price")
    df["date"] = df.index.date
    df["hour"] = df.index.hour

    dispatch_rows: List[pd.DataFrame] = []
    daily_profit: Dict[dt.date, float] = {}

    for d, g in df.groupby("date"):
        idx = pd.date_range(pd.Timestamp(d), periods=24, freq="h")
        g2 = g.reindex(idx)
        prices = g2["price"].to_numpy(dtype=float)

        if np.isnan(prices).any():
            continue

        res = optimise_day(
            prices,
            power_mw=power_mw,
            duration_h=duration_h,
            roundtrip_eff=roundtrip_eff,
            max_throughput_mwh=max_throughput_mwh,
            max_cycles_per_day=max_cycles_per_day,
        )

        out = pd.DataFrame({
            "datetime": idx,
            "charge_mw": res.charge_mw,
            "discharge_mw": res.discharge_mw,
            "dispatch_grid_mw": res.discharge_mw - res.charge_mw,
            "soc_mwh": res.soc_mwh,
            "solver_status": res.status,
        }).set_index("datetime")

        dispatch_rows.append(out)
        daily_profit[d] = res.profit

    dispatch_df = pd.concat(dispatch_rows).sort_index() if dispatch_rows else pd.DataFrame()
    profit_s = pd.Series(daily_profit).sort_index()
    profit_s.name = "profit"
    return dispatch_df, profit_s


# =============================================================================
# Upserts
# =============================================================================
def upsert_rt_forecast(engine, schema: str, province: str, model: str, rt_pred: pd.Series):
    if rt_pred.empty:
        return
    df = rt_pred.to_frame("rt_pred").copy()
    df["province"] = province
    df["model"] = model
    df = df.reset_index().rename(columns={"index": "datetime"})
    if "datetime" not in df.columns:
        # if index had a name, reset_index uses that name
        df = df.rename(columns={df.columns[0]: "datetime"})
    df["datetime"] = pd.to_datetime(df["datetime"])
    df["rt_pred"] = pd.to_numeric(df["rt_pred"], errors="coerce")

    rows = list(df[["province", "datetime", "model", "rt_pred"]].itertuples(index=False, name=None))
    sql = f"""
        INSERT INTO {schema}.spot_prices_hourly_rt_forecast
          (province, datetime, model, rt_pred)
        VALUES %s
        ON CONFLICT (province, datetime, model)
        DO UPDATE SET
          rt_pred = EXCLUDED.rt_pred,
          updated_at = NOW()
    """
    with engine.begin() as conn:
        _execute_values(conn, sql, rows)


def upsert_dispatch_rt_forecast(engine, schema: str, province: str, model: str, duration_h: float, power_mw: float, rte: float, dispatch: pd.DataFrame):
    if dispatch.empty:
        return
    df = dispatch.reset_index().copy()
    df["province"] = province
    df["model"] = model
    df["duration_h"] = float(duration_h)
    df["power_mw"] = float(power_mw)
    df["roundtrip_eff"] = float(rte)

    for c in ["charge_mw", "discharge_mw", "dispatch_grid_mw", "soc_mwh"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    rows = list(df[[
        "province", "datetime", "model", "duration_h", "power_mw", "roundtrip_eff",
        "charge_mw", "discharge_mw", "dispatch_grid_mw", "soc_mwh", "solver_status"
    ]].itertuples(index=False, name=None))

    sql = f"""
        INSERT INTO {schema}.spot_dispatch_hourly_rt_forecast
          (province, datetime, model, duration_h, power_mw, roundtrip_eff,
           charge_mw, discharge_mw, dispatch_grid_mw, soc_mwh, solver_status)
        VALUES %s
        ON CONFLICT (province, datetime, model, duration_h, power_mw, roundtrip_eff)
        DO UPDATE SET
          charge_mw = EXCLUDED.charge_mw,
          discharge_mw = EXCLUDED.discharge_mw,
          dispatch_grid_mw = EXCLUDED.dispatch_grid_mw,
          soc_mwh = EXCLUDED.soc_mwh,
          solver_status = EXCLUDED.solver_status,
          updated_at = NOW()
    """
    with engine.begin() as conn:
        _execute_values(conn, sql, rows)


def upsert_dispatch_theoretical(engine, schema: str, province: str, duration_h: float, power_mw: float, rte: float, dispatch: pd.DataFrame):
    if dispatch.empty:
        return
    df = dispatch.reset_index().copy()
    df["province"] = province
    df["duration_h"] = float(duration_h)
    df["power_mw"] = float(power_mw)
    df["roundtrip_eff"] = float(rte)

    for c in ["charge_mw", "discharge_mw", "dispatch_grid_mw", "soc_mwh"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    rows = list(df[[
        "province", "datetime", "duration_h", "power_mw", "roundtrip_eff",
        "charge_mw", "discharge_mw", "dispatch_grid_mw", "soc_mwh", "solver_status"
    ]].itertuples(index=False, name=None))

    sql = f"""
        INSERT INTO {schema}.spot_dispatch_hourly_theoretical
          (province, datetime, duration_h, power_mw, roundtrip_eff,
           charge_mw, discharge_mw, dispatch_grid_mw, soc_mwh, solver_status)
        VALUES %s
        ON CONFLICT (province, datetime, duration_h, power_mw, roundtrip_eff)
        DO UPDATE SET
          charge_mw = EXCLUDED.charge_mw,
          discharge_mw = EXCLUDED.discharge_mw,
          dispatch_grid_mw = EXCLUDED.dispatch_grid_mw,
          soc_mwh = EXCLUDED.soc_mwh,
          solver_status = EXCLUDED.solver_status,
          updated_at = NOW()
    """
    with engine.begin() as conn:
        _execute_values(conn, sql, rows)


def upsert_capture_daily(
    engine,
    schema: str,
    province: str,
    model: str,
    duration_h: float,
    power_mw: float,
    rte: float,
    realized_profit_by_day: pd.Series,
    theoretical_profit_by_day: pd.Series
):
    if theoretical_profit_by_day.empty:
        return

    e_cap = float(power_mw * duration_h)

    idx = theoretical_profit_by_day.index
    r = realized_profit_by_day.reindex(idx)

    df = pd.DataFrame({
        "date": pd.to_datetime(idx).date,
        "theoretical_profit": pd.to_numeric(theoretical_profit_by_day.values, errors="coerce"),
        "realized_profit": pd.to_numeric(r.values, errors="coerce"),
    })

    df["province"] = province
    df["model"] = model
    df["duration_h"] = float(duration_h)
    df["power_mw"] = float(power_mw)
    df["roundtrip_eff"] = float(rte)

    df["theoretical_profit_per_mwh_day"] = df["theoretical_profit"] / e_cap
    df["realized_profit_per_mwh_day"] = df["realized_profit"] / e_cap

    denom = df["theoretical_profit_per_mwh_day"].replace({0.0: np.nan})
    df["capture_rate"] = df["realized_profit_per_mwh_day"] / denom
    df["capturable_profit_per_mwh_day"] = df["realized_profit_per_mwh_day"]

    for c in ["capture_rate", "theoretical_profit_per_mwh_day", "realized_profit_per_mwh_day", "capturable_profit_per_mwh_day"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").replace([np.inf, -np.inf], np.nan)

    rows = list(df[[
        "province", "date", "model", "duration_h", "power_mw", "roundtrip_eff",
        "realized_profit", "realized_profit_per_mwh_day",
        "theoretical_profit", "theoretical_profit_per_mwh_day",
        "capture_rate", "capturable_profit_per_mwh_day"
    ]].itertuples(index=False, name=None))

    sql = f"""
        INSERT INTO {schema}.bess_capture_daily
          (province, date, model, duration_h, power_mw, roundtrip_eff,
           realized_profit, realized_profit_per_mwh_day,
           theoretical_profit, theoretical_profit_per_mwh_day,
           capture_rate, capturable_profit_per_mwh_day)
        VALUES %s
        ON CONFLICT (province, date, model, duration_h, power_mw, roundtrip_eff)
        DO UPDATE SET
          realized_profit = EXCLUDED.realized_profit,
          realized_profit_per_mwh_day = EXCLUDED.realized_profit_per_mwh_day,
          theoretical_profit = EXCLUDED.theoretical_profit,
          theoretical_profit_per_mwh_day = EXCLUDED.theoretical_profit_per_mwh_day,
          capture_rate = EXCLUDED.capture_rate,
          capturable_profit_per_mwh_day = EXCLUDED.capturable_profit_per_mwh_day,
          updated_at = NOW()
    """
    with engine.begin() as conn:
        _execute_values(conn, sql, rows)


# =============================================================================
# Main
# =============================================================================
def main():
    ap = argparse.ArgumentParser()
    # ap.add_argument("--env", required=True, help="Path to .env with PGURL / DB creds")
    ap.add_argument("--env", default=None, help='Path to .env (optional). Use "--env none" to skip file and read from process env only.')
    
    ap.add_argument("--schema", default="marketdata")
    ap.add_argument("--duration-h", type=float, required=True)
    ap.add_argument("--power-mw", type=float, default=1.0)
    ap.add_argument("--roundtrip-eff", type=float, default=0.85)
    ap.add_argument("--model", default="ols_da_time_v1", help="naive_da | ols_da_time_v1")
    ap.add_argument("--province", default=None)
    ap.add_argument("--min-train-days", type=int, default=7)
    ap.add_argument("--lookback-days", type=int, default=60)
    ap.add_argument("--force", action="store_true", help="Recompute even if capture exists")
    ap.add_argument(
        "--force-theoretical",
        action="store_true",
        help="Force recompute of theoretical dispatch only"
    )
    ap.add_argument("--max-throughput-mwh", type=float, default=None, help="Daily discharge energy cap (MWh)")
    ap.add_argument("--max-cycles-per-day", type=float, default=None, help="Daily cycle cap (EFC/day)")
    ap.add_argument("--province-list", default=None,
                    help="Comma separated list of provinces to run")



    args = ap.parse_args()

    # engine = make_engine_from_env(args.env)
    engine = make_engine_from_env(args.env if args.env else None)
    ensure_tables(engine, args.schema)

    if args.province_list:
        provinces = [p.strip() for p in args.province_list.split(",") if p.strip()]
    elif args.province:
        provinces = [args.province]
    else:
        provinces = fetch_provinces(engine, args.schema)

    if not provinces:
        raise RuntimeError(f"No provinces found in {args.schema}.spot_prices_hourly")

    for p in provinces:
        hourly = fetch_hourly_prices(engine, args.schema, p)
        if hourly.empty:
            print(f"[SKIP] {p}: no hourly prices")
            continue

        # incremental trim (based on capture table)
        last_day = get_last_capture_day(
            engine,
            args.schema,
            p,
            args.model,
            args.duration_h,
            args.power_mw,
            args.roundtrip_eff,
        )
        

        if last_day is not None and not args.force:
            cutoff = pd.Timestamp(last_day) + pd.Timedelta(days=1)
            hourly_new = hourly.loc[hourly.index >= cutoff]
            if hourly_new.empty:
                print(f"[SKIP] {p}: capture already up to date for duration={args.duration_h} model={args.model}")
                continue
            hourly = hourly_new

        hourly_full = hourly.copy()


        # ==========================
        # THEORETICAL INCREMENTAL
        # ==========================
        last_theo_sql = f"""
        SELECT MAX(datetime)
        FROM {args.schema}.spot_dispatch_hourly_theoretical
        WHERE province=:p
          AND duration_h=:d
          AND power_mw=:pw
          AND roundtrip_eff=:rte
        """
        
        with engine.connect() as conn:
            last_theo = conn.execute(
                sql_text(last_theo_sql),
                {"p": p, "d": args.duration_h, "pw": args.power_mw, "rte": args.roundtrip_eff}
            ).scalar()
        
        hourly_theo = hourly_full
        
        if last_theo and not args.force_theoretical:
            hourly_theo = hourly_full.loc[hourly_full.index > pd.Timestamp(last_theo)]
        
        if hourly_theo.empty and not args.force_theoretical:
            print(f"[SKIP] {p}: theoretical already up to date for duration={args.duration_h}")
            theo_dispatch = pd.DataFrame()
            theo_profit_by_day = pd.Series(dtype=float)
        else:
            theo_dispatch, theo_profit_by_day = compute_dispatch_from_hourly_prices(
                hourly_prices=hourly_theo["rt_price"],
                power_mw=args.power_mw,
                duration_h=args.duration_h,
                roundtrip_eff=args.roundtrip_eff,
                max_throughput_mwh=args.max_throughput_mwh,
                max_cycles_per_day=args.max_cycles_per_day,
            )
        
            upsert_dispatch_theoretical(
                engine,
                args.schema,
                p,
                args.duration_h,
                args.power_mw,
                args.roundtrip_eff,
                theo_dispatch
            )
        

        # ---------------- FORECAST ----------------
        rt_pred = build_forecast(hourly, model=args.model, min_train_days=args.min_train_days, lookback_days=args.lookback_days)
        rt_pred = rt_pred.dropna().sort_index()
        upsert_rt_forecast(engine, args.schema, p, args.model, rt_pred)

        # ---------------- CAPTURED DISPATCH (on forecast) ----------------
        cap_dispatch, _cap_profit_forecast = compute_dispatch_from_hourly_prices(
            hourly_prices=rt_pred,
            power_mw=args.power_mw,
            duration_h=args.duration_h,
            roundtrip_eff=args.roundtrip_eff,
            max_throughput_mwh=args.max_throughput_mwh,
            max_cycles_per_day=args.max_cycles_per_day,
        )
        upsert_dispatch_rt_forecast(engine, args.schema, p, args.model, args.duration_h, args.power_mw, args.roundtrip_eff, cap_dispatch)

        # realized profit: apply captured dispatch to actual RT
        if cap_dispatch.empty:
            realized_profit_by_day = pd.Series(dtype=float)
        else:
            aligned = cap_dispatch.join(hourly["rt_price"].rename("rt_actual"), how="inner").dropna()
            aligned["pnl"] = aligned["rt_actual"] * aligned["dispatch_grid_mw"]
            realized_profit_by_day = aligned["pnl"].groupby(aligned.index.date).sum()

        # ---------------- CAPTURE DAILY ----------------
        upsert_capture_daily(
            engine, args.schema, p, args.model, args.duration_h, args.power_mw, args.roundtrip_eff,
            realized_profit_by_day=realized_profit_by_day,
            theoretical_profit_by_day=theo_profit_by_day,
        )

        print(
            f"[OK] {p} | duration={args.duration_h}h | model={args.model} "
            f"| days(theo)={len(theo_profit_by_day)} days(real)={len(realized_profit_by_day)} "
            f"| pred_hours={len(rt_pred)} cap_dispatch_hours={len(cap_dispatch)}"
        )

    print("[DONE] run_capture_pipeline finished.")


if __name__ == "__main__":
    main()
