# -*- coding: utf-8 -*-
"""
DB-backed Mengxi P&L dashboard with:
- schema-compatible readers (old/new column names)
- classic dashboard mode (weekly/monthly revenue/spread/cycles/efficiency)
- reporting mode (detailed operational tables)
- calculation mode (PNL_CALC_ENABLED=true): triggers run_pnl_refresh inline
"""
from __future__ import annotations

import datetime as dt
import os
from typing import Iterable

import numpy as np
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# calc.py is co-located with app.py in ECS (/apps/calc.py) and Streamlit adds
# the script's directory to sys.path, so `import calc` works both locally and
# in ECS. The fallback covers edge cases where the repo root is the CWD.
try:
    from calc import (  # noqa: E402
        DEFAULT_COMPENSATION_YUAN_PER_MWH as _CALC_DEFAULT_COMP,
        DEFAULT_SCENARIO_AVAILABILITY,
        build_daily_scenario_rows,
        build_daily_attribution_row,
        scenario_availability_df,
    )
    _CALC_SCENARIOS = [
        "perfect_foresight_unrestricted",
        "perfect_foresight_grid_feasible",
        "cleared_actual",
        "nominated_dispatch",
        "tt_forecast_optimal",
        "tt_strategy",
    ]
except ImportError:
    from apps.trading.bess.mengxi.pnl_attribution.calc import (
        DEFAULT_COMPENSATION_YUAN_PER_MWH as _CALC_DEFAULT_COMP,
        DEFAULT_SCENARIO_AVAILABILITY,
        build_daily_scenario_rows,
        build_daily_attribution_row,
        scenario_availability_df,
    )
    _CALC_SCENARIOS = [
        "perfect_foresight_unrestricted",
        "perfect_foresight_grid_feasible",
        "cleared_actual",
        "nominated_dispatch",
        "tt_forecast_optimal",
        "tt_strategy",
    ]

st.set_page_config(page_title="Mengxi P&L Attribution", layout="wide")

DB_URL = os.getenv("DB_DSN") or os.getenv("PGURL")
if not DB_URL:
    st.error("Missing DB_DSN / PGURL")
    st.stop()

# PNL_CALC_ENABLED: enables the inline calculation trigger (equivalent to the
# ECS scheduled run_pnl_refresh.py job).  Defaults to true in dev mode so
# local runs get full calculation capability without extra config.
_auth_mode = os.getenv("AUTH_MODE", "").lower()
PNL_CALC_ENABLED = os.getenv(
    "PNL_CALC_ENABLED",
    "true" if _auth_mode == "dev" else "false",
).lower() == "true"

# Compensation rate override: mirrors DEFAULT_COMPENSATION_YUAN_PER_MWH env var
# used by run_pnl_refresh.py in the trading_jobs ECS task.
_DEFAULT_COMP = float(os.getenv("DEFAULT_COMPENSATION_YUAN_PER_MWH", str(_CALC_DEFAULT_COMP)))

# Lookback window for the refresh trigger.
_DEFAULT_LOOKBACK = int(os.getenv("PNL_REFRESH_LOOKBACK_DAYS", "7"))


@st.cache_resource
def get_engine() -> Engine:
    return create_engine(DB_URL, pool_pre_ping=True)


ENGINE = get_engine()
ASSET_CODES = ["suyou", "wulate", "wuhai", "wulanchabu", "hetao", "hangjinqi", "siziwangqi", "gushanliang"]
NO_SUBSIDY_ASSETS = {"wulanchabu"}
ASSET_DISPLAY = {
    "suyou": "SuYou",
    "wulate": "WuLaTe",
    "wuhai": "WuHai",
    "wulanchabu": "WuLanChaBu",
    "hetao": "HeTao",
    "hangjinqi": "HangJinQi",
    "siziwangqi": "SiZiWangQi",
    "gushanliang": "GuShanLiang",
}


# ── Existing display helpers ───────────────────────────────────────────────────

def _safe_read_sql(engine: Engine, sql: str, params: dict | None = None, cols: list[str] | None = None) -> pd.DataFrame:
    try:
        return pd.read_sql(text(sql), engine, params=params)
    except Exception:
        return pd.DataFrame(columns=cols or [])


def _display_df(df: pd.DataFrame) -> "pd.io.formats.style.Styler | pd.DataFrame":
    if df.empty:
        return df
    out = df.copy()
    fmt: dict[str, str] = {}
    for col in out.columns:
        if pd.api.types.is_numeric_dtype(out[col]):
            s = pd.to_numeric(out[col], errors="coerce")
            non_null = s.dropna()
            if len(non_null) == 0:
                fmt[col] = "{:,.0f}"
            elif np.all(np.isclose(non_null, np.round(non_null))):
                fmt[col] = "{:,.0f}"
            else:
                fmt[col] = "{:,.2f}"
    return out.style.format(fmt, na_rep="")


def _table_columns(engine: Engine, schema_name: str, table_name: str) -> set[str]:
    sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :schema_name
          AND table_name = :table_name
    """
    df = _safe_read_sql(engine, sql, params={"schema_name": schema_name, "table_name": table_name}, cols=["column_name"])
    if df.empty:
        return set()
    return set(df["column_name"].astype(str).str.lower().tolist())


def _pick_expr(cols: set[str], target_alias: str, candidates: Iterable[str], default_expr: str = "NULL") -> str:
    for c in candidates:
        c_lc = c.lower()
        if c_lc in cols:
            return f"{c_lc} AS {target_alias}"
    return f"{default_expr} AS {target_alias}"


# ── Calculation engine helpers (mirrors run_pnl_refresh.py exactly) ────────────

_REPORT_TABLES_DDL = """
CREATE SCHEMA IF NOT EXISTS reports;
CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS reports.bess_asset_daily_scenario_pnl (
    trade_date date NOT NULL,
    asset_code text NOT NULL,
    scenario_name text NOT NULL,
    scenario_available boolean NOT NULL,
    compensation_yuan_per_mwh numeric,
    market_revenue_yuan numeric,
    subsidy_revenue_yuan numeric,
    total_revenue_yuan numeric,
    discharge_mwh numeric,
    charge_mwh numeric,
    avg_daily_cycles numeric,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (trade_date, asset_code, scenario_name)
);

CREATE TABLE IF NOT EXISTS core.asset_monthly_compensation (
    asset_code text NOT NULL,
    effective_month date NOT NULL,
    compensation_yuan_per_mwh numeric NOT NULL,
    source_system text,
    notes text,
    active_flag boolean NOT NULL DEFAULT TRUE,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (asset_code, effective_month)
);

CREATE TABLE IF NOT EXISTS reports.bess_asset_daily_attribution (
    trade_date date NOT NULL,
    asset_code text NOT NULL,
    pf_unrestricted_pnl numeric,
    pf_grid_feasible_pnl numeric,
    cleared_actual_pnl numeric,
    nominated_pnl numeric,
    tt_forecast_optimal_pnl numeric,
    tt_strategy_pnl numeric,
    grid_restriction_loss numeric,
    forecast_error_loss numeric,
    strategy_error_loss numeric,
    nomination_loss numeric,
    execution_clearing_loss numeric,
    realisation_gap_vs_pf numeric,
    realisation_gap_vs_pf_grid numeric,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (trade_date, asset_code)
)
"""


def _ensure_report_tables(engine: Engine) -> None:
    """Create reports and core schema tables if they don't exist yet."""
    with engine.begin() as con:
        for stmt in _REPORT_TABLES_DDL.split(";"):
            sql = stmt.strip()
            if sql:
                con.execute(text(sql))


def _fetch_compensation_df(engine: Engine) -> pd.DataFrame:
    """Read monthly compensation rates from core.asset_monthly_compensation."""
    try:
        return pd.read_sql(
            text("""
                SELECT asset_code, effective_month, compensation_yuan_per_mwh
                FROM core.asset_monthly_compensation
                WHERE active_flag = TRUE
            """),
            engine,
        )
    except Exception:
        return pd.DataFrame(columns=["asset_code", "effective_month", "compensation_yuan_per_mwh"])


def _fetch_availability_df(engine: Engine) -> pd.DataFrame:
    """Read scenario availability flags; falls back to hardcoded defaults."""
    try:
        return pd.read_sql(
            text("""
                SELECT asset_code, scenario_name, available_flag
                FROM core.asset_scenario_availability
                WHERE active_flag = TRUE
            """),
            engine,
        )
    except Exception:
        return scenario_availability_df()


def _build_availability_map(availability_df: pd.DataFrame, asset_code: str) -> dict[str, bool]:
    """Build scenario → bool dict for one asset (mirrors run_pnl_refresh.py)."""
    out: dict[str, bool] = {s: False for s in _CALC_SCENARIOS}
    hit = availability_df[availability_df["asset_code"] == asset_code]
    if hit.empty:
        # Fall back to hardcoded defaults
        for scenario, flag in DEFAULT_SCENARIO_AVAILABILITY.get(asset_code, {}).items():
            out[scenario] = bool(flag)
    else:
        for _, row in hit.iterrows():
            out[str(row["scenario_name"])] = bool(row["available_flag"])
    return out


def _load_actual_price_raw(engine: Engine, asset_code: str, trade_date: dt.date) -> pd.DataFrame:
    """Read 15-min actual RT prices from canon schema (same query as run_pnl_refresh.py)."""
    sql = text("""
        SELECT time, price
        FROM canon.nodal_rt_price_15min
        WHERE asset_code = :asset_code
          AND time >= :start_ts
          AND time < :end_ts
        ORDER BY time
    """)
    start_ts = pd.Timestamp(trade_date)
    end_ts = start_ts + pd.Timedelta(days=1)
    try:
        return pd.read_sql(
            sql, engine,
            params={"asset_code": asset_code, "start_ts": start_ts, "end_ts": end_ts},
        )
    except Exception:
        return pd.DataFrame(columns=["time", "price"])


def _load_dispatch_raw(engine: Engine, asset_code: str, scenario_name: str, trade_date: dt.date) -> pd.DataFrame:
    """Read 15-min dispatch MWs for a scenario from canon schema (same query as run_pnl_refresh.py)."""
    sql = text("""
        SELECT time, dispatch_mw
        FROM canon.scenario_dispatch_15min
        WHERE asset_code = :asset_code
          AND scenario_name = :scenario_name
          AND time >= :start_ts
          AND time < :end_ts
        ORDER BY time
    """)
    start_ts = pd.Timestamp(trade_date)
    end_ts = start_ts + pd.Timedelta(days=1)
    try:
        return pd.read_sql(
            sql, engine,
            params={
                "asset_code": asset_code,
                "scenario_name": scenario_name,
                "start_ts": start_ts,
                "end_ts": end_ts,
            },
        )
    except Exception:
        return pd.DataFrame(columns=["time", "dispatch_mw"])


def _upsert_df(engine: Engine, table_name: str, df: pd.DataFrame, pk_cols: list[str]) -> None:
    """Bulk upsert via temp-table pattern (identical to run_pnl_refresh.py)."""
    if df.empty:
        return
    stage_name = f"_tmp_{table_name.replace('.', '_')}_{int(pd.Timestamp.utcnow().timestamp())}"
    schema_name, _ = table_name.split(".", 1)
    with engine.begin() as con:
        df.to_sql(stage_name, con=con, schema=schema_name, if_exists="replace", index=False)
        cols = list(df.columns)
        insert_cols = ", ".join(cols)
        select_cols = ", ".join(cols)
        update_cols = ", ".join([f"{c}=EXCLUDED.{c}" for c in cols if c not in pk_cols])
        con.execute(text(f"""
            INSERT INTO {table_name} ({insert_cols})
            SELECT {select_cols}
            FROM {schema_name}."{stage_name}"
            ON CONFLICT ({", ".join(pk_cols)})
            DO UPDATE SET {update_cols}, updated_at = now()
        """))
        con.execute(text(f'DROP TABLE IF EXISTS {schema_name}."{stage_name}"'))


def _run_pnl_refresh_range(
    engine: Engine,
    start_date: dt.date,
    end_date: dt.date,
    default_comp: float,
    progress_cb=None,
) -> tuple[int, list[str]]:
    """
    Run the full P&L calculation for start_date..end_date across all assets.

    Mirrors run_pnl_refresh.py main() exactly:
      1. ensure tables
      2. load availability + compensation from DB (or defaults)
      3. for each (date, asset): load prices + dispatch, calc, collect rows
      4. bulk upsert into reports.*

    Returns (n_asset_days_written, list_of_warnings).
    """
    _ensure_report_tables(engine)
    availability_df = _fetch_availability_df(engine)
    compensation_df = _fetch_compensation_df(engine)

    dates = [
        start_date + dt.timedelta(days=i)
        for i in range((end_date - start_date).days + 1)
    ]
    total_steps = len(dates) * len(ASSET_CODES)

    scenario_rows_all: list[pd.DataFrame] = []
    attribution_rows_all: list[pd.DataFrame] = []
    warnings: list[str] = []
    n_written = 0
    step = 0

    for trade_date in dates:
        for asset_code in ASSET_CODES:
            step += 1
            if progress_cb:
                progress_cb(step / total_steps, f"{trade_date} / {asset_code}")

            availability_map = _build_availability_map(availability_df, asset_code)
            if not any(availability_map.values()):
                continue

            actual_price_df = _load_actual_price_raw(engine, asset_code, trade_date)
            if actual_price_df.empty:
                warnings.append(f"No price data: asset={asset_code} date={trade_date}")
                continue

            scenario_dispatch_map: dict[str, pd.DataFrame] = {}
            for scenario_name, available in availability_map.items():
                if not available:
                    continue
                scenario_dispatch_map[scenario_name] = _load_dispatch_raw(
                    engine, asset_code, scenario_name, trade_date
                )

            scenario_rows = build_daily_scenario_rows(
                trade_date=pd.Timestamp(trade_date),
                asset_code=asset_code,
                actual_price_df=actual_price_df,
                scenario_dispatch_map=scenario_dispatch_map,
                availability_map=availability_map,
                compensation_df=compensation_df,
                default_compensation_yuan_per_mwh=default_comp,
            )
            attribution_row = build_daily_attribution_row(scenario_rows)

            scenario_rows_all.append(scenario_rows)
            attribution_rows_all.append(attribution_row)
            n_written += 1

    if scenario_rows_all:
        _upsert_df(
            engine,
            "reports.bess_asset_daily_scenario_pnl",
            pd.concat(scenario_rows_all, ignore_index=True),
            pk_cols=["trade_date", "asset_code", "scenario_name"],
        )
    if attribution_rows_all:
        _upsert_df(
            engine,
            "reports.bess_asset_daily_attribution",
            pd.concat(attribution_rows_all, ignore_index=True),
            pk_cols=["trade_date", "asset_code"],
        )

    return n_written, warnings


# ── Existing reporting data loaders ───────────────────────────────────────────

@st.cache_data(ttl=300)
def load_scenario_pnl(_engine: Engine, start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    out_cols = [
        "trade_date",
        "asset_code",
        "scenario_name",
        "scenario_available",
        "compensation_yuan_per_mwh",
        "compensation_blocked",
        "compensation_block_reason",
        "market_revenue_yuan",
        "subsidy_revenue_yuan",
        "total_revenue_yuan",
        "discharge_mwh",
        "charge_mwh",
        "avg_daily_cycles",
        "asset_label",
    ]
    cols = _table_columns(_engine, "reports", "bess_asset_daily_scenario_pnl")
    if not cols:
        return pd.DataFrame(columns=out_cols)

    select_parts = [
        _pick_expr(cols, "trade_date", ["trade_date"]),
        _pick_expr(cols, "asset_code", ["asset_code"]),
        _pick_expr(cols, "scenario_name", ["scenario_name"]),
        _pick_expr(cols, "scenario_available", ["scenario_available"], default_expr="TRUE"),
        _pick_expr(cols, "compensation_yuan_per_mwh", ["compensation_yuan_per_mwh", "compensation_rate"]),
        _pick_expr(cols, "compensation_blocked", ["compensation_blocked"], default_expr="FALSE"),
        _pick_expr(cols, "compensation_block_reason", ["compensation_block_reason"]),
        _pick_expr(cols, "market_revenue_yuan", ["market_revenue_yuan", "market_revenue"]),
        _pick_expr(cols, "subsidy_revenue_yuan", ["subsidy_revenue_yuan", "compensation_revenue"]),
        _pick_expr(cols, "total_revenue_yuan", ["total_revenue_yuan", "total_pnl"]),
        _pick_expr(cols, "discharge_mwh", ["discharge_mwh"]),
        _pick_expr(cols, "charge_mwh", ["charge_mwh"]),
        _pick_expr(cols, "avg_daily_cycles", ["avg_daily_cycles"]),
    ]
    sql = f"""
        SELECT {", ".join(select_parts)}
        FROM reports.bess_asset_daily_scenario_pnl
        WHERE trade_date >= :start_date
          AND trade_date <= :end_date
          AND asset_code = ANY(:asset_codes)
        ORDER BY trade_date, asset_code, scenario_name
    """
    df = _safe_read_sql(
        _engine,
        sql,
        params={"start_date": start_date, "end_date": end_date, "asset_codes": ASSET_CODES},
        cols=out_cols,
    )
    if df.empty:
        return pd.DataFrame(columns=out_cols)

    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    df["asset_code"] = df["asset_code"].astype(str).str.strip().str.lower()
    df["scenario_name"] = df["scenario_name"].astype(str).str.strip().str.lower()
    df["scenario_name"] = df["scenario_name"].replace(
        {
            "nominated": "nominated_dispatch",
            "tt_forecast": "tt_forecast_optimal",
            "tt_optimal": "tt_forecast_optimal",
        }
    )
    for c in ["compensation_yuan_per_mwh", "market_revenue_yuan", "subsidy_revenue_yuan", "total_revenue_yuan", "discharge_mwh", "charge_mwh", "avg_daily_cycles"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    no_subsidy_mask = df["asset_code"].isin(NO_SUBSIDY_ASSETS)
    df.loc[no_subsidy_mask, "subsidy_revenue_yuan"] = 0.0
    df.loc[no_subsidy_mask, "total_revenue_yuan"] = (
        df.loc[no_subsidy_mask, "market_revenue_yuan"].fillna(0.0)
        + df.loc[no_subsidy_mask, "subsidy_revenue_yuan"].fillna(0.0)
    )
    df["compensation_blocked"] = df["compensation_blocked"].fillna(False).astype(bool)
    df["asset_label"] = df["asset_code"].map(ASSET_DISPLAY).fillna(df["asset_code"])
    return df


@st.cache_data(ttl=300)
def load_attribution(_engine: Engine, start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    cols = _table_columns(_engine, "reports", "bess_asset_daily_attribution")
    if not cols:
        return pd.DataFrame()

    select_parts = [
        _pick_expr(cols, "trade_date", ["trade_date"]),
        _pick_expr(cols, "asset_code", ["asset_code"]),
        _pick_expr(cols, "pf_unrestricted_pnl", ["pf_unrestricted_pnl"]),
        _pick_expr(cols, "pf_grid_feasible_pnl", ["pf_grid_feasible_pnl"]),
        _pick_expr(cols, "cleared_actual_pnl", ["cleared_actual_pnl"]),
        _pick_expr(cols, "nominated_pnl", ["nominated_pnl", "nominated_dispatch_pnl"]),
        _pick_expr(cols, "tt_forecast_optimal_pnl", ["tt_forecast_optimal_pnl"]),
        _pick_expr(cols, "tt_strategy_pnl", ["tt_strategy_pnl"]),
        _pick_expr(cols, "grid_restriction_loss", ["grid_restriction_loss"]),
        _pick_expr(cols, "forecast_error_loss", ["forecast_error_loss"]),
        _pick_expr(cols, "strategy_error_loss", ["strategy_error_loss"]),
        _pick_expr(cols, "nomination_loss", ["nomination_loss"]),
        _pick_expr(cols, "execution_clearing_loss", ["execution_clearing_loss"]),
        _pick_expr(cols, "realisation_gap_vs_pf", ["realisation_gap_vs_pf", "unexplained_gap"]),
        _pick_expr(cols, "realisation_gap_vs_pf_grid", ["realisation_gap_vs_pf_grid", "total_explained_loss"]),
    ]
    sql = f"""
        SELECT {", ".join(select_parts)}
        FROM reports.bess_asset_daily_attribution
        WHERE trade_date >= :start_date
          AND trade_date <= :end_date
          AND asset_code = ANY(:asset_codes)
        ORDER BY trade_date, asset_code
    """
    df = _safe_read_sql(_engine, sql, params={"start_date": start_date, "end_date": end_date, "asset_codes": ASSET_CODES})
    if df.empty:
        return df

    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    metric_cols = [c for c in df.columns if c not in ["trade_date", "asset_code"]]
    for c in metric_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


@st.cache_data(ttl=300)
def load_scenario_window(_engine: Engine) -> pd.DataFrame:
    sql = """
        SELECT
            MIN(trade_date) AS min_trade_date,
            MAX(trade_date) AS max_trade_date,
            COUNT(*) AS row_count
        FROM reports.bess_asset_daily_scenario_pnl
    """
    df = _safe_read_sql(_engine, sql, cols=["min_trade_date", "max_trade_date", "row_count"])
    if not df.empty:
        for c in ["min_trade_date", "max_trade_date"]:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    return df


# ── Classic dashboard builders ─────────────────────────────────────────────────

def _safe_pct(n: pd.Series, d: pd.Series) -> pd.Series:
    n_num = pd.to_numeric(n, errors="coerce").astype(float)
    d_num = pd.to_numeric(d, errors="coerce").astype(float).replace(0, np.nan)
    return pd.to_numeric((n_num / d_num) * 100, errors="coerce").round(0)


def _safe_div_round(n: pd.Series, d: pd.Series, decimals: int = 0) -> pd.Series:
    n_num = pd.to_numeric(n, errors="coerce").astype(float)
    d_num = pd.to_numeric(d, errors="coerce").astype(float).replace(0, np.nan)
    return pd.to_numeric(n_num / d_num, errors="coerce").round(decimals)


def _fmt_pct(v: object) -> str:
    if pd.isna(v):
        return ""
    return f"{int(v)}%"


def _build_classic_tables(asset_df: pd.DataFrame, freq: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df = asset_df.copy()
    if df.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    if freq == "W":
        df["period_start"] = pd.to_datetime(df["trade_date"]).dt.to_period("W-SUN").apply(lambda p: p.start_time.date())
    else:
        df["period_start"] = pd.to_datetime(df["trade_date"]).dt.to_period("M").apply(lambda p: p.start_time.date())

    grp = df.groupby(["period_start", "scenario_name"], as_index=False).agg(
        market_revenue_yuan=("market_revenue_yuan", "sum"),
        subsidy_revenue_yuan=("subsidy_revenue_yuan", "sum"),
        total_revenue_yuan=("total_revenue_yuan", "sum"),
        discharge_mwh=("discharge_mwh", "sum"),
        charge_mwh=("charge_mwh", "sum"),
        avg_daily_cycles=("avg_daily_cycles", "mean"),
    )

    idx = pd.Index(sorted(grp["period_start"].unique()), name="period_start")

    def pvt(col: str) -> pd.DataFrame:
        return grp.pivot(index="period_start", columns="scenario_name", values=col).reindex(idx)

    def _num_series(df: pd.DataFrame, col: str) -> pd.Series:
        if col not in df.columns:
            return pd.Series(np.nan, index=df.index, dtype="float64")
        return pd.to_numeric(df[col], errors="coerce").astype(float)

    p_total = pvt("total_revenue_yuan")
    p_market = pvt("market_revenue_yuan")
    p_subsidy = pvt("subsidy_revenue_yuan")
    p_discharge = pvt("discharge_mwh")
    p_charge = pvt("charge_mwh")
    p_cycles = pvt("avg_daily_cycles")

    opt = "perfect_foresight_grid_feasible" if "perfect_foresight_grid_feasible" in p_total.columns else "perfect_foresight_unrestricted"

    rev = pd.DataFrame({"period_start": idx})
    rev["actual_revenue"] = (_num_series(p_total, "cleared_actual") / 10000).round(0)
    rev["market_revenue"] = (_num_series(p_market, "cleared_actual") / 10000).round(0)
    rev["subsidy_revenue"] = (_num_series(p_subsidy, "cleared_actual") / 10000).round(0)
    rev["nominated_revenue"] = (_num_series(p_total, "nominated_dispatch") / 10000).round(0)
    rev["strategy_revenue"] = (_num_series(p_total, "tt_strategy") / 10000).round(0)
    rev["optimal_revenue"] = (_num_series(p_total, opt) / 10000).round(0)
    rev["discharged volume in MWh"] = _num_series(p_discharge, "cleared_actual").round(0)
    rev["actual/optimal"] = _safe_pct(rev["actual_revenue"], rev["optimal_revenue"]).map(_fmt_pct)
    rev["actual/nominated"] = _safe_pct(rev["actual_revenue"], rev["nominated_revenue"]).map(_fmt_pct)

    spread = pd.DataFrame({"period_start": idx})
    spread["actual_spread"] = _safe_div_round(_num_series(p_total, "cleared_actual"), _num_series(p_discharge, "cleared_actual"), 0)
    spread["market_spread"] = _safe_div_round(_num_series(p_market, "cleared_actual"), _num_series(p_discharge, "cleared_actual"), 0)
    spread["Subsidies"] = _safe_div_round(_num_series(p_subsidy, "cleared_actual"), _num_series(p_discharge, "cleared_actual"), 0)
    spread["nominated_spread"] = _safe_div_round(_num_series(p_total, "nominated_dispatch"), _num_series(p_discharge, "nominated_dispatch"), 0)
    spread["strategy_spread"] = _safe_div_round(_num_series(p_total, "tt_strategy"), _num_series(p_discharge, "tt_strategy"), 0)
    spread["optimal_spread"] = _safe_div_round(_num_series(p_total, opt), _num_series(p_discharge, opt), 0)
    spread["unit cycle spread"] = (spread["actual_spread"] * _num_series(p_cycles, "cleared_actual")).round(0)
    spread["expected unit cycle spread"] = ((spread["market_spread"] + 350) * _num_series(p_cycles, "cleared_actual")).round(0)

    cycles = pd.DataFrame({"period_start": idx})
    cycles["actual_avg_daily_cycles"] = _num_series(p_cycles, "cleared_actual").round(2)
    cycles["nominated_avg_daily_cycles"] = _num_series(p_cycles, "nominated_dispatch").round(2)
    cycles["strategy_avg_daily_cycles"] = _num_series(p_cycles, "tt_strategy").round(2)
    cycles["optimal_avg_daily_cycles"] = _num_series(p_cycles, opt).round(2)
    cycles["actual/optimal"] = _safe_pct(cycles["actual_avg_daily_cycles"], cycles["optimal_avg_daily_cycles"]).map(_fmt_pct)
    cycles["actual/nominated"] = _safe_pct(cycles["actual_avg_daily_cycles"], cycles["nominated_avg_daily_cycles"]).map(_fmt_pct)

    eff = pd.DataFrame({"period_start": idx})
    eff["actual_efficiency"] = (_safe_div_round(_num_series(p_discharge, "cleared_actual"), _num_series(p_charge, "cleared_actual"), 3) * 100).round(1).map(_fmt_pct)

    return rev, spread, cycles, eff


def _render_classic_asset(asset_df: pd.DataFrame, label: str) -> None:
    week_tab, month_tab = st.tabs(["Weekly", "Monthly"])
    for tab, freq in ((week_tab, "W"), (month_tab, "M")):
        with tab:
            rev, spr, cyc, eff = _build_classic_tables(asset_df, freq)
            if rev.empty:
                st.warning("No data for this asset in selected period.")
                continue
            st.subheader(f"Revenue (10k CNY) - {label}")
            st.dataframe(_display_df(rev), use_container_width=True)
            st.subheader("Spread (CNY/MWh)")
            st.dataframe(_display_df(spr), use_container_width=True)
            st.subheader("Cycles")
            st.dataframe(_display_df(cyc), use_container_width=True)
            st.subheader("Efficiency")
            st.dataframe(_display_df(eff), use_container_width=True)


# -------------------- UI --------------------
st.sidebar.header("Filters")
default_end = dt.date.today()
default_start = default_end - dt.timedelta(days=30)
date_range = st.sidebar.date_input("Trade date range", value=(default_start, default_end))
if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
    start_date, end_date = date_range[0], date_range[1]
elif isinstance(date_range, (list, tuple)) and len(date_range) == 1:
    start_date = date_range[0]
    end_date = dt.date.today()
elif isinstance(date_range, dt.date):
    start_date = date_range
    end_date = dt.date.today()
else:
    start_date, end_date = default_start, default_end
if start_date > end_date:
    st.warning("Invalid date range: start date is after end date.")
    st.stop()

asset_choice = st.sidebar.selectbox(
    "Asset drilldown",
    options=["fleet"] + ASSET_CODES,
    format_func=lambda x: "Fleet" if x == "fleet" else f"{x} / {ASSET_DISPLAY.get(x, x)}",
)
scenario_choice = st.sidebar.selectbox(
    "Scenario",
    options=["cleared_actual", "perfect_foresight_unrestricted", "perfect_foresight_grid_feasible", "nominated_dispatch", "tt_forecast_optimal", "tt_strategy"],
)

# ── Calculation trigger (local / explicit PNL_CALC_ENABLED=true) ───────────────
# In ECS, PNL_CALC_ENABLED is not set, so this section is hidden.
# Locally (AUTH_MODE=dev) or when explicitly set, this exposes the same
# calculation engine that run_pnl_refresh.py runs as a scheduled ECS job.
if PNL_CALC_ENABLED:
    st.sidebar.divider()
    with st.sidebar.expander("Recalculate P&L", expanded=False):
        st.caption(
            "Runs the P&L engine against `canon.nodal_rt_price_15min` and "
            "`canon.scenario_dispatch_15min`, then writes to `reports.*` tables. "
            "Identical to the ECS-scheduled `run_pnl_refresh.py` job."
        )
        refresh_lookback = st.number_input(
            "Lookback days",
            min_value=1,
            max_value=365,
            value=_DEFAULT_LOOKBACK,
            step=1,
            key="refresh_lookback",
        )
        refresh_comp = st.number_input(
            "Default compensation (CNY/MWh)",
            min_value=0.0,
            max_value=10000.0,
            value=_DEFAULT_COMP,
            step=10.0,
            key="refresh_comp",
        )
        if st.button("Run P&L Refresh", type="primary", key="run_pnl_refresh_btn"):
            r_end = dt.date.today()
            r_start = r_end - dt.timedelta(days=int(refresh_lookback))
            _progress = st.progress(0, text="Starting…")
            _status = st.empty()

            def _on_progress(frac: float, label: str) -> None:
                pct = int(frac * 100)
                _progress.progress(pct, text=label)

            try:
                n, warns = _run_pnl_refresh_range(
                    ENGINE, r_start, r_end, float(refresh_comp), _on_progress
                )
                _progress.progress(100, text="Done")
                if warns:
                    _status.warning(
                        f"Refreshed {n} asset-days with {len(warns)} warning(s):\n"
                        + "\n".join(f"• {w}" for w in warns[:10])
                    )
                else:
                    _status.success(f"Refreshed {n} asset-days ({r_start} → {r_end}).")
                # Clear cached reads so the display reloads fresh data
                load_scenario_pnl.clear()
                load_attribution.clear()
                load_scenario_window.clear()
                st.rerun()
            except Exception as exc:
                _progress.empty()
                _status.error(f"Refresh failed: {exc}")

scenario_df = load_scenario_pnl(ENGINE, start_date, end_date)
attribution_df = load_attribution(ENGINE, start_date, end_date)
window_df = load_scenario_window(ENGINE)

st.title("Mengxi Trading - P&L Attribution")
_mode = "calculation + reporting" if PNL_CALC_ENABLED else "read-only reporting"
st.caption(
    f"Mode: **{_mode}** · "
    "Reads `reports.bess_asset_daily_scenario_pnl` and `reports.bess_asset_daily_attribution`. "
    + ("Use **Recalculate P&L** in the sidebar to refresh data from source tables."
       if PNL_CALC_ENABLED else
       "Set `PNL_CALC_ENABLED=true` to enable the inline calculation engine.")
)

view_mode = st.radio("View mode", ["Classic Dashboard", "Reporting Tables"], horizontal=True)

if scenario_df.empty:
    if not window_df.empty and pd.notna(window_df.loc[0, "min_trade_date"]) and pd.notna(window_df.loc[0, "max_trade_date"]):
        min_d = window_df.loc[0, "min_trade_date"].date()
        max_d = window_df.loc[0, "max_trade_date"].date()
        row_count = int(window_df.loc[0, "row_count"] or 0)
        st.warning(
            f"No scenario P&L rows found for selected range. "
            f"Available DB window: {min_d} to {max_d} (rows: {row_count:,})."
            + (" Run **Recalculate P&L** to populate." if PNL_CALC_ENABLED else "")
        )
    else:
        st.warning(
            "No scenario P&L rows found for selected range."
            + (" Run **Recalculate P&L** to populate." if PNL_CALC_ENABLED else "")
        )

if view_mode == "Classic Dashboard":
    if "asset_code" not in scenario_df.columns:
        st.info("No assets available in selected range.")
        st.stop()

    if asset_choice == "fleet":
        focus_assets = [a for a in ["suyou", "wulate"] if a in scenario_df["asset_code"].unique().tolist()]
        if not focus_assets:
            focus_assets = sorted(scenario_df["asset_code"].dropna().unique().tolist())
    else:
        focus_assets = [asset_choice]

    if focus_assets:
        tabs = st.tabs([f"{a} / {ASSET_DISPLAY.get(a, a)}" for a in focus_assets])
        for tab, a in zip(tabs, focus_assets):
            with tab:
                _render_classic_asset(scenario_df[scenario_df["asset_code"] == a].copy(), ASSET_DISPLAY.get(a, a))
    else:
        st.info("No assets available in selected range.")
else:
    base_df = scenario_df[scenario_df["scenario_name"] == scenario_choice].copy()
    if asset_choice != "fleet":
        base_df = base_df[base_df["asset_code"] == asset_choice].copy()

    st.subheader("Daily rows")
    st.dataframe(_display_df(base_df.sort_values(["trade_date", "asset_code"])), use_container_width=True)

    st.subheader("Daily attribution")
    if attribution_df.empty:
        st.info("No attribution rows found in selected date range.")
    else:
        attr = attribution_df.copy()
        if asset_choice != "fleet":
            attr = attr[attr["asset_code"] == asset_choice]
        st.dataframe(_display_df(attr.sort_values(["trade_date", "asset_code"])), use_container_width=True)
