"""
libs/decision_models/resources/bess_context.py

Database loaders and asset metadata for the strategy comparison workflow.

DB access pattern
-----------------
Uses shared.agents.db.run_query (psycopg2 / pandas) — the same pattern as
shared/agents/execution_agent.py and shared/metrics/*.py.

Graceful degradation
--------------------
All loaders catch exceptions and return empty DataFrames / None with a note
in data_quality_notes.  Callers must check emptiness and surface these notes.

Known data gaps (TODO)
----------------------
1. Asset physical parameters (power_mw, duration_h, roundtrip_eff):
   Not yet in a DB table.  Loaded from ASSET_PHYSICAL_PARAMS below.
   TODO: create core.asset_master table with physical params.

2. Outage / curtailment / restriction flags:
   No table exists yet.  load_outage_flags() always returns (None, note).
   TODO: create ops.asset_outage_log or similar.

3. DA prices per asset/node:
   marketdata.md_settlement_ref_price holds a province-level settlement
   reference price, not a per-asset DA price.  load_da_prices_hourly()
   uses this as the best available proxy.
   TODO: confirm whether per-node DA prices are available in a different table.

4. Forecast-driven dispatch P&L:
   Settled against hourly mean of 15-min actual prices.  This is an
   approximation — results are not directly comparable to 15-min P&L from
   reports.bess_asset_daily_scenario_pnl.

5. Cleared energy vs actual dispatch (Inner Mongolia / Mengxi):
   marketdata.md_id_cleared_energy holds DA market-cleared trading energy per
   dispatch unit — NOT actual physical dispatch / generation output.
   Actual output may differ from cleared energy due to asset issues, BOP faults,
   grid operator real-time re-dispatch, or SOC constraints.
   load_id_cleared_energy() returns explicit unit fields:
     cleared_energy_mwh_15min         : MWh cleared per 15-min interval
     cleared_power_mw_implied_15min   : implied average power = mwh / 0.25
     cleared_price                    : CNY/MWh DA cleared price
   Do NOT label these columns as 'dispatch_mw'.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Asset physical parameters
# Hardcoded fallback until core.asset_master table is available.
# Keys: stable asset_code from apps/trading/bess/mengxi/pnl_attribution/calc.py
# TODO: replace with DB query when core.asset_master is created.
# ---------------------------------------------------------------------------
ASSET_PHYSICAL_PARAMS: dict = {
    "suyou":       {"power_mw": 100.0, "duration_h": 2.0, "roundtrip_eff": 0.85},
    "wulate":      {"power_mw": 100.0, "duration_h": 2.0, "roundtrip_eff": 0.85},
    "wuhai":       {"power_mw": 100.0, "duration_h": 2.0, "roundtrip_eff": 0.85},
    "wulanchabu":  {"power_mw": 100.0, "duration_h": 2.0, "roundtrip_eff": 0.85},
    "hetao":       {"power_mw": 100.0, "duration_h": 2.0, "roundtrip_eff": 0.85},
    "hangjinqi":   {"power_mw": 100.0, "duration_h": 2.0, "roundtrip_eff": 0.85},
    "siziwangqi":  {"power_mw": 100.0, "duration_h": 2.0, "roundtrip_eff": 0.85},
    "gushanliang": {"power_mw": 100.0, "duration_h": 2.0, "roundtrip_eff": 0.85},
}
# TODO: When core.asset_master is available, remove ASSET_PHYSICAL_PARAMS
# and use load_asset_physical_params() below.


def _run_query_safe(sql: str, params=None) -> Tuple[pd.DataFrame, Optional[str]]:
    """
    Run a DB query; return (df, None) on success or (empty_df, error_note) on failure.
    Uses shared.agents.db.run_query.
    """
    try:
        from shared.agents.db import run_query
        df = run_query(sql, params=params)
        return df, None
    except Exception as exc:
        return pd.DataFrame(), str(exc)


# ---------------------------------------------------------------------------
# Actual RT prices — canon.nodal_rt_price_15min
# ---------------------------------------------------------------------------

def load_actual_prices_15min(
    asset_code: str,
    date_from: date,
    date_to: date,
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Load 15-min actual RT prices for one asset over date_from..date_to (inclusive).

    Returns
    -------
    df    : columns [time (datetime), price (float)]
    notes : list of data quality / warning strings
    """
    notes: List[str] = []
    sql = """
        SELECT time, price
        FROM canon.nodal_rt_price_15min
        WHERE asset_code = %(asset_code)s
          AND time >= %(start_ts)s
          AND time < %(end_ts)s
        ORDER BY time
    """
    params = {
        "asset_code": asset_code,
        "start_ts": pd.Timestamp(date_from),
        "end_ts": pd.Timestamp(date_to) + pd.Timedelta(days=1),
    }
    df, err = _run_query_safe(sql, params)
    if err:
        notes.append(f"actual_prices_15min: query failed — {err}")
        return pd.DataFrame(columns=["time", "price"]), notes
    if df.empty:
        notes.append(
            f"actual_prices_15min: no data for {asset_code} "
            f"between {date_from} and {date_to}"
        )
    else:
        df["time"] = pd.to_datetime(df["time"])
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        n_null = df["price"].isna().sum()
        if n_null > 0:
            notes.append(
                f"actual_prices_15min: {n_null} null price values for {asset_code}"
            )
    return df, notes


def resample_15min_to_hourly(df_15min: pd.DataFrame) -> pd.DataFrame:
    """
    Resample 15-min {time, price} to hourly {datetime, price} by taking
    the mean within each hour.  Returns empty df if input is empty.
    """
    if df_15min.empty:
        return pd.DataFrame(columns=["datetime", "price"])
    df = df_15min.copy()
    df["time"] = pd.to_datetime(df["time"])
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    hourly = (
        df.set_index("time")["price"]
        .resample("1h")
        .mean()
        .reset_index()
        .rename(columns={"time": "datetime"})
    )
    return hourly


# ---------------------------------------------------------------------------
# DA prices — marketdata.md_settlement_ref_price (best available proxy)
# ---------------------------------------------------------------------------

def load_da_prices_hourly(
    date_from: date,
    date_to: date,
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Load province-level DA settlement reference prices (hourly or sub-hourly).

    Source: marketdata.md_settlement_ref_price
    NOTE: This is a province-level settlement reference price, not a
    per-asset nodal DA price.  Using it as a proxy for forecasting.
    TODO: verify with ops team whether per-node DA prices are available.

    Returns
    -------
    df    : columns [datetime (datetime), da_price (float)]
    notes : list of data quality / warning strings
    """
    notes: List[str] = [
        "da_prices_hourly: using marketdata.md_settlement_ref_price as DA price proxy "
        "(province-level, not nodal — may diverge from asset-level DA price)"
    ]
    sql = """
        SELECT datetime, system_settlement_price AS da_price
        FROM marketdata.md_settlement_ref_price
        WHERE data_date >= %(date_from)s
          AND data_date <= %(date_to)s
        ORDER BY datetime
    """
    params = {"date_from": date_from, "date_to": date_to}
    df, err = _run_query_safe(sql, params)
    if err:
        notes.append(f"da_prices_hourly: query failed — {err}")
        return pd.DataFrame(columns=["datetime", "da_price"]), notes
    if df.empty:
        notes.append(
            f"da_prices_hourly: no settlement ref price data for "
            f"{date_from} to {date_to} — forecast model will use naive_da"
        )
        return pd.DataFrame(columns=["datetime", "da_price"]), notes

    df["datetime"] = pd.to_datetime(df["datetime"])
    df["da_price"] = pd.to_numeric(df["da_price"], errors="coerce")

    # Resample to hourly if sub-hourly
    if not df.empty and df["datetime"].dt.minute.any():
        df = (
            df.set_index("datetime")["da_price"]
            .resample("1h")
            .mean()
            .reset_index()
            .rename(columns={"datetime": "datetime", "da_price": "da_price"})
        )
    return df, notes


# ---------------------------------------------------------------------------
# Dispatch from DB — canon.scenario_dispatch_15min
# ---------------------------------------------------------------------------

def load_scenario_dispatch_15min(
    asset_code: str,
    scenario_name: str,
    date_from: date,
    date_to: date,
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Load 15-min dispatch for one asset + scenario from canon.scenario_dispatch_15min.

    Returns
    -------
    df    : columns [time (datetime), dispatch_mw (float)]
    notes : list of data quality / warning strings
    """
    notes: List[str] = []
    sql = """
        SELECT time, dispatch_mw
        FROM canon.scenario_dispatch_15min
        WHERE asset_code = %(asset_code)s
          AND scenario_name = %(scenario_name)s
          AND time >= %(start_ts)s
          AND time < %(end_ts)s
        ORDER BY time
    """
    params = {
        "asset_code": asset_code,
        "scenario_name": scenario_name,
        "start_ts": pd.Timestamp(date_from),
        "end_ts": pd.Timestamp(date_to) + pd.Timedelta(days=1),
    }
    df, err = _run_query_safe(sql, params)
    if err:
        notes.append(
            f"{scenario_name} dispatch: query failed — {err}"
        )
        return pd.DataFrame(columns=["time", "dispatch_mw"]), notes
    if df.empty:
        notes.append(
            f"{scenario_name} dispatch: no data for {asset_code} "
            f"between {date_from} and {date_to}"
        )
    else:
        df["time"] = pd.to_datetime(df["time"])
        df["dispatch_mw"] = pd.to_numeric(df["dispatch_mw"], errors="coerce")
    return df, notes


def load_available_scenarios(
    asset_code: str,
    date_from: date,
    date_to: date,
) -> Tuple[List[str], List[str]]:
    """
    Return scenario names present in canon.scenario_dispatch_15min for this asset/period.
    """
    notes: List[str] = []
    sql = """
        SELECT DISTINCT scenario_name
        FROM canon.scenario_dispatch_15min
        WHERE asset_code = %(asset_code)s
          AND time >= %(start_ts)s
          AND time < %(end_ts)s
    """
    params = {
        "asset_code": asset_code,
        "start_ts": pd.Timestamp(date_from),
        "end_ts": pd.Timestamp(date_to) + pd.Timedelta(days=1),
    }
    df, err = _run_query_safe(sql, params)
    if err:
        notes.append(f"available_scenarios: query failed — {err}")
        return [], notes
    return df["scenario_name"].tolist(), notes


# ---------------------------------------------------------------------------
# Asset metadata
# ---------------------------------------------------------------------------

def load_asset_metadata(
    asset_code: str,
    trade_month: Optional[date] = None,
) -> Tuple[dict, List[str]]:
    """
    Return physical params, display name, and compensation rate for one asset.

    Physical params come from ASSET_PHYSICAL_PARAMS (hardcoded fallback).
    Compensation rate comes from core.asset_monthly_compensation if available.

    TODO: Replace physical params fallback with core.asset_master DB query.
    """
    from apps.trading.bess.mengxi.pnl_attribution.calc import (
        ASSET_ALIAS_MAP,
    )
    notes: List[str] = []
    physical = ASSET_PHYSICAL_PARAMS.get(asset_code, {
        "power_mw": 100.0,
        "duration_h": 2.0,
        "roundtrip_eff": 0.85,
    })
    if asset_code not in ASSET_PHYSICAL_PARAMS:
        notes.append(
            f"asset_metadata: {asset_code!r} not in ASSET_PHYSICAL_PARAMS — "
            "using default 100MW/2h/85% params"
        )

    alias = ASSET_ALIAS_MAP.get(asset_code, {})
    display_name = alias.get("display_name_cn", asset_code)
    province = alias.get("province", "")

    # Compensation rate
    comp_rate = 350.0
    if trade_month is not None:
        effective_month = date(trade_month.year, trade_month.month, 1)
        sql = """
            SELECT compensation_yuan_per_mwh
            FROM core.asset_monthly_compensation
            WHERE asset_code = %(asset_code)s
              AND effective_month = %(effective_month)s
              AND active_flag = TRUE
            LIMIT 1
        """
        df, err = _run_query_safe(
            sql, {"asset_code": asset_code, "effective_month": effective_month}
        )
        if err:
            notes.append(
                f"asset_metadata: compensation query failed — {err}; "
                "using default 350 CNY/MWh"
            )
        elif not df.empty:
            comp_rate = float(df.iloc[0]["compensation_yuan_per_mwh"])
        else:
            notes.append(
                f"asset_metadata: no compensation rate for {asset_code} "
                f"month {effective_month} — using default 350 CNY/MWh"
            )

    return {
        "asset_code": asset_code,
        "display_name": display_name,
        "power_mw": physical["power_mw"],
        "duration_h": physical["duration_h"],
        "roundtrip_eff": physical["roundtrip_eff"],
        "compensation_yuan_per_mwh": comp_rate,
        "province": province,
        "source": "hardcoded_fallback",
    }, notes


# ---------------------------------------------------------------------------
# Outage / curtailment flags — TODO placeholder
# ---------------------------------------------------------------------------

def load_outage_flags(
    asset_code: str,
    date_from: date,
    date_to: date,
) -> Tuple[None, List[str]]:
    """
    Load asset outage / unavailability flags.

    TODO: No outage table exists yet.  Returns (None, note) always.
    When ops.asset_outage_log (or equivalent) is created, implement this
    to query: asset_code, date, outage_flag, outage_type, notes.
    Without outage data the 'asset_issue' attribution bucket will always be None.
    """
    return None, [
        "outage_flags: ops.asset_outage_log table not yet implemented — "
        "'asset_issue' attribution bucket will be None"
    ]


def load_curtailment_flags(
    asset_code: str,
    date_from: date,
    date_to: date,
) -> Tuple[None, List[str]]:
    """
    Load grid restriction / curtailment flags.

    TODO: No curtailment table exists yet.  Returns (None, note) always.
    grid_restriction attribution will use PF_unrestricted vs PF_grid_feasible
    from reports.bess_asset_daily_attribution if available; otherwise None.
    """
    return None, [
        "curtailment_flags: no curtailment table yet — "
        "grid_restriction bucket derived from existing DB scenarios if available"
    ]


# ---------------------------------------------------------------------------
# Pre-computed P&L from reports schema (for nominated / actual from DB)
# ---------------------------------------------------------------------------

def load_precomputed_scenario_pnl(
    asset_code: str,
    date_from: date,
    date_to: date,
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Load pre-computed scenario P&L from reports.bess_asset_daily_scenario_pnl.

    Columns: trade_date, scenario_name, total_revenue_yuan, market_revenue_yuan,
             subsidy_revenue_yuan, discharge_mwh, charge_mwh, scenario_available
    """
    notes: List[str] = []
    sql = """
        SELECT
            trade_date,
            scenario_name,
            total_revenue_yuan,
            market_revenue_yuan,
            subsidy_revenue_yuan,
            discharge_mwh,
            charge_mwh,
            scenario_available
        FROM reports.bess_asset_daily_scenario_pnl
        WHERE asset_code = %(asset_code)s
          AND trade_date >= %(date_from)s
          AND trade_date <= %(date_to)s
        ORDER BY trade_date, scenario_name
    """
    params = {
        "asset_code": asset_code,
        "date_from": date_from,
        "date_to": date_to,
    }
    df, err = _run_query_safe(sql, params)
    if err:
        notes.append(f"precomputed_scenario_pnl: query failed — {err}")
        return pd.DataFrame(), notes
    if df.empty:
        notes.append(
            f"precomputed_scenario_pnl: no rows in reports.bess_asset_daily_scenario_pnl "
            f"for {asset_code} {date_from} to {date_to}"
        )
    return df, notes


# ---------------------------------------------------------------------------
# Inner Mongolia DA cleared energy — marketdata.md_id_cleared_energy
# ---------------------------------------------------------------------------

def load_id_cleared_energy(
    asset_code: str,
    date_from: date,
    date_to: date,
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Load Inner Mongolia day-ahead cleared energy (DA market award).

    Source: marketdata.md_id_cleared_energy
    Scope: Inner Mongolia (Mengxi) assets only — assets with dispatch_unit_name_cn
           in ASSET_ALIAS_MAP.

    IMPORTANT — Semantic distinction
    ---------------------------------
    This table records TRADING CLEARED ENERGY — the volume awarded in the DA market.
    It is NOT actual physical dispatch / generation output.
    The cleared amount represents what the market operator cleared; actual physical
    output may differ due to:
      - Asset issues (forced outage, BOP faults)
      - Grid operator real-time re-dispatch or curtailment
      - SOC / ramping constraints not reflected in the DA schedule

    Unit semantics (explicit)
    -------------------------
    cleared_energy_mwh_15min         : energy cleared in each 15-min interval [MWh]
    cleared_power_mw_implied_15min   : implied average power = mwh / 0.25 [MW]
                                       (informational — NOT a physical measurement)
    cleared_price                    : CNY/MWh DA cleared price for this interval

    DB columns: datetime, plant_name, dispatch_unit_name, energy_mwh,
                cleared_energy_mwh, cleared_price, data_date

    Returns
    -------
    df    : columns [datetime, dispatch_unit_name, cleared_energy_mwh_15min,
                     cleared_power_mw_implied_15min, cleared_price]
    notes : data quality / warning strings
    """
    from apps.trading.bess.mengxi.pnl_attribution.calc import ASSET_ALIAS_MAP

    notes: List[str] = []
    alias = ASSET_ALIAS_MAP.get(asset_code, {})
    dispatch_unit_name = alias.get("dispatch_unit_name_cn")

    if not dispatch_unit_name:
        notes.append(
            f"id_cleared_energy: no dispatch_unit_name_cn mapping for {asset_code!r} in "
            "ASSET_ALIAS_MAP — cannot query marketdata.md_id_cleared_energy"
        )
        return pd.DataFrame(columns=[
            "datetime", "dispatch_unit_name", "cleared_energy_mwh_15min",
            "cleared_power_mw_implied_15min", "cleared_price",
        ]), notes

    sql = """
        SELECT
            datetime,
            dispatch_unit_name,
            cleared_energy_mwh   AS cleared_energy_mwh_15min,
            cleared_price
        FROM marketdata.md_id_cleared_energy
        WHERE dispatch_unit_name = %(dispatch_unit_name)s
          AND data_date >= %(date_from)s
          AND data_date <= %(date_to)s
        ORDER BY datetime
    """
    params = {
        "dispatch_unit_name": dispatch_unit_name,
        "date_from": date_from,
        "date_to": date_to,
    }
    df, err = _run_query_safe(sql, params)
    if err:
        notes.append(f"id_cleared_energy: query failed — {err}")
        return pd.DataFrame(columns=[
            "datetime", "dispatch_unit_name", "cleared_energy_mwh_15min",
            "cleared_power_mw_implied_15min", "cleared_price",
        ]), notes

    if df.empty:
        notes.append(
            f"id_cleared_energy: no DA cleared energy data for {asset_code} "
            f"(dispatch_unit={dispatch_unit_name!r}) {date_from} to {date_to} — "
            "gap between cleared_energy and actual_dispatch cannot be computed"
        )
        return pd.DataFrame(columns=[
            "datetime", "dispatch_unit_name", "cleared_energy_mwh_15min",
            "cleared_power_mw_implied_15min", "cleared_price",
        ]), notes

    df["datetime"] = pd.to_datetime(df["datetime"])
    df["cleared_energy_mwh_15min"] = pd.to_numeric(df["cleared_energy_mwh_15min"], errors="coerce")
    df["cleared_price"] = pd.to_numeric(df["cleared_price"], errors="coerce")

    # Explicit unit derivation: implied power = energy / interval_hours
    # This is informational — NOT a physical power measurement
    df["cleared_power_mw_implied_15min"] = df["cleared_energy_mwh_15min"] / 0.25

    notes.append(
        "id_cleared_energy: loaded from marketdata.md_id_cleared_energy — "
        "DA market-cleared trading energy, NOT actual physical dispatch; "
        "cleared_power_mw_implied_15min = cleared_energy_mwh_15min / 0.25 (implied, not measured)"
    )
    return df[[
        "datetime", "dispatch_unit_name", "cleared_energy_mwh_15min",
        "cleared_power_mw_implied_15min", "cleared_price",
    ]], notes


def load_precomputed_attribution(
    asset_code: str,
    date_from: date,
    date_to: date,
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Load pre-computed attribution rows from reports.bess_asset_daily_attribution.
    """
    notes: List[str] = []
    sql = """
        SELECT *
        FROM reports.bess_asset_daily_attribution
        WHERE asset_code = %(asset_code)s
          AND trade_date >= %(date_from)s
          AND trade_date <= %(date_to)s
        ORDER BY trade_date
    """
    params = {
        "asset_code": asset_code,
        "date_from": date_from,
        "date_to": date_to,
    }
    df, err = _run_query_safe(sql, params)
    if err:
        notes.append(f"precomputed_attribution: query failed — {err}")
        return pd.DataFrame(), notes
    if df.empty:
        notes.append(
            f"precomputed_attribution: no rows in reports.bess_asset_daily_attribution "
            f"for {asset_code} {date_from} to {date_to}"
        )
    return df, notes
