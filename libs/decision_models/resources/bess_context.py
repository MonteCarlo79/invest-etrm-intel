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
    # 4-hour assets: cap PF LP at 2 full cycles/day to prevent unrealistic churning.
    # The LP with compensation=350 CNY/MWh would otherwise do many short (~50% DOD)
    # cycles because every MWh discharged earns the subsidy regardless of cycle depth.
    "suyou":       {"power_mw": 100.0, "duration_h": 4.0, "roundtrip_eff": 0.85, "max_cycles_per_day": 2.0},
    "wulate":      {"power_mw": 100.0, "duration_h": 2.0, "roundtrip_eff": 0.85},
    "wuhai":       {"power_mw": 100.0, "duration_h": 2.0, "roundtrip_eff": 0.85},
    "wulanchabu":  {"power_mw": 100.0, "duration_h": 2.0, "roundtrip_eff": 0.85},
    "hetao":       {"power_mw": 100.0, "duration_h": 2.0, "roundtrip_eff": 0.85},
    "hangjinqi":   {"power_mw": 100.0, "duration_h": 4.0, "roundtrip_eff": 0.85, "max_cycles_per_day": 2.0},
    "siziwangqi":  {"power_mw": 100.0, "duration_h": 4.0, "roundtrip_eff": 0.85, "max_cycles_per_day": 2.0},
    "gushanliang": {"power_mw": 500.0, "duration_h": 4.0, "roundtrip_eff": 0.85, "max_cycles_per_day": 2.0},
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
    # Localise boundaries to CST (Asia/Shanghai) so that psycopg2 sends a
    # TZ-aware parameter and PostgreSQL compares against the TIMESTAMPTZ column
    # in the correct timezone.  Without this, naive pd.Timestamp is treated as
    # UTC, shifting the returned price window 8 h forward relative to the CST
    # dispatch data.
    params = {
        "asset_code": asset_code,
        "start_ts": pd.Timestamp(date_from).tz_localize("Asia/Shanghai"),
        "end_ts": (pd.Timestamp(date_to) + pd.Timedelta(days=1)).tz_localize("Asia/Shanghai"),
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
        "max_cycles_per_day": physical.get("max_cycles_per_day"),
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
            total_pnl               AS total_revenue_yuan,
            market_revenue          AS market_revenue_yuan,
            compensation_revenue    AS subsidy_revenue_yuan,
            discharge_mwh,
            charge_mwh,
            COALESCE(scenario_available, TRUE) AS scenario_available
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

    # DB column cleared_energy_mwh stores hourly-equivalent energy using the
    # ops-file sign convention (positive = charge, negative = discharge).
    # 1. Convert to per-15-min-interval energy: × 0.25 h.
    # 2. Negate so the result follows LP convention (positive = discharge,
    #    negative = charge) used by all downstream P&L and stats calculations.
    df["cleared_energy_mwh_15min"] = -df["cleared_energy_mwh_15min"] * 0.25
    # Implied power = per-15-min energy / 0.25 h = MW (positive = discharge)
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


def load_ops_dispatch_15min(
    asset_code: str,
    date_from: date,
    date_to: date,
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Load 15-min ops dispatch from marketdata.ops_bess_dispatch_15min.

    This is the Inner Mongolia operations ingestion pipeline's output —
    data parsed directly from the daily Excel operations files.

    IMPORTANT — Semantic distinctions
    ----------------------------------
    nominated_dispatch_mw : 申报曲线 (BESS operator nomination to grid operator).
                            NOT the same as md_id_cleared_energy.cleared_energy_mwh.
    actual_dispatch_mw    : 实际充放曲线 (physical output as reported in ops file).
                            NOT the same as md_id_cleared_energy.cleared_energy_mwh.
    nodal_price_excel     : 节点电价 from Excel col E.  May differ from
                            canon.nodal_rt_price_15min (DB source of truth).

    Returns
    -------
    df    : columns [interval_start (datetime), nominated_dispatch_mw (float),
                     actual_dispatch_mw (float), nodal_price_excel (float)]
    notes : list of data quality / warning strings
    """
    notes: List[str] = []
    sql = """
        SELECT
            interval_start,
            nominated_dispatch_mw,
            actual_dispatch_mw,
            nodal_price_excel
        FROM marketdata.ops_bess_dispatch_15min
        WHERE asset_code = %(asset_code)s
          AND data_date >= %(date_from)s
          AND data_date <= %(date_to)s
        ORDER BY interval_start
    """
    params = {
        "asset_code": asset_code,
        "date_from": date_from,
        "date_to": date_to,
    }
    df, err = _run_query_safe(sql, params)
    if err:
        notes.append(f"ops_dispatch_15min: query failed — {err}")
        return pd.DataFrame(columns=[
            "interval_start", "nominated_dispatch_mw", "actual_dispatch_mw", "nodal_price_excel",
        ]), notes
    if df.empty:
        notes.append(
            f"ops_dispatch_15min: no data for {asset_code} "
            f"between {date_from} and {date_to} in marketdata.ops_bess_dispatch_15min "
            "— ops ingestion may not have run for this date range yet"
        )
        return pd.DataFrame(columns=[
            "interval_start", "nominated_dispatch_mw", "actual_dispatch_mw", "nodal_price_excel",
        ]), notes

    df["interval_start"] = pd.to_datetime(df["interval_start"])
    df["nominated_dispatch_mw"] = pd.to_numeric(df["nominated_dispatch_mw"], errors="coerce")
    df["actual_dispatch_mw"] = pd.to_numeric(df["actual_dispatch_mw"], errors="coerce")
    df["nodal_price_excel"] = pd.to_numeric(df["nodal_price_excel"], errors="coerce")

    n_nom_null = df["nominated_dispatch_mw"].isna().sum()
    n_act_null = df["actual_dispatch_mw"].isna().sum()
    if n_nom_null > 0:
        notes.append(
            f"ops_dispatch_15min: {n_nom_null} null nominated_dispatch_mw values for {asset_code}"
        )
    if n_act_null > 0:
        notes.append(
            f"ops_dispatch_15min: {n_act_null} null actual_dispatch_mw values for {asset_code}"
        )
    notes.append(
        f"ops_dispatch_15min: {len(df)} rows loaded from marketdata.ops_bess_dispatch_15min "
        f"for {asset_code} {date_from} to {date_to} — "
        "nominated_dispatch_mw is the operator nomination (NOT market-cleared energy); "
        "actual_dispatch_mw is physical output (NOT market-cleared energy)"
    )
    return df, notes


# ---------------------------------------------------------------------------
# Forecast prices — marketdata.hist_mengxi_*_forecast_15min
# ---------------------------------------------------------------------------

# Asset → forecast table mapping (known tables only; extend as more are confirmed)
_FORECAST_TABLE_MAP: dict = {
    "suyou":  "hist_mengxi_suyou_forecast_15min",
    "wulate": "hist_mengxi_wulate_forecast_15min",
}


def load_forecast_prices_15min(
    asset_code: str,
    date_from: date,
    date_to: date,
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Load 15-min forecast prices for assets that have a forecast table.

    Source: marketdata.hist_mengxi_{asset}_forecast_15min
    Currently known: suyou, wulate.

    Returns
    -------
    df    : columns [time (datetime), price (float)]
    notes : list of data quality / warning strings
    """
    notes: List[str] = []
    table = _FORECAST_TABLE_MAP.get(asset_code)
    if not table:
        notes.append(
            f"forecast_prices_15min: no forecast table configured for {asset_code!r} "
            f"(known assets: {list(_FORECAST_TABLE_MAP.keys())})"
        )
        return pd.DataFrame(columns=["time", "price"]), notes

    sql = f"""
        SELECT time, price
        FROM marketdata.{table}
        WHERE time >= %(start_ts)s
          AND time < %(end_ts)s
        ORDER BY time
    """
    params = {
        "start_ts": pd.Timestamp(date_from),
        "end_ts": pd.Timestamp(date_to) + pd.Timedelta(days=1),
    }
    df, err = _run_query_safe(sql, params)
    if err:
        notes.append(f"forecast_prices_15min: query failed for {asset_code} ({table}) — {err}")
        return pd.DataFrame(columns=["time", "price"]), notes
    if df.empty:
        notes.append(
            f"forecast_prices_15min: no data in marketdata.{table} "
            f"for {date_from} to {date_to}"
        )
        return pd.DataFrame(columns=["time", "price"]), notes

    df["time"] = pd.to_datetime(df["time"])
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    notes.append(f"forecast_prices_15min: {len(df)} rows from marketdata.{table}")
    return df[["time", "price"]], notes


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


# ---------------------------------------------------------------------------
# LP result persistence — write PF / forecast dispatch + P&L to DB
# ---------------------------------------------------------------------------

def _ensure_pnl_columns(engine) -> None:
    """
    Add all columns required by write_lp_results_to_db to
    reports.bess_asset_daily_scenario_pnl.

    The table was created by an early migration with a partial schema; the LP
    pre-compute batch needs additional revenue/metadata columns.  Each ALTER is
    idempotent (IF NOT EXISTS).
    """
    from sqlalchemy import text
    cols = [
        ("market_revenue",      "numeric"),
        ("compensation_revenue","numeric"),
        ("total_pnl",           "numeric"),
        ("discharge_mwh",       "numeric"),
        ("charge_mwh",          "numeric"),
        ("scenario_available",  "boolean DEFAULT true"),
        ("avg_daily_cycles",    "numeric"),
        ("source_system",       "text"),
    ]
    with engine.begin() as conn:
        for col, col_type in cols:
            conn.execute(text(
                f"ALTER TABLE reports.bess_asset_daily_scenario_pnl "
                f"ADD COLUMN IF NOT EXISTS {col} {col_type}"
            ))


def _ensure_dispatch_table(engine) -> None:
    """Create reports.bess_strategy_dispatch_15min if it doesn't exist."""
    from sqlalchemy import text
    ddl = """
        CREATE TABLE IF NOT EXISTS reports.bess_strategy_dispatch_15min (
            trade_date        date            NOT NULL,
            asset_code        text            NOT NULL,
            scenario_name     text            NOT NULL,
            interval_start    timestamptz     NOT NULL,
            dispatch_grid_mw  numeric,
            charge_mw         numeric,
            discharge_mw      numeric,
            soc_mwh           numeric,
            price             numeric,
            created_at        timestamptz     NOT NULL DEFAULT now(),
            updated_at        timestamptz     NOT NULL DEFAULT now(),
            PRIMARY KEY (trade_date, asset_code, scenario_name, interval_start)
        )
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def write_lp_results_to_db(
    asset_code: str,
    trade_date: date,
    pf_result: dict,
    forecast_suite: dict,
    energy_capacity_mwh: Optional[float] = None,
) -> List[str]:
    """
    Persist LP results (P&L summary + dispatch time series) to DB so the UI
    can read them back and skip re-running the CBC solver.

    Writes to:
      reports.bess_asset_daily_scenario_pnl  — P&L summary per scenario
      reports.bess_strategy_dispatch_15min   — full 15-min dispatch time series

    Parameters
    ----------
    asset_code           : e.g. "suyou"
    trade_date           : the trading date
    pf_result            : output of run_perfect_foresight_dispatch()
    forecast_suite       : output of run_forecast_dispatch_suite()
    energy_capacity_mwh  : power_mw × duration_h (for avg_daily_cycles calc)

    Returns
    -------
    List of warning/error notes.
    """
    notes: List[str] = []
    try:
        from services.common.db_utils import get_engine
        from sqlalchemy import text
        engine = get_engine()
    except Exception as exc:
        notes.append(f"write_lp_results_to_db: cannot get engine — {exc}")
        return notes

    try:
        _ensure_pnl_columns(engine)
        _ensure_dispatch_table(engine)
    except Exception as exc:
        notes.append(f"write_lp_results_to_db: cannot ensure tables — {exc}")
        return notes

    # ── Collect scenario records ─────────────────────────────────────────────
    pnl_rows: List[dict] = []
    dispatch_rows: List[dict] = []

    def _collect(scenario_name: str, result: dict) -> None:
        pnl = result.get("pnl", {})
        if not pnl or not pnl.get("n_days_solved", 0):
            return
        disch = float(pnl.get("discharge_mwh") or 0.0)
        cycles = (disch / energy_capacity_mwh) if energy_capacity_mwh else None
        pnl_rows.append({
            "trade_date": trade_date,
            "asset_code": asset_code,
            "scenario_name": scenario_name,
            "scenario_available": True,
            "market_revenue_yuan": float(pnl.get("pnl_market_yuan") or 0.0),
            "subsidy_revenue_yuan": float(pnl.get("pnl_compensation_yuan") or 0.0),
            "total_revenue_yuan": float(pnl.get("pnl_total_yuan") or 0.0),
            "discharge_mwh": disch,
            "charge_mwh": float(pnl.get("charge_mwh") or 0.0),
            "avg_daily_cycles": round(cycles, 4) if cycles is not None else None,
        })
        for rec in result.get("dispatch_hourly", []):
            ts = rec.get("datetime")
            if ts is None:
                continue
            # LP generates CST-naive timestamps (e.g. "2026-04-17T00:00:00").
            # Must localize to Asia/Shanghai before inserting into timestamptz column.
            # Without this, PostgreSQL (session TZ=UTC) treats them as UTC midnight,
            # so when read back they appear 8 hours ahead on the CST dispatch chart.
            _t = pd.Timestamp(ts)
            if _t.tzinfo is None:
                _t = _t.tz_localize("Asia/Shanghai")
            else:
                _t = _t.tz_convert("Asia/Shanghai")
            dispatch_rows.append({
                "trade_date": trade_date,
                "asset_code": asset_code,
                "scenario_name": scenario_name,
                "interval_start": _t.isoformat(),
                "dispatch_grid_mw": rec.get("dispatch_grid_mw"),
                "charge_mw": rec.get("charge_mw"),
                "discharge_mw": rec.get("discharge_mw"),
                "soc_mwh": rec.get("soc_mwh"),
                "price": rec.get("price"),
            })

    # PF
    if pf_result:
        _collect("perfect_foresight_hourly", pf_result)

    # Forecast strategies
    for strat in (forecast_suite or {}).get("strategies", []):
        sname = strat.get("strategy_name") or strat.get("pnl", {}).get("strategy_name")
        if sname:
            _collect(sname, strat)

    if not pnl_rows:
        notes.append(
            f"write_lp_results_to_db: no solved LP results to write for "
            f"{asset_code} {trade_date}"
        )
        return notes

    # ── Upsert P&L summary ────────────────────────────────────────────────────
    # Column names match the actual table schema in reports.bess_asset_daily_scenario_pnl
    # (market_revenue / compensation_revenue / total_pnl — not the _yuan aliases).
    # scenario_available + avg_daily_cycles added by migration 001_add_lp_scenario_columns.sql
    # and also auto-created by _ensure_pnl_columns() above.
    pnl_upsert = text("""
        INSERT INTO reports.bess_asset_daily_scenario_pnl
            (trade_date, asset_code, scenario_name, scenario_available,
             market_revenue, compensation_revenue, total_pnl,
             discharge_mwh, charge_mwh, avg_daily_cycles, source_system, updated_at)
        VALUES
            (:trade_date, :asset_code, :scenario_name, :scenario_available,
             :market_revenue_yuan, :subsidy_revenue_yuan, :total_revenue_yuan,
             :discharge_mwh, :charge_mwh, :avg_daily_cycles, 'lp_precompute', now())
        ON CONFLICT (asset_code, trade_date, scenario_name) DO UPDATE SET
            scenario_available   = EXCLUDED.scenario_available,
            market_revenue       = EXCLUDED.market_revenue,
            compensation_revenue = EXCLUDED.compensation_revenue,
            total_pnl            = EXCLUDED.total_pnl,
            discharge_mwh        = EXCLUDED.discharge_mwh,
            charge_mwh           = EXCLUDED.charge_mwh,
            avg_daily_cycles     = EXCLUDED.avg_daily_cycles,
            source_system        = EXCLUDED.source_system,
            updated_at           = now()
    """)
    try:
        with engine.begin() as conn:
            for row in pnl_rows:
                conn.execute(pnl_upsert, row)
        notes.append(
            f"write_lp_results_to_db: wrote {len(pnl_rows)} P&L row(s) for "
            f"{asset_code} {trade_date}"
        )
    except Exception as exc:
        notes.append(f"write_lp_results_to_db: P&L upsert failed — {exc}")
        return notes

    # ── Upsert dispatch time series ──────────────────────────────────────────
    if not dispatch_rows:
        return notes
    dispatch_upsert = text("""
        INSERT INTO reports.bess_strategy_dispatch_15min
            (trade_date, asset_code, scenario_name, interval_start,
             dispatch_grid_mw, charge_mw, discharge_mw, soc_mwh, price, updated_at)
        VALUES
            (:trade_date, :asset_code, :scenario_name, :interval_start,
             :dispatch_grid_mw, :charge_mw, :discharge_mw, :soc_mwh, :price, now())
        ON CONFLICT (trade_date, asset_code, scenario_name, interval_start) DO UPDATE SET
            dispatch_grid_mw = EXCLUDED.dispatch_grid_mw,
            charge_mw        = EXCLUDED.charge_mw,
            discharge_mw     = EXCLUDED.discharge_mw,
            soc_mwh          = EXCLUDED.soc_mwh,
            price            = EXCLUDED.price,
            updated_at       = now()
    """)
    try:
        with engine.begin() as conn:
            for row in dispatch_rows:
                conn.execute(dispatch_upsert, row)
        notes.append(
            f"write_lp_results_to_db: wrote {len(dispatch_rows)} dispatch row(s) for "
            f"{asset_code} {trade_date}"
        )
    except Exception as exc:
        notes.append(f"write_lp_results_to_db: dispatch upsert failed — {exc}")

    return notes


def load_precomputed_lp_dispatch(
    asset_code: str,
    trade_date: date,
    scenario_name: str,
) -> List[dict]:
    """
    Load pre-computed LP dispatch from reports.bess_strategy_dispatch_15min.

    Returns a list of dicts matching the dispatch_hourly format produced by
    run_perfect_foresight_dispatch():
      {"datetime": str, "dispatch_grid_mw": float, "charge_mw": float,
       "discharge_mw": float, "soc_mwh": float, "price": float}

    Returns [] when the table is empty or the query fails.
    """
    sql = """
        SELECT
            interval_start  AS datetime,
            dispatch_grid_mw,
            charge_mw,
            discharge_mw,
            soc_mwh,
            price
        FROM reports.bess_strategy_dispatch_15min
        WHERE asset_code   = %(asset_code)s
          AND trade_date   = %(trade_date)s
          AND scenario_name = %(scenario_name)s
        ORDER BY interval_start
    """
    params = {
        "asset_code": asset_code,
        "trade_date": trade_date,
        "scenario_name": scenario_name,
    }
    df, err = _run_query_safe(sql, params)
    if err or df.empty:
        return []
    records = []
    for _, row in df.iterrows():
        records.append({
            "datetime": str(row["datetime"]),
            "dispatch_grid_mw": float(row["dispatch_grid_mw"]) if row["dispatch_grid_mw"] is not None else 0.0,
            "charge_mw": float(row["charge_mw"]) if row["charge_mw"] is not None else 0.0,
            "discharge_mw": float(row["discharge_mw"]) if row["discharge_mw"] is not None else 0.0,
            "soc_mwh": float(row["soc_mwh"]) if row["soc_mwh"] is not None else 0.0,
            "price": float(row["price"]) if row["price"] is not None else 0.0,
        })
    return records


def write_ops_pnl_to_db(
    asset_code: str,
    trade_date: date,
    analysis_result: dict,
    energy_capacity_mwh: Optional[float] = None,
) -> List[str]:
    """
    Persist ops-derived scenario P&L to reports.bess_asset_daily_scenario_pnl.

    Writes three scenarios using P&L values from the strategy ranking and
    dispatch MWh from the context:
      nominated_dispatch  — ops nominated dispatch × nodal_price_excel
      cleared_actual      — ops actual dispatch × nodal_price_excel
      trading_cleared     — id_cleared_energy × cleared_price (renamed from
                            id_cleared_energy_da for clarity)

    Called by run_daily_strategy_batch.py after write_lp_results_to_db.

    Parameters
    ----------
    asset_code          : e.g. "suyou"
    trade_date          : the trading date
    analysis_result     : full dict returned by run_bess_daily_strategy_analysis()
    energy_capacity_mwh : power_mw × duration_h (for avg_daily_cycles calc)

    Returns
    -------
    List of warning/error notes.
    """
    notes: List[str] = []

    ranking_rows = analysis_result.get("ranking", {}).get("rows", [])
    context = analysis_result.get("context", {})

    # Build lookup: ranking_name → row dict (only available rows)
    row_by_name = {
        r["strategy_name"]: r
        for r in ranking_rows
        if r.get("data_available") and r.get("pnl_total_yuan") is not None
    }

    def _mwh_from_dispatch(dispatch_records: list) -> tuple:
        """Sum discharge and charge MWh from LP-convention dispatch records.
        Records have dispatch_mw (already in MWh per 15-min, positive=discharge)."""
        if not dispatch_records:
            return 0.0, 0.0
        disch = sum(max(0.0, float(r.get("dispatch_mw") or 0.0)) for r in dispatch_records)
        chrg  = sum(max(0.0, -float(r.get("dispatch_mw") or 0.0)) for r in dispatch_records)
        return disch, chrg

    # (db_scenario_name, source_system, context_dispatch_key, ranking_name)
    _SCENARIO_MAP = [
        ("nominated_dispatch", "ops_excel",         "nominated_dispatch_15min", "nominated_dispatch"),
        ("cleared_actual",     "ops_excel",         "actual_dispatch_15min",    "cleared_actual"),
        ("trading_cleared",    "md_id_cleared_energy", None,                    "id_cleared_energy_da"),
    ]

    pnl_rows: List[dict] = []
    for db_name, source_sys, dispatch_key, ranking_name in _SCENARIO_MAP:
        row = row_by_name.get(ranking_name)
        if row is None:
            continue

        pnl_total  = row.get("pnl_total_yuan")
        pnl_market = row.get("pnl_market_yuan")
        pnl_comp   = row.get("pnl_compensation_yuan")
        if pnl_total is None:
            continue

        # Discharge / charge MWh
        if dispatch_key and context.get(dispatch_key):
            disch, chrg = _mwh_from_dispatch(context[dispatch_key])
        elif db_name == "trading_cleared":
            id_recs = context.get("id_cleared_energy_15min") or []
            disch = sum(max(0.0, float(r.get("cleared_energy_mwh_15min") or 0.0))
                        for r in id_recs)
            chrg  = sum(max(0.0, -float(r.get("cleared_energy_mwh_15min") or 0.0))
                        for r in id_recs)
        else:
            disch, chrg = 0.0, 0.0

        cycles = (disch / energy_capacity_mwh) if energy_capacity_mwh and energy_capacity_mwh > 0 else None
        pnl_rows.append({
            "trade_date":           trade_date,
            "asset_code":           asset_code,
            "scenario_name":        db_name,
            "scenario_available":   True,
            "market_revenue_yuan":  float(pnl_market or 0.0),
            "subsidy_revenue_yuan": float(pnl_comp or 0.0),
            "total_revenue_yuan":   float(pnl_total),
            "discharge_mwh":        disch,
            "charge_mwh":           chrg,
            "avg_daily_cycles":     round(cycles, 4) if cycles is not None else None,
            "source_system":        source_sys,
        })

    if not pnl_rows:
        notes.append(
            f"write_ops_pnl_to_db: no available ops scenarios to write for "
            f"{asset_code} {trade_date}"
        )
        return notes

    try:
        from services.common.db_utils import get_engine
        from sqlalchemy import text
        engine = get_engine()
    except Exception as exc:
        notes.append(f"write_ops_pnl_to_db: cannot get engine — {exc}")
        return notes

    # Same upsert SQL as write_lp_results_to_db
    pnl_upsert = text("""
        INSERT INTO reports.bess_asset_daily_scenario_pnl
            (trade_date, asset_code, scenario_name, scenario_available,
             market_revenue, compensation_revenue, total_pnl,
             discharge_mwh, charge_mwh, avg_daily_cycles, source_system, updated_at)
        VALUES
            (:trade_date, :asset_code, :scenario_name, :scenario_available,
             :market_revenue_yuan, :subsidy_revenue_yuan, :total_revenue_yuan,
             :discharge_mwh, :charge_mwh, :avg_daily_cycles, :source_system, now())
        ON CONFLICT (asset_code, trade_date, scenario_name) DO UPDATE SET
            scenario_available   = EXCLUDED.scenario_available,
            market_revenue       = EXCLUDED.market_revenue,
            compensation_revenue = EXCLUDED.compensation_revenue,
            total_pnl            = EXCLUDED.total_pnl,
            discharge_mwh        = EXCLUDED.discharge_mwh,
            charge_mwh           = EXCLUDED.charge_mwh,
            avg_daily_cycles     = EXCLUDED.avg_daily_cycles,
            source_system        = EXCLUDED.source_system,
            updated_at           = now()
    """)
    try:
        with engine.begin() as conn:
            for row in pnl_rows:
                conn.execute(pnl_upsert, row)
        notes.append(
            f"write_ops_pnl_to_db: wrote {len(pnl_rows)} ops P&L row(s) for "
            f"{asset_code} {trade_date} "
            f"({[r['scenario_name'] for r in pnl_rows]})"
        )
    except Exception as exc:
        notes.append(f"write_ops_pnl_to_db: upsert failed — {exc}")

    return notes
