"""
services/bess_mcp/tools.py

Dual-use tool implementations for the BESS data MCP server and the
BessDataAnalystAgent.

Each function is a pure callable that queries the DB or triggers ETL/LP
batch subprocesses.  Return values are always JSON-serialisable.

Used by:
  - services/bess_mcp/server.py   (MCP stdio server — Claude Desktop)
  - libs/decision_models/adapters/agent/data_analyst_agent.py  (in-process)

Data sources queried
--------------------
  canon.nodal_rt_price_15min            — 15-min RT nodal prices (4 IM assets)
  marketdata.ops_bess_dispatch_15min    — ops Excel ingestion (nominated/actual)
  marketdata.md_id_cleared_energy       — intraday cleared energy + price
  reports.bess_asset_daily_scenario_pnl — LP + ops P&L per scenario
  reports.bess_strategy_dispatch_15min  — LP dispatch time series

Platform design context
-----------------------
See docs/platform-design/ for architecture, data contracts and agent skills.
Use bess_get_platform_docs() to retrieve any of those docs from within an agent.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

_REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO))

_log = logging.getLogger(__name__)

# The 4 actively managed Inner Mongolia BESS assets
_IM_ASSETS = ["suyou", "hangjinqi", "siziwangqi", "gushanliang"]

# Scenario names we track in reports.bess_asset_daily_scenario_pnl
_LP_SCENARIOS   = ["perfect_foresight_hourly", "forecast_ols_rt_time_v1"]
_OPS_SCENARIOS  = ["nominated_dispatch", "cleared_actual", "trading_cleared"]
_ALL_SCENARIOS  = _LP_SCENARIOS + _OPS_SCENARIOS

# Platform design docs directory
_DOCS_DIR = _REPO / "docs" / "platform-design"


# ── JSON helpers ──────────────────────────────────────────────────────────────

def _serial(obj: Any) -> Any:
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Not JSON-serialisable: {type(obj)}")


def _jsonify(rows: list[dict]) -> list[dict]:
    return json.loads(json.dumps(rows, default=_serial))


# ── DB engine helper ─────────────────────────────────────────────────────────

def _get_engine():
    from services.common.db_utils import get_engine
    return get_engine()


# ── Tool 1: bess_check_data_completeness ─────────────────────────────────────

def bess_check_data_completeness(
    asset_codes: list[str] | None,
    start_date: str,
    end_date: str,
) -> dict:
    """
    Build a coverage matrix: for each asset × date, which data layers are present.

    Checks five data layers:
      prices        — canon.nodal_rt_price_15min has at least one row
      ops_dispatch  — marketdata.ops_bess_dispatch_15min has at least one row
      lp_pf         — reports.bess_strategy_dispatch_15min has perfect_foresight_hourly
      lp_forecast   — reports.bess_strategy_dispatch_15min has forecast_ols_rt_time_v1
      trading_cleared — marketdata.md_id_cleared_energy has at least one row

    Args:
        asset_codes : list of asset codes, e.g. ["suyou", "hangjinqi"].
                      Pass null/omit for all 4 IM assets.
        start_date  : ISO date string, e.g. "2026-03-01"
        end_date    : ISO date string, e.g. "2026-04-23"

    Returns:
        {
          "matrix": {
            "suyou": {
              "2026-04-17": {
                "prices": true, "ops_dispatch": true,
                "lp_pf": true, "lp_forecast": false, "trading_cleared": true
              }, ...
            }, ...
          },
          "summary": {
            "suyou": {"dates_checked": 30, "prices_ok": 28, "ops_ok": 25, ...}
          },
          "missing_any": [{"asset": "suyou", "date": "2026-04-01", "missing": ["lp_forecast"]}]
        }
    """
    assets = asset_codes or _IM_ASSETS
    engine = _get_engine()

    from sqlalchemy import text
    matrix: dict = {a: {} for a in assets}

    with engine.connect() as conn:
        # ── prices: one query per asset ──────────────────────────────────────
        for asset in assets:
            rows = conn.execute(text("""
                SELECT data_date::text AS d
                FROM (
                    SELECT generate_series(:s::date, :e::date, '1 day'::interval)::date AS data_date
                ) dates
                WHERE EXISTS (
                    SELECT 1 FROM canon.nodal_rt_price_15min
                    WHERE asset_code = :asset
                      AND time >= (data_date AT TIME ZONE 'Asia/Shanghai')
                      AND time <  ((data_date + 1) AT TIME ZONE 'Asia/Shanghai')
                )
            """), {"s": start_date, "e": end_date, "asset": asset}).fetchall()
            dates_with_prices = {r[0] for r in rows}

            # ── ops_dispatch ─────────────────────────────────────────────────
            rows2 = conn.execute(text("""
                SELECT data_date::text AS d
                FROM (
                    SELECT generate_series(:s::date, :e::date, '1 day'::interval)::date AS data_date
                ) dates
                WHERE EXISTS (
                    SELECT 1 FROM marketdata.ops_bess_dispatch_15min
                    WHERE asset_code = :asset
                      AND interval_start >= (data_date AT TIME ZONE 'Asia/Shanghai')
                      AND interval_start <  ((data_date + 1) AT TIME ZONE 'Asia/Shanghai')
                )
            """), {"s": start_date, "e": end_date, "asset": asset}).fetchall()
            dates_with_ops = {r[0] for r in rows2}

            # ── lp_pf ────────────────────────────────────────────────────────
            rows3 = conn.execute(text("""
                SELECT trade_date::text AS d
                FROM reports.bess_strategy_dispatch_15min
                WHERE asset_code   = :asset
                  AND scenario_name = 'perfect_foresight_hourly'
                  AND trade_date   BETWEEN :s AND :e
                GROUP BY trade_date
            """), {"s": start_date, "e": end_date, "asset": asset}).fetchall()
            dates_with_lp_pf = {r[0] for r in rows3}

            # ── lp_forecast ──────────────────────────────────────────────────
            rows4 = conn.execute(text("""
                SELECT trade_date::text AS d
                FROM reports.bess_strategy_dispatch_15min
                WHERE asset_code   = :asset
                  AND scenario_name = 'forecast_ols_rt_time_v1'
                  AND trade_date   BETWEEN :s AND :e
                GROUP BY trade_date
            """), {"s": start_date, "e": end_date, "asset": asset}).fetchall()
            dates_with_lp_fc = {r[0] for r in rows4}

            # ── trading_cleared (md_id_cleared_energy) ───────────────────────
            # Map asset_code → dispatch_unit_name
            _UNIT_MAP = {
                "suyou":       "景蓝乌尔图储能电站",
                "hangjinqi":   "悦杭独贵储能电站",
                "siziwangqi":  "景通四益堂储能电站",
                "gushanliang": "裕昭沙子坝储能电站",
            }
            unit_name = _UNIT_MAP.get(asset)
            if unit_name:
                rows5 = conn.execute(text("""
                    SELECT data_date::text AS d
                    FROM marketdata.md_id_cleared_energy
                    WHERE dispatch_unit_name = :unit
                      AND data_date BETWEEN :s AND :e
                      AND cleared_price IS NOT NULL
                    GROUP BY data_date
                """), {"s": start_date, "e": end_date, "unit": unit_name}).fetchall()
                dates_with_tc = {r[0] for r in rows5}
            else:
                dates_with_tc = set()

            # Build per-date matrix for this asset
            from datetime import timedelta
            d_cur = date.fromisoformat(start_date)
            d_end = date.fromisoformat(end_date)
            while d_cur <= d_end:
                ds = d_cur.isoformat()
                matrix[asset][ds] = {
                    "prices":          ds in dates_with_prices,
                    "ops_dispatch":    ds in dates_with_ops,
                    "lp_pf":           ds in dates_with_lp_pf,
                    "lp_forecast":     ds in dates_with_lp_fc,
                    "trading_cleared": ds in dates_with_tc,
                }
                d_cur += timedelta(days=1)

    # Build summary + missing_any
    summary: dict = {}
    missing_any: list[dict] = []
    for asset, dates in matrix.items():
        n = len(dates)
        summary[asset] = {
            "dates_checked":    n,
            "prices_ok":        sum(v["prices"] for v in dates.values()),
            "ops_ok":           sum(v["ops_dispatch"] for v in dates.values()),
            "lp_pf_ok":         sum(v["lp_pf"] for v in dates.values()),
            "lp_forecast_ok":   sum(v["lp_forecast"] for v in dates.values()),
            "trading_cleared_ok": sum(v["trading_cleared"] for v in dates.values()),
        }
        for ds, flags in dates.items():
            missing = [k for k, v in flags.items() if not v]
            if missing:
                missing_any.append({"asset": asset, "date": ds, "missing": missing})

    return {"matrix": matrix, "summary": summary, "missing_any": missing_any}


# ── Tool 2: bess_list_price_gaps ──────────────────────────────────────────────

def bess_list_price_gaps(
    asset_code: str,
    start_date: str,
    end_date: str,
) -> dict:
    """
    List dates where canon.nodal_rt_price_15min has no rows for the asset.

    Args:
        asset_code  : e.g. "suyou"
        start_date  : ISO date string
        end_date    : ISO date string

    Returns:
        {"asset_code": str, "gaps": [date_str, ...], "count": int,
         "total_dates_checked": int}
    """
    engine = _get_engine()
    from sqlalchemy import text

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT data_date::text AS d
            FROM (
                SELECT generate_series(:s::date, :e::date, '1 day'::interval)::date AS data_date
            ) dates
            WHERE NOT EXISTS (
                SELECT 1 FROM canon.nodal_rt_price_15min
                WHERE asset_code = :asset
                  AND time >= (data_date AT TIME ZONE 'Asia/Shanghai')
                  AND time <  ((data_date + 1) AT TIME ZONE 'Asia/Shanghai')
            )
            ORDER BY data_date
        """), {"s": start_date, "e": end_date, "asset": asset_code}).fetchall()

    gaps = [r[0] for r in rows]

    from datetime import timedelta
    d_cur = date.fromisoformat(start_date)
    d_end = date.fromisoformat(end_date)
    total = (d_end - d_cur).days + 1

    return {"asset_code": asset_code, "gaps": gaps, "count": len(gaps),
            "total_dates_checked": total}


# ── Tool 3: bess_list_ops_dispatch_gaps ───────────────────────────────────────

def bess_list_ops_dispatch_gaps(
    asset_code: str,
    start_date: str,
    end_date: str,
) -> dict:
    """
    List dates where marketdata.ops_bess_dispatch_15min has no rows for the asset.

    This indicates an Excel ops file for that date has not yet been ingested.

    Args:
        asset_code  : e.g. "suyou"
        start_date  : ISO date string
        end_date    : ISO date string

    Returns:
        {"asset_code": str, "gaps": [date_str, ...], "count": int,
         "total_dates_checked": int}
    """
    engine = _get_engine()
    from sqlalchemy import text

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT data_date::text AS d
            FROM (
                SELECT generate_series(:s::date, :e::date, '1 day'::interval)::date AS data_date
            ) dates
            WHERE NOT EXISTS (
                SELECT 1 FROM marketdata.ops_bess_dispatch_15min
                WHERE asset_code = :asset
                  AND interval_start >= (data_date AT TIME ZONE 'Asia/Shanghai')
                  AND interval_start <  ((data_date + 1) AT TIME ZONE 'Asia/Shanghai')
            )
            ORDER BY data_date
        """), {"s": start_date, "e": end_date, "asset": asset_code}).fetchall()

    gaps = [r[0] for r in rows]
    from datetime import timedelta
    total = (date.fromisoformat(end_date) - date.fromisoformat(start_date)).days + 1
    return {"asset_code": asset_code, "gaps": gaps, "count": len(gaps),
            "total_dates_checked": total}


# ── Tool 4: bess_list_lp_gaps ─────────────────────────────────────────────────

def bess_list_lp_gaps(
    asset_codes: list[str] | None,
    start_date: str,
    end_date: str,
) -> dict:
    """
    List dates missing LP pre-computed results for each asset.

    Checks both perfect_foresight_hourly and forecast_ols_rt_time_v1 in
    reports.bess_strategy_dispatch_15min.

    Args:
        asset_codes : list of asset codes or null for all 4 IM assets
        start_date  : ISO date string
        end_date    : ISO date string

    Returns:
        {
          "suyou": {"pf_gaps": [...], "forecast_gaps": [...]},
          "hangjinqi": {...}, ...
        }
    """
    assets = asset_codes or _IM_ASSETS
    engine = _get_engine()
    from sqlalchemy import text
    from datetime import timedelta

    result: dict = {}
    with engine.connect() as conn:
        for asset in assets:
            # dates where PF dispatch exists
            pf_rows = conn.execute(text("""
                SELECT DISTINCT trade_date::text AS d
                FROM reports.bess_strategy_dispatch_15min
                WHERE asset_code = :asset
                  AND scenario_name = 'perfect_foresight_hourly'
                  AND trade_date BETWEEN :s AND :e
            """), {"s": start_date, "e": end_date, "asset": asset}).fetchall()
            pf_dates = {r[0] for r in pf_rows}

            fc_rows = conn.execute(text("""
                SELECT DISTINCT trade_date::text AS d
                FROM reports.bess_strategy_dispatch_15min
                WHERE asset_code = :asset
                  AND scenario_name = 'forecast_ols_rt_time_v1'
                  AND trade_date BETWEEN :s AND :e
            """), {"s": start_date, "e": end_date, "asset": asset}).fetchall()
            fc_dates = {r[0] for r in fc_rows}

            d_cur = date.fromisoformat(start_date)
            d_end = date.fromisoformat(end_date)
            all_dates = []
            while d_cur <= d_end:
                all_dates.append(d_cur.isoformat())
                d_cur += timedelta(days=1)

            result[asset] = {
                "pf_gaps":       sorted(d for d in all_dates if d not in pf_dates),
                "forecast_gaps": sorted(d for d in all_dates if d not in fc_dates),
            }
    return result


# ── Tool 5: bess_run_canon_etl ────────────────────────────────────────────────

def bess_run_canon_etl(
    start_date: str,
    end_date: str,
) -> dict:
    """
    Run the canon nodal price ETL for the given date range.

    Executes services/data_ingestion/populate_canon_nodal_prices.py, which:
      1. Reads md_id_cleared_energy.cleared_price for the 4 IM assets
      2. Upserts into canon.nodal_rt_price_15min_id_cleared
      3. Recreates the canon.nodal_rt_price_15min UNION view

    Args:
        start_date : ISO date string, e.g. "2026-04-01"
        end_date   : ISO date string, e.g. "2026-04-23"

    Returns:
        {"returncode": int, "stdout": str, "stderr": str, "command": str}
    """
    script = str(_REPO / "services" / "data_ingestion" / "populate_canon_nodal_prices.py")
    cmd = [sys.executable, script, "--start-date", start_date, "--end-date", end_date]
    _log.info("bess_run_canon_etl: %s", " ".join(cmd))
    proc = subprocess.run(cmd, capture_output=True, text=True, cwd=str(_REPO))
    return {
        "returncode": proc.returncode,
        "stdout": proc.stdout[-4000:],   # truncate for MCP response size
        "stderr": proc.stderr[-4000:],
        "command": " ".join(cmd),
    }


# ── Tool 6: bess_run_lp_batch ─────────────────────────────────────────────────

def bess_run_lp_batch(
    asset_codes: list[str] | None,
    start_date: str,
    end_date: str,
    force: bool = False,
) -> dict:
    """
    Run the LP pre-computation batch for the given assets and date range.

    Executes services/decision_models/run_daily_strategy_batch.py for each
    asset individually (safer than all-4 in one process — one hung asset
    won't block the others).

    Results are written to:
      reports.bess_asset_daily_scenario_pnl
      reports.bess_strategy_dispatch_15min

    Args:
        asset_codes : list of asset codes or null for all 4 IM assets
        start_date  : ISO date string
        end_date    : ISO date string
        force       : if true, re-compute even if DB already has results

    Returns:
        {"results": [{"asset": str, "returncode": int, "stdout": str, "stderr": str}]}
    """
    assets = asset_codes or _IM_ASSETS
    from datetime import timedelta
    d_start = date.fromisoformat(start_date)
    d_end   = date.fromisoformat(end_date)
    lookback = (d_end - d_start).days + 1

    script = str(_REPO / "services" / "decision_models" / "run_daily_strategy_batch.py")
    results = []
    for asset in assets:
        cmd = [
            sys.executable, "-m", "services.decision_models.run_daily_strategy_batch",
            "--date",     end_date,
            "--lookback", str(lookback),
            "--asset",    asset,
        ]
        if force:
            cmd.append("--force")
        _log.info("bess_run_lp_batch: %s", " ".join(cmd))
        proc = subprocess.run(cmd, capture_output=True, text=True, cwd=str(_REPO))
        results.append({
            "asset":      asset,
            "returncode": proc.returncode,
            "stdout":     proc.stdout[-3000:],
            "stderr":     proc.stderr[-3000:],
        })
    return {"results": results}


# ── Tool 7: bess_get_portfolio_pnl ────────────────────────────────────────────

def bess_get_portfolio_pnl(
    asset_codes: list[str] | None,
    start_date: str,
    end_date: str,
) -> dict:
    """
    Retrieve all 5-strategy P&L from reports.bess_asset_daily_scenario_pnl.

    The 5 strategies:
      perfect_foresight_hourly  — LP on actual RT prices (upper bound benchmark)
      forecast_ols_rt_time_v1   — LP on forecasted RT prices
      nominated_dispatch        — ops nominated dispatch × nodal price (Excel)
      cleared_actual            — ops actual dispatch × nodal price (Excel)
      trading_cleared           — id_cleared_energy × cleared_price (md_id_cleared_energy)

    Args:
        asset_codes : list of asset codes or null for all 4 IM assets
        start_date  : ISO date string
        end_date    : ISO date string

    Returns:
        {"rows": [...], "count": int}
        Each row: asset_code, trade_date, scenario_name, total_pnl,
                  market_revenue, compensation_revenue, discharge_mwh,
                  charge_mwh, avg_daily_cycles, scenario_available, source_system
    """
    assets = asset_codes or _IM_ASSETS
    engine = _get_engine()
    from sqlalchemy import text

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT
                asset_code, trade_date::text, scenario_name,
                total_pnl, market_revenue, compensation_revenue,
                discharge_mwh, charge_mwh, avg_daily_cycles,
                scenario_available, source_system
            FROM reports.bess_asset_daily_scenario_pnl
            WHERE asset_code = ANY(:assets)
              AND trade_date BETWEEN :s AND :e
              AND scenario_name = ANY(:scenarios)
            ORDER BY trade_date, asset_code, scenario_name
        """), {
            "assets":    assets,
            "s":         start_date,
            "e":         end_date,
            "scenarios": _ALL_SCENARIOS,
        }).fetchall()

    cols = ["asset_code", "trade_date", "scenario_name", "total_pnl",
            "market_revenue", "compensation_revenue", "discharge_mwh",
            "charge_mwh", "avg_daily_cycles", "scenario_available", "source_system"]
    result = _jsonify([dict(zip(cols, r)) for r in rows])
    return {"rows": result, "count": len(result)}


# ── Tool 8: bess_get_dispatch_series ──────────────────────────────────────────

def bess_get_dispatch_series(
    asset_code: str,
    trade_date: str,
    scenario_name: str,
) -> dict:
    """
    Retrieve 15-min dispatch time series for one asset / date / scenario.

    Looks in reports.bess_strategy_dispatch_15min first (LP scenarios).
    For ops scenarios (nominated_dispatch, cleared_actual), falls back to
    marketdata.ops_bess_dispatch_15min.

    Args:
        asset_code    : e.g. "suyou"
        trade_date    : ISO date string, e.g. "2026-04-17"
        scenario_name : one of the 5 scenario names

    Returns:
        {
          "asset_code": str, "trade_date": str, "scenario_name": str,
          "intervals": [
            {"time": str, "dispatch_mw": float, "price": float, ...}
          ],
          "count": int, "source": str
        }
    """
    engine = _get_engine()
    from sqlalchemy import text

    # LP scenarios — from reports.bess_strategy_dispatch_15min
    if scenario_name in _LP_SCENARIOS:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT
                    interval_start AT TIME ZONE 'Asia/Shanghai' AS t,
                    dispatch_grid_mw, charge_mw, discharge_mw, soc_mwh, price
                FROM reports.bess_strategy_dispatch_15min
                WHERE asset_code    = :asset
                  AND trade_date    = :td
                  AND scenario_name = :sn
                ORDER BY interval_start
            """), {"asset": asset_code, "td": trade_date, "sn": scenario_name}).fetchall()
        intervals = _jsonify([{
            "time": r[0].isoformat() if hasattr(r[0], "isoformat") else str(r[0]),
            "dispatch_grid_mw": r[1],
            "charge_mw": r[2],
            "discharge_mw": r[3],
            "soc_mwh": r[4],
            "price": r[5],
        } for r in rows])
        return {
            "asset_code": asset_code, "trade_date": trade_date,
            "scenario_name": scenario_name, "intervals": intervals,
            "count": len(intervals),
            "source": "reports.bess_strategy_dispatch_15min",
        }

    # Ops scenarios — from marketdata.ops_bess_dispatch_15min
    if scenario_name in ("nominated_dispatch", "cleared_actual"):
        col = "nominated_dispatch_mw" if scenario_name == "nominated_dispatch" else "actual_dispatch_mw"
        with engine.connect() as conn:
            rows = conn.execute(text(f"""
                SELECT
                    interval_start AT TIME ZONE 'Asia/Shanghai' AS t,
                    {col} AS dispatch_mw_raw,
                    nodal_price_excel AS price
                FROM marketdata.ops_bess_dispatch_15min
                WHERE asset_code = :asset
                  AND interval_start >= (:td::date AT TIME ZONE 'Asia/Shanghai')
                  AND interval_start <  ((:td::date + 1) AT TIME ZONE 'Asia/Shanghai')
                ORDER BY interval_start
            """), {"asset": asset_code, "td": trade_date}).fetchall()
        # Negate to get LP convention (positive = discharge)
        intervals = _jsonify([{
            "time": r[0].isoformat() if hasattr(r[0], "isoformat") else str(r[0]),
            "dispatch_mw": (-r[1] * 0.25) if r[1] is not None else None,
            "price": r[2],
        } for r in rows])
        return {
            "asset_code": asset_code, "trade_date": trade_date,
            "scenario_name": scenario_name, "intervals": intervals,
            "count": len(intervals),
            "source": "marketdata.ops_bess_dispatch_15min",
        }

    # Trading cleared — from marketdata.md_id_cleared_energy
    if scenario_name == "trading_cleared":
        _UNIT_MAP = {
            "suyou":       "景蓝乌尔图储能电站",
            "hangjinqi":   "悦杭独贵储能电站",
            "siziwangqi":  "景通四益堂储能电站",
            "gushanliang": "裕昭沙子坝储能电站",
        }
        unit_name = _UNIT_MAP.get(asset_code)
        if not unit_name:
            return {"asset_code": asset_code, "trade_date": trade_date,
                    "scenario_name": scenario_name, "intervals": [], "count": 0,
                    "source": "not_found"}
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT
                    datetime AT TIME ZONE 'Asia/Shanghai' AS t,
                    cleared_energy_mwh,
                    cleared_price
                FROM marketdata.md_id_cleared_energy
                WHERE dispatch_unit_name = :unit
                  AND data_date = :td
                ORDER BY datetime
            """), {"unit": unit_name, "td": trade_date}).fetchall()
        intervals = _jsonify([{
            "time": r[0].isoformat() if hasattr(r[0], "isoformat") else str(r[0]),
            "cleared_energy_mwh": r[1],
            "cleared_price": r[2],
            "dispatch_mw": (-r[1] * 0.25 / 0.25) if r[1] is not None else None,
        } for r in rows])
        return {
            "asset_code": asset_code, "trade_date": trade_date,
            "scenario_name": scenario_name, "intervals": intervals,
            "count": len(intervals),
            "source": "marketdata.md_id_cleared_energy",
        }

    return {"error": f"Unknown scenario_name: {scenario_name!r}",
            "valid_scenarios": _ALL_SCENARIOS}


# ── Tool 9: bess_get_platform_docs ────────────────────────────────────────────

def bess_get_platform_docs(
    doc_name: str | None = None,
) -> dict:
    """
    Read platform design documentation from docs/platform-design/.

    These docs describe the data contracts, agent skills (AS1–AS8),
    decision modules (M1–M10), DB schema, UI patterns, and implementation plan.

    Args:
        doc_name : filename without .md extension, e.g. "data_contracts".
                   Pass null/omit to list all available docs.

    Returns:
        {"content": str}           when a specific doc is requested
        {"docs": [str, ...]}       when listing all available docs
    """
    if not _DOCS_DIR.exists():
        return {"error": f"docs dir not found: {_DOCS_DIR}", "docs": []}

    if doc_name is None:
        available = sorted(p.stem for p in _DOCS_DIR.glob("*.md"))
        return {
            "docs": available,
            "hint": "Call bess_get_platform_docs with one of these names to read the doc.",
        }

    target = _DOCS_DIR / f"{doc_name}.md"
    if not target.exists():
        available = sorted(p.stem for p in _DOCS_DIR.glob("*.md"))
        return {
            "error": f"Doc {doc_name!r} not found.",
            "available": available,
        }

    return {"doc_name": doc_name, "content": target.read_text(encoding="utf-8")}


# ── Tool 10: bess_get_data_quality_report ─────────────────────────────────────

def bess_get_data_quality_report(
    asset_codes: list[str] | None,
    start_date: str,
    end_date: str,
) -> dict:
    """
    Comprehensive data quality report combining all gap checks.

    Runs bess_check_data_completeness and aggregates the results into a
    human-readable summary with actionable recommendations.

    Args:
        asset_codes : list of asset codes or null for all 4 IM assets
        start_date  : ISO date string
        end_date    : ISO date string

    Returns:
        {
          "summary":         {asset: {ok/gap counts}},
          "price_gaps":      {asset: [date, ...]},
          "ops_gaps":        {asset: [date, ...]},
          "lp_pf_gaps":      {asset: [date, ...]},
          "lp_forecast_gaps": {asset: [date, ...]},
          "trading_cleared_gaps": {asset: [date, ...]},
          "recommendations": [str, ...]
        }
    """
    assets = asset_codes or _IM_ASSETS
    completeness = bess_check_data_completeness(assets, start_date, end_date)
    matrix = completeness["matrix"]
    summary = completeness["summary"]

    price_gaps:   dict = {}
    ops_gaps:     dict = {}
    lp_pf_gaps:   dict = {}
    lp_fc_gaps:   dict = {}
    tc_gaps:      dict = {}

    for asset, dates in matrix.items():
        price_gaps[asset]  = sorted(d for d, v in dates.items() if not v["prices"])
        ops_gaps[asset]    = sorted(d for d, v in dates.items() if not v["ops_dispatch"])
        lp_pf_gaps[asset]  = sorted(d for d, v in dates.items() if not v["lp_pf"])
        lp_fc_gaps[asset]  = sorted(d for d, v in dates.items() if not v["lp_forecast"])
        tc_gaps[asset]     = sorted(d for d, v in dates.items() if not v["trading_cleared"])

    # Build recommendations
    recs: list[str] = []
    any_price_gaps = any(len(v) > 0 for v in price_gaps.values())
    any_lp_gaps    = any(len(v) > 0 for v in lp_pf_gaps.values())
    any_fc_gaps    = any(len(v) > 0 for v in lp_fc_gaps.values())
    any_ops_gaps   = any(len(v) > 0 for v in ops_gaps.values())

    if any_price_gaps:
        recs.append(
            "Run canon ETL to fill RT price gaps: "
            f"bess_run_canon_etl('{start_date}', '{end_date}')"
        )
    if any_lp_gaps or any_fc_gaps:
        missing_assets = sorted({
            a for a in assets
            if lp_pf_gaps.get(a) or lp_fc_gaps.get(a)
        })
        recs.append(
            f"Run LP batch for assets {missing_assets} with --force: "
            f"bess_run_lp_batch({missing_assets}, '{start_date}', '{end_date}', force=True)"
        )
    if any_ops_gaps:
        recs.append(
            "Ops dispatch gaps indicate Excel files not yet ingested. "
            "Upload missing ops Excel files via the Data Management UI."
        )
    if not recs:
        recs.append("All data layers are complete for the requested period.")

    return {
        "period": {"start_date": start_date, "end_date": end_date},
        "assets_checked": assets,
        "summary": summary,
        "price_gaps":          price_gaps,
        "ops_gaps":            ops_gaps,
        "lp_pf_gaps":          lp_pf_gaps,
        "lp_forecast_gaps":    lp_fc_gaps,
        "trading_cleared_gaps": tc_gaps,
        "recommendations": recs,
    }
