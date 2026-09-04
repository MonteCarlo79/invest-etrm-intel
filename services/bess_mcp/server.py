"""
BESS Data MCP Server
====================
Exposes 10 tools over the Model Context Protocol (MCP) stdio transport so that
Claude Desktop (or any MCP-compatible client) can query BESS data, detect gaps,
and trigger ETL / LP batch runs.

Usage
-----
Run directly:
    python services/bess_mcp/server.py

Claude Desktop config (add to claude_desktop_config.json):
    {
      "mcpServers": {
        "bess-data": {
          "command": "python",
          "args": ["C:/path/to/bess-platform/services/bess_mcp/server.py"],
          "env": {
            "PGURL": "postgresql://user:pass@host:5432/db?sslmode=require"
          }
        }
      }
    }

Requirements:
    pip install mcp>=1.0
"""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO))

# Load .env so the server can find PGURL when launched by Claude Desktop
try:
    from dotenv import load_dotenv
    for _env in [_REPO / "config" / ".env", _REPO / ".env"]:
        if _env.exists():
            load_dotenv(_env)
            break
except ImportError:
    pass

from mcp.server.fastmcp import FastMCP  # pip install mcp

from services.bess_mcp.tools import (
    bess_check_data_completeness,
    bess_list_price_gaps,
    bess_list_ops_dispatch_gaps,
    bess_list_lp_gaps,
    bess_run_canon_etl,
    bess_run_lp_batch,
    bess_get_portfolio_pnl,
    bess_get_dispatch_series,
    bess_get_platform_docs,
    bess_get_data_quality_report,
)

mcp = FastMCP(
    "bess-data",
    instructions=(
        "You have access to BESS trading data for the 4 Inner Mongolia (Mengxi) assets: "
        "suyou, hangjinqi, siziwangqi, gushanliang. "
        "Data sources: RT nodal prices (canon.nodal_rt_price_15min), ops dispatch from "
        "Excel files (marketdata.ops_bess_dispatch_15min), LP pre-computed results "
        "(reports.bess_strategy_dispatch_15min + bess_asset_daily_scenario_pnl), and "
        "ID-cleared energy (marketdata.md_id_cleared_energy). "
        "Use bess_get_data_quality_report to audit gaps across all data layers. "
        "Use bess_run_canon_etl and bess_run_lp_batch to fill gaps. "
        "Use bess_get_platform_docs to read the platform architecture docs. "
        "Prices are in CNY/MWh. Dispatch is in MW (positive=discharge, negative=charge)."
    ),
)


@mcp.tool()
def bess_check_completeness(
    asset_codes: list[str] | None,
    start_date: str,
    end_date: str,
) -> dict:
    """
    Build a per-asset × per-date coverage matrix across 5 data layers:
    prices, ops_dispatch, lp_pf, lp_forecast, trading_cleared.

    Args:
        asset_codes : list of asset codes, e.g. ["suyou"]. Null = all 4 IM assets.
        start_date  : ISO date, e.g. "2026-03-01"
        end_date    : ISO date, e.g. "2026-04-23"
    """
    return bess_check_data_completeness(asset_codes, start_date, end_date)


@mcp.tool()
def bess_price_gaps(
    asset_code: str,
    start_date: str,
    end_date: str,
) -> dict:
    """
    List dates where canon.nodal_rt_price_15min has no RT price rows for an asset.

    Args:
        asset_code : e.g. "suyou"
        start_date : ISO date
        end_date   : ISO date
    """
    return bess_list_price_gaps(asset_code, start_date, end_date)


@mcp.tool()
def bess_ops_gaps(
    asset_code: str,
    start_date: str,
    end_date: str,
) -> dict:
    """
    List dates where marketdata.ops_bess_dispatch_15min has no rows for an asset
    (Excel ops file not yet ingested for that date).

    Args:
        asset_code : e.g. "suyou"
        start_date : ISO date
        end_date   : ISO date
    """
    return bess_list_ops_dispatch_gaps(asset_code, start_date, end_date)


@mcp.tool()
def bess_lp_gaps(
    asset_codes: list[str] | None,
    start_date: str,
    end_date: str,
) -> dict:
    """
    List dates missing LP pre-computed results (PF and/or forecast) for each asset.

    Args:
        asset_codes : list or null for all 4 IM assets
        start_date  : ISO date
        end_date    : ISO date
    """
    return bess_list_lp_gaps(asset_codes, start_date, end_date)


@mcp.tool()
def bess_canon_etl(
    start_date: str,
    end_date: str,
) -> dict:
    """
    Run the canon RT nodal price ETL (populate_canon_nodal_prices.py).

    Reads cleared_price from marketdata.md_id_cleared_energy for the 4 IM assets
    and upserts into canon.nodal_rt_price_15min_id_cleared, then recreates
    the canon.nodal_rt_price_15min UNION view.

    Args:
        start_date : ISO date, e.g. "2026-04-01"
        end_date   : ISO date, e.g. "2026-04-23"
    """
    return bess_run_canon_etl(start_date, end_date)


@mcp.tool()
def bess_lp_batch(
    asset_codes: list[str] | None,
    start_date: str,
    end_date: str,
    force: bool = False,
) -> dict:
    """
    Run the LP pre-computation batch for the given assets and date range.

    Runs perfect_foresight and forecast_ols_rt_time_v1 LP solves per asset/day
    and persists results to DB. Each asset is run in a separate subprocess
    so one hung asset does not block the others.

    Args:
        asset_codes : list or null for all 4 IM assets
        start_date  : ISO date
        end_date    : ISO date
        force       : if true, re-compute even if DB already has results
    """
    return bess_run_lp_batch(asset_codes, start_date, end_date, force)


@mcp.tool()
def bess_portfolio_pnl(
    asset_codes: list[str] | None,
    start_date: str,
    end_date: str,
) -> dict:
    """
    Retrieve all 5-strategy P&L from reports.bess_asset_daily_scenario_pnl.

    Strategies: perfect_foresight_hourly, forecast_ols_rt_time_v1,
    nominated_dispatch, cleared_actual, trading_cleared.

    Args:
        asset_codes : list or null for all 4 IM assets
        start_date  : ISO date
        end_date    : ISO date
    """
    return bess_get_portfolio_pnl(asset_codes, start_date, end_date)


@mcp.tool()
def bess_dispatch_series(
    asset_code: str,
    trade_date: str,
    scenario_name: str,
) -> dict:
    """
    Retrieve 15-min dispatch time series for one asset / date / scenario.

    Valid scenario_name values:
      perfect_foresight_hourly, forecast_ols_rt_time_v1,
      nominated_dispatch, cleared_actual, trading_cleared

    Args:
        asset_code    : e.g. "suyou"
        trade_date    : ISO date, e.g. "2026-04-17"
        scenario_name : one of the 5 strategies above
    """
    return bess_get_dispatch_series(asset_code, trade_date, scenario_name)


@mcp.tool()
def bess_platform_docs(
    doc_name: str | None = None,
) -> dict:
    """
    Read platform design documentation from docs/platform-design/.

    Available docs: agent_skills, data_contracts, db_spot_market,
    decision_modules, implementation_design, platform_roadmap,
    platform_skills, ui_china_geo_map, ui_data_management_tab.

    Args:
        doc_name : filename without .md extension, e.g. "data_contracts".
                   Omit to list all available docs.
    """
    return bess_get_platform_docs(doc_name)


@mcp.tool()
def bess_data_quality_report(
    asset_codes: list[str] | None,
    start_date: str,
    end_date: str,
) -> dict:
    """
    Comprehensive data quality report with gap counts and recommendations.

    Combines checks for RT prices, ops dispatch, LP results (PF + forecast),
    and trading-cleared energy across all requested assets and dates.

    Args:
        asset_codes : list or null for all 4 IM assets
        start_date  : ISO date
        end_date    : ISO date
    """
    return bess_get_data_quality_report(asset_codes, start_date, end_date)


if __name__ == "__main__":
    mcp.run(transport="stdio")
