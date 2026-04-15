"""
libs/decision_models/adapters/agent/tools.py

OpenClaw / Claude API tool definitions for registered decision models.

Each DECISION_MODEL_TOOLS entry can be passed in the `tools` parameter of a
Claude API messages call. handle_tool_call() dispatches the tool_use block to
the appropriate model runner.

Usage:
    from libs.decision_models.adapters.agent.tools import DECISION_MODEL_TOOLS, handle_tool_call

    response = client.messages.create(
        model="claude-opus-4-6",
        tools=DECISION_MODEL_TOOLS,
        messages=[...],
    )
    if response.stop_reason == "tool_use":
        for block in response.content:
            if block.type == "tool_use":
                result_json = handle_tool_call(block.name, block.input)
"""
from __future__ import annotations

import json
from typing import Any, Dict, List

# Trigger model registration
import libs.decision_models.bess_dispatch_optimization          # noqa: F401
import libs.decision_models.bess_dispatch_simulation_multiday   # noqa: F401
import libs.decision_models.revenue_scenario_engine             # noqa: F401
import libs.decision_models.price_forecast_dayahead             # noqa: F401

from libs.decision_models.runners.local import run


DECISION_MODEL_TOOLS: List[Dict[str, Any]] = [
    {
        "name": "run_bess_dispatch_optimization",
        "description": (
            "Run perfect-foresight BESS arbitrage dispatch optimisation for one day. "
            "Provide 24 hourly prices and battery parameters. "
            "Returns hour-by-hour charge/discharge schedule, SOC trajectory, and daily profit. "
            "Uses the CBC LP solver via PuLP. "
            "Typical use: compute maximum achievable daily revenue for a given asset/price scenario."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "prices_24": {
                    "type": "array",
                    "items": {"type": "number"},
                    "minItems": 24,
                    "maxItems": 24,
                    "description": (
                        "Array of exactly 24 hourly prices (Yuan/MWh or relevant market unit), "
                        "ordered from hour 0 (midnight) to hour 23."
                    ),
                },
                "power_mw": {
                    "type": "number",
                    "description": "Inverter / power rating in MW (charge and discharge ceiling).",
                    "exclusiveMinimum": 0,
                },
                "duration_h": {
                    "type": "number",
                    "description": (
                        "Battery duration in hours. Energy capacity = power_mw × duration_h. "
                        "E.g. 2.0 for a 2-hour battery."
                    ),
                    "exclusiveMinimum": 0,
                },
                "roundtrip_eff": {
                    "type": "number",
                    "description": (
                        "Round-trip efficiency in (0, 1], e.g. 0.85 for 85%. "
                        "Applied symmetrically: eta_charge = eta_discharge = sqrt(roundtrip_eff)."
                    ),
                    "exclusiveMinimum": 0,
                    "maximum": 1,
                    "default": 0.85,
                },
                "max_throughput_mwh": {
                    "type": "number",
                    "description": (
                        "Optional: cap on total discharge energy per day (MWh). "
                        "Simple degradation proxy. Omit if not needed."
                    ),
                },
                "max_cycles_per_day": {
                    "type": "number",
                    "description": (
                        "Optional: cap on equivalent full cycles per day "
                        "(discharge_energy ≤ cycles × energy_capacity). "
                        "Omit if not needed."
                    ),
                },
            },
            "required": ["prices_24", "power_mw", "duration_h"],
        },
    },
    {
        "name": "run_bess_dispatch_simulation_multiday",
        "description": (
            "Run BESS dispatch simulation over multiple days of hourly prices. "
            "Each day is solved independently (SOC resets to 0 per day; no cross-day carryover). "
            "Returns per-hour dispatch records and per-day profit summary. "
            "Use this when you have a price series spanning multiple days. "
            "For a single day use run_bess_dispatch_optimization instead."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "hourly_prices": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "datetime": {
                                "type": "string",
                                "description": "ISO8601 timestamp, e.g. '2026-01-01T08:00:00'",
                            },
                            "price": {
                                "type": "number",
                                "description": "Hourly price (Yuan/MWh or market unit).",
                            },
                        },
                        "required": ["datetime", "price"],
                    },
                    "description": (
                        "List of hourly price records. Must be hourly granularity. "
                        "Days with any missing hour are skipped automatically."
                    ),
                },
                "power_mw": {
                    "type": "number",
                    "description": "Inverter / power rating (MW).",
                    "exclusiveMinimum": 0,
                },
                "duration_h": {
                    "type": "number",
                    "description": "Battery duration (hours). Energy capacity = power_mw × duration_h.",
                    "exclusiveMinimum": 0,
                },
                "roundtrip_eff": {
                    "type": "number",
                    "description": "Round-trip efficiency in (0, 1], e.g. 0.85.",
                    "exclusiveMinimum": 0,
                    "maximum": 1,
                    "default": 0.85,
                },
                "max_throughput_mwh": {
                    "type": "number",
                    "description": "Optional daily discharge energy cap (MWh).",
                },
                "max_cycles_per_day": {
                    "type": "number",
                    "description": "Optional daily cycle cap.",
                },
            },
            "required": ["hourly_prices", "power_mw", "duration_h"],
        },
    },
    {
        "name": "run_revenue_scenario_engine",
        "description": (
            "Calculate daily BESS P&L attribution for a given asset and date. "
            "Accepts actual RT prices and per-scenario dispatch profiles (MW per 15-min interval). "
            "Returns per-scenario revenues and the attribution ladder "
            "(grid_restriction_loss, forecast_error_loss, etc.). "
            "Only scenarios present in scenario_dispatch are computed; missing scenarios yield null losses."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "asset_code": {
                    "type": "string",
                    "description": (
                        "Stable internal asset code, e.g. 'suyou', 'wulate', 'wuhai'. "
                        "See AGENTS.md asset naming rules."
                    ),
                },
                "trade_date": {
                    "type": "string",
                    "description": "ISO date string, e.g. '2026-04-01'.",
                },
                "actual_price": {
                    "type": "array",
                    "items": {"type": "number"},
                    "description": "96 values of 15-min real-time nodal price (Yuan/MWh) for the day.",
                },
                "scenario_dispatch": {
                    "type": "object",
                    "description": (
                        "Map of scenario_name -> array of 96 dispatch MW values for the day. "
                        "Positive = discharge, negative = charge. "
                        "Include only scenarios available for this asset. "
                        "Valid scenario names: perfect_foresight_unrestricted, "
                        "perfect_foresight_grid_feasible, cleared_actual, nominated_dispatch, "
                        "tt_forecast_optimal, tt_strategy."
                    ),
                    "additionalProperties": {
                        "type": "array",
                        "items": {"type": "number"},
                    },
                },
                "compensation_yuan_per_mwh": {
                    "type": "number",
                    "description": (
                        "Asset/month compensation rate (Yuan/MWh). "
                        "Fetch from core.asset_monthly_compensation; default 350 if not available."
                    ),
                    "default": 350.0,
                },
            },
            "required": ["asset_code", "trade_date", "actual_price", "scenario_dispatch"],
        },
    },
]


def handle_tool_call(tool_name: str, tool_input: Dict[str, Any]) -> str:
    """
    Dispatch a tool_use block from the Claude API to the appropriate model runner.

    Args:
        tool_name:  The tool name from the tool_use content block.
        tool_input: The input dict from the tool_use content block.

    Returns:
        JSON string suitable for passing back as a tool_result content block.
    """
    if tool_name == "run_bess_dispatch_optimization":
        result = run("bess_dispatch_optimization", tool_input)

    elif tool_name == "run_bess_dispatch_simulation_multiday":
        result = run("bess_dispatch_simulation_multiday", tool_input)

    elif tool_name == "run_revenue_scenario_engine":
        from datetime import date
        inp = dict(tool_input)
        inp["trade_date"] = date.fromisoformat(inp["trade_date"])
        result = run("revenue_scenario_engine", inp)

    else:
        result = {"error": f"Unknown tool: {tool_name!r}"}

    return json.dumps(result, default=str)
