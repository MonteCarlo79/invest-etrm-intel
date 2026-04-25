"""
libs/decision_models/adapters/agent/tools.py

OpenClaw / Claude API tool definitions for registered decision models
and strategy comparison workflow skills.

Each entry in DECISION_MODEL_TOOLS or STRATEGY_COMPARISON_TOOLS can be passed
in the `tools` parameter of a Claude API messages call.
handle_tool_call() dispatches any tool_use block to the correct handler.

Usage:
    from libs.decision_models.adapters.agent.tools import (
        DECISION_MODEL_TOOLS,
        STRATEGY_COMPARISON_TOOLS,
        ALL_TOOLS,
        handle_tool_call,
    )

    response = client.messages.create(
        model="claude-opus-4-6",
        tools=ALL_TOOLS,    # or choose a subset
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
import libs.decision_models.bess_dispatch_optimization           # noqa: F401
import libs.decision_models.bess_dispatch_simulation_multiday    # noqa: F401
import libs.decision_models.bess_spread_call_strip               # noqa: F401
import libs.decision_models.dispatch_pnl_attribution             # noqa: F401
import libs.decision_models.revenue_scenario_engine              # noqa: F401
import libs.decision_models.price_forecast_dayahead              # noqa: F401

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
        "name": "run_price_forecast_dayahead",
        "description": (
            "Forecast province-level hourly RT prices for a target date using day-ahead (DA) prices as input. "
            "Two models available: 'ols_da_time_v1' (default, rolling OLS with DA price + hour-of-day features) "
            "and 'naive_da' (RT = DA price, no training). "
            "Returns 24 hourly RT predictions for the target date. "
            "SCOPE: province-level only — NOT nodal/asset. Hourly only — NOT 15-min. "
            "Requires a lookback window of historical RT+DA prices plus the target date's DA prices."
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
                                "description": "ISO8601 timestamp, e.g. '2026-04-15T08:00:00'",
                            },
                            "da_price": {
                                "type": "number",
                                "description": "Day-ahead clearing price (Yuan/MWh). Required for all hours.",
                            },
                            "rt_price": {
                                "type": ["number", "null"],
                                "description": (
                                    "Actual RT settlement price (Yuan/MWh). "
                                    "Required for history days used as training. "
                                    "Should be null/absent for target_date hours (not yet known)."
                                ),
                            },
                        },
                        "required": ["datetime", "da_price"],
                    },
                    "description": (
                        "Hourly price records. Include lookback history (both rt_price and da_price) "
                        "plus all 24 hours of target_date with da_price (rt_price=null). "
                        "For ols_da_time_v1: include at least min_train_days × 24 prior hours with both prices."
                    ),
                },
                "target_date": {
                    "type": "string",
                    "description": "ISO date string for the day to forecast, e.g. '2026-04-15'. Must be present in hourly_prices.",
                },
                "model": {
                    "type": "string",
                    "enum": ["ols_rt_time_v1", "naive_rt_lag1", "naive_rt_lag7", "ols_da_time_v1", "naive_da"],
                    "description": (
                        "'ols_da_time_v1' (default): rolling OLS with [intercept, da_price, sin(2πh/24), cos(2πh/24)] features. "
                        "'naive_da': RT prediction = DA price."
                    ),
                    "default": "ols_da_time_v1",
                },
                "min_train_days": {
                    "type": "integer",
                    "description": (
                        "Minimum complete training days required before OLS is used. "
                        "Falls back to naive_da if fewer days available. Default: 7."
                    ),
                    "minimum": 1,
                    "default": 7,
                },
                "lookback_days": {
                    "type": "integer",
                    "description": "Rolling training window width in days. Default: 60.",
                    "minimum": 1,
                    "default": 60,
                },
            },
            "required": ["hourly_prices", "target_date"],
        },
    },
    {
        "name": "run_dispatch_pnl_attribution",
        "description": (
            "Compute BESS dispatch P&L attribution ladder from per-scenario PnL values. "
            "Accepts pre-computed daily PnL for up to 6 scenarios in the dispatch chain "
            "(perfect_foresight_unrestricted → pf_grid_feasible → tt_forecast_optimal → "
            "tt_strategy → nominated → cleared_actual) and returns the loss at each step. "
            "Use this when scenario PnL values are already known (e.g. from the DB or from "
            "run_revenue_scenario_engine). Losses are None for missing scenario pairs. "
            "Persisted daily to reports.bess_asset_daily_attribution."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "asset_code": {
                    "type": "string",
                    "description": "Stable internal asset code, e.g. 'suyou', 'wulate'.",
                },
                "trade_date": {
                    "type": "string",
                    "description": "ISO date string, e.g. '2026-04-01'.",
                },
                "pf_unrestricted_pnl": {
                    "type": ["number", "null"],
                    "description": "Perfect foresight unrestricted daily PnL (Yuan).",
                },
                "pf_grid_feasible_pnl": {
                    "type": ["number", "null"],
                    "description": "Perfect foresight grid-feasible daily PnL (Yuan).",
                },
                "tt_forecast_optimal_pnl": {
                    "type": ["number", "null"],
                    "description": "TT forecast-optimal dispatch daily PnL (Yuan).",
                },
                "tt_strategy_pnl": {
                    "type": ["number", "null"],
                    "description": "TT strategy dispatch daily PnL (Yuan).",
                },
                "nominated_pnl": {
                    "type": ["number", "null"],
                    "description": "Nominated dispatch daily PnL (Yuan).",
                },
                "cleared_actual_pnl": {
                    "type": ["number", "null"],
                    "description": "Cleared actual dispatch daily PnL (Yuan).",
                },
            },
            "required": ["asset_code", "trade_date"],
        },
    },
    {
        "name": "query_asset_realization_status",
        "description": (
            "Fetch current realization status for one or all assets from the monitoring DB. "
            "Returns rolling realization ratio (cleared_actual / pf_grid_feasible), "
            "attribution breakdown averages, and status level (NORMAL/WARN/ALERT/CRITICAL). "
            "This is a read-only DB query — no model computation. "
            "Data is updated daily by the realization monitor batch job."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "asset_code": {
                    "type": ["string", "null"],
                    "description": "Asset code to filter on. Omit or null to return all assets.",
                },
                "snapshot_date": {
                    "type": ["string", "null"],
                    "description": (
                        "ISO date string. Defaults to today's snapshot. "
                        "Use latest available if today is not yet computed."
                    ),
                },
                "lookback_days": {
                    "type": "integer",
                    "description": "Window size to query (default: 30).",
                    "default": 30,
                },
            },
            "required": [],
        },
    },
    {
        "name": "query_asset_fragility_status",
        "description": (
            "Fetch current fragility status for one or all assets from the monitoring DB. "
            "Returns composite fragility score (0–1), fragility level (LOW/MEDIUM/HIGH/CRITICAL), "
            "component scores (realization, trend), dominant factor, and narrative. "
            "This is a read-only DB query — no model computation. "
            "Data is updated daily by the fragility monitor batch job, after realization monitor."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "asset_code": {
                    "type": ["string", "null"],
                    "description": "Asset code to filter on. Omit or null to return all assets.",
                },
                "snapshot_date": {
                    "type": ["string", "null"],
                    "description": "ISO date string. Defaults to today's snapshot.",
                },
                "min_level": {
                    "type": "string",
                    "enum": ["LOW", "MEDIUM", "HIGH", "CRITICAL"],
                    "description": "Return only assets at or above this fragility level.",
                    "default": "LOW",
                },
            },
            "required": [],
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
    {
        "name": "run_bess_spread_call_strip",
        "description": (
            "Price a BESS asset as a strip of N daily spread call options using "
            "Kirk/Margrabe approximation (closed-form, no external dependencies). "
            "Returns: strip_value_yuan (total embedded option value), per_day_value_yuan, "
            "intrinsic_value_yuan, time_value_yuan, moneyness_pct, net_spread_forward, "
            "and Greeks: delta_yuan_per_yuan (dV/dF_peak), vega_yuan_per_vol_point, "
            "theta_yuan_per_day. "
            "Use for: BESS embedded option valuation, investment analysis, realization overlay "
            "(actual PnL / option value = capture rate), and fleet-level spread call decomposition."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "asset_code": {
                    "type": "string",
                    "description": "BESS asset identifier, e.g. 'suyou'.",
                },
                "as_of_date": {
                    "type": "string",
                    "description": "Valuation date (ISO format), e.g. '2026-04-21'.",
                },
                "n_days_remaining": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 365,
                    "description": "Number of daily options in the strip (e.g. 252 for ~1 trading year).",
                },
                "peak_forward_yuan": {
                    "type": "number",
                    "description": "Average daily peak clearing price forward (¥/MWh).",
                },
                "offpeak_forward_yuan": {
                    "type": "number",
                    "description": "Average daily offpeak clearing price forward (¥/MWh).",
                },
                "peak_vol": {
                    "type": "number",
                    "description": "Annualised peak price volatility, e.g. 0.30 for 30%.",
                },
                "offpeak_vol": {
                    "type": "number",
                    "description": "Annualised offpeak price volatility, e.g. 0.25 for 25%.",
                },
                "peak_offpeak_corr": {
                    "type": "number",
                    "minimum": -1.0,
                    "maximum": 1.0,
                    "description": "Correlation between peak and offpeak prices. Default: 0.85.",
                    "default": 0.85,
                },
                "roundtrip_eff": {
                    "type": "number",
                    "minimum": 0.1,
                    "maximum": 1.0,
                    "description": "BESS roundtrip efficiency η ∈ (0, 1]. Default: 0.85.",
                    "default": 0.85,
                },
                "power_mw": {
                    "type": "number",
                    "description": "BESS power rating in MW. Default: 100.",
                    "default": 100.0,
                },
                "duration_h": {
                    "type": "number",
                    "description": "BESS storage duration in hours. Default: 2.",
                    "default": 2.0,
                },
                "om_cost_yuan_per_mwh": {
                    "type": "number",
                    "description": "O&M cost per MWh discharged (acts as effective strike K). Default: 0.",
                    "default": 0.0,
                },
                "risk_free_rate": {
                    "type": "number",
                    "description": "Annualised risk-free rate (CNY). Default: 0.",
                    "default": 0.0,
                },
            },
            "required": [
                "asset_code", "as_of_date", "n_days_remaining",
                "peak_forward_yuan", "offpeak_forward_yuan",
                "peak_vol", "offpeak_vol",
            ],
        },
    },
]


STRATEGY_COMPARISON_TOOLS: List[Dict[str, Any]] = [
    {
        "name": "load_bess_strategy_comparison_context",
        "description": (
            "Load all data required for BESS dispatch strategy comparison: actual 15-min RT prices, "
            "hourly DA prices, nominated and actual dispatch, asset physical/commercial metadata. "
            "Returns a context bundle used by the other 5 strategy comparison skills. "
            "Data quality notes are included for any missing or degraded data fields."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "asset_code": {
                    "type": "string",
                    "description": (
                        "Stable internal asset code, e.g. 'suyou', 'wulate', 'wuhai'. "
                        "See ASSET_ALIAS_MAP in calc.py for full list."
                    ),
                },
                "date_from": {
                    "type": "string",
                    "description": "ISO date string for start of period, e.g. '2026-03-01'.",
                },
                "date_to": {
                    "type": "string",
                    "description": "ISO date string for end of period (inclusive), e.g. '2026-03-31'.",
                },
            },
            "required": ["asset_code", "date_from", "date_to"],
        },
    },
    {
        "name": "run_perfect_foresight_dispatch",
        "description": (
            "Compute perfect-foresight BESS dispatch using actual realised prices as input. "
            "Uses bess_dispatch_simulation_multiday (LP per day, hourly, SOC resets each day). "
            "P&L settled at hourly granularity against hourly mean of actual 15-min prices. "
            "This is the benchmark / upper-bound strategy for comparison. "
            "Input: context dict from load_bess_strategy_comparison_context."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "context": {
                    "type": "object",
                    "description": "Context dict returned by load_bess_strategy_comparison_context.",
                },
            },
            "required": ["context"],
        },
    },
    {
        "name": "run_forecast_dispatch_suite",
        "description": (
            "Run one or more day-ahead price forecast models, optimise BESS dispatch on each forecast, "
            "and settle the resulting dispatch on actual realised prices. "
            "Returns per-model strategy results including forecast prices, hourly dispatch, "
            "and realised P&L. "
            "SCOPE: price_forecast_dayahead is province-level (not nodal) and hourly (not 15-min). "
            "P&L settled on actual 15-min asset prices resampled to hourly mean."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "context": {
                    "type": "object",
                    "description": "Context dict returned by load_bess_strategy_comparison_context.",
                },
                "forecast_models": {
                    "type": "array",
                    "items": {
                        "type": "string",
                        "enum": ["ols_rt_time_v1", "naive_rt_lag1", "naive_rt_lag7", "ols_da_time_v1", "naive_da"],
                    },
                    "description": (
                        "List of forecast model names to run. "
                        "Defaults to ['ols_rt_time_v1'] if omitted. "
                        "RT-only (Inner Mongolia / no DA market): "
                        "'ols_rt_time_v1' (rolling OLS on RT history + time-of-day, recommended), "
                        "'naive_rt_lag1' (yesterday same hour), "
                        "'naive_rt_lag7' (7 days ago same hour). "
                        "DA-based (legacy): 'ols_da_time_v1', 'naive_da'."
                    ),
                },
            },
            "required": ["context"],
        },
    },
    {
        "name": "rank_dispatch_strategies",
        "description": (
            "Rank all available dispatch strategies by realised P&L. "
            "Strategies compared: perfect foresight (hourly), forecast-driven (hourly), "
            "nominated dispatch (15-min from DB), actual/cleared dispatch (15-min from DB). "
            "Returns strategy ranking with gaps vs perfect foresight, vs best forecast, "
            "vs nominated, and vs actual. "
            "NOTE: hourly and 15-min P&L are not directly comparable — use gaps directionally."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "context": {
                    "type": "object",
                    "description": "Context dict from load_bess_strategy_comparison_context.",
                },
                "pf_result": {
                    "type": "object",
                    "description": "Perfect foresight result from run_perfect_foresight_dispatch.",
                },
                "forecast_suite": {
                    "type": "object",
                    "description": "Forecast suite result from run_forecast_dispatch_suite.",
                },
            },
            "required": ["context", "pf_result", "forecast_suite"],
        },
    },
    {
        "name": "attribute_dispatch_discrepancy",
        "description": (
            "Decompose the P&L gap between perfect foresight and actual dispatch into attribution buckets. "
            "Method: rules-based waterfall (not causal proof). "
            "Buckets: forecast_error, grid_restriction, execution_nomination, execution_clearing, "
            "asset_issue (None until outage table exists), residual. "
            "Uses pre-computed attribution from reports.bess_asset_daily_attribution when available."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "context": {
                    "type": "object",
                    "description": "Context dict from load_bess_strategy_comparison_context.",
                },
                "ranking": {
                    "type": "object",
                    "description": "Ranking result from rank_dispatch_strategies.",
                },
            },
            "required": ["context", "ranking"],
        },
    },
    {
        "name": "generate_asset_strategy_report",
        "description": (
            "Generate a reusable daily / weekly / monthly BESS asset strategy report. "
            "If context / ranking / attribution are not supplied, they are computed on the fly. "
            "Returns: executive summary, strategy ranking, P&L comparison, discrepancy waterfall, "
            "YTD summary, forecast-to-year-end, and data quality caveats. "
            "Output includes structured sections, period-aggregated tables, and a markdown string "
            "suitable for Slack / email distribution."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "asset_code": {
                    "type": "string",
                    "description": "Stable internal asset code, e.g. 'suyou'.",
                },
                "date_from": {
                    "type": "string",
                    "description": "ISO date string for report period start, e.g. '2026-03-01'.",
                },
                "date_to": {
                    "type": "string",
                    "description": "ISO date string for report period end, e.g. '2026-03-31'.",
                },
                "period_type": {
                    "type": "string",
                    "enum": ["daily", "weekly", "monthly"],
                    "description": "Aggregation period for the report tables. Default: 'monthly'.",
                    "default": "monthly",
                },
                "context": {
                    "type": "object",
                    "description": "Optional: pre-computed context from load_bess_strategy_comparison_context.",
                },
                "ranking": {
                    "type": "object",
                    "description": "Optional: pre-computed ranking from rank_dispatch_strategies.",
                },
                "attribution": {
                    "type": "object",
                    "description": "Optional: pre-computed attribution from attribute_dispatch_discrepancy.",
                },
            },
            "required": ["asset_code", "date_from", "date_to"],
        },
    },
]

DAILY_OPS_TOOLS: List[Dict[str, Any]] = [
    {
        "name": "run_bess_daily_strategy_analysis",
        "description": (
            "Run the full 4-strategy performance analysis for one Inner Mongolia BESS asset on one day. "
            "Strategies compared: perfect foresight (LP benchmark), forecast-price-optimised, "
            "nominated dispatch (from ops Excel ingestion), and actual dispatch (from ops Excel). "
            "Ops dispatch data is loaded from marketdata.ops_bess_dispatch_15min when available "
            "and preferred over canon.scenario_dispatch_15min. "
            "Returns: context, strategy ranking, discrepancy attribution, and full report. "
            "Assets: suyou, hangjinqi, siziwangqi, gushanliang (Inner Mongolia only)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "asset_code": {
                    "type": "string",
                    "enum": ["suyou", "hangjinqi", "siziwangqi", "gushanliang"],
                    "description": "Inner Mongolia BESS asset code.",
                },
                "date": {
                    "type": "string",
                    "description": "ISO date string, e.g. '2026-04-17'.",
                },
                "forecast_models": {
                    "type": "array",
                    "items": {"type": "string", "enum": ["ols_rt_time_v1", "naive_rt_lag1", "naive_rt_lag7", "ols_da_time_v1", "naive_da"]},
                    "description": "Forecast models to run. Default: ['ols_da_time_v1'].",
                },
                "use_ops_dispatch": {
                    "type": "boolean",
                    "description": (
                        "When true (default), load and prefer ops dispatch data from "
                        "marketdata.ops_bess_dispatch_15min for nominated/actual strategies."
                    ),
                    "default": True,
                },
            },
            "required": ["asset_code", "date"],
        },
    },
    {
        "name": "generate_bess_daily_strategy_report",
        "description": (
            "Generate a daily strategy report for one Inner Mongolia BESS asset. "
            "Output formats: 'markdown' (default, returns string), 'html' (returns HTML string), "
            "'pdf' (returns PDF bytes — requires reportlab; falls back to markdown bytes if absent). "
            "If analysis is not provided, runs run_bess_daily_strategy_analysis() on the fly. "
            "Report includes: executive summary, strategy ranking, P&L comparison, "
            "discrepancy waterfall, YTD summary, and data quality caveats."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "asset_code": {
                    "type": "string",
                    "enum": ["suyou", "hangjinqi", "siziwangqi", "gushanliang"],
                    "description": "Inner Mongolia BESS asset code.",
                },
                "date": {
                    "type": "string",
                    "description": "ISO date string, e.g. '2026-04-17'.",
                },
                "output_format": {
                    "type": "string",
                    "enum": ["markdown", "html", "pdf"],
                    "description": "Output format. Default: 'markdown'.",
                    "default": "markdown",
                },
                "forecast_models": {
                    "type": "array",
                    "items": {"type": "string", "enum": ["ols_rt_time_v1", "naive_rt_lag1", "naive_rt_lag7", "ols_da_time_v1", "naive_da"]},
                    "description": "Forecast models to run. Default: ['ols_da_time_v1'].",
                },
                "use_ops_dispatch": {
                    "type": "boolean",
                    "description": "Prefer ops dispatch data. Default: true.",
                    "default": True,
                },
            },
            "required": ["asset_code", "date"],
        },
    },
    {
        "name": "render_bess_strategy_dashboard_payload",
        "description": (
            "Return a structured Streamlit-ready payload for the daily BESS strategy view. "
            "Includes: summary_cards (for st.metric), strategy_table (for st.dataframe), "
            "dispatch_chart_data (nominated vs actual MW time series), price_chart_data, "
            "waterfall_data (discrepancy attribution buckets), pnl_comparison, and caveats. "
            "Use this when building a custom dashboard or agent response with structured data."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "asset_code": {
                    "type": "string",
                    "enum": ["suyou", "hangjinqi", "siziwangqi", "gushanliang"],
                    "description": "Inner Mongolia BESS asset code.",
                },
                "date": {
                    "type": "string",
                    "description": "ISO date string, e.g. '2026-04-17'.",
                },
                "forecast_models": {
                    "type": "array",
                    "items": {"type": "string", "enum": ["ols_rt_time_v1", "naive_rt_lag1", "naive_rt_lag7", "ols_da_time_v1", "naive_da"]},
                    "description": "Forecast models to run. Default: ['ols_da_time_v1'].",
                },
                "use_ops_dispatch": {
                    "type": "boolean",
                    "description": "Prefer ops dispatch data. Default: true.",
                    "default": True,
                },
            },
            "required": ["asset_code", "date"],
        },
    },
]

# Combined list for passing to the Claude API
ALL_TOOLS: List[Dict[str, Any]] = DECISION_MODEL_TOOLS + STRATEGY_COMPARISON_TOOLS + DAILY_OPS_TOOLS


def handle_tool_call(tool_name: str, tool_input: Dict[str, Any]) -> str:
    """
    Dispatch a tool_use block from the Claude API to the appropriate handler.

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

    elif tool_name == "run_price_forecast_dayahead":
        result = run("price_forecast_dayahead", tool_input)

    elif tool_name == "run_dispatch_pnl_attribution":
        from datetime import date
        inp = dict(tool_input)
        inp["trade_date"] = date.fromisoformat(inp["trade_date"])
        result = run("dispatch_pnl_attribution", inp)

    elif tool_name == "query_asset_realization_status":
        from services.monitoring.realization_monitor import query_realization_status
        result = query_realization_status(
            asset_code=tool_input.get("asset_code"),
            snapshot_date=tool_input.get("snapshot_date"),
            lookback_days=tool_input.get("lookback_days", 30),
        )

    elif tool_name == "query_asset_fragility_status":
        from services.monitoring.fragility_monitor import query_fragility_status
        result = query_fragility_status(
            asset_code=tool_input.get("asset_code"),
            snapshot_date=tool_input.get("snapshot_date"),
            min_level=tool_input.get("min_level", "LOW"),
        )

    elif tool_name == "run_bess_spread_call_strip":
        result = run("bess_spread_call_strip", tool_input)

    elif tool_name == "run_revenue_scenario_engine":
        from datetime import date
        inp = dict(tool_input)
        inp["trade_date"] = date.fromisoformat(inp["trade_date"])
        result = run("revenue_scenario_engine", inp)

    # -----------------------------------------------------------------------
    # Strategy comparison workflow tools
    # -----------------------------------------------------------------------
    elif tool_name == "load_bess_strategy_comparison_context":
        from libs.decision_models.workflows.strategy_comparison import (
            load_bess_strategy_comparison_context,
        )
        result = load_bess_strategy_comparison_context(
            asset_code=tool_input["asset_code"],
            date_from=tool_input["date_from"],
            date_to=tool_input["date_to"],
        )

    elif tool_name == "run_perfect_foresight_dispatch":
        from libs.decision_models.workflows.strategy_comparison import (
            run_perfect_foresight_dispatch,
        )
        result = run_perfect_foresight_dispatch(context=tool_input["context"])

    elif tool_name == "run_forecast_dispatch_suite":
        from libs.decision_models.workflows.strategy_comparison import (
            run_forecast_dispatch_suite,
        )
        result = run_forecast_dispatch_suite(
            context=tool_input["context"],
            forecast_models=tool_input.get("forecast_models"),
        )

    elif tool_name == "rank_dispatch_strategies":
        from libs.decision_models.workflows.strategy_comparison import (
            rank_dispatch_strategies,
        )
        result = rank_dispatch_strategies(
            context=tool_input["context"],
            pf_result=tool_input["pf_result"],
            forecast_suite=tool_input["forecast_suite"],
        )

    elif tool_name == "attribute_dispatch_discrepancy":
        from libs.decision_models.workflows.strategy_comparison import (
            attribute_dispatch_discrepancy,
        )
        result = attribute_dispatch_discrepancy(
            context=tool_input["context"],
            ranking=tool_input["ranking"],
        )

    elif tool_name == "generate_asset_strategy_report":
        from libs.decision_models.workflows.strategy_comparison import (
            generate_asset_strategy_report,
        )
        result = generate_asset_strategy_report(
            asset_code=tool_input["asset_code"],
            date_from=tool_input["date_from"],
            date_to=tool_input["date_to"],
            period_type=tool_input.get("period_type", "monthly"),
            context=tool_input.get("context"),
            ranking=tool_input.get("ranking"),
            attribution=tool_input.get("attribution"),
        )

    # -----------------------------------------------------------------------
    # Daily ops strategy tools
    # -----------------------------------------------------------------------
    elif tool_name == "run_bess_daily_strategy_analysis":
        from libs.decision_models.workflows.daily_strategy_report import (
            run_bess_daily_strategy_analysis,
        )
        result = run_bess_daily_strategy_analysis(
            asset_code=tool_input["asset_code"],
            date=tool_input["date"],
            forecast_models=tool_input.get("forecast_models"),
            use_ops_dispatch=tool_input.get("use_ops_dispatch", True),
        )
        # Strip the raw context to keep the response size manageable
        result = {k: v for k, v in result.items() if k != "context"}

    elif tool_name == "generate_bess_daily_strategy_report":
        from libs.decision_models.workflows.daily_strategy_report import (
            generate_bess_daily_strategy_report,
        )
        output = generate_bess_daily_strategy_report(
            asset_code=tool_input["asset_code"],
            date=tool_input["date"],
            output_format=tool_input.get("output_format", "markdown"),
            forecast_models=tool_input.get("forecast_models"),
            use_ops_dispatch=tool_input.get("use_ops_dispatch", True),
        )
        if isinstance(output, bytes):
            result = {
                "output_format": tool_input.get("output_format", "markdown"),
                "content_bytes_len": len(output),
                "note": (
                    "PDF/bytes output cannot be JSON-serialised via tool_call. "
                    "Use generate_bess_daily_strategy_report() directly in Python to obtain bytes."
                ),
            }
        else:
            result = {
                "output_format": tool_input.get("output_format", "markdown"),
                "content": output,
            }

    elif tool_name == "render_bess_strategy_dashboard_payload":
        from libs.decision_models.workflows.daily_strategy_report import (
            render_bess_strategy_dashboard_payload,
        )
        result = render_bess_strategy_dashboard_payload(
            asset_code=tool_input["asset_code"],
            date=tool_input["date"],
            forecast_models=tool_input.get("forecast_models"),
            use_ops_dispatch=tool_input.get("use_ops_dispatch", True),
        )

    else:
        result = {"error": f"Unknown tool: {tool_name!r}"}

    return json.dumps(result, default=str)
