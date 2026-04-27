"""
libs/decision_models/workflows/daily_strategy_report.py

Daily BESS strategy performance analysis for the 4 Inner Mongolia assets.

This workflow wraps the 6-skill strategy_comparison workflow for single-day
use and adds:
  - Ops dispatch data loader (marketdata.ops_bess_dispatch_15min)
  - Multi-asset aggregation for the 4 Inner Mongolia BESS assets
  - PDF export (requires reportlab; gracefully falls back to markdown bytes)
  - Streamlit dashboard payload renderer

Entry points
------------
run_bess_daily_strategy_analysis(asset_code, date, ...)
    → per-asset daily analysis dict containing strategy results and full report

run_all_assets_daily_strategy_analysis(date, ...)
    → 4-asset daily summary dict

generate_bess_daily_strategy_report(asset_code, date, output_format, ...)
    → markdown str | HTML str | PDF bytes

render_bess_strategy_dashboard_payload(asset_code, date, ...)
    → structured payload dict for Streamlit rendering

Data sources
------------
Ops dispatch    : marketdata.ops_bess_dispatch_15min  ← preferred for nominated/actual
Canon dispatch  : canon.scenario_dispatch_15min        ← fallback / cross-check
Prices (RT)     : canon.nodal_rt_price_15min
Pre-computed    : reports.bess_asset_daily_scenario_pnl
Attribution DB  : reports.bess_asset_daily_attribution
Compensation    : core.asset_monthly_compensation

Source priority for nominated/actual dispatch (per-date)
---------------------------------------------------------
1. marketdata.ops_bess_dispatch_15min  (Excel ingestion — direct measurement)
2. canon.scenario_dispatch_15min       (canonical scenarios — fallback if ops empty)
When ops data is available, it is loaded into context["nominated_dispatch_15min"]
and context["actual_dispatch_15min"].  Both sources are kept as separate keys.
The report explicitly notes which source was used for each P&L calculation.

Caveats carried through
-----------------------
- ops.nominated_dispatch_mw ≠ md_id_cleared_energy.cleared_energy_mwh
- ops.actual_dispatch_mw ≠ md_id_cleared_energy.cleared_energy_mwh
- Hourly PF / forecast P&L not directly comparable to 15-min ops P&L
- Province-level forecast prices vs asset-level nodal prices
- Rules-based waterfall attribution — not causal proof
"""
from __future__ import annotations

import dataclasses
import datetime
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Union

import pandas as pd

logger = logging.getLogger(__name__)

# Module-level imports of workflow functions (enables test patching via module attribute)
from libs.decision_models.resources.bess_context import (  # noqa: E402
    load_forecast_prices_15min,
    load_ops_dispatch_15min,
    load_precomputed_attribution,
    load_precomputed_scenario_pnl,
)
from libs.decision_models.workflows.strategy_comparison import (  # noqa: E402
    attribute_dispatch_discrepancy,
    generate_asset_strategy_report,
    load_bess_strategy_comparison_context,
    rank_dispatch_strategies,
    run_forecast_dispatch_suite,
    run_perfect_foresight_dispatch,
)

# The 4 Inner Mongolia BESS assets managed through the ops ingestion pipeline
_IM_ASSET_CODES: List[str] = ["suyou", "hangjinqi", "siziwangqi", "gushanliang"]

_INTERVAL_HRS_15MIN = 0.25


# ===========================================================================
# Public API
# ===========================================================================

def run_bess_daily_strategy_analysis(
    asset_code: str,
    date: str,
    forecast_models: Optional[List[str]] = None,
    use_ops_dispatch: bool = True,
) -> Dict[str, Any]:
    """
    Run the full strategy comparison for one Inner Mongolia asset on one day.

    Parameters
    ----------
    asset_code      : e.g. "suyou", "hangjinqi", "siziwangqi", "gushanliang"
    date            : ISO date string, e.g. "2026-04-17"
    forecast_models : list of forecast model names; defaults to ["ols_da_time_v1"]
    use_ops_dispatch: when True, load ops dispatch from
                      marketdata.ops_bess_dispatch_15min and prefer it over
                      canon.scenario_dispatch_15min for nominated/actual strategies

    Returns
    -------
    Dict with keys:
      asset_code, date, generated_at
      context        : loaded context (enriched with ops data if available)
      pf_result      : perfect foresight dispatch
      forecast_suite : forecast-driven dispatch suite
      ranking        : strategy ranking
      attribution    : discrepancy attribution
      report         : full report (sections, markdown, period rows)
      ops_dispatch_available : bool — True if ops data was loaded
    """
    generated_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
    d = datetime.date.fromisoformat(date)

    # Step 1: Load base context (single-day window).
    # skip_ops_fallback=use_ops_dispatch: when ops data will be loaded in Step 2
    # by _enrich_context_with_ops_dispatch, skip the duplicate fallback query here.
    context = load_bess_strategy_comparison_context(
        asset_code, date, date, skip_ops_fallback=use_ops_dispatch
    )

    # Step 2: Enrich with ops dispatch if requested
    ops_dispatch_available = False
    if use_ops_dispatch:
        context, ops_dispatch_available = _enrich_context_with_ops_dispatch(context, d)

    # Step 2b: Compute tt_forecast_optimal dispatch from forecast price table
    context = _enrich_context_with_forecast_dispatch(context, d)

    # Step 3: Perfect foresight benchmark
    pf_result = run_perfect_foresight_dispatch(context)

    # Step 4: Forecast-driven dispatch suite
    if forecast_models is None:
        forecast_models = ["ols_da_time_v1"]
    forecast_suite = run_forecast_dispatch_suite(context, forecast_models)

    # Step 5: Load pre-computed DB P&L (most accurate for nominated / actual)
    db_pnl_df, db_pnl_notes = load_precomputed_scenario_pnl(asset_code, d, d)
    db_attr_df, db_attr_notes = load_precomputed_attribution(asset_code, d, d)
    context.setdefault("data_quality_notes", [])
    context["data_quality_notes"].extend(db_pnl_notes)
    context["data_quality_notes"].extend(db_attr_notes)

    # Step 6: Rank strategies
    ranking = rank_dispatch_strategies(
        context, pf_result, forecast_suite,
        db_pnl_df=db_pnl_df if not db_pnl_df.empty else None,
    )

    # Step 7: Attribute discrepancy
    attribution = attribute_dispatch_discrepancy(
        context, ranking,
        attribution_df=db_attr_df if not db_attr_df.empty else None,
    )

    # Step 8: Generate full report (pass pre-loaded DB frames to avoid duplicate queries)
    report = generate_asset_strategy_report(
        asset_code, date, date,
        period_type="daily",
        context=context,
        ranking=ranking,
        attribution=attribution,
        db_pnl_df=db_pnl_df if not db_pnl_df.empty else None,
        db_attr_df=db_attr_df if not db_attr_df.empty else None,
    )

    return {
        "asset_code": asset_code,
        "date": date,
        "generated_at": generated_at,
        "context": context,
        "pf_result": pf_result,
        "forecast_suite": forecast_suite,
        "ranking": ranking,
        "attribution": attribution,
        "report": report,
        "ops_dispatch_available": ops_dispatch_available,
    }


def run_all_assets_daily_strategy_analysis(
    date: str,
    asset_codes: Optional[List[str]] = None,
    forecast_models: Optional[List[str]] = None,
    use_ops_dispatch: bool = True,
) -> Dict[str, Any]:
    """
    Run daily strategy analysis for all 4 Inner Mongolia BESS assets.

    Parameters
    ----------
    date         : ISO date string, e.g. "2026-04-17"
    asset_codes  : subset of assets to run; defaults to all 4 IM assets
    forecast_models : list of forecast models; defaults to ["ols_da_time_v1"]
    use_ops_dispatch : prefer ops dispatch data when available

    Returns
    -------
    Dict with keys:
      date, generated_at
      asset_results  : {asset_code -> run_bess_daily_strategy_analysis() result}
      summary        : cross-asset summary (best strategy, total P&L, rankings)
      errors         : {asset_code -> error message} for failed assets
    """
    generated_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
    codes = asset_codes if asset_codes is not None else _IM_ASSET_CODES

    asset_results: Dict[str, Any] = {}
    errors: Dict[str, str] = {}

    # Run all assets in parallel — each opens its own DB connections, safe for threading.
    # ThreadPoolExecutor releases the GIL during I/O so 4 assets run concurrently.
    with ThreadPoolExecutor(max_workers=len(codes)) as executor:
        future_to_code = {
            executor.submit(
                run_bess_daily_strategy_analysis,
                code, date,
                forecast_models=forecast_models,
                use_ops_dispatch=use_ops_dispatch,
            ): code
            for code in codes
        }
        for future in as_completed(future_to_code):
            code = future_to_code[future]
            try:
                asset_results[code] = future.result()
            except Exception as exc:
                logger.exception("Daily analysis failed for %s on %s: %s", code, date, exc)
                errors[code] = str(exc)

    summary = build_cross_asset_summary(date, asset_results)

    return {
        "date": date,
        "generated_at": generated_at,
        "asset_results": asset_results,
        "summary": summary,
        "errors": errors,
    }


def generate_bess_daily_strategy_report(
    asset_code: str,
    date: str,
    output_format: str = "markdown",
    analysis: Optional[Dict[str, Any]] = None,
    forecast_models: Optional[List[str]] = None,
    use_ops_dispatch: bool = True,
) -> Union[str, bytes]:
    """
    Generate a daily strategy report for one Inner Mongolia asset.

    Parameters
    ----------
    asset_code    : e.g. "suyou"
    date          : ISO date string, e.g. "2026-04-17"
    output_format : "markdown" (default) | "html" | "pdf"
                    "pdf" requires reportlab; falls back to markdown bytes if absent.
    analysis      : optional pre-computed result from run_bess_daily_strategy_analysis().
                    If None, the analysis is run on the fly.
    forecast_models : passed to run_bess_daily_strategy_analysis() if analysis is None
    use_ops_dispatch : passed to run_bess_daily_strategy_analysis() if analysis is None

    Returns
    -------
    str   for output_format in ("markdown", "html")
    bytes for output_format == "pdf"
    """
    if analysis is None:
        analysis = run_bess_daily_strategy_analysis(
            asset_code, date,
            forecast_models=forecast_models,
            use_ops_dispatch=use_ops_dispatch,
        )

    report = analysis.get("report", {})
    markdown_str = report.get("markdown", "")

    if output_format == "markdown":
        return markdown_str

    if output_format == "html":
        return _build_html(asset_code, date, markdown_str)

    if output_format == "pdf":
        pdf_bytes = _build_pdf_bytes(asset_code, date, report)
        if pdf_bytes is not None:
            return pdf_bytes
        # Fallback: return markdown encoded as bytes with a header note
        header = (
            f"# PDF generation requires reportlab — install with: pip install reportlab\n"
            f"# Falling back to plain text for {asset_code} {date}\n\n"
        )
        return (header + markdown_str).encode("utf-8")

    raise ValueError(f"Unknown output_format: {output_format!r}. Use 'markdown', 'html', or 'pdf'.")


def render_bess_strategy_dashboard_payload(
    asset_code: str,
    date: str,
    analysis: Optional[Dict[str, Any]] = None,
    forecast_models: Optional[List[str]] = None,
    use_ops_dispatch: bool = True,
) -> Dict[str, Any]:
    """
    Return a structured payload suitable for Streamlit rendering.

    Parameters
    ----------
    asset_code    : e.g. "suyou"
    date          : ISO date string, e.g. "2026-04-17"
    analysis      : optional pre-computed result from run_bess_daily_strategy_analysis()
    forecast_models : passed to run_bess_daily_strategy_analysis() if analysis is None
    use_ops_dispatch : passed to run_bess_daily_strategy_analysis() if analysis is None

    Returns
    -------
    Dict with keys:
      asset_code, date, generated_at
      summary_cards       : list of {label, value, delta} dicts for st.metric
      strategy_table      : list of row dicts for st.dataframe
      dispatch_chart_data : {labels, nominated_mw, actual_mw, timestamps}
      price_chart_data    : {timestamps, prices_15min, prices_hourly}
      waterfall_data      : {buckets: [{label, value_yuan}], total_gap}
      pnl_comparison      : headers + rows for st.dataframe
      caveats             : list of caveat strings
      ops_dispatch_available : bool
    """
    if analysis is None:
        analysis = run_bess_daily_strategy_analysis(
            asset_code, date,
            forecast_models=forecast_models,
            use_ops_dispatch=use_ops_dispatch,
        )

    report = analysis.get("report", {})
    ranking = analysis.get("ranking", {})
    attribution = analysis.get("attribution", {})
    context = analysis.get("context", {})
    sections = report.get("sections", {})

    # Summary cards
    pf_pnl = ranking.get("perfect_foresight_pnl")
    actual_pnl = ranking.get("actual_pnl")
    total_gap = attribution.get("total_gap")
    best = ranking.get("best_strategy", "—")
    ytd = report.get("ytd_summary")

    summary_cards = [
        {"label": "Best Strategy", "value": best, "delta": None},
        {
            "label": "PF Benchmark (hourly)",
            "value": f"{pf_pnl:,.0f} CNY" if pf_pnl is not None else "—",
            "delta": None,
        },
        {
            "label": "Actual P&L",
            "value": f"{actual_pnl:,.0f} CNY" if actual_pnl is not None else "—",
            "delta": f"{-total_gap:,.0f} vs PF" if total_gap is not None else None,
        },
        {
            "label": "YTD Capture Rate",
            "value": (
                f"{ytd['ytd_capture_rate']:.1%}"
                if (ytd and ytd.get("ytd_capture_rate") is not None)
                else "—"
            ),
            "delta": None,
        },
    ]

    # Strategy table
    strategy_table = []
    for row in ranking.get("rows", []):
        _cycles = row.get("avg_daily_cycles")
        _spread = row.get("captured_spread_yuan_per_mwh")
        strategy_table.append({
            "Rank": row["rank"],
            "Strategy": row["strategy_name"],
            "Avg Daily P&L (CNY)": _fmt_yuan(row.get("avg_daily_pnl_yuan")),
            "Total P&L (CNY)": _fmt_yuan(row.get("pnl_total_yuan")),
            "Market P&L (CNY)": _fmt_yuan(row.get("pnl_market_yuan")),
            "Subsidy (CNY)": _fmt_yuan(row.get("pnl_compensation_yuan")),
            "Gap vs PF (CNY)": _fmt_yuan(row.get("gap_vs_perfect_foresight_yuan")),
            "Capture vs PF": _fmt_pct(row.get("capture_rate_vs_pf")),
            "Avg Daily Cycles": f"{_cycles:.2f}" if _cycles is not None else "—",
            "Captured Spread (CNY/MWh)": f"{_spread:,.0f}" if _spread is not None else "—",
            "Granularity": row.get("granularity", "—"),
            "Available": row.get("data_available", False),
        })

    # Dispatch chart data (from ops dispatch or context dispatch)
    pf_result = analysis.get("pf_result", {})
    dispatch_chart_data = _build_dispatch_chart_data(context, date, pf_result=pf_result)

    # Price chart data
    price_chart_data = _build_price_chart_data(context)

    # Waterfall data
    buckets = attribution.get("buckets", {})
    waterfall_data = {
        "total_gap": total_gap,
        "buckets": [
            {"label": "Grid restriction", "value_yuan": buckets.get("grid_restriction")},
            {"label": "Forecast error", "value_yuan": buckets.get("forecast_error")},
            {"label": "Execution / nomination", "value_yuan": buckets.get("execution_nomination")},
            {"label": "Execution / clearing", "value_yuan": buckets.get("execution_clearing")},
            {"label": "Asset issue", "value_yuan": buckets.get("asset_issue")},
            {"label": "Residual", "value_yuan": buckets.get("residual")},
        ],
    }

    # P&L comparison table (passthrough from report)
    pnl_comparison = report.get("pnl_comparison", {"headers": [], "rows": []})

    caveats = list(dict.fromkeys(
        report.get("data_quality_caveats", [])
        + ranking.get("caveats", [])
        + attribution.get("caveats", [])
    ))

    return {
        "asset_code": asset_code,
        "date": date,
        "generated_at": analysis.get("generated_at"),
        "summary_cards": summary_cards,
        "strategy_table": strategy_table,
        "dispatch_chart_data": dispatch_chart_data,
        "price_chart_data": price_chart_data,
        "waterfall_data": waterfall_data,
        "pnl_comparison": pnl_comparison,
        "caveats": caveats,
        "ops_dispatch_available": analysis.get("ops_dispatch_available", False),
    }


# ===========================================================================
# Internal helpers
# ===========================================================================

def _enrich_context_with_ops_dispatch(
    context: Dict[str, Any],
    d: datetime.date,
) -> tuple[Dict[str, Any], bool]:
    """
    Load ops dispatch data and enrich the context dict.

    If ops data is available:
      - Sets context["nominated_dispatch_15min"] if it was None/empty
      - Sets context["actual_dispatch_15min"] if it was None/empty
      - Always adds context["ops_dispatch_15min"] with the raw ops data
    Adds data quality notes about the source used.

    Returns (enriched_context, ops_dispatch_available).
    """
    asset_code = context["asset_code"]
    ops_df, ops_notes = load_ops_dispatch_15min(asset_code, d, d)
    context.setdefault("data_quality_notes", [])
    context["data_quality_notes"].extend(ops_notes)

    if ops_df.empty:
        context["ops_dispatch_15min"] = None
        return context, False

    # Convert ops dispatch to the {time, dispatch_mw} format used by existing skills
    ops_records = ops_df.to_dict("records")
    context["ops_dispatch_15min"] = ops_records

    # Nominated: prefer ops over canon when canon is empty
    if not context.get("nominated_dispatch_15min"):
        nominated_records = [
            {
                "time": str(r["interval_start"]),
                "dispatch_mw": r["nominated_dispatch_mw"],
            }
            for r in ops_records
            if r["nominated_dispatch_mw"] is not None
        ]
        if nominated_records:
            context["nominated_dispatch_15min"] = nominated_records
            context["data_quality_notes"].append(
                "nominated_dispatch: sourced from marketdata.ops_bess_dispatch_15min "
                "(申报曲线 from Excel ops file) — canon.scenario_dispatch_15min had no data; "
                "nominated_dispatch_mw is the operator nomination, NOT market-cleared energy"
            )

    # Actual: prefer ops over canon when canon is empty
    if not context.get("actual_dispatch_15min"):
        actual_records = [
            {
                "time": str(r["interval_start"]),
                "dispatch_mw": r["actual_dispatch_mw"],
            }
            for r in ops_records
            if r["actual_dispatch_mw"] is not None
        ]
        if actual_records:
            context["actual_dispatch_15min"] = actual_records
            context["data_quality_notes"].append(
                "actual_dispatch: sourced from marketdata.ops_bess_dispatch_15min "
                "(实际充放曲线 from Excel ops file) — canon.scenario_dispatch_15min had no data; "
                "actual_dispatch_mw is physical output, NOT market-cleared energy"
            )

    return context, True


def _enrich_context_with_forecast_dispatch(
    context: dict,
    d: datetime.date,
) -> dict:
    """
    Load 15-min forecast prices and compute the LP-optimal dispatch on them.

    Injects context["tt_forecast_optimal_dispatch_15min"] as a list of
    {"time": str, "dispatch_mw": float} records, settled against actual prices
    in rank_dispatch_strategies via _calc_15min_pnl.

    Returns the enriched context.  Never raises — failures are logged as
    data_quality_notes.
    """
    asset_code = context["asset_code"]
    meta = context["asset_metadata"]
    context.setdefault("data_quality_notes", [])

    # Load forecast prices
    forecast_df, notes = load_forecast_prices_15min(asset_code, d, d)
    context["data_quality_notes"].extend(notes)

    if forecast_df.empty:
        context["tt_forecast_optimal_dispatch_15min"] = None
        return context

    try:
        from services.bess_map.optimisation_engine import compute_dispatch_from_15min_prices

        price_series = forecast_df.set_index("time")["price"].dropna()
        price_series.index = pd.DatetimeIndex(price_series.index)
        if price_series.index.tz is not None:
            price_series.index = price_series.index.tz_localize(None)

        dispatch_df, _ = compute_dispatch_from_15min_prices(
            price_series,
            power_mw=meta["power_mw"],
            duration_h=meta["duration_h"],
            roundtrip_eff=meta["roundtrip_eff"],
            compensation_yuan_per_mwh=meta["compensation_yuan_per_mwh"],
            window_days=1,
        )

        if dispatch_df.empty:
            context["tt_forecast_optimal_dispatch_15min"] = None
            context["data_quality_notes"].append(
                "tt_forecast_optimal: LP returned empty dispatch — "
                "check forecast price completeness for this date"
            )
            return context

        # Convert to list of {time, dispatch_mw} for _calc_15min_pnl
        tt_records = [
            {
                "time": str(dt_idx),
                "dispatch_mw": float(row["dispatch_grid_mw"]) * _INTERVAL_HRS_15MIN,
            }
            for dt_idx, row in dispatch_df.iterrows()
        ]
        context["tt_forecast_optimal_dispatch_15min"] = tt_records
        context["data_quality_notes"].append(
            f"tt_forecast_optimal: dispatch computed from forecast prices via 15-min LP "
            f"({len(tt_records)} intervals for {asset_code} on {d})"
        )
    except Exception as exc:
        logger.exception("tt_forecast_optimal dispatch failed for %s on %s: %s", asset_code, d, exc)
        context["tt_forecast_optimal_dispatch_15min"] = None
        context["data_quality_notes"].append(
            f"tt_forecast_optimal: LP dispatch failed — {exc}"
        )

    return context


def build_cross_asset_summary(
    date: str,
    asset_results: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    """Build a summary dict comparing results across all 4 assets."""
    rows = []
    for asset_code, result in asset_results.items():
        ranking = result.get("ranking", {})
        attribution = result.get("attribution", {})
        meta = result.get("context", {}).get("asset_metadata", {})
        # Pull per-day stats for the actual (cleared_actual) strategy from ranking rows
        _actual_row = next(
            (r for r in ranking.get("rows", []) if r.get("strategy_name") == "cleared_actual"),
            None,
        )
        rows.append({
            "asset_code": asset_code,
            "display_name": meta.get("display_name", asset_code),
            "best_strategy": ranking.get("best_strategy", "—"),
            "pf_pnl": ranking.get("perfect_foresight_pnl"),
            "actual_pnl": ranking.get("actual_pnl"),
            "avg_daily_pnl": (
                _actual_row.get("avg_daily_pnl_yuan") if _actual_row
                else ranking.get("actual_pnl")
            ),
            "avg_daily_cycles": _actual_row.get("avg_daily_cycles") if _actual_row else None,
            "captured_spread_yuan_per_mwh": (
                _actual_row.get("captured_spread_yuan_per_mwh") if _actual_row else None
            ),
            "total_gap": attribution.get("total_gap"),
            "capture_rate": (
                (ranking.get("actual_pnl", 0) or 0) / ranking.get("perfect_foresight_pnl")
                if ranking.get("perfect_foresight_pnl") and ranking.get("perfect_foresight_pnl") > 0
                else None
            ),
            "ops_dispatch_available": result.get("ops_dispatch_available", False),
        })

    actual_pnls = [r["actual_pnl"] for r in rows if r["actual_pnl"] is not None]
    pf_pnls = [r["pf_pnl"] for r in rows if r["pf_pnl"] is not None]
    total_actual = sum(actual_pnls) if actual_pnls else None
    total_pf = sum(pf_pnls) if pf_pnls else None
    portfolio_capture = (
        total_actual / total_pf if (total_actual is not None and total_pf is not None and total_pf > 0)
        else None
    )

    return {
        "date": date,
        "asset_rows": rows,
        "portfolio_total_actual_pnl": total_actual,
        "portfolio_total_pf_pnl": total_pf,
        "portfolio_capture_rate": portfolio_capture,
        "n_assets_with_ops_data": sum(1 for r in rows if r["ops_dispatch_available"]),
    }


def _build_dispatch_chart_data(
    context: Dict[str, Any],
    date: str,
    pf_result: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build chart data for the dispatch comparison (nominated vs actual vs PF).

    Prefers ops_dispatch_15min if available, otherwise uses
    nominated_dispatch_15min and actual_dispatch_15min from context.
    PF dispatch (hourly) is added as a separate series when pf_result is provided.
    """
    ops_data = context.get("ops_dispatch_15min")
    if ops_data:
        timestamps = [str(r["interval_start"]) for r in ops_data]
        nominated_mw = [r.get("nominated_dispatch_mw") for r in ops_data]
        actual_mw = [r.get("actual_dispatch_mw") for r in ops_data]
        source = "ops_bess_dispatch_15min"
    else:
        nominated = context.get("nominated_dispatch_15min") or []
        actual = context.get("actual_dispatch_15min") or []
        all_ts = sorted({
            r.get("time") or r.get("datetime") for r in nominated + actual
            if r.get("time") or r.get("datetime")
        })
        nom_map = {r.get("time") or r.get("datetime"): r.get("dispatch_mw") for r in nominated}
        act_map = {r.get("time") or r.get("datetime"): r.get("dispatch_mw") for r in actual}
        timestamps = list(all_ts)
        nominated_mw = [nom_map.get(ts) for ts in timestamps]
        actual_mw = [act_map.get(ts) for ts in timestamps]
        source = "canon.scenario_dispatch_15min"

    # PF dispatch — hourly LP in MW; divide by 4 to get MWh per 15-min slot for
    # apples-to-apples comparison with the 15-min ops MWh series.
    pf_timestamps: list = []
    pf_dispatch_mwh: list = []
    if pf_result:
        for rec in pf_result.get("dispatch_hourly", []):
            mw_val = rec.get("dispatch_grid_mw")
            pf_timestamps.append(str(rec.get("datetime", "")))
            pf_dispatch_mwh.append(mw_val / 4.0 if mw_val is not None else None)

    # DA cleared energy — use cleared_energy_mwh_15min directly (already MWh)
    id_cleared_timestamps: list = []
    id_cleared_mwh: list = []
    id_cleared_records = context.get("id_cleared_energy_15min") or []
    for rec in id_cleared_records:
        ts = rec.get("datetime")
        if ts is not None:
            id_cleared_timestamps.append(str(ts))
            id_cleared_mwh.append(rec.get("cleared_energy_mwh_15min"))

    return {
        "timestamps": timestamps,
        "nominated_mwh": nominated_mw,     # MWh per 15-min interval
        "actual_mwh": actual_mw,           # MWh per 15-min interval
        "pf_timestamps": pf_timestamps,
        "pf_dispatch_mwh": pf_dispatch_mwh,  # hourly MW ÷ 4 → comparable MWh per 15-min
        "id_cleared_timestamps": id_cleared_timestamps,
        "id_cleared_mwh": id_cleared_mwh,    # MWh per 15-min interval
        "source": source,
        "date": date,
    }


def _build_price_chart_data(context: Dict[str, Any]) -> Dict[str, Any]:
    """Build chart data for the price series."""
    prices_15min = context.get("actual_prices_15min", [])
    prices_hourly = context.get("actual_prices_hourly", [])
    return {
        "timestamps_15min": [str(r.get("time") or r.get("datetime")) for r in prices_15min],
        "prices_15min": [r.get("price") for r in prices_15min],
        "timestamps_hourly": [str(r.get("datetime") or r.get("time")) for r in prices_hourly],
        "prices_hourly": [r.get("price") for r in prices_hourly],
    }


def _build_pdf_bytes(
    asset_code: str,
    date: str,
    report: Dict[str, Any],
) -> Optional[bytes]:
    """
    Generate a PDF report using reportlab.
    Returns None if reportlab is not installed.
    """
    try:
        import io
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import mm
        from reportlab.platypus import (
            Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle,
        )
    except ImportError:
        logger.debug("reportlab not installed — PDF export unavailable; pip install reportlab")
        return None

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=A4,
        leftMargin=20 * mm,
        rightMargin=20 * mm,
        topMargin=20 * mm,
        bottomMargin=20 * mm,
    )
    styles = getSampleStyleSheet()
    h1 = styles["Heading1"]
    h2 = styles["Heading2"]
    normal = styles["Normal"]
    small = ParagraphStyle("small", parent=normal, fontSize=8, leading=10)

    story = []
    sections = report.get("sections", {})

    # Title
    story.append(Paragraph(f"BESS Daily Strategy Report — {asset_code}", h1))
    story.append(Paragraph(f"Date: {date}", normal))
    story.append(Spacer(1, 6 * mm))

    # Executive summary
    story.append(Paragraph("Executive Summary", h2))
    exec_text = sections.get("executive_summary", "No summary available.")
    for line in exec_text.splitlines():
        if line.strip():
            story.append(Paragraph(line, normal))
    story.append(Spacer(1, 4 * mm))

    # Strategy ranking table
    ranking_rows = report.get("strategy_ranking", [])
    if ranking_rows:
        story.append(Paragraph("Strategy Ranking", h2))
        tdata = [["Rank", "Strategy", "P&L (CNY)", "Gap vs PF", "Capture %", "Granularity"]]
        for row in ranking_rows:
            if not row.get("data_available"):
                continue
            tdata.append([
                str(row.get("rank", "")),
                row.get("strategy_name", ""),
                _fmt_yuan(row.get("pnl_total_yuan")),
                _fmt_yuan(row.get("gap_vs_perfect_foresight_yuan")),
                _fmt_pct(row.get("capture_rate_vs_pf")),
                row.get("granularity", ""),
            ])
        if len(tdata) > 1:
            t = Table(tdata, repeatRows=1)
            t.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), colors.grey),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                ("FONTSIZE", (0, 0), (-1, -1), 8),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.black),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.lightgrey]),
            ]))
            story.append(t)
            story.append(Spacer(1, 4 * mm))

    # Discrepancy waterfall
    waterfall = sections.get("discrepancy_waterfall", {})
    buckets = waterfall.get("buckets", {})
    total_gap = waterfall.get("total_gap_yuan")
    if buckets:
        story.append(Paragraph("Discrepancy Attribution (Waterfall)", h2))
        if total_gap is not None:
            story.append(Paragraph(f"Total gap (PF − actual): {total_gap:,.0f} CNY", normal))
        wdata = [["Bucket", "Loss (CNY)"]]
        for key, label in [
            ("grid_restriction", "Grid restriction"),
            ("forecast_error", "Forecast error"),
            ("execution_nomination", "Execution / nomination"),
            ("execution_clearing", "Execution / clearing"),
            ("asset_issue", "Asset issue"),
            ("residual", "Residual"),
        ]:
            val = buckets.get(key)
            wdata.append([label, "—" if val is None else f"{val:,.0f}"])
        wt = Table(wdata, repeatRows=1)
        wt.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.grey),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("GRID", (0, 0), (-1, -1), 0.25, colors.black),
        ]))
        story.append(wt)
        story.append(Spacer(1, 4 * mm))

    # Caveats
    caveats = report.get("data_quality_caveats", [])
    if caveats:
        story.append(Paragraph("Data Quality Caveats", h2))
        for c in caveats[:15]:
            story.append(Paragraph(f"• {c}", small))

    doc.build(story)
    return buf.getvalue()


def _build_html(asset_code: str, date: str, markdown_str: str) -> str:
    """Convert markdown report string to HTML."""
    try:
        import markdown as md_lib
        body = md_lib.markdown(markdown_str, extensions=["tables"])
    except ImportError:
        # Minimal fallback: wrap in <pre> block
        escaped = markdown_str.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        body = f"<pre>{escaped}</pre>"

    return (
        "<!DOCTYPE html>"
        "<html><head>"
        f"<title>BESS Daily Strategy Report — {asset_code} {date}</title>"
        "<style>"
        "body{font-family:sans-serif;max-width:900px;margin:0 auto;padding:20px}"
        "table{border-collapse:collapse;width:100%}"
        "th,td{border:1px solid #ccc;padding:4px 8px;text-align:left}"
        "th{background:#444;color:#fff}"
        "tr:nth-child(even){background:#f5f5f5}"
        "</style>"
        "</head><body>"
        f"{body}"
        "</body></html>"
    )


def _fmt_yuan(v: Any) -> str:
    if v is None:
        return "—"
    try:
        return f"{float(v):,.0f}"
    except (TypeError, ValueError):
        return "—"


def _fmt_pct(v: Any) -> str:
    if v is None:
        return "—"
    try:
        return f"{float(v):.1%}"
    except (TypeError, ValueError):
        return "—"
