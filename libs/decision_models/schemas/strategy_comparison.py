"""
libs/decision_models/schemas/strategy_comparison.py

Input/output contracts for the 6-skill dispatch strategy comparison workflow.

Granularity note
----------------
The comparison workflow bridges two granularities:
  - 15-min : actual prices (canon.nodal_rt_price_15min) and dispatch from DB
             (canon.scenario_dispatch_15min) — used for nominated / actual
  - hourly  : dispatch optimization and forecast models operate at hourly resolution.
             Hourly P&L is settled against hourly mean of actual 15-min prices.

This is an intentional approximation.  Results for PF / forecast strategies are
computed at hourly granularity and ARE NOT DIRECTLY COMPARABLE to 15-min P&L
figures from the DB.  All outputs carry a 'data_caveats' or 'notes' field that
states the active approximations.

Attribution method
------------------
Discrepancy decomposition is a rules-based waterfall, not causal proof.
Attribution buckets:
  forecast_error            : PF_pnl  − forecast_optimal_pnl
  asset_issue               : proxy — available flag from DB or outage indicator
  grid_restriction          : PF_unrestricted_pnl − PF_grid_feasible_pnl (if both available)
  execution_nomination      : forecast_optimal_pnl − nominated_pnl
  execution_clearing        : nominated_pnl − actual_pnl
  residual                  : total_gap − sum(explained buckets)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Skill 1 — context loader
# ---------------------------------------------------------------------------

@dataclass
class AssetMetadata:
    """Physical and commercial parameters for one BESS asset."""
    asset_code: str
    display_name: str
    power_mw: float                   # inverter / power rating
    duration_h: float                 # energy = power_mw × duration_h
    roundtrip_eff: float              # round-trip efficiency (0, 1]
    compensation_yuan_per_mwh: float  # current-month compensation rate
    province: str
    # TODO: load from DB when core.asset_master table is available
    source: str = "hardcoded_fallback"


@dataclass
class StrategyComparisonContext:
    """
    Loaded context bundle for one asset over a date range.

    Output of: load_bess_strategy_comparison_context()

    Fields
    ------
    asset_code, date_from, date_to   : selection
    asset_metadata                   : physical + commercial params
    actual_prices_15min              : list of {time, price} dicts — canon.nodal_rt_price_15min
    actual_prices_hourly             : hourly mean of 15-min prices — {datetime, price}
    da_prices_hourly                 : DA prices for forecast input — {datetime, da_price}
                                       None when not available (see data_quality_notes)
    nominated_dispatch_15min         : list of {time, dispatch_mw} or None
                                       Source: canon.scenario_dispatch_15min (nominated_dispatch)
    actual_dispatch_15min            : list of {time, dispatch_mw} or None
                                       Source: canon.scenario_dispatch_15min (cleared_actual)
                                       IMPORTANT: this is the "cleared_actual" scenario from the
                                       canonical dispatch table — it may represent clearing or
                                       actual physical output depending on what was ingested.
                                       Do NOT conflate with id_cleared_energy_15min.
    id_cleared_energy_15min          : list of {datetime, dispatch_unit_name,
                                         cleared_energy_mwh_15min, cleared_power_mw_implied_15min,
                                         cleared_price} or None
                                       Source: marketdata.md_id_cleared_energy
                                       Inner Mongolia (Mengxi) assets only.
                                       IMPORTANT: DA MARKET-CLEARED TRADING ENERGY — NOT actual
                                       physical dispatch. Actual output may differ due to asset
                                       issues, BOP constraints, or grid operator intervention.
                                       Unit semantics are explicit: mwh_15min, not dispatch_mw.
    available_scenarios              : scenario names present in canon.scenario_dispatch_15min
    outage_flags                     : list of {date, flag, note} or None
                                       TODO: not yet implemented — always None
    curtailment_flags                : list of {date, flag, note} or None
                                       TODO: not yet implemented — always None
    data_quality_notes               : list of human-readable notes about missing / degraded data
    """
    asset_code: str
    date_from: str           # ISO date
    date_to: str             # ISO date
    asset_metadata: AssetMetadata = field(default_factory=lambda: AssetMetadata(
        asset_code="", display_name="", power_mw=100.0, duration_h=2.0,
        roundtrip_eff=0.85, compensation_yuan_per_mwh=350.0, province="",
    ))
    actual_prices_15min: List[dict] = field(default_factory=list)   # {time, price}
    actual_prices_hourly: List[dict] = field(default_factory=list)  # {datetime, price}
    da_prices_hourly: List[dict] = field(default_factory=list)      # {datetime, da_price}
    nominated_dispatch_15min: Optional[List[dict]] = None           # {time, dispatch_mw}
    actual_dispatch_15min: Optional[List[dict]] = None              # {time, dispatch_mw} (cleared_actual scenario)
    id_cleared_energy_15min: Optional[List[dict]] = None
    # {datetime, dispatch_unit_name, cleared_energy_mwh_15min, cleared_power_mw_implied_15min, cleared_price}
    # Source: marketdata.md_id_cleared_energy (Inner Mongolia only)
    # NOT actual physical dispatch — DA market-cleared trading energy only
    available_scenarios: List[str] = field(default_factory=list)
    outage_flags: Optional[List[dict]] = None        # TODO placeholder
    curtailment_flags: Optional[List[dict]] = None   # TODO placeholder
    data_quality_notes: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Skill 2 — perfect foresight dispatch
# ---------------------------------------------------------------------------

@dataclass
class StrategyPnLResult:
    """P&L summary for one strategy over the full date range."""
    strategy_name: str
    pnl_market_yuan: float
    pnl_compensation_yuan: float
    pnl_total_yuan: float
    discharge_mwh: float
    charge_mwh: float
    n_days_solved: int
    granularity: str        # "hourly" or "15min"
    notes: List[str] = field(default_factory=list)


@dataclass
class PerfectForesightResult:
    """
    Output of: run_perfect_foresight_dispatch()

    Computes multi-day hourly dispatch using actual prices as input.
    P&L is settled at HOURLY granularity against hourly mean of actual 15-min prices.

    APPROXIMATION: hourly optimisation + hourly P&L may differ from 15-min DB figures.
    """
    strategy_name: str           # "perfect_foresight_hourly"
    pnl: StrategyPnLResult
    dispatch_hourly: List[dict]  # {datetime, charge_mw, discharge_mw, dispatch_grid_mw, soc_mwh}
    daily_profit: List[dict]     # {date, profit}
    energy_capacity_mwh: float
    solver_statuses: Dict[str, str] = field(default_factory=dict)  # date -> status
    caveats: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Skill 3 — forecast dispatch suite
# ---------------------------------------------------------------------------

@dataclass
class ForecastStrategyResult:
    """One forecast-model-driven dispatch strategy."""
    model_name: str              # e.g. "ols_da_time_v1", "naive_da"
    strategy_name: str           # e.g. "forecast_ols_da_time_v1"
    pnl: StrategyPnLResult
    forecast_prices_hourly: List[dict]  # {datetime, rt_pred}
    dispatch_hourly: List[dict]         # {datetime, charge_mw, discharge_mw, dispatch_grid_mw}
    daily_profit: List[dict]            # {date, profit_forecast_prices, profit_actual_prices}
    n_days_with_forecast: int
    n_days_missing_da_prices: int
    model_used_per_day: Dict[str, str] = field(default_factory=dict)  # date -> "ols" | "naive_da"
    caveats: List[str] = field(default_factory=list)


@dataclass
class ForecastDispatchSuiteResult:
    """Output of: run_forecast_dispatch_suite()"""
    strategies: List[ForecastStrategyResult] = field(default_factory=list)
    requested_models: List[str] = field(default_factory=list)
    suite_caveats: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Skill 4 — strategy ranking
# ---------------------------------------------------------------------------

@dataclass
class StrategyRankRow:
    rank: int
    strategy_name: str
    pnl_total_yuan: float
    gap_vs_perfect_foresight_yuan: Optional[float]   # PF_pnl - this_pnl (positive = worse)
    gap_vs_best_forecast_yuan: Optional[float]
    gap_vs_nominated_yuan: Optional[float]
    gap_vs_actual_yuan: Optional[float]
    capture_rate_vs_pf: Optional[float]              # pnl / pf_pnl; None if PF not available
    granularity: str                                 # "hourly" or "15min"
    data_available: bool


@dataclass
class StrategyRankingResult:
    """Output of: rank_dispatch_strategies()"""
    asset_code: str
    date_from: str
    date_to: str
    rows: List[StrategyRankRow] = field(default_factory=list)
    best_strategy: Optional[str] = None
    best_forecast_strategy: Optional[str] = None
    perfect_foresight_pnl: Optional[float] = None
    actual_pnl: Optional[float] = None
    caveats: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Skill 5 — discrepancy attribution
# ---------------------------------------------------------------------------

@dataclass
class DiscrepancyBuckets:
    """
    Waterfall attribution of the gap between perfect foresight and actual P&L.

    All values are in Yuan.  Positive = loss (i.e. actual < benchmark).
    None means the bucket could not be estimated due to missing data.

    IMPORTANT: This is a rules-based waterfall approximation, NOT causal proof.
    Buckets may overlap or leave an unexplained residual.
    See attribution_method and caveats for details.
    """
    forecast_error: Optional[float]            # PF_pnl − forecast_optimal_pnl
    asset_issue: Optional[float]               # proxy; None without outage data
    grid_restriction: Optional[float]          # PF_unrestricted − PF_grid_feasible; None if missing
    execution_nomination: Optional[float]      # forecast_optimal_pnl − nominated_pnl
    execution_clearing: Optional[float]        # nominated_pnl − actual_pnl
    residual: Optional[float]                  # total_gap − sum(explained buckets)
    total_explained: Optional[float]           # sum of non-None non-residual buckets


@dataclass
class DailyDiscrepancyRow:
    date: str
    pf_pnl: Optional[float]
    forecast_pnl: Optional[float]
    nominated_pnl: Optional[float]
    actual_pnl: Optional[float]
    forecast_error: Optional[float]
    execution_nomination: Optional[float]
    execution_clearing: Optional[float]
    residual: Optional[float]


@dataclass
class DiscrepancyAttributionResult:
    """Output of: attribute_dispatch_discrepancy()"""
    asset_code: str
    date_from: str
    date_to: str
    total_pf_pnl: Optional[float]
    total_actual_pnl: Optional[float]
    total_gap: Optional[float]              # pf_pnl - actual_pnl
    buckets: DiscrepancyBuckets = field(default_factory=lambda: DiscrepancyBuckets(
        forecast_error=None, asset_issue=None, grid_restriction=None,
        execution_nomination=None, execution_clearing=None,
        residual=None, total_explained=None,
    ))
    attribution_method: str = "rules_based_waterfall"
    daily_rows: List[DailyDiscrepancyRow] = field(default_factory=list)
    caveats: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Skill 6 — report generation
# ---------------------------------------------------------------------------

@dataclass
class PnLComparisonTable:
    """Comparable P&L across all strategies for the period."""
    headers: List[str]         # ["strategy", "pnl_yuan", "gap_vs_pf", "capture_rate"]
    rows: List[List[Any]]      # one row per strategy


@dataclass
class YTDSummary:
    asset_code: str
    year: int
    ytd_actual_pnl: Optional[float]
    ytd_pf_pnl: Optional[float]
    ytd_capture_rate: Optional[float]
    ytd_days_with_data: int
    data_through: str          # ISO date of most recent data


@dataclass
class ForecastToYearEnd:
    asset_code: str
    year: int
    realized_ytd: Optional[float]
    projected_remainder: Optional[float]    # simple run-rate extrapolation
    projected_total: Optional[float]
    projection_method: str                  # "ytd_daily_avg_run_rate"
    caveats: List[str] = field(default_factory=list)


@dataclass
class AssetStrategyReport:
    """
    Output of: generate_asset_strategy_report()

    Reusable for:
      - Streamlit app display (use 'dataframes' and 'sections')
      - Agent/API response (use 'sections' dict)
      - Scheduled markdown distribution (use 'markdown')

    Period types: "daily", "weekly", "monthly"
    """
    asset_code: str
    date_from: str
    date_to: str
    period_type: str          # "daily" | "weekly" | "monthly"
    generated_at: str         # ISO datetime

    # Structured data for rendering
    pnl_comparison: PnLComparisonTable = field(default_factory=lambda: PnLComparisonTable([], []))
    strategy_ranking: List[dict] = field(default_factory=list)
    discrepancy_waterfall: Optional[dict] = None
    ytd_summary: Optional[YTDSummary] = None
    forecast_to_year_end: Optional[ForecastToYearEnd] = None
    daily_rows: List[dict] = field(default_factory=list)
    weekly_rows: List[dict] = field(default_factory=list)
    monthly_rows: List[dict] = field(default_factory=list)

    # Report sections (text)
    sections: Dict[str, Any] = field(default_factory=dict)
    # Rendered markdown suitable for distribution
    markdown: str = ""
    # Data quality caveats carried through from context + attribution
    data_quality_caveats: List[str] = field(default_factory=list)
