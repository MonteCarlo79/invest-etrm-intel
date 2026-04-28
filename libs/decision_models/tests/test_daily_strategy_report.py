"""
libs/decision_models/tests/test_daily_strategy_report.py

Unit tests for the daily strategy report workflow.
No DB or external dependencies — pure logic with mocked calls.

Tests cover:
  - run_bess_daily_strategy_analysis: output structure, ops enrichment, error handling
  - run_all_assets_daily_strategy_analysis: all 4 assets, partial failure handling
  - generate_bess_daily_strategy_report: markdown, html, pdf fallback
  - render_bess_strategy_dashboard_payload: required keys, chart data
  - _enrich_context_with_ops_dispatch: ops data preferred over empty canon
  - Caveat propagation: ops source notes appear in results
"""
from __future__ import annotations

import dataclasses
from datetime import date
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from libs.decision_models.workflows.daily_strategy_report import (
    _build_dispatch_chart_data,
    _build_price_chart_data,
    _enrich_context_with_ops_dispatch,
    _fmt_pct,
    _fmt_yuan,
    generate_bess_daily_strategy_report,
    render_bess_strategy_dashboard_payload,
    run_all_assets_daily_strategy_analysis,
    run_bess_daily_strategy_analysis,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

def _make_context(
    asset_code: str = "suyou",
    date_str: str = "2026-04-17",
    with_nominated: bool = True,
    with_actual: bool = True,
) -> Dict[str, Any]:
    """Build a minimal context dict for testing (no DB required)."""
    prices_15min = [
        {"time": f"2026-04-17T{h:02d}:{q*15:02d}:00+08:00", "price": 80.0 + h}
        for h in range(24)
        for q in range(4)
    ]
    prices_hourly = [
        {"datetime": f"2026-04-17T{h:02d}:00:00+08:00", "price": 80.0 + h}
        for h in range(24)
    ]
    nominated = (
        [{"time": f"2026-04-17T{h:02d}:{q*15:02d}:00+08:00", "dispatch_mw": 50.0}
         for h in range(8, 12) for q in range(4)]
        if with_nominated else None
    )
    actual = (
        [{"time": f"2026-04-17T{h:02d}:{q*15:02d}:00+08:00", "dispatch_mw": 45.0}
         for h in range(8, 12) for q in range(4)]
        if with_actual else None
    )
    return {
        "asset_code": asset_code,
        "date_from": date_str,
        "date_to": date_str,
        "asset_metadata": {
            "asset_code": asset_code,
            "display_name": asset_code,
            "power_mw": 100.0,
            "duration_h": 2.0,
            "roundtrip_eff": 0.85,
            "compensation_yuan_per_mwh": 350.0,
            "province": "InnerMongolia",
            "source": "test",
        },
        "actual_prices_15min": prices_15min,
        "actual_prices_hourly": prices_hourly,
        "da_prices_hourly": [],
        "nominated_dispatch_15min": nominated,
        "actual_dispatch_15min": actual,
        "id_cleared_energy_15min": None,
        "available_scenarios": [],
        "outage_flags": None,
        "curtailment_flags": None,
        "data_quality_notes": ["test: synthetic context"],
    }


def _make_pf_result() -> Dict[str, Any]:
    return {
        "strategy_name": "perfect_foresight_hourly",
        "pnl": {
            "strategy_name": "perfect_foresight_hourly",
            "pnl_market_yuan": 10000.0,
            "pnl_compensation_yuan": 500.0,
            "pnl_total_yuan": 10500.0,
            "discharge_mwh": 100.0,
            "charge_mwh": 120.0,
            "n_days_solved": 1,
            "granularity": "hourly",
            "notes": [],
        },
        "dispatch_hourly": [],
        "daily_profit": [{"date": "2026-04-17", "profit": 10500.0}],
        "energy_capacity_mwh": 200.0,
        "solver_statuses": {"2026-04-17": "Optimal"},
        "caveats": ["perfect_foresight: hourly granularity"],
    }


def _make_forecast_suite() -> Dict[str, Any]:
    return {
        "strategies": [
            {
                "model_name": "ols_da_time_v1",
                "strategy_name": "forecast_ols_da_time_v1",
                "pnl": {
                    "strategy_name": "forecast_ols_da_time_v1",
                    "pnl_market_yuan": 8000.0,
                    "pnl_compensation_yuan": 450.0,
                    "pnl_total_yuan": 8450.0,
                    "discharge_mwh": 90.0,
                    "charge_mwh": 110.0,
                    "n_days_solved": 1,
                    "granularity": "hourly",
                    "notes": [],
                },
                "forecast_prices_hourly": [],
                "dispatch_hourly": [],
                "daily_profit": [],
                "n_days_with_forecast": 1,
                "n_days_missing_da_prices": 0,
                "model_used_per_day": {"2026-04-17": "ols_da_time_v1"},
                "caveats": [],
            }
        ],
        "requested_models": ["ols_da_time_v1"],
        "suite_caveats": ["forecast_suite: hourly granularity"],
    }


def _make_ranking() -> Dict[str, Any]:
    return {
        "asset_code": "suyou",
        "date_from": "2026-04-17",
        "date_to": "2026-04-17",
        "rows": [
            {
                "rank": 1,
                "strategy_name": "perfect_foresight_hourly",
                "pnl_total_yuan": 10500.0,
                "gap_vs_perfect_foresight_yuan": 0.0,
                "gap_vs_best_forecast_yuan": None,
                "gap_vs_nominated_yuan": None,
                "gap_vs_actual_yuan": None,
                "capture_rate_vs_pf": 1.0,
                "granularity": "hourly",
                "data_available": True,
            },
            {
                "rank": 2,
                "strategy_name": "cleared_actual",
                "pnl_total_yuan": 7000.0,
                "gap_vs_perfect_foresight_yuan": 3500.0,
                "gap_vs_best_forecast_yuan": None,
                "gap_vs_nominated_yuan": None,
                "gap_vs_actual_yuan": None,
                "capture_rate_vs_pf": 0.667,
                "granularity": "15min",
                "data_available": True,
            },
        ],
        "best_strategy": "perfect_foresight_hourly",
        "best_forecast_strategy": "forecast_ols_da_time_v1",
        "perfect_foresight_pnl": 10500.0,
        "actual_pnl": 7000.0,
        "caveats": ["ranking: hourly and 15-min not directly comparable"],
    }


def _make_attribution() -> Dict[str, Any]:
    return {
        "asset_code": "suyou",
        "date_from": "2026-04-17",
        "date_to": "2026-04-17",
        "total_pf_pnl": 10500.0,
        "total_actual_pnl": 7000.0,
        "total_gap": 3500.0,
        "buckets": {
            "forecast_error": 2050.0,
            "asset_issue": None,
            "grid_restriction": None,
            "execution_nomination": None,
            "execution_clearing": 400.0,
            "residual": 1050.0,
            "total_explained": 2450.0,
        },
        "attribution_method": "rules_based_waterfall",
        "daily_rows": [],
        "caveats": [
            "attribution: rules-based waterfall — not causal proof",
            "asset_issue: None — outage table not yet implemented",
        ],
    }


def _make_report(asset_code: str = "suyou", date_str: str = "2026-04-17") -> Dict[str, Any]:
    return {
        "asset_code": asset_code,
        "date_from": date_str,
        "date_to": date_str,
        "period_type": "daily",
        "generated_at": "2026-04-20T00:00:00+00:00",
        "pnl_comparison": {"headers": ["strategy", "total_pnl_yuan"], "rows": [["pf", "10,500"]]},
        "strategy_ranking": [],
        "discrepancy_waterfall": {"total_gap_yuan": 3500.0, "buckets": {}},
        "ytd_summary": None,
        "forecast_to_year_end": None,
        "daily_rows": [],
        "weekly_rows": [],
        "monthly_rows": [],
        "sections": {
            "executive_summary": f"Asset: {asset_code}  |  Period: {date_str}",
            "discrepancy_waterfall": {"total_gap_yuan": 3500.0, "buckets": {}},
        },
        "markdown": f"# BESS Strategy Report — {asset_code}\n**Period:** {date_str}\n",
        "data_quality_caveats": ["test caveat from context"],
    }


# ---------------------------------------------------------------------------
# Tests: run_bess_daily_strategy_analysis
# ---------------------------------------------------------------------------

class TestRunBessDailyStrategyAnalysis:

    @patch("libs.decision_models.workflows.daily_strategy_report._enrich_context_with_ops_dispatch")
    @patch("libs.decision_models.workflows.daily_strategy_report.run_all_assets_daily_strategy_analysis", create=True)
    def test_returns_expected_top_level_keys(self, *_mocks):
        """result dict must have the documented keys."""
        ctx = _make_context()
        pf = _make_pf_result()
        suite = _make_forecast_suite()
        ranking = _make_ranking()
        attribution = _make_attribution()
        report = _make_report()

        with (
            patch(
                "libs.decision_models.workflows.daily_strategy_report.load_bess_strategy_comparison_context",
                return_value=ctx,
            ),
            patch(
                "libs.decision_models.workflows.daily_strategy_report.run_perfect_foresight_dispatch",
                return_value=pf,
            ),
            patch(
                "libs.decision_models.workflows.daily_strategy_report.run_forecast_dispatch_suite",
                return_value=suite,
            ),
            patch(
                "libs.decision_models.workflows.daily_strategy_report.load_precomputed_scenario_pnl",
                return_value=(pd.DataFrame(), []),
            ),
            patch(
                "libs.decision_models.workflows.daily_strategy_report.load_precomputed_attribution",
                return_value=(pd.DataFrame(), []),
            ),
            patch(
                "libs.decision_models.workflows.daily_strategy_report.rank_dispatch_strategies",
                return_value=ranking,
            ),
            patch(
                "libs.decision_models.workflows.daily_strategy_report.attribute_dispatch_discrepancy",
                return_value=attribution,
            ),
            patch(
                "libs.decision_models.workflows.daily_strategy_report.generate_asset_strategy_report",
                return_value=report,
            ),
            patch(
                "libs.decision_models.workflows.daily_strategy_report._enrich_context_with_ops_dispatch",
                return_value=(ctx, True),
            ),
        ):
            result = run_bess_daily_strategy_analysis("suyou", "2026-04-17")

        assert "asset_code" in result
        assert "date" in result
        assert "generated_at" in result
        assert "context" in result
        assert "pf_result" in result
        assert "forecast_suite" in result
        assert "ranking" in result
        assert "attribution" in result
        assert "report" in result
        assert "ops_dispatch_available" in result

        assert result["asset_code"] == "suyou"
        assert result["date"] == "2026-04-17"

    def test_ops_dispatch_available_false_when_disabled(self):
        """use_ops_dispatch=False must result in ops_dispatch_available=False."""
        ctx = _make_context()

        with (
            patch(
                "libs.decision_models.workflows.daily_strategy_report.load_bess_strategy_comparison_context",
                return_value=ctx,
            ),
            patch(
                "libs.decision_models.workflows.daily_strategy_report.run_perfect_foresight_dispatch",
                return_value=_make_pf_result(),
            ),
            patch(
                "libs.decision_models.workflows.daily_strategy_report.run_forecast_dispatch_suite",
                return_value=_make_forecast_suite(),
            ),
            patch(
                "libs.decision_models.workflows.daily_strategy_report.load_precomputed_scenario_pnl",
                return_value=(pd.DataFrame(), []),
            ),
            patch(
                "libs.decision_models.workflows.daily_strategy_report.load_precomputed_attribution",
                return_value=(pd.DataFrame(), []),
            ),
            patch(
                "libs.decision_models.workflows.daily_strategy_report.rank_dispatch_strategies",
                return_value=_make_ranking(),
            ),
            patch(
                "libs.decision_models.workflows.daily_strategy_report.attribute_dispatch_discrepancy",
                return_value=_make_attribution(),
            ),
            patch(
                "libs.decision_models.workflows.daily_strategy_report.generate_asset_strategy_report",
                return_value=_make_report(),
            ),
        ):
            result = run_bess_daily_strategy_analysis(
                "suyou", "2026-04-17", use_ops_dispatch=False
            )

        assert result["ops_dispatch_available"] is False


# ---------------------------------------------------------------------------
# Tests: _enrich_context_with_ops_dispatch
# ---------------------------------------------------------------------------

class TestEnrichContextWithOpsDispatch:

    def test_ops_data_fills_empty_nominated(self):
        """Ops nominated data is used when canon nominated is absent."""
        ctx = _make_context(with_nominated=False, with_actual=False)
        # Remove nominated/actual so they're None
        ctx["nominated_dispatch_15min"] = None
        ctx["actual_dispatch_15min"] = None

        ops_df = pd.DataFrame({
            "interval_start": pd.to_datetime([
                "2026-04-17T08:00:00+08:00",
                "2026-04-17T08:15:00+08:00",
            ]),
            "nominated_dispatch_mw": [50.0, 60.0],
            "actual_dispatch_mw": [45.0, 55.0],
            "nodal_price_excel": [100.0, 100.0],
        })

        with patch(
            "libs.decision_models.workflows.daily_strategy_report.load_ops_dispatch_15min",
            return_value=(ops_df, ["ops_dispatch_15min: 2 rows loaded"]),
        ):
            enriched, available = _enrich_context_with_ops_dispatch(
                ctx, date(2026, 4, 17)
            )

        assert available is True
        assert enriched["nominated_dispatch_15min"] is not None
        assert len(enriched["nominated_dispatch_15min"]) == 2
        # dispatch_mw = -nominated_dispatch_mw * 0.25 (sign flip + MW→MWh)
        # 50.0 MW (charging in ops convention) → -50.0 * 0.25 = -12.5 MWh (charging in LP convention)
        assert enriched["nominated_dispatch_15min"][0]["dispatch_mw"] == pytest.approx(-12.5)

        assert enriched["actual_dispatch_15min"] is not None
        assert len(enriched["actual_dispatch_15min"]) == 2

        assert enriched["ops_dispatch_15min"] is not None

    def test_ops_data_overwrites_existing_canon(self):
        """Ops data is authoritative and ALWAYS overwrites canon data when available.

        Canon dispatch (scenario_dispatch_15min) may store values in raw MW without
        the ×0.25 MWh conversion, which would cause 4× P&L overcount and cycles=21+.
        Ops data is the direct Excel measurement and must take priority.
        """
        ctx = _make_context(with_nominated=True, with_actual=True)
        # Canon has 16 records at dispatch_mw=50.0; ops has 1 record at -999.0*0.25=-249.75
        ops_df = pd.DataFrame({
            "interval_start": pd.to_datetime(["2026-04-17T08:00:00+08:00"]),
            "nominated_dispatch_mw": [999.0],
            "actual_dispatch_mw": [888.0],
            "nodal_price_excel": [100.0],
        })

        with patch(
            "libs.decision_models.workflows.daily_strategy_report.load_ops_dispatch_15min",
            return_value=(ops_df, []),
        ):
            enriched, available = _enrich_context_with_ops_dispatch(
                ctx, date(2026, 4, 17)
            )

        # Ops data must overwrite canon — 1 ops record, not 16 canon records
        assert len(enriched["nominated_dispatch_15min"]) == 1
        # dispatch_mw = -999.0 * 0.25 = -249.75 (ops overwrite, not original canon 50.0)
        assert enriched["nominated_dispatch_15min"][0]["dispatch_mw"] == pytest.approx(-249.75)

    def test_empty_ops_df_returns_false(self):
        """Empty ops DataFrame returns available=False and does not modify context."""
        ctx = _make_context(with_nominated=False, with_actual=False)
        ctx["nominated_dispatch_15min"] = None

        with patch(
            "libs.decision_models.workflows.daily_strategy_report.load_ops_dispatch_15min",
            return_value=(pd.DataFrame(columns=[
                "interval_start", "nominated_dispatch_mw",
                "actual_dispatch_mw", "nodal_price_excel",
            ]), ["ops_dispatch_15min: no data"]),
        ):
            enriched, available = _enrich_context_with_ops_dispatch(
                ctx, date(2026, 4, 17)
            )

        assert available is False
        assert enriched["nominated_dispatch_15min"] is None

    def test_ops_source_caveat_added_to_notes(self):
        """A caveat noting the ops source must appear in data_quality_notes."""
        ctx = _make_context(with_nominated=False)
        ctx["nominated_dispatch_15min"] = None

        ops_df = pd.DataFrame({
            "interval_start": pd.to_datetime(["2026-04-17T08:00:00+08:00"]),
            "nominated_dispatch_mw": [50.0],
            "actual_dispatch_mw": [45.0],
            "nodal_price_excel": [100.0],
        })

        with patch(
            "libs.decision_models.workflows.daily_strategy_report.load_ops_dispatch_15min",
            return_value=(ops_df, ["ops_dispatch_15min: 1 rows loaded"]),
        ):
            enriched, _ = _enrich_context_with_ops_dispatch(ctx, date(2026, 4, 17))

        notes = " ".join(enriched["data_quality_notes"]).lower()
        assert "ops_bess_dispatch_15min" in notes or "marketdata" in notes


# ---------------------------------------------------------------------------
# Tests: run_all_assets_daily_strategy_analysis
# ---------------------------------------------------------------------------

class TestRunAllAssetsDailyStrategyAnalysis:

    def test_returns_all_4_assets_on_success(self):
        """All 4 Inner Mongolia assets appear in asset_results."""
        single_result = {
            "asset_code": "suyou",
            "date": "2026-04-17",
            "generated_at": "2026-04-20T00:00:00+00:00",
            "context": _make_context(),
            "pf_result": _make_pf_result(),
            "forecast_suite": _make_forecast_suite(),
            "ranking": _make_ranking(),
            "attribution": _make_attribution(),
            "report": _make_report(),
            "ops_dispatch_available": True,
        }

        def _fake_run(asset_code, date, **kwargs):
            r = dict(single_result)
            r["asset_code"] = asset_code
            r["ranking"] = dict(_make_ranking())
            r["ranking"]["asset_code"] = asset_code
            r["attribution"] = _make_attribution()
            r["context"] = _make_context(asset_code=asset_code)
            return r

        with patch(
            "libs.decision_models.workflows.daily_strategy_report.run_bess_daily_strategy_analysis",
            side_effect=_fake_run,
        ):
            result = run_all_assets_daily_strategy_analysis("2026-04-17")

        assert "asset_results" in result
        assert set(result["asset_results"].keys()) == {
            "suyou", "hangjinqi", "siziwangqi", "gushanliang"
        }
        assert result["errors"] == {}

    def test_partial_failure_recorded_in_errors(self):
        """Failed assets are recorded in errors dict, not asset_results."""
        def _fake_run(asset_code, date, **kwargs):
            if asset_code == "hangjinqi":
                raise RuntimeError("DB connection failed")
            r = {
                "asset_code": asset_code,
                "date": date,
                "generated_at": "2026-04-20T00:00:00+00:00",
                "context": _make_context(asset_code=asset_code),
                "pf_result": _make_pf_result(),
                "forecast_suite": _make_forecast_suite(),
                "ranking": _make_ranking(),
                "attribution": _make_attribution(),
                "report": _make_report(asset_code=asset_code),
                "ops_dispatch_available": False,
            }
            return r

        with patch(
            "libs.decision_models.workflows.daily_strategy_report.run_bess_daily_strategy_analysis",
            side_effect=_fake_run,
        ):
            result = run_all_assets_daily_strategy_analysis("2026-04-17")

        assert "hangjinqi" in result["errors"]
        assert "hangjinqi" not in result["asset_results"]
        assert len(result["asset_results"]) == 3

    def test_summary_has_required_keys(self):
        """summary dict must contain portfolio-level aggregation keys."""
        def _fake_run(asset_code, date, **kwargs):
            return {
                "asset_code": asset_code,
                "date": date,
                "generated_at": "2026-04-20T00:00:00+00:00",
                "context": _make_context(asset_code=asset_code),
                "pf_result": _make_pf_result(),
                "forecast_suite": _make_forecast_suite(),
                "ranking": _make_ranking(),
                "attribution": _make_attribution(),
                "report": _make_report(asset_code=asset_code),
                "ops_dispatch_available": True,
            }

        with patch(
            "libs.decision_models.workflows.daily_strategy_report.run_bess_daily_strategy_analysis",
            side_effect=_fake_run,
        ):
            result = run_all_assets_daily_strategy_analysis("2026-04-17")

        summary = result["summary"]
        assert "portfolio_total_actual_pnl" in summary
        assert "portfolio_total_pf_pnl" in summary
        assert "portfolio_capture_rate" in summary
        assert "asset_rows" in summary
        assert len(summary["asset_rows"]) == 4

    def test_summary_portfolio_totals_none_when_all_pnl_none(self):
        """portfolio_total_actual_pnl/pf_pnl must be None (not 0) when all assets lack P&L data."""
        def _make_no_pnl_ranking():
            r = _make_ranking()
            r["actual_pnl"] = None
            r["perfect_foresight_pnl"] = None
            return r

        def _fake_run(asset_code, date, **kwargs):
            return {
                "asset_code": asset_code,
                "date": date,
                "generated_at": "2026-04-20T00:00:00+00:00",
                "context": _make_context(asset_code=asset_code),
                "pf_result": _make_pf_result(),
                "forecast_suite": _make_forecast_suite(),
                "ranking": _make_no_pnl_ranking(),
                "attribution": _make_attribution(),
                "report": _make_report(asset_code=asset_code),
                "ops_dispatch_available": True,
            }

        with patch(
            "libs.decision_models.workflows.daily_strategy_report.run_bess_daily_strategy_analysis",
            side_effect=_fake_run,
        ):
            result = run_all_assets_daily_strategy_analysis("2026-04-17")

        summary = result["summary"]
        assert summary["portfolio_total_actual_pnl"] is None, (
            "Expected None when all actual_pnl values are None, got "
            f"{summary['portfolio_total_actual_pnl']}"
        )
        assert summary["portfolio_total_pf_pnl"] is None
        assert summary["portfolio_capture_rate"] is None


# ---------------------------------------------------------------------------
# Tests: generate_bess_daily_strategy_report
# ---------------------------------------------------------------------------

class TestGenerateBessDailyStrategyReport:

    def _make_analysis(self, asset_code="suyou", date_str="2026-04-17"):
        return {
            "asset_code": asset_code,
            "date": date_str,
            "generated_at": "2026-04-20T00:00:00+00:00",
            "context": _make_context(asset_code=asset_code),
            "pf_result": _make_pf_result(),
            "forecast_suite": _make_forecast_suite(),
            "ranking": _make_ranking(),
            "attribution": _make_attribution(),
            "report": _make_report(asset_code=asset_code, date_str=date_str),
            "ops_dispatch_available": True,
        }

    def test_markdown_output_is_string(self):
        analysis = self._make_analysis()
        output = generate_bess_daily_strategy_report(
            "suyou", "2026-04-17", output_format="markdown", analysis=analysis
        )
        assert isinstance(output, str)
        assert len(output) > 0

    def test_markdown_contains_asset_code(self):
        analysis = self._make_analysis()
        output = generate_bess_daily_strategy_report(
            "suyou", "2026-04-17", output_format="markdown", analysis=analysis
        )
        assert "suyou" in output

    def test_html_output_is_string_with_html_tags(self):
        analysis = self._make_analysis()
        output = generate_bess_daily_strategy_report(
            "suyou", "2026-04-17", output_format="html", analysis=analysis
        )
        assert isinstance(output, str)
        assert "<html>" in output.lower() or "<body>" in output.lower() or "<pre>" in output.lower()

    def test_pdf_fallback_when_reportlab_absent(self):
        """When reportlab is not importable, output should be bytes with a fallback note."""
        import sys
        import builtins
        real_import = builtins.__import__

        def _block_reportlab(name, *args, **kwargs):
            if name == "reportlab" or name.startswith("reportlab."):
                raise ImportError(f"Mocked absence of reportlab: {name}")
            return real_import(name, *args, **kwargs)

        analysis = self._make_analysis()
        with patch("builtins.__import__", side_effect=_block_reportlab):
            output = generate_bess_daily_strategy_report(
                "suyou", "2026-04-17", output_format="pdf", analysis=analysis
            )

        assert isinstance(output, bytes)
        decoded = output.decode("utf-8")
        assert "reportlab" in decoded.lower() or "suyou" in decoded

    def test_invalid_output_format_raises(self):
        analysis = self._make_analysis()
        with pytest.raises(ValueError, match="Unknown output_format"):
            generate_bess_daily_strategy_report(
                "suyou", "2026-04-17", output_format="docx", analysis=analysis
            )


# ---------------------------------------------------------------------------
# Tests: render_bess_strategy_dashboard_payload
# ---------------------------------------------------------------------------

class TestRenderBessStrategyDashboardPayload:

    def _make_analysis(self, asset_code="suyou", date_str="2026-04-17"):
        return {
            "asset_code": asset_code,
            "date": date_str,
            "generated_at": "2026-04-20T00:00:00+00:00",
            "context": _make_context(asset_code=asset_code),
            "pf_result": _make_pf_result(),
            "forecast_suite": _make_forecast_suite(),
            "ranking": _make_ranking(),
            "attribution": _make_attribution(),
            "report": _make_report(asset_code=asset_code, date_str=date_str),
            "ops_dispatch_available": True,
        }

    def test_has_all_required_keys(self):
        analysis = self._make_analysis()
        payload = render_bess_strategy_dashboard_payload(
            "suyou", "2026-04-17", analysis=analysis
        )
        for key in [
            "asset_code", "date", "generated_at", "summary_cards",
            "strategy_table", "dispatch_chart_data", "price_chart_data",
            "waterfall_data", "pnl_comparison", "caveats", "ops_dispatch_available",
        ]:
            assert key in payload, f"Missing key: {key}"

    def test_summary_cards_is_list_of_dicts(self):
        analysis = self._make_analysis()
        payload = render_bess_strategy_dashboard_payload(
            "suyou", "2026-04-17", analysis=analysis
        )
        cards = payload["summary_cards"]
        assert isinstance(cards, list)
        assert len(cards) > 0
        for card in cards:
            assert "label" in card
            assert "value" in card

    def test_strategy_table_has_rank_column(self):
        analysis = self._make_analysis()
        payload = render_bess_strategy_dashboard_payload(
            "suyou", "2026-04-17", analysis=analysis
        )
        table = payload["strategy_table"]
        assert isinstance(table, list)
        if table:
            assert "Rank" in table[0]

    def test_waterfall_has_buckets(self):
        analysis = self._make_analysis()
        payload = render_bess_strategy_dashboard_payload(
            "suyou", "2026-04-17", analysis=analysis
        )
        wf = payload["waterfall_data"]
        assert "buckets" in wf
        assert "total_gap" in wf
        assert isinstance(wf["buckets"], list)
        bucket_labels = [b["label"] for b in wf["buckets"]]
        assert "Forecast error" in bucket_labels
        assert "Residual" in bucket_labels

    def test_caveats_propagated(self):
        """Caveats from attribution and ranking appear in the payload."""
        analysis = self._make_analysis()
        payload = render_bess_strategy_dashboard_payload(
            "suyou", "2026-04-17", analysis=analysis
        )
        caveats = payload["caveats"]
        assert isinstance(caveats, list)
        # The attribution "rules-based waterfall" caveat should appear
        all_text = " ".join(caveats).lower()
        assert "waterfall" in all_text or "causal" in all_text or "test caveat" in all_text

    def test_dispatch_chart_has_timestamps(self):
        analysis = self._make_analysis()
        payload = render_bess_strategy_dashboard_payload(
            "suyou", "2026-04-17", analysis=analysis
        )
        chart = payload["dispatch_chart_data"]
        assert "timestamps" in chart
        assert "nominated_mwh" in chart
        assert "actual_mwh" in chart


# ---------------------------------------------------------------------------
# Tests: _build_dispatch_chart_data
# ---------------------------------------------------------------------------

class TestBuildDispatchChartData:

    def test_prefers_ops_data_when_present(self):
        ctx = _make_context()
        ops_records = [
            {
                "interval_start": pd.Timestamp("2026-04-17T08:00:00+08:00"),
                "nominated_dispatch_mw": 50.0,
                "actual_dispatch_mw": 45.0,
                "nodal_price_excel": 100.0,
            },
            {
                "interval_start": pd.Timestamp("2026-04-17T08:15:00+08:00"),
                "nominated_dispatch_mw": 55.0,
                "actual_dispatch_mw": 40.0,
                "nodal_price_excel": 100.0,
            },
        ]
        ctx["ops_dispatch_15min"] = ops_records

        chart = _build_dispatch_chart_data(ctx, "2026-04-17")

        assert chart["source"] == "ops_bess_dispatch_15min"
        assert len(chart["timestamps"]) == 2
        # Values are negated and converted MW→MWh: -(50.0) * 0.25 = -12.5, -(45.0) * 0.25 = -11.25
        # Positive ops values = charging → negative LP convention (positive=discharge)
        assert chart["nominated_mwh"][0] == pytest.approx(-12.5)
        assert chart["actual_mwh"][0] == pytest.approx(-11.25)

    def test_falls_back_to_canon_when_no_ops(self):
        ctx = _make_context()
        ctx["ops_dispatch_15min"] = None

        chart = _build_dispatch_chart_data(ctx, "2026-04-17")

        assert chart["source"] == "canon.scenario_dispatch_15min"

    def test_empty_when_no_data(self):
        ctx = _make_context(with_nominated=False, with_actual=False)
        ctx["ops_dispatch_15min"] = None
        ctx["nominated_dispatch_15min"] = None
        ctx["actual_dispatch_15min"] = None

        chart = _build_dispatch_chart_data(ctx, "2026-04-17")

        assert chart["timestamps"] == []


# ---------------------------------------------------------------------------
# Tests: format helpers
# ---------------------------------------------------------------------------

class TestFormatHelpers:

    def test_fmt_yuan_formats_number(self):
        assert _fmt_yuan(10500.0) == "10,500"
        assert _fmt_yuan(0) == "0"
        assert _fmt_yuan(None) == "—"
        assert _fmt_yuan("bad") == "—"

    def test_fmt_pct_formats_number(self):
        assert _fmt_pct(0.667) == "66.7%"
        assert _fmt_pct(1.0) == "100.0%"
        assert _fmt_pct(None) == "—"
