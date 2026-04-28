"""
libs/decision_models/tests/test_strategy_comparison.py

Unit tests for the strategy comparison workflow.
No DB or external dependencies — pure logic.

Tests cover:
  - Skill 2: run_perfect_foresight_dispatch (with synthetic context)
  - Skill 3: run_forecast_dispatch_suite (mocked forecast + dispatch)
  - Skill 4: rank_dispatch_strategies
  - Skill 5: attribute_dispatch_discrepancy
  - Skill 6: generate_asset_strategy_report (report shape + sections)
  - Schemas: dataclass round-trip via dataclasses.asdict
"""
from __future__ import annotations

import dataclasses
from datetime import date, timedelta
from typing import Any, Dict, List

import pytest

from libs.decision_models.schemas.strategy_comparison import (
    AssetMetadata,
    DiscrepancyBuckets,
    StrategyComparisonContext,
    StrategyPnLResult,
    StrategyRankRow,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_prices_hourly(date_from: date, date_to: date, low: float = 30.0, high: float = 120.0) -> List[dict]:
    """Generate synthetic hourly prices: low for hours 0-7, high for 8-15, low rest."""
    records = []
    d = date_from
    while d <= date_to:
        for h in range(24):
            price = high if 8 <= h < 16 else low
            records.append({
                "datetime": f"{d}T{h:02d}:00:00",
                "price": price,
            })
        d += timedelta(days=1)
    return records


def _make_prices_15min(date_from: date, date_to: date) -> List[dict]:
    """Generate synthetic 15-min prices."""
    records = []
    d = date_from
    while d <= date_to:
        for h in range(24):
            price = 120.0 if 8 <= h < 16 else 30.0
            for q in range(4):
                records.append({
                    "time": f"{d}T{h:02d}:{q*15:02d}:00",
                    "price": price,
                })
        d += timedelta(days=1)
    return records


def _make_dispatch_15min(date_from: date, date_to: date, charge_h: range, discharge_h: range) -> List[dict]:
    records = []
    d = date_from
    while d <= date_to:
        for h in range(24):
            mw = 100.0 if h in discharge_h else (-100.0 if h in charge_h else 0.0)
            for q in range(4):
                records.append({
                    "time": f"{d}T{h:02d}:{q*15:02d}:00",
                    "dispatch_mw": mw,
                })
        d += timedelta(days=1)
    return records


def _make_context(
    asset_code: str = "suyou",
    date_from: str = "2026-03-01",
    date_to: str = "2026-03-03",
    include_nominated: bool = True,
    include_actual: bool = True,
) -> Dict[str, Any]:
    d_from = date.fromisoformat(date_from)
    d_to = date.fromisoformat(date_to)

    prices_hourly = _make_prices_hourly(d_from, d_to)
    prices_15min = _make_prices_15min(d_from, d_to)
    nominated = _make_dispatch_15min(d_from, d_to, range(0, 8), range(8, 16)) if include_nominated else None
    actual = _make_dispatch_15min(d_from, d_to, range(0, 8), range(8, 14)) if include_actual else None

    meta = AssetMetadata(
        asset_code=asset_code,
        display_name="SuYou",
        power_mw=100.0,
        duration_h=2.0,
        roundtrip_eff=0.85,
        compensation_yuan_per_mwh=350.0,
        province="Mengxi",
    )
    ctx = StrategyComparisonContext(
        asset_code=asset_code,
        date_from=date_from,
        date_to=date_to,
        asset_metadata=meta,
        actual_prices_15min=prices_15min,
        actual_prices_hourly=prices_hourly,
        da_prices_hourly=[],   # empty — will trigger naive_da fallback in forecast
        nominated_dispatch_15min=nominated,
        actual_dispatch_15min=actual,
        available_scenarios=["nominated_dispatch", "cleared_actual"],
        data_quality_notes=["test: synthetic data — no DB queries"],
    )
    return dataclasses.asdict(ctx)


# ---------------------------------------------------------------------------
# Schema / dataclass tests
# ---------------------------------------------------------------------------

class TestSchemas:
    def test_asset_metadata_round_trip(self):
        meta = AssetMetadata(
            asset_code="suyou", display_name="SuYou",
            power_mw=100.0, duration_h=2.0, roundtrip_eff=0.85,
            compensation_yuan_per_mwh=350.0, province="Mengxi",
        )
        d = dataclasses.asdict(meta)
        assert d["asset_code"] == "suyou"
        assert d["power_mw"] == 100.0

    def test_strategy_pnl_result_fields(self):
        pnl = StrategyPnLResult(
            strategy_name="test",
            pnl_market_yuan=1000.0,
            pnl_compensation_yuan=500.0,
            pnl_total_yuan=1500.0,
            discharge_mwh=10.0,
            charge_mwh=9.0,
            n_days_solved=3,
            granularity="hourly",
        )
        d = dataclasses.asdict(pnl)
        assert d["pnl_total_yuan"] == 1500.0

    def test_discrepancy_buckets_all_none(self):
        b = DiscrepancyBuckets(
            forecast_error=None, asset_issue=None, grid_restriction=None,
            execution_nomination=None, execution_clearing=None,
            residual=None, total_explained=None,
        )
        d = dataclasses.asdict(b)
        assert all(v is None for v in d.values())

    def test_context_round_trip(self):
        ctx = _make_context()
        assert ctx["asset_code"] == "suyou"
        assert len(ctx["actual_prices_hourly"]) == 3 * 24  # 3 days × 24 hours


# ---------------------------------------------------------------------------
# Skill 2: Perfect foresight dispatch
# ---------------------------------------------------------------------------

class TestRunPerfectForesightDispatch:
    def test_returns_expected_keys(self):
        import libs.decision_models.bess_dispatch_simulation_multiday  # noqa
        from libs.decision_models.workflows.strategy_comparison import run_perfect_foresight_dispatch

        ctx = _make_context()
        result = run_perfect_foresight_dispatch(ctx)
        assert "pnl" in result
        assert "dispatch_hourly" in result
        assert "daily_profit" in result
        assert "caveats" in result

    def test_pnl_positive(self):
        import libs.decision_models.bess_dispatch_simulation_multiday  # noqa
        from libs.decision_models.workflows.strategy_comparison import run_perfect_foresight_dispatch

        ctx = _make_context()
        result = run_perfect_foresight_dispatch(ctx)
        pnl = result["pnl"]
        # With low/high price pattern and 100MW/2h battery there should be positive profit
        assert pnl["pnl_market_yuan"] >= 0.0

    def test_n_days_solved(self):
        import libs.decision_models.bess_dispatch_simulation_multiday  # noqa
        from libs.decision_models.workflows.strategy_comparison import run_perfect_foresight_dispatch

        ctx = _make_context(date_from="2026-03-01", date_to="2026-03-03")
        result = run_perfect_foresight_dispatch(ctx)
        assert result["pnl"]["n_days_solved"] == 3

    def test_empty_prices_returns_empty(self):
        from libs.decision_models.workflows.strategy_comparison import run_perfect_foresight_dispatch

        ctx = _make_context()
        ctx["actual_prices_hourly"] = []
        ctx["actual_prices_15min"] = []
        result = run_perfect_foresight_dispatch(ctx)
        assert result["pnl"]["n_days_solved"] == 0

    def test_granularity_is_15min_when_prices_available(self):
        import libs.decision_models.bess_dispatch_simulation_multiday  # noqa
        from libs.decision_models.workflows.strategy_comparison import run_perfect_foresight_dispatch

        ctx = _make_context()
        result = run_perfect_foresight_dispatch(ctx)
        # PF prefers 15-min prices (true upper bound) when available
        assert result["pnl"]["granularity"] == "15min"


# ---------------------------------------------------------------------------
# Skill 4: Rank strategies
# ---------------------------------------------------------------------------

class TestRankDispatchStrategies:
    def _build_pf(self, pnl_total: float) -> Dict[str, Any]:
        return {
            "pnl": {
                "strategy_name": "perfect_foresight_hourly",
                "pnl_total_yuan": pnl_total,
                "n_days_solved": 3,
                "granularity": "hourly",
            },
            "dispatch_hourly": [],
            "caveats": [],
        }

    def _build_forecast_suite(self, pnl_total: float) -> Dict[str, Any]:
        return {
            "strategies": [{
                "model_name": "ols_da_time_v1",
                "strategy_name": "forecast_ols_da_time_v1",
                "pnl": {
                    "strategy_name": "forecast_ols_da_time_v1",
                    "pnl_total_yuan": pnl_total,
                    "n_days_solved": 3,
                    "granularity": "hourly",
                },
                "n_days_with_forecast": 3,
                "n_days_missing_da_prices": 0,
                "caveats": [],
            }],
            "requested_models": ["ols_da_time_v1"],
            "suite_caveats": [],
        }

    def test_pf_ranks_first(self):
        from libs.decision_models.workflows.strategy_comparison import rank_dispatch_strategies

        # No dispatch in context so inline calc yields 0 for nominated/actual
        ctx = _make_context(include_nominated=False, include_actual=False)
        pf = self._build_pf(pnl_total=10_000.0)
        suite = self._build_forecast_suite(pnl_total=8_000.0)
        result = rank_dispatch_strategies(ctx, pf, suite)

        rows = result["rows"]
        available = [r for r in rows if r["data_available"]]
        assert available[0]["strategy_name"] == "perfect_foresight_hourly"

    def test_gap_vs_pf_is_zero_for_pf_itself(self):
        from libs.decision_models.workflows.strategy_comparison import rank_dispatch_strategies

        ctx = _make_context()
        pf = self._build_pf(pnl_total=10_000.0)
        suite = self._build_forecast_suite(pnl_total=8_000.0)
        result = rank_dispatch_strategies(ctx, pf, suite)

        pf_row = next(r for r in result["rows"] if r["strategy_name"] == "perfect_foresight_hourly")
        assert pf_row["gap_vs_perfect_foresight_yuan"] == pytest.approx(0.0, abs=1.0)

    def test_forecast_gap_positive(self):
        from libs.decision_models.workflows.strategy_comparison import rank_dispatch_strategies

        ctx = _make_context()
        pf = self._build_pf(pnl_total=10_000.0)
        suite = self._build_forecast_suite(pnl_total=7_000.0)
        result = rank_dispatch_strategies(ctx, pf, suite)

        fc_row = next(r for r in result["rows"] if r["strategy_name"] == "forecast_ols_da_time_v1")
        assert fc_row["gap_vs_perfect_foresight_yuan"] == pytest.approx(3_000.0, abs=1.0)

    def test_unavailable_strategy_excluded_from_rank(self):
        from libs.decision_models.workflows.strategy_comparison import rank_dispatch_strategies

        ctx = _make_context()
        pf = self._build_pf(pnl_total=10_000.0)
        empty_suite = {
            "strategies": [{
                "model_name": "ols_da_time_v1",
                "strategy_name": "forecast_ols_da_time_v1",
                "pnl": {"pnl_total_yuan": 0.0, "n_days_solved": 0, "granularity": "hourly"},
                "n_days_with_forecast": 0,
                "caveats": [],
            }],
            "requested_models": ["ols_da_time_v1"],
            "suite_caveats": [],
        }
        result = rank_dispatch_strategies(ctx, pf, empty_suite)
        fc_row = next(r for r in result["rows"] if r["strategy_name"] == "forecast_ols_da_time_v1")
        assert not fc_row["data_available"]

    def test_best_strategy_set(self):
        from libs.decision_models.workflows.strategy_comparison import rank_dispatch_strategies

        ctx = _make_context(include_nominated=False, include_actual=False)
        pf = self._build_pf(pnl_total=10_000.0)
        suite = self._build_forecast_suite(pnl_total=8_000.0)
        result = rank_dispatch_strategies(ctx, pf, suite)
        assert result["best_strategy"] == "perfect_foresight_hourly"


# ---------------------------------------------------------------------------
# Skill 5: Attribute discrepancy
# ---------------------------------------------------------------------------

class TestAttributeDispatchDiscrepancy:
    def _make_ranking(
        self,
        pf_pnl: float,
        actual_pnl: float,
        forecast_pnl: float = 8_000.0,
        nominated_pnl: float = 7_000.0,
    ) -> Dict[str, Any]:
        return {
            "asset_code": "suyou",
            "date_from": "2026-03-01",
            "date_to": "2026-03-03",
            "perfect_foresight_pnl": pf_pnl,
            "actual_pnl": actual_pnl,
            "best_forecast_strategy": "forecast_ols_da_time_v1",
            "rows": [
                {"strategy_name": "perfect_foresight_hourly", "pnl_total_yuan": pf_pnl, "data_available": True},
                {"strategy_name": "forecast_ols_da_time_v1", "pnl_total_yuan": forecast_pnl, "data_available": True},
                {"strategy_name": "nominated_dispatch", "pnl_total_yuan": nominated_pnl, "data_available": True},
                {"strategy_name": "cleared_actual", "pnl_total_yuan": actual_pnl, "data_available": True},
            ],
            "caveats": [],
        }

    def test_total_gap_correct(self):
        from libs.decision_models.workflows.strategy_comparison import attribute_dispatch_discrepancy

        ctx = _make_context()
        ranking = self._make_ranking(pf_pnl=10_000.0, actual_pnl=4_000.0)
        result = attribute_dispatch_discrepancy(ctx, ranking)
        assert result["total_gap"] == pytest.approx(6_000.0, abs=1.0)

    def test_forecast_error_bucket(self):
        from libs.decision_models.workflows.strategy_comparison import attribute_dispatch_discrepancy

        ctx = _make_context()
        ranking = self._make_ranking(pf_pnl=10_000.0, actual_pnl=4_000.0, forecast_pnl=8_000.0)
        result = attribute_dispatch_discrepancy(ctx, ranking)
        assert result["buckets"]["forecast_error"] == pytest.approx(2_000.0, abs=1.0)

    def test_execution_nomination_bucket(self):
        from libs.decision_models.workflows.strategy_comparison import attribute_dispatch_discrepancy

        ctx = _make_context()
        ranking = self._make_ranking(
            pf_pnl=10_000.0, actual_pnl=4_000.0,
            forecast_pnl=8_000.0, nominated_pnl=6_000.0,
        )
        result = attribute_dispatch_discrepancy(ctx, ranking)
        assert result["buckets"]["execution_nomination"] == pytest.approx(2_000.0, abs=1.0)

    def test_execution_clearing_bucket(self):
        from libs.decision_models.workflows.strategy_comparison import attribute_dispatch_discrepancy

        ctx = _make_context()
        ranking = self._make_ranking(
            pf_pnl=10_000.0, actual_pnl=4_000.0,
            forecast_pnl=8_000.0, nominated_pnl=6_000.0,
        )
        result = attribute_dispatch_discrepancy(ctx, ranking)
        assert result["buckets"]["execution_clearing"] == pytest.approx(2_000.0, abs=1.0)

    def test_residual_is_nonnegative_with_full_ladder(self):
        from libs.decision_models.workflows.strategy_comparison import attribute_dispatch_discrepancy

        ctx = _make_context()
        # PF=10k, forecast=8k, nominated=6k, actual=4k → all explained, residual=0
        ranking = self._make_ranking(
            pf_pnl=10_000.0, actual_pnl=4_000.0,
            forecast_pnl=8_000.0, nominated_pnl=6_000.0,
        )
        result = attribute_dispatch_discrepancy(ctx, ranking)
        assert result["buckets"]["residual"] == pytest.approx(0.0, abs=1.0)

    def test_asset_issue_always_none(self):
        from libs.decision_models.workflows.strategy_comparison import attribute_dispatch_discrepancy

        ctx = _make_context()
        ranking = self._make_ranking(pf_pnl=10_000.0, actual_pnl=5_000.0)
        result = attribute_dispatch_discrepancy(ctx, ranking)
        assert result["buckets"]["asset_issue"] is None

    def test_attribution_method_is_waterfall(self):
        from libs.decision_models.workflows.strategy_comparison import attribute_dispatch_discrepancy

        ctx = _make_context()
        ranking = self._make_ranking(pf_pnl=10_000.0, actual_pnl=5_000.0)
        result = attribute_dispatch_discrepancy(ctx, ranking)
        assert result["attribution_method"] == "rules_based_waterfall"


# ---------------------------------------------------------------------------
# Skill 6: Generate report
# ---------------------------------------------------------------------------

class TestGenerateAssetStrategyReport:
    def _make_ranking_dict(self) -> Dict[str, Any]:
        return {
            "asset_code": "suyou",
            "date_from": "2026-03-01",
            "date_to": "2026-03-03",
            "perfect_foresight_pnl": 10_000.0,
            "actual_pnl": 6_000.0,
            "best_forecast_strategy": "forecast_ols_da_time_v1",
            "rows": [
                {"rank": 1, "strategy_name": "perfect_foresight_hourly", "pnl_total_yuan": 10_000.0,
                 "gap_vs_perfect_foresight_yuan": 0.0, "gap_vs_best_forecast_yuan": 2_000.0,
                 "gap_vs_nominated_yuan": None, "gap_vs_actual_yuan": 4_000.0,
                 "capture_rate_vs_pf": 1.0, "granularity": "hourly", "data_available": True},
                {"rank": 2, "strategy_name": "cleared_actual", "pnl_total_yuan": 6_000.0,
                 "gap_vs_perfect_foresight_yuan": 4_000.0, "gap_vs_best_forecast_yuan": None,
                 "gap_vs_nominated_yuan": None, "gap_vs_actual_yuan": 0.0,
                 "capture_rate_vs_pf": 0.6, "granularity": "15min", "data_available": True},
            ],
            "caveats": [],
        }

    def _make_attribution_dict(self) -> Dict[str, Any]:
        return {
            "asset_code": "suyou",
            "date_from": "2026-03-01",
            "date_to": "2026-03-03",
            "total_pf_pnl": 10_000.0,
            "total_actual_pnl": 6_000.0,
            "total_gap": 4_000.0,
            "buckets": {
                "forecast_error": 2_000.0,
                "asset_issue": None,
                "grid_restriction": None,
                "execution_nomination": 1_000.0,
                "execution_clearing": 1_000.0,
                "residual": 0.0,
                "total_explained": 4_000.0,
            },
            "attribution_method": "rules_based_waterfall",
            "daily_rows": [],
            "caveats": [],
        }

    def test_report_has_expected_sections(self):
        from libs.decision_models.workflows.strategy_comparison import generate_asset_strategy_report

        ctx = _make_context()
        result = generate_asset_strategy_report(
            asset_code="suyou",
            date_from="2026-03-01",
            date_to="2026-03-03",
            period_type="daily",
            context=ctx,
            ranking=self._make_ranking_dict(),
            attribution=self._make_attribution_dict(),
        )
        assert "sections" in result
        sections = result["sections"]
        assert "executive_summary" in sections
        assert "strategy_ranking" in sections
        assert "discrepancy_waterfall" in sections

    def test_markdown_non_empty(self):
        from libs.decision_models.workflows.strategy_comparison import generate_asset_strategy_report

        ctx = _make_context()
        result = generate_asset_strategy_report(
            asset_code="suyou",
            date_from="2026-03-01",
            date_to="2026-03-03",
            period_type="monthly",
            context=ctx,
            ranking=self._make_ranking_dict(),
            attribution=self._make_attribution_dict(),
        )
        assert len(result["markdown"]) > 100
        assert "suyou" in result["markdown"]

    def test_pnl_comparison_table_structure(self):
        from libs.decision_models.workflows.strategy_comparison import generate_asset_strategy_report

        ctx = _make_context()
        result = generate_asset_strategy_report(
            asset_code="suyou",
            date_from="2026-03-01",
            date_to="2026-03-03",
            period_type="monthly",
            context=ctx,
            ranking=self._make_ranking_dict(),
            attribution=self._make_attribution_dict(),
        )
        pnl_table = result["pnl_comparison"]
        assert "headers" in pnl_table
        assert "rows" in pnl_table
        assert len(pnl_table["headers"]) > 0

    def test_period_type_stored(self):
        from libs.decision_models.workflows.strategy_comparison import generate_asset_strategy_report

        ctx = _make_context()
        for period in ["daily", "weekly", "monthly"]:
            result = generate_asset_strategy_report(
                asset_code="suyou",
                date_from="2026-03-01",
                date_to="2026-03-03",
                period_type=period,
                context=ctx,
                ranking=self._make_ranking_dict(),
                attribution=self._make_attribution_dict(),
            )
            assert result["period_type"] == period

    def test_data_quality_caveats_propagated(self):
        from libs.decision_models.workflows.strategy_comparison import generate_asset_strategy_report

        ctx = _make_context()
        ctx["data_quality_notes"] = ["test: synthetic data — no DB queries"]
        result = generate_asset_strategy_report(
            asset_code="suyou",
            date_from="2026-03-01",
            date_to="2026-03-03",
            period_type="monthly",
            context=ctx,
            ranking=self._make_ranking_dict(),
            attribution=self._make_attribution_dict(),
        )
        assert any("synthetic" in c for c in result["data_quality_caveats"])


# ---------------------------------------------------------------------------
# Cleared energy unit semantics
# ---------------------------------------------------------------------------

# Synthetic helper — one cleared energy record with correct units
def _make_cleared_energy_record(
    dt: str = "2026-03-01T08:00:00",
    mwh_15min: float = 12.5,
    price: float = 280.0,
) -> dict:
    """
    Build a synthetic id_cleared_energy_15min record with explicit unit fields.
    cleared_power_mw_implied_15min = mwh_15min / 0.25
    """
    return {
        "datetime": dt,
        "dispatch_unit_name": "景蓝乌尔图储能电站",
        "cleared_energy_mwh_15min": mwh_15min,
        "cleared_power_mw_implied_15min": mwh_15min / 0.25,
        "cleared_price": price,
    }


class TestIDClearedEnergy:
    """Unit-semantics tests for DA cleared energy (marketdata.md_id_cleared_energy)."""

    def test_context_schema_has_id_cleared_energy_field(self):
        """StrategyComparisonContext must expose id_cleared_energy_15min (default None)."""
        ctx = _make_context()
        assert "id_cleared_energy_15min" in ctx
        # Default is None — test context has no DB and no cleared energy
        assert ctx["id_cleared_energy_15min"] is None

    def test_cleared_energy_unit_field_naming(self):
        """Cleared energy records must use *_mwh_15min and *_mw_implied naming,
        never dispatch_mw — these are distinct energy concepts."""
        rec = _make_cleared_energy_record(mwh_15min=10.0)
        assert "cleared_energy_mwh_15min" in rec, (
            "unit field must be named cleared_energy_mwh_15min, not dispatch_mw"
        )
        assert "cleared_power_mw_implied_15min" in rec
        assert "dispatch_mw" not in rec, (
            "cleared energy must NOT use the dispatch_mw field name"
        )
        assert "cleared_price" in rec

    def test_implied_power_formula(self):
        """cleared_power_mw_implied_15min must equal cleared_energy_mwh_15min / 0.25."""
        mwh = 12.5
        rec = _make_cleared_energy_record(mwh_15min=mwh)
        expected_mw = mwh / 0.25  # = 50.0
        assert rec["cleared_power_mw_implied_15min"] == pytest.approx(expected_mw)

    def test_cleared_energy_and_actual_dispatch_are_independent_fields(self):
        """id_cleared_energy_15min and actual_dispatch_15min must be separate,
        independently nullable context fields."""
        ctx = _make_context(include_actual=True)
        # Both fields must exist
        assert "id_cleared_energy_15min" in ctx
        assert "actual_dispatch_15min" in ctx
        # Nulling cleared energy must not affect actual_dispatch
        ctx["id_cleared_energy_15min"] = None
        assert ctx["actual_dispatch_15min"] is not None

    def test_attribution_caveats_include_cleared_vs_actual_note(self):
        """When id_cleared_energy_15min is present, attribution caveats must
        note that cleared trading energy ≠ actual physical dispatch."""
        from libs.decision_models.workflows.strategy_comparison import attribute_dispatch_discrepancy

        ctx = _make_context()
        ctx["id_cleared_energy_15min"] = [_make_cleared_energy_record()]

        ranking = {
            "asset_code": "suyou",
            "date_from": "2026-03-01",
            "date_to": "2026-03-03",
            "perfect_foresight_pnl": 10_000.0,
            "actual_pnl": 6_000.0,
            "best_forecast_strategy": None,
            "rows": [
                {"strategy_name": "perfect_foresight_hourly", "pnl_total_yuan": 10_000.0, "data_available": True},
                {"strategy_name": "cleared_actual", "pnl_total_yuan": 6_000.0, "data_available": True},
            ],
            "caveats": [],
        }
        result = attribute_dispatch_discrepancy(ctx, ranking)
        caveats_text = " ".join(result["caveats"]).lower()
        assert "cleared" in caveats_text, (
            "attribution caveats must reference cleared energy when id_cleared_energy_15min is present"
        )
        assert "actual" in caveats_text, (
            "attribution caveats must contrast cleared vs actual dispatch"
        )
