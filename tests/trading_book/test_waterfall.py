"""DB-free unit tests for the waterfall assembler — line ordering, exact-sum
identity, residual balancing. build_waterfall itself runs against live RDS
via callers; only its SQL shape and the pure month-span helper are pinned here.
"""
from datetime import date

from libs.trading_book.waterfall import _ATTRIBUTION_SQL, _map_lines, _month_periods


def test_lines_sum_to_settled():
    w = _map_lines(strategy_pnl=10000.0, forecast_err=-500.0, strat_sel=-200.0,
                   clearing=-300.0, curtail=-400.0, settled=8500.0)
    names = [l.name for l in w.lines]
    assert names == ["forecast_error", "strategy_selection", "clearing",
                     "curtailment_so", "settlement_residual"]
    total = w.strategy_pnl_cny + sum(l.delta_cny for l in w.lines)
    assert abs(total - w.settled_pnl_cny) < 1e-6


def test_residual_is_balancing():
    w = _map_lines(strategy_pnl=10000.0, forecast_err=-500.0, strat_sel=0.0,
                   clearing=0.0, curtail=0.0, settled=9000.0)
    resid = [l for l in w.lines if l.name == "settlement_residual"][0]
    assert resid.delta_cny == -500.0


def test_month_periods_spans_year_boundary():
    assert _month_periods(date(2026, 11, 15), date(2027, 2, 1)) == \
        ["2026-11", "2026-12", "2027-01", "2027-02"]
    assert _month_periods(date(2026, 10, 1), date(2026, 10, 31)) == ["2026-10"]


def test_sql_targets_daily_attribution():
    assert "bess_asset_daily_attribution" in _ATTRIBUTION_SQL
    assert "asset_code" in _ATTRIBUTION_SQL and "trade_date" in _ATTRIBUTION_SQL
    assert "realisation_gap_vs_pf_grid" in _ATTRIBUTION_SQL
