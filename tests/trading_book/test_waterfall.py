"""DB-free unit tests for the waterfall assembler — line ordering, exact-sum
identity, residual balancing, attribution-code alias, and the zero-attribution
loud guard. build_waterfall itself runs against live RDS via callers; here it
is driven with a monkeypatched read_sql + stub book (no DB).
"""
from datetime import date

import pandas as pd

from libs.trading_book.models import Settlement
from libs.trading_book.registry import get_station
from libs.trading_book.waterfall import (
    _ATTRIBUTION_SQL,
    _attribution_code,
    _attribution_params,
    _map_lines,
    _month_periods,
    build_waterfall,
)


class _StubBook:
    """Duck-typed AssetBook: registry station + canned settled daily rows."""

    def __init__(self, asset_code, settled_daily):
        self.station = get_station(asset_code)
        self._daily = settled_daily

    def settlement(self, period):
        return Settlement(
            period=period,
            settled_cny=sum(a for _, a, _ in self._daily),
            daily=self._daily)


def _agg_df(n_rows, strategy_pnl=None, forecast_error_loss=None,
            strategy_selection=None, execution_clearing_loss=None,
            realisation_gap=None):
    """One-row DataFrame shaped like the _ATTRIBUTION_SQL aggregate."""
    return pd.DataFrame([{
        "n_rows": n_rows,
        "strategy_pnl": strategy_pnl,
        "forecast_error_loss": forecast_error_loss,
        "strategy_selection": strategy_selection,
        "execution_clearing_loss": execution_clearing_loss,
        "realisation_gap": realisation_gap,
    }])


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


def test_attribution_code_alias():
    # 景怡查干哈达储能电站: registry 'bameng' == attribution-stack 'hetao'.
    assert _attribution_code("bameng") == "hetao"
    for code in ("suyou", "hangjinqi", "siziwangqi", "gushanliang", "wulate"):
        assert _attribution_code(code) == code
    params = _attribution_params(_StubBook("bameng", []),
                                 date(2026, 10, 1), date(2026, 10, 31))
    assert params == {"ac": "hetao", "s": "2026-10-01", "e": "2026-10-31"}


def test_build_waterfall_passes_alias_to_sql(monkeypatch):
    captured = {}

    def fake_read_sql(sql, engine, params=None):
        captured.update(params or {})
        return _agg_df(n_rows=1, strategy_pnl=10.0)

    monkeypatch.setattr("libs.trading_book.waterfall.pd.read_sql", fake_read_sql)
    w = build_waterfall(engine=None, book=_StubBook("bameng", []),
                        start=date(2026, 10, 1), end=date(2026, 10, 31))
    assert captured["ac"] == "hetao"
    assert w.strategy_pnl_cny == 10.0


def test_zero_attribution_with_settled_warns(monkeypatch, capsys):
    monkeypatch.setattr("libs.trading_book.waterfall.pd.read_sql",
                        lambda *a, **k: _agg_df(n_rows=0))
    book = _StubBook("bameng", [(date(2026, 10, 5), 100.0, "bill")])
    w = build_waterfall(engine=None, book=book,
                        start=date(2026, 10, 1), end=date(2026, 10, 31))
    out = capsys.readouterr().out
    assert "WARNING" in out and "hetao" in out and "bameng" in out
    # Degenerate shape: strategy_pnl=0, deltas=0, residual=settled.
    assert w.strategy_pnl_cny == 0.0 and w.settled_pnl_cny == 100.0
    resid = [l for l in w.lines if l.name == "settlement_residual"][0]
    assert resid.delta_cny == 100.0
    assert sum(l.delta_cny for l in w.lines[:4]) == 0.0


def test_no_warning_when_attribution_present(monkeypatch, capsys):
    monkeypatch.setattr(
        "libs.trading_book.waterfall.pd.read_sql",
        lambda *a, **k: _agg_df(n_rows=3, strategy_pnl=10.0))
    book = _StubBook("suyou", [(date(2026, 10, 5), 9.0, "bill")])
    build_waterfall(engine=None, book=book,
                    start=date(2026, 10, 1), end=date(2026, 10, 31))
    assert "WARNING" not in capsys.readouterr().out
