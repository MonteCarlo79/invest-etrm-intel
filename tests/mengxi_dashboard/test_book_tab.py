"""Trader Book tab — waterfall figure assembly + degenerate-guard predicate
(Task 6 of the trading-book plan, 2026-10-31).

Import pattern follows tests/apps/test_nodal_trading_tab.py: the app dir
(apps/mengxi-dashboard) is not a package (dash in the name, no __init__.py),
so it goes on sys.path and the tab module is imported bare. The plan's
literal sketch (`from apps.mengxi_dashboard.book_tab import ...`) cannot
resolve for the same reason.
"""
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
APPDIR = REPO / "apps" / "mengxi-dashboard"
for _p in (str(REPO), str(APPDIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from book_tab import _is_degenerate, _waterfall_fig_data  # noqa: E402


def _sample_waterfall():
    from libs.trading_book.models import Waterfall, WaterfallLine
    return Waterfall(strategy_pnl_cny=10000.0,
                     lines=(WaterfallLine("forecast_error", -500.0),
                            WaterfallLine("strategy_selection", -200.0),
                            WaterfallLine("clearing", -300.0),
                            WaterfallLine("curtailment_so", -400.0),
                            WaterfallLine("settlement_residual", -100.0)),
                     settled_pnl_cny=8500.0)


def test_waterfall_fig_has_all_lines():
    labels, values = _waterfall_fig_data(_sample_waterfall())
    assert labels[0] == "Strategy P&L" and labels[-1] == "Settled P&L"
    assert len(labels) == 7


def test_waterfall_fig_values_chain():
    # absolute endpoints + 5 signed deltas; chain must close on settled.
    labels, values = _waterfall_fig_data(_sample_waterfall())
    assert values[0] == 10000.0
    assert values[-1] == 8500.0
    assert values[1:-1] == [-500.0, -200.0, -300.0, -400.0, -100.0]
    assert values[0] + sum(values[1:-1]) == values[-1]


def test_residual_label_says_balancing():
    # Carried ruling: settlement_residual is a balancing line (absorbs
    # deliberate overlap + settlement-only items) — the copy must say
    # "balancing", never "settlement items".
    labels, _ = _waterfall_fig_data(_sample_waterfall())
    residual_label = labels[-2].lower()
    assert "balanc" in residual_label
    assert "settlement item" not in residual_label


def test_degenerate_guard_predicate():
    # Same condition as build_waterfall's loud guard: settled rows present
    # but zero attribution rows -> the tab must st.warning it.
    assert _is_degenerate(n_attr_rows=0, settled_row_count=3) is True
    assert _is_degenerate(n_attr_rows=5, settled_row_count=3) is False
    assert _is_degenerate(n_attr_rows=0, settled_row_count=0) is False


# ---------------------------------------------------------- render smoke
# Harness pattern follows tests/apps/test_nodal_trading_tab.py: AppTest runs
# in-process; pandas.read_sql is faked per source table so no DB is needed.

_HARNESS = """
import json
import sys
from datetime import date, timedelta

sys.path.insert(0, %r)
sys.path.insert(0, %r)

import pandas as pd

_N_ATTR = %d
_SETTLED_OK = %r
_D = date.today()
_DAY = _D - timedelta(days=1)
_TS = pd.date_range(f"{_DAY} 00:00", periods=96, freq="15min")
_MW = [20.0] * 48 + [-15.0] * 48
_MW2 = [19.0] * 48 + [-14.0] * 48
_PRICE = [350.0 + i for i in range(96)]
_CURVE = [25.0] * 48 + [-10.0] * 48

_BRIDGE_ROWS = pd.DataFrame({
    "data_date": [_D - timedelta(days=5), _D - timedelta(days=6)],
    "amount_cny": [800.0, -200.0],
    "accrual_grade": ["bill", "accrual"],
})
_TAB_ROWS = pd.DataFrame({
    "data_date": [_D - timedelta(days=5), _D - timedelta(days=5),
                  _D - timedelta(days=6)],
    "source": ["discharge_energy", "charge_energy", "lump_accrual"],
    "accrual_grade": ["bill", "bill", "accrual"],
    "amount_cny": [600.0, 200.0, -200.0],
})
_ATTR_AGG = pd.DataFrame({
    "n_rows": [_N_ATTR],
    "strategy_pnl": [10000.0 if _N_ATTR else None],
    "forecast_error_loss": [500.0 if _N_ATTR else None],
    "strategy_selection": [-200.0 if _N_ATTR else None],
    "execution_clearing_loss": [300.0 if _N_ATTR else None],
    "realisation_gap": [400.0 if _N_ATTR else None],
})

# Task 14 backtest section: per-day ladder rows (4 covered days).
_BT_DAYS = pd.date_range((_D - timedelta(days=4)).isoformat(), periods=4,
                         freq="D")
_BT_ROWS = pd.DataFrame({
    "trade_date": _BT_DAYS,
    "pf_unrestricted_pnl": [1100.0, 1200.0, 1300.0, 1400.0],
    "pf_grid_feasible_pnl": [1000.0, 1000.0, 1000.0, 1000.0],
    "tt_forecast_optimal_pnl": [950.0, 950.0, 950.0, 950.0],
    "tt_strategy_pnl": [900.0, 900.0, 900.0, 900.0],
    "nominated_pnl": [850.0, 850.0, 850.0, 850.0],
    "cleared_actual_pnl": [800.0, 800.0, 800.0, 800.0],
    "grid_restriction_loss": [100.0, 200.0, 300.0, 400.0],
    "forecast_error_loss": [50.0, 50.0, 50.0, 50.0],
    "strategy_error_loss": [50.0, 50.0, 50.0, 50.0],
    "nomination_loss": [50.0, 50.0, 50.0, 50.0],
    "execution_clearing_loss": [50.0, 50.0, 50.0, 50.0],
    "realisation_gap_vs_pf": [300.0, 400.0, 500.0, 600.0],
    "realisation_gap_vs_pf_grid": [200.0, 200.0, 200.0, 200.0],
})


def _fake_read_sql(sql, con, params=None):
    s = " ".join(str(getattr(sql, "text", sql)).split())
    if "to_regclass('reports.book_settled_daily')" in s:
        return pd.DataFrame(
            {"r": ["reports.book_settled_daily" if _SETTLED_OK else None]})
    if "FROM marketdata.md_id_cleared_energy" in s:
        return pd.DataFrame({"datetime": _TS, "cleared_price": _PRICE,
                             "cleared_energy_mwh": _MW})
    if "FROM marketdata.ops_bess_dispatch_15min" in s:
        return pd.DataFrame({"interval_start": _TS,
                             "nominated_dispatch_mw": _MW,
                             "actual_dispatch_mw": _MW2})
    if "FROM marketdata.nodal_strategy_daily" in s:
        return pd.DataFrame({"target_date": [_DAY],
                             "curve_json": [json.dumps(_CURVE)],
                             "model_version": ["nodal_agent_v1"]})
    if "SELECT COUNT(*) AS n FROM reports.bess_asset_daily_attribution" in s:
        return pd.DataFrame({"n": [_N_ATTR]})
    # Task 14 per-day ladder read — must match BEFORE the aggregate branch.
    if ("SELECT trade_date" in s
            and "FROM reports.bess_asset_daily_attribution" in s):
        return _BT_ROWS.copy()
    if "FROM reports.bess_asset_daily_attribution" in s:
        return _ATTR_AGG
    if "SELECT data_date, source" in s:
        return _TAB_ROWS.copy()
    if "FROM reports.book_settled_daily" in s:
        return _BRIDGE_ROWS.copy()
    raise AssertionError("unexpected query: " + s[:140])


pd.read_sql = _fake_read_sql

import book_tab
book_tab.render_book_tab(object())
"""


def _at(n_attr: int, settled_ok: bool = True):
    from streamlit.testing.v1 import AppTest
    at = AppTest.from_string(
        _HARNESS % (str(REPO), str(APPDIR), n_attr, settled_ok),
        default_timeout=60)
    at.run()
    return at


def test_tab_renders_all_sections():
    at = _at(n_attr=5)
    assert not at.exception, f"render exception: {at.exception}"
    headers = [h.value for h in at.header]
    assert any("Dispatch chain" in h for h in headers), headers
    assert any("Attribution waterfall" in h for h in headers), headers
    assert any("Settled rows" in h for h in headers), headers
    # 12-station registry behind the selectbox (Task 11 fleet extension)
    assert len(at.selectbox[0].options) == 12
    # strategy P&L metric from the mocked attribution aggregate
    assert any(m.label == "Strategy P&L" and "10,000" in m.value
               for m in at.metric), [f"{m.label}={m.value}" for m in at.metric]
    # waterfall detail table + settled rows table both rendered
    assert len(at.dataframe) >= 2
    # healthy range: no degenerate warning
    assert not at.warning, [w.value for w in at.warning]


def test_tab_warns_on_degenerate_waterfall():
    at = _at(n_attr=0)
    assert not at.exception, f"render exception: {at.exception}"
    assert any("degenerates" in w.value for w in at.warning), \
        [w.value for w in at.warning]


def test_tab_degrades_dispatch_chain_for_non_full_book():
    # Task 11: a settlement_only wind book has no dispatch chain — the tab
    # must say so (st.info) instead of dying on AssetBook's NotImplementedError.
    from libs.trading_book.registry import get_station
    at = _at(n_attr=0)
    assert not at.exception, f"render exception: {at.exception}"
    at.selectbox[0].set_value(get_station("lingt46"))
    at.run()
    assert not at.exception, f"render exception: {at.exception}"
    assert any("no dispatch-chain data path" in i.value for i in at.info), \
        [i.value for i in at.info]


def test_tab_degrades_when_settled_table_missing():
    # reports.book_settled_daily is created by Task 4's writer, not by
    # migration — before that first ops run the tab must warn (with the
    # remediation command) and still render the dispatch chain, not crash.
    at = _at(n_attr=5, settled_ok=False)
    assert not at.exception, f"render exception: {at.exception}"
    assert any("book_settled_daily" in w.value
               and "refresh_settled_daily" in w.value
               for w in at.warning), [w.value for w in at.warning]
    headers = [h.value for h in at.header]
    assert any("Dispatch chain" in h for h in headers), headers
    assert not any("Attribution waterfall" in h for h in headers), headers


# ---------------------------------------------------------- backtest (Task 14)

def test_tab_renders_backtest_section():
    at = _at(n_attr=5)
    assert not at.exception, f"render exception: {at.exception}"
    headers = [h.value for h in at.header]
    assert any("Backtest" in h for h in headers), headers
    # summary strip: Σ(nominated − tt_strategy) = 4 × (850 − 900) = −200
    assert any(m.label.startswith("Strategy-selection cost")
               and "-200" in m.value
               for m in at.metric), [f"{m.label}={m.value}" for m in at.metric]
    # capture = Σnominated / Σpf_grid_feasible = 3400 / 4000 = 85.0%
    assert any(m.label.startswith("Capture rate") and "85.0" in m.value
               for m in at.metric), [f"{m.label}={m.value}" for m in at.metric]
    # ladder table joins the waterfall detail + settled tables
    assert len(at.dataframe) >= 3
    # coverage caption: 4 covered days out of the 31-day default range
    assert any("Coverage:" in c.value and "4" in c.value
               for c in at.caption), [c.value for c in at.caption]


def test_tab_backtest_renders_without_settled_table():
    # The backtest reads only bess_asset_daily_attribution — it sits before
    # the settled-table gate and must render even when that gate fires.
    at = _at(n_attr=5, settled_ok=False)
    assert not at.exception, f"render exception: {at.exception}"
    headers = [h.value for h in at.header]
    assert any("Backtest" in h for h in headers), headers


def test_tab_backtest_info_for_settlement_only():
    # settlement_only books have no dispatch chain — the backtest section
    # must say so (st.info) instead of querying the attribution ladder.
    from libs.trading_book.registry import get_station
    at = _at(n_attr=5)
    assert not at.exception, f"render exception: {at.exception}"
    at.selectbox[0].set_value(get_station("wuhai"))
    at.run()
    assert not at.exception, f"render exception: {at.exception}"
    assert any("backtest needs the dispatch chain" in i.value
               for i in at.info), [i.value for i in at.info]


def test_tab_backtest_skipped_for_registry_only():
    # registry_only books skip the backtest section entirely (convention).
    from libs.trading_book.registry import get_station
    at = _at(n_attr=5)
    at.selectbox[0].set_value(get_station("wuchuan"))
    at.run()
    assert not at.exception, f"render exception: {at.exception}"
    headers = [h.value for h in at.header]
    assert not any("Backtest" in h for h in headers), headers
