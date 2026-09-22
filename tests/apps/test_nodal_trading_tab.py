"""Nodal Trading tab — render smoke (AppTest, mocked loaders) + _realized_pnl units.

DB is unreachable from dev machines; every loader in nodal_trading_tab is
patched in the AppTest harness below. Harness pattern follows
tests/apps/test_committee_tab_render.py.
"""
import json
import sys
from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

REPO = Path(__file__).resolve().parents[2]
APPDIR = REPO / "apps" / "mengxi-dashboard"
for _p in (str(REPO), str(APPDIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import nodal_trading_tab as ntt  # noqa: E402

HARNESS = """
import json
import sys
from datetime import date
sys.path.insert(0, %r)
sys.path.insert(0, %r)

import pandas as pd
import nodal_trading_tab as ntt

_D = date(2026, 9, 21)
_CURVE = [20.0] * 48 + [-10.0] * 48
_ASM = {
    "plant_name": "甲站", "node": "N1", "substation": "S1", "zone": "Z1",
    "settle_node": "SN1", "capacity_mw": 100.0, "duration_h": 2.0,
    "rte_pct": 85.0, "cap_mw": 200.0, "power_mw_eff": 100.0,
    "substation_headroom_min_mw": 150.0, "zone_no_discharge_intervals": 4,
    "zone_no_charge_intervals": 2, "expected_profit_yuan": 12345.0,
    "solver_status": "Optimal", "grid_price_hat": 350.0,
    "bess_sensitivity": 0.02, "model_version": "nodal_agent_v1",
}

def _strat(e, s, en):
    return pd.DataFrame({
        "plant_name": ["甲站"], "target_date": [_D],
        "curve_json": [json.dumps(_CURVE)],
        "assumptions_json": [json.dumps(_ASM, ensure_ascii=False)],
        "iterations": [2], "convergence_delta_mwh": [0.5],
        "model_version": ["nodal_agent_v1"],
    })

ntt.load_node_registry = lambda e: pd.DataFrame({
    "name": ["N1"], "voltage_kv": [220], "substation": ["S1"],
    "transformers": [2], "rated_mva": [240.0], "n1_firm_mva": [200.0],
    "fengxing_node_name": ["FN1"], "zone": ["Z1"],
    "connected_plants": ["甲站"], "source": ["reviewed"],
})
ntt.load_asset_registry = lambda e: pd.DataFrame({
    "plant_name": ["甲站"], "node": ["N1"], "substation": ["S1"],
    "capacity_mw": [100.0], "duration_h": [2.0], "rte_pct": [85.0],
    "zone": ["Z1"], "settle_node": ["SN1"], "active": [True],
    "updated_at": pd.to_datetime(["2026-09-21 08:00:00+08:00"]),
})
ntt.load_grid_forecast = lambda e, d: {
    "price_hat": 350.0, "model_version": "nodal_agent_v1",
    "source": "L1 nodal_fc_grid_daily",
}
ntt.load_strategies = _strat
ntt.load_promote_status = lambda e: pd.DataFrame({
    "plant_name": ["甲站"], "model": ["nodal_agent_v1"],
    "status": ["champion"], "mean_capture_rate": [0.9],
    "delta_vs_champion": [0.0], "window_end": [_D],
})
ntt.load_node_actual_curve = lambda e, n, d: [300.0 + float(i) for i in range(96)]
ntt.load_node_shape = lambda e, n, d: [1.0] * 96
ntt.load_trader_attribution = lambda e, s, en: pd.DataFrame({
    "trade_date": [_D], "asset_code": ["甲站"],
    "cleared_actual_pnl": [10000.0], "grid_restriction_loss": [100.0],
    "forecast_error_loss": [200.0], "strategy_error_loss": [50.0],
})
ntt.load_price_node_map = lambda e: pd.DataFrame({
    "plant_name": ["甲站"], "node": ["N1"], "settle_node": ["SN1"],
    "fengxing_node_name": ["FN1"],
})
ntt.load_actual_price_vectors = lambda e, nodes, s, en: {
    ("FN1", "2026-09-21"): [400.0] * 96,
}
ntt.save_asset_registry_rows = lambda e, rows: len(rows)
ntt.refresh_registry_from_md = lambda e: {
    "upserted": 1, "retired": 0, "source": "md_extract_ui",
}

ntt.render(lambda: object())
""" % (str(REPO), str(APPDIR))


def _at():
    at = AppTest.from_string(HARNESS, default_timeout=60)
    at.run()
    return at


def test_tab_renders_all_sections():
    at = _at()
    assert not at.exception, f"render exception: {at.exception}"
    headers = [h.value for h in at.header]
    assert any("Forecast & Stack" in h for h in headers), headers
    assert any("Asset Agents" in h for h in headers), headers
    assert any("Trader vs Recursive-Optimal" in h for h in headers), headers
    assert any("Registry & Data" in h for h in headers), headers
    # S1 grid forecast metric rendered from the mocked L1 loader
    assert any("350.0" in str(m.value) for m in at.metric), \
        [f"{m.label}={m.value}" for m in at.metric]
    # S2 summary + S3 comparison + S4 node map all rendered as dataframes
    assert len(at.dataframe) >= 3, f"dataframes rendered: {len(at.dataframe)}"
    # promote badge from the mocked champion row appears in S2 summary
    s2 = at.dataframe[0].value
    assert "🟢 champion" in s2.to_string(), s2.to_string()


def test_refresh_button_runs_extraction():
    at = _at()
    assert not at.exception, f"render exception: {at.exception}"
    btn = [b for b in at.button if b.key == "nt_reg_refresh"]
    assert len(btn) == 1, f"refresh button missing: {[b.label for b in at.button]}"
    btn[0].click().run()
    assert not at.exception, f"exception after refresh: {at.exception}"
    assert any("upserted 1" in s.value for s in at.success), \
        [s.value for s in at.success]


def test_save_button_no_changes_is_clean():
    at = _at()
    assert not at.exception, f"render exception: {at.exception}"
    btn = [b for b in at.button if b.key == "nt_reg_save"]
    assert len(btn) == 1, f"save button missing: {[b.label for b in at.button]}"
    btn[0].click().run()
    assert not at.exception, f"exception after save: {at.exception}"
    assert any("No registry changes" in i.value for i in at.info), \
        [i.value for i in at.info]


# ------------------------------------------------------------ _realized_pnl

def test_realized_pnl_arithmetic():
    # 48 intervals discharge 100 MW @ 400, 48 intervals charge 100 MW @ 100:
    # (48*100*400 + 48*-100*100) * 0.25 = (1,920,000 - 480,000) * 0.25 = 360,000
    curve = [100.0] * 48 + [-100.0] * 48
    prices = [400.0] * 48 + [100.0] * 48
    assert ntt._realized_pnl(curve, prices) == pytest.approx(360000.0)


def test_realized_pnl_matches_writer_example():
    # writer docstring arithmetic: curve MW net x price x 0.25 per interval
    curve = [20.0] * 48 + [-10.0] * 48
    prices = [400.0] * 96
    expected = (48 * 20.0 * 400.0 + 48 * -10.0 * 400.0) * 0.25
    assert ntt._realized_pnl(curve, prices) == pytest.approx(expected)


def test_realized_pnl_rejects_bad_shapes():
    assert ntt._realized_pnl([1.0] * 95, [1.0] * 96) is None
    assert ntt._realized_pnl([1.0] * 96, [1.0] * 97) is None
    assert ntt._realized_pnl([], []) is None


def test_realized_pnl_rejects_non_finite():
    nan_curve = [float("nan")] + [1.0] * 95
    assert ntt._realized_pnl(nan_curve, [1.0] * 96) is None
    inf_prices = [float("inf")] + [1.0] * 95
    assert ntt._realized_pnl([1.0] * 96, inf_prices) is None
