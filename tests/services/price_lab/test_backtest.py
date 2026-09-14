import numpy as np
import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "price_forecast_tab",
    Path(__file__).resolve().parents[3] / "apps" / "bess-map" / "price_forecast_tab.py")
pft = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pft)


def test_smape_definition():
    a = np.array([100.0, 200.0])
    p = np.array([110.0, 190.0])
    v = pft.smape(a, p)
    expect = np.mean([10/105, 10/195]) * 100
    assert abs(v - expect) < 1e-9


def test_smape_skips_zero_pairs():
    a = np.array([0.0, 200.0])
    p = np.array([0.0, 210.0])
    v = pft.smape(a, p)
    assert abs(v - (10/205*100)) < 1e-9


def _synthetic_market(net_export_mw):
    """35 full days of history + 1 target day. Demand sits between the coal and
    gas blocks so that ±2000 MW of net_export flips the marginal segment."""
    import pandas as pd
    hours = pd.date_range("2026-08-01", periods=36 * 24, freq="h")
    fund = pd.DataFrame({
        "load_d1_mw": 26000.0,
        "renewable_total_d1_mw": 5000.0,
        "bidding_space_d1_mw": 21000.0,
        "wind_d1_mw": 3000.0,
        "solar_d1_mw": 2000.0,
        "net_export_d1_mw": float(net_export_mw),
    }, index=hours)
    prices = pd.DataFrame({"rt_price": 300.0}, index=hours[: 35 * 24])
    ff = {"coal_price_yuan_t": 850.0, "gas_price_yuan_m3": 3.0,
          "fleet_segments": [
              {"fuel": "coal", "label": "coal", "capacity_mw": 20000.0,
               "heat_rate_kj_kwh": 9000.0, "vom_yuan_mwh": 15.0},   # vc ≈ 276
              {"fuel": "gas", "label": "gas", "capacity_mw": 10000.0,
               "heat_rate_kj_kwh": 7000.0, "vom_yuan_mwh": 30.0}],  # vc ≈ 569
          "effective_date": None}
    return fund, prices, ff


def test_residual_demand_exports_raise_level(monkeypatch):
    """positive net_export = outflow = ADDED demand: an exporting province must
    see a higher/equal marginal-price level than an importing one."""
    from datetime import date as _date
    target = _date(2026, 9, 5)
    levels = {}
    for name, nx in [("exporting", 2000.0), ("importing", -2000.0)]:
        fund, prices, ff = _synthetic_market(nx)
        monkeypatch.setattr(pft, "load_confirmed_fuel_fleet", lambda e, p: ff)
        monkeypatch.setattr(pft, "load_import_blocks", lambda e, p: [])
        monkeypatch.setattr(pft, "load_landing_price_latest", lambda e, p: None)
        monkeypatch.setattr(pft, "load_rt_prices", lambda e, p, s, t: prices)
        monkeypatch.setattr(pft, "load_fundamentals_d1", lambda e, p, s, t: fund)
        r = pft.run_hybrid_forecast(None, "测试省", target)
        assert not r.empty
        levels[name] = float(np.nanmean(r["level"]))
    assert levels["exporting"] >= levels["importing"]
