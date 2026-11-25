"""Traffic-light zones in the nodal writer (2026-11-24) — no LP needed."""
import numpy as np

from services.nodal_agents import writer
from services.nodal_agents.agent import ZONE_GREEN, ZONE_NO_CHARGE, ZONE_NO_DISCHARGE


def test_zone_codes_red_is_no_charge_yellow_is_no_discharge():
    z = writer.zone_codes(["red", "yellow", "green", "unknown"])
    assert list(z) == [ZONE_NO_CHARGE, ZONE_NO_DISCHARGE, ZONE_GREEN, ZONE_GREEN]


def _fc(code):
    table = {
        "bameng": (["red"] * 8 + ["green"] * 88, [0.9] * 96),          # fleet
        "suyou": (["yellow"] * 4 + ["green"] * 92, [0.8] * 96),        # own history
        "wuchuan": (["green"] * 96, [0.0] * 96),                        # no history
    }
    return table.get(code, (["green"] * 96, [0.7] * 96))


def test_owned_station_uses_own_forecast_others_use_fleet():
    assets = [{"plant_name": "景蓝乌尔图储能电站"},      # suyou
              {"plant_name": "旭纯哈乐储能电站"},        # wuchuan account, no history
              {"plant_name": "别家储能电站"}]            # not ours
    zm, summary = writer.build_zones_map(assets, _fc)
    assert list(zm["景蓝乌尔图储能电站"][:5]) == [ZONE_NO_DISCHARGE] * 4 + [ZONE_GREEN]
    fleet = [ZONE_NO_CHARGE] * 8 + [ZONE_GREEN]
    assert list(zm["旭纯哈乐储能电站"][:9]) == fleet        # falls back to fleet
    assert list(zm["别家储能电站"][:9]) == fleet
    assert summary == {"own": 1, "fleet": 2, "fleet_red_slots": 8, "fleet_yellow_slots": 0}
    assert all(v.shape == (96,) for v in zm.values())


def test_zone_load_failure_degrades_to_green(monkeypatch, capsys):
    monkeypatch.delenv("PGURL", raising=False)
    monkeypatch.delenv("DB_DSN", raising=False)
    assert writer._load_zones_map([{"plant_name": "x"}], __import__("datetime").date(2026, 11, 25)) == {}
    assert "ZONES_ERR" in capsys.readouterr().out


# ---- operator rules for owned stations (2026-11-25) — solver stubbed --------

def test_owned_operator_params_cover_owned_accounts_only():
    p = writer.owned_operator_params()
    assert p["景蓝乌尔图储能电站"] == {"operator_rules": True, "compensation_yuan_per_mwh": 350.0}
    assert p["旭纯哈乐储能电站"]["compensation_yuan_per_mwh"] == 280.0      # post-2026 unlisted
    assert p["旭纯哈乐储能电站二"]["operator_rules"] is True
    assert "别家储能电站" not in p and len(p) == 9


def _stub_window(monkeypatch, charge=None, discharge=None, status="Optimal"):
    from types import SimpleNamespace
    from services.nodal_agents import agent
    seen = {}

    def fake(prices, **kw):
        seen.update(kw, prices=prices)
        ch = np.zeros(96) if charge is None else charge
        di = np.zeros(96) if discharge is None else discharge
        return SimpleNamespace(charge_mw=ch, discharge_mw=di, soc_mwh=np.zeros(96),
                               profit=0.0, status=status)

    monkeypatch.setattr(agent, "optimise_window", fake)
    monkeypatch.setattr(agent, "compute_dispatch_from_15min_prices",
                        lambda *a, **k: (_ for _ in ()).throw(AssertionError("spread LP used")))
    return seen


def test_owned_station_solves_with_operator_rules(monkeypatch):
    from services.nodal_agents import agent
    ch = np.zeros(96); ch[10:14] = 100.0
    di = np.zeros(96); di[70:74] = 100.0
    seen = _stub_window(monkeypatch, ch, di)
    zones = np.zeros(96, dtype=int)
    zones[0:4] = ZONE_NO_CHARGE; zones[90:96] = ZONE_NO_DISCHARGE
    asset = dict(plant_name="景蓝乌尔图储能电站", capacity_mw=100.0, duration_h=4.0,
                 rte_pct=85.0, operator_rules=True, compensation_yuan_per_mwh=350.0)
    prices = np.full(96, 200.0)
    out = agent.optimize_asset(asset, prices, np.zeros(96), cap_mw=None, zones=zones)
    assert seen["compensation_yuan_per_mwh"] == 350.0
    assert seen["max_cycles_per_day"] == 1.5 and seen["ramp_rate_pct_per_min"] == 3.3
    assert seen["dt"] == 0.25 and seen["solver_time_limit_s"] == agent.OPERATOR_SOLVE_LIMIT_S
    assert seen["no_charge_slots"] == {0, 1, 2, 3}                 # 0-based for the core
    assert seen["no_discharge_slots"] == {90, 91, 92, 93, 94, 95}
    assert (seen["prices"] == prices).all()                        # real prices, no masking
    c = out["curve"]
    assert c[10] == -100.0 and c[70] == 100.0                      # discharge - charge
    a = out["assumptions"]
    assert a["operator_rules"] is True and a["compensation_yuan_per_mwh"] == 350.0
    # 100 MWh in, 100 MWh out at a flat 200 -> market 0, compensation 100 x 350
    assert a["expected_profit_yuan"] == 35000.0


def test_no_incumbent_within_time_limit_stands_down(monkeypatch):
    from services.nodal_agents import agent
    _stub_window(monkeypatch, np.full(96, np.nan), np.full(96, np.nan), status="Not Solved")
    asset = dict(plant_name="x", capacity_mw=100.0, duration_h=4.0, rte_pct=85.0,
                 operator_rules=True, compensation_yuan_per_mwh=350.0)
    out = agent.optimize_asset(asset, np.full(96, 200.0), np.zeros(96), None,
                               np.zeros(96, dtype=int))
    assert not out["curve"].any()
    assert out["assumptions"]["solver_status"].startswith("NoIncumbent")


def test_non_owned_plant_keeps_spread_only_lp(monkeypatch):
    import pandas as pd
    from services.nodal_agents import agent
    monkeypatch.setattr(agent, "optimise_window",
                        lambda *a, **k: (_ for _ in ()).throw(AssertionError("operator LP used")))
    called = {}

    def fake(s, **kw):
        called.update(kw)
        df = pd.DataFrame({"dispatch_grid_mw": np.zeros(96), "solver_status": ["Optimal"] * 96},
                          index=s.index)
        return df, pd.Series([0.0])

    monkeypatch.setattr(agent, "compute_dispatch_from_15min_prices", fake)
    asset = dict(plant_name="别家储能电站", capacity_mw=200.0, duration_h=2.0, rte_pct=85.0)
    out = agent.optimize_asset(asset, np.full(96, 200.0), np.zeros(96), None,
                               np.zeros(96, dtype=int))
    assert "compensation_yuan_per_mwh" not in called and "max_cycles_per_day" not in called
    assert out["assumptions"]["operator_rules"] is False
