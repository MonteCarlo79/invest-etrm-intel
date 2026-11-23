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
