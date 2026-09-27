# -*- coding: utf-8 -*-
"""Tests for the settlement replication engine (services/wind_settlement/replicate.py)."""
from __future__ import annotations

import pandas as pd
import pytest


def _position(rows):
    return pd.DataFrame(rows, columns=["channel", "trade_type", "volume_mwh", "energy_price", "env_value"])


class TestCfdValue:
    def test_basic(self):
        from services.wind_settlement.replicate import cfd_value

        pos = _position([
            ("省内", "多年期绿电", 20000.0, 251.4, 31.5),
            ("跨省", "蒙西送京", 15000.0, 409.169, 3.725),
        ])
        # ref 240: 20000*(251.4-240) + 15000*(409.169-240) = 228000 + 2537535
        assert cfd_value(pos, 240.0) == pytest.approx(228000.0 + 2537535.0)

    def test_empty(self):
        from services.wind_settlement.replicate import cfd_value

        assert cfd_value(_position([]), 240.0) == 0.0


class TestGreenValue:
    def test_basic(self):
        from services.wind_settlement.replicate import green_value

        pos = _position([
            ("省内", "多年期绿电", 20000.0, 251.4, 31.5),
            ("跨省", "蒙西送京", 15000.0, 409.169, 3.725),
        ])
        # 20000*31.5 + 15000*3.725 = 630000 + 55875
        assert green_value(pos) == pytest.approx(685875.0)

    def test_missing_env_treated_zero(self):
        from services.wind_settlement.replicate import green_value

        pos = _position([("省内", "年度挂牌", 1000.0, 259.0, None)])
        assert green_value(pos) == 0.0


class TestCapturePrice:
    def test_volume_weighted(self):
        from services.wind_settlement.replicate import capture_price

        iv = pd.DataFrame({
            "gen_mwh": [10.0, 20.0, 30.0],
            "rt_price": [100.0, 200.0, 300.0],
        })
        # (10*100 + 20*200 + 30*300) / 60 = 14000/60
        assert capture_price(iv) == pytest.approx(14000.0 / 60.0)

    def test_zero_generation(self):
        from services.wind_settlement.replicate import capture_price

        iv = pd.DataFrame({"gen_mwh": [0.0, 0.0], "rt_price": [100.0, 200.0]})
        assert capture_price(iv) is None


class TestIntervalEnergy:
    """md_id_cleared_energy stores MW per 15-min interval — ×0.25 → MWh."""

    def test_mw_to_mwh(self):
        from services.wind_settlement.replicate import id_cleared_to_energy

        raw = pd.DataFrame({
            "datetime": pd.to_datetime(["2026-06-01 00:15", "2026-06-01 00:30"]),
            "cleared_energy_mwh": [200.0, 240.0],
        })
        out = id_cleared_to_energy(raw)
        assert out["gen_mwh"].tolist() == [50.0, 60.0]

    def test_negative_charging_excluded_from_generation(self):
        from services.wind_settlement.replicate import id_cleared_to_energy

        raw = pd.DataFrame({
            "datetime": pd.to_datetime(["2026-06-01 00:15", "2026-06-01 00:30"]),
            "cleared_energy_mwh": [-100.0, 240.0],
        })
        out = id_cleared_to_energy(raw)
        assert out["gen_mwh"].tolist() == [0.0, 60.0]
