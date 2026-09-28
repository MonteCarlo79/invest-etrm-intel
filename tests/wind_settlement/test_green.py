# -*- coding: utf-8 -*-
"""Tests for the Σmin green-premium model (合约曲线 vs 实际计量 per-interval min)."""
from __future__ import annotations

import pandas as pd
import pytest


def _position(rows):
    return pd.DataFrame(rows, columns=["channel", "trade_type", "volume_mwh", "energy_price", "env_value"])


def _intervals(gen_by_hour):
    idx = pd.date_range("2026-06-01", periods=len(gen_by_hour), freq="15min")
    return pd.DataFrame({"datetime": idx, "gen_mwh": gen_by_hour})


class TestGreenCovered:
    def test_contract_always_excess_covers_actual(self):
        """Contract flat rate >> actual → covered = Σ actual."""
        from services.wind_settlement.replicate import green_covered

        pos = _position([("省内", "多年期绿电(20250101-20361231)", 10000.0, 251.4, 31.5)])
        iv = _intervals([1.0] * 96)  # 96 MWh total, tiny vs contract
        cov = green_covered(pos, iv, "2026-06")
        assert cov["covered_mwh"].sum() == pytest.approx(96.0)

    def test_contract_always_short_covers_contract(self):
        """Actual >> contract flat rate → covered = Σ contract."""
        from services.wind_settlement.replicate import green_covered

        pos = _position([("省内", "多年期绿电(20250101-20361231)", 10.0, 251.4, 31.5)])
        iv = _intervals([50.0] * 96)  # 4800 MWh actual, huge vs 10 MWh contract
        cov = green_covered(pos, iv, "2026-06")
        assert cov["covered_mwh"].sum() == pytest.approx(10.0)

    def test_mixed_intervals_take_min(self):
        from services.wind_settlement.replicate import green_covered

        # contract 10 MWh over 30 days → flat 10/(30*96) per 15min; June has 2880 intervals
        # but our iv has 96 rows — function uses month length for flat rate, so use June-sized frame
        idx = pd.date_range("2026-06-01", periods=2880, freq="15min")
        gen = [0.0] * 2880
        # make half the intervals very high, half zero: covered = full contract only if
        # flat rate < high intervals... choose contract 28.8 MWh → flat 0.01 MWh/interval
        gen = [1.0] * 1440 + [0.0] * 1440
        iv = pd.DataFrame({"datetime": idx, "gen_mwh": gen})
        pos = _position([("省内", "年度挂牌全天直线交易(20260101-20261231)", 28.8, 259.0, 31.5)])
        cov = green_covered(pos, iv, "2026-06")
        # flat rate = 28.8/2880 = 0.01 MWh per interval; high intervals min(0.01, 1.0)=0.01
        # zero intervals min(0.01, 0)=0 → covered = 1440*0.01 = 14.4
        assert cov["covered_mwh"].sum() == pytest.approx(14.4)

    def test_window_trade_zero_outside_window(self):
        from services.wind_settlement.replicate import green_covered

        idx = pd.date_range("2026-06-01", periods=2880, freq="15min")
        gen = [1.0] * 2880
        iv = pd.DataFrame({"datetime": idx, "gen_mwh": gen})
        # window June 10-11 (2 days = 192 intervals): vol 19.2 MWh → 0.1 MWh/interval
        pos = _position([("省内", "月内融合交易(20260610-20260611)", 19.2, 200.0, 28.7)])
        cov = green_covered(pos, iv, "2026-06")
        assert cov["covered_mwh"].sum() == pytest.approx(19.2)  # fully covered inside window
        by_contract = cov.groupby("trade_type")["covered_mwh"].sum().iloc[0]
        assert by_contract == pytest.approx(19.2)

    def test_env_allocation_by_rate_share(self):
        from services.wind_settlement.replicate import green_premium

        idx = pd.date_range("2026-06-01", periods=2880, freq="15min")
        gen = [0.02] * 1440 + [0.0] * 1440  # 28.8 MWh actual
        iv = pd.DataFrame({"datetime": idx, "gen_mwh": gen})
        pos = _position([
            ("省内", "年度挂牌全天直线交易(20260101-20261231)", 14.4, 259.0, 30.0),
            ("省内", "多年期绿电(20250101-20361231)", 14.4, 251.4, 10.0),
        ])
        # both flat 0.005/interval → covered per interval min(0.01, 0.02)=0.01 split 50/50
        # covered each = 7.2 MWh → green = 7.2*30 + 7.2*10 = 288
        assert green_premium(pos, iv, "2026-06") == pytest.approx(288.0)
