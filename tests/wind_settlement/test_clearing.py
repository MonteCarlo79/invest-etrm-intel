# -*- coding: utf-8 """Tests for the exchange daily-clearing parser (日清算)."""
from __future__ import annotations

from pathlib import Path

import pytest

CLEARING_FILE = (
    Path(__file__).resolve().parents[2]
    / "data/raw/零碳46交易数据&月度复盘/零碳46交易数据（2026年1~7月）/日清算202601~202608.xlsx"
)


@pytest.fixture(scope="module")
def clearing():
    from services.wind_settlement.clearing import parse_clearing_file

    return parse_clearing_file(CLEARING_FILE)


class TestParse:
    def test_row_count(self, clearing):
        assert len(clearing) == 25632

    def test_datetime_combined(self, clearing):
        """日期 + 时刻 combine into 15-min timestamps (00:00 = 24:00 convention)."""
        assert clearing["datetime"].min() == pd.Timestamp("2026-01-01 00:00")
        assert clearing["datetime"].is_monotonic_increasing
        assert not clearing["datetime"].duplicated().any()
        # 96 intervals per day
        per_day = clearing.groupby(clearing["datetime"].dt.date).size()
        assert (per_day == 96).all()

    def test_interval_identity(self, clearing):
        """电能电费 = 计量×RT + 合约×(合约价 − ref): backed-out ref must be
        finite wherever contract volume exists."""
        d = clearing[clearing["contract_mwh"] > 0]
        ref = d["contract_price"] - (d["energy_fee"] - d["metered_mwh"] * d["rt_nodal_price"]) / d["contract_mwh"]
        assert ref.notna().all()
        jan = d[d["datetime"] < "2026-02-01"]
        jan_ref = (ref[jan.index] * d["contract_mwh"]).sum() / jan["contract_mwh"].sum()
        # Jan effective ref ≈ 呼包以东 341.8 (±10%)
        assert jan_ref == pytest.approx(341.8, rel=0.10)


class TestMonthlyAggregate:
    def test_metered_matches_bill(self, clearing):
        from services.wind_settlement.clearing import monthly_metered

        m = monthly_metered(clearing)
        # bill volumes (rm_settlement_items book 6)
        bill = {"2026-01": 94713.0, "2026-02": 75831.7, "2026-03": 69240.9,
                "2026-04": 72529.7, "2026-05": 83282.8, "2026-06": 61116.0,
                "2026-07": 47804.8}
        for month, vol in bill.items():
            assert m.loc[month, "metered_mwh"] == pytest.approx(vol, rel=0.001)

    def test_curve_min_is_interval_min(self, clearing):
        """曲线合理度取小值 = min(合约电量, 计量电量) per interval."""
        d = clearing.dropna(subset=["contract_mwh", "metered_mwh", "curve_min"])
        expect = d[["contract_mwh", "metered_mwh"]].min(axis=1)
        assert (d["curve_min"] - expect).abs().max() < 0.01


import pandas as pd  # noqa: E402  (module-level for fixture type hints)
