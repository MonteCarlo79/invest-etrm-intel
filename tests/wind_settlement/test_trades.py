# -*- coding: utf-8 -*-
"""Tests for the 零碳46 trades Excel parser (services/wind_settlement/trades.py)."""
from __future__ import annotations

from pathlib import Path

import pytest

TRADES_DIR = (
    Path(__file__).resolve().parents[2]
    / "data/raw/零碳46交易数据&月度复盘/零碳46交易数据（2026年1~7月）"
)


@pytest.fixture(scope="module")
def july_intra():
    from services.wind_settlement.trades import parse_trades_file

    return parse_trades_file(TRADES_DIR / "3.省内成交单分月总表/省内-全月成交单--2026-07.xlsx")


@pytest.fixture(scope="module")
def jan_intra():
    from services.wind_settlement.trades import parse_trades_file

    return parse_trades_file(TRADES_DIR / "3.省内成交单分月总表/省内-全月成交单-2026-01.xlsx")


@pytest.fixture(scope="module")
def july_cross():
    from services.wind_settlement.trades import parse_trades_file

    return parse_trades_file(TRADES_DIR / "2.跨省成交单分月总表/跨省-全月成交单-2026-07.xlsx")


class TestParseIntra:
    def test_july_row_count(self, july_intra):
        assert len(july_intra) == 6124

    def test_july_grand_total_row(self, july_intra):
        """全部交易品种/汇总 row = net monthly position 35,075.3 MWh @ 264.6."""
        gt = july_intra[july_intra["energy_kind"] == "汇总"]
        gt = gt[gt["trade_type"].str.startswith("全部交易品种")]
        assert len(gt) == 1
        row = gt.iloc[0]
        assert row["volume_mwh"] == pytest.approx(35075.3, abs=0.1)
        assert row["energy_price"] == pytest.approx(264.6, abs=0.05)

    def test_july_metadata(self, july_intra):
        assert set(july_intra["delivery_month"]) == {"2026-07"}
        assert set(july_intra["channel"]) == {"省内"}

    def test_jan_green_trade_fields(self, jan_intra):
        """Jan row for 多年期绿电协商: 20,000.208 MWh @251.4, env 31.5."""
        rows = jan_intra[
            jan_intra["trade_type"].str.startswith("多年期绿电")
            & (jan_intra["energy_kind"] == "正常")
        ]
        assert len(rows) == 1
        row = rows.iloc[0]
        assert row["volume_mwh"] == pytest.approx(20000.208, abs=0.001)
        assert row["energy_price"] == pytest.approx(251.4)
        assert row["env_value"] == pytest.approx(31.5)

    def test_energy_kinds_present(self, july_intra):
        assert {"正常", "发电置换", "用电置换", "汇总"} <= set(july_intra["energy_kind"])


class TestParseCross:
    def test_july_single_row(self, july_cross):
        assert len(july_cross) == 1
        row = july_cross.iloc[0]
        assert row["volume_mwh"] == pytest.approx(15648.698, abs=0.001)
        assert row["energy_price"] == pytest.approx(409.169, abs=0.001)
        assert row["env_value"] == pytest.approx(3.725, abs=0.001)

    def test_july_metadata(self, july_cross):
        assert set(july_cross["delivery_month"]) == {"2026-07"}
        assert set(july_cross["channel"]) == {"跨省"}


class TestMonthlyPosition:
    """Net contract position per month = 省内 汇总 rows + all 跨省 rows."""

    def test_july_total_matches_deck_position(self, july_intra, july_cross):
        """35,075.3 + 15,648.698 = 50,724 MWh ≈ 106% of July bill volume."""
        from services.wind_settlement.trades import monthly_position

        pos = monthly_position(july_intra, july_cross)
        assert pos["volume_mwh"].sum() == pytest.approx(50724.0, abs=1.0)

    def test_position_uses_summary_rows_only_for_intra(self, july_intra, july_cross):
        from services.wind_settlement.trades import monthly_position

        pos = monthly_position(july_intra, july_cross)
        intra = pos[pos["channel"] == "省内"]
        # one row per trade_type, volumes from 汇总 rows
        green = intra[intra["trade_type"].str.startswith("多年期绿电")]
        assert len(green) == 1
        assert green.iloc[0]["volume_mwh"] == pytest.approx(20000.2, abs=0.1)
