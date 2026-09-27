# -*- coding: utf-8 -*-
"""Tests for wind trading tab helpers (waterfall assembly, KPI calcs)."""
from __future__ import annotations

import pandas as pd
import pytest


class TestWaterfallComponents:
    def _bill_items(self):
        rows = [
            ("discharge_energy", 47804.8, 0.2234, 10678553.86, "放电结算: 现货"),
            ("discharge_energy", None, None, 78504.03, "放电结算: 现货"),
            ("other", 44224.0, 0.0210, 931126.49, "放电结算: 填电"),
            ("capacity_compensation", None, None, -1213535.40, "放电结算: 储能容量补偿费用"),
            ("frequency", None, None, -198714.19, "放电结算: 调频"),
            ("other", None, None, -42227.50, "放电结算: 市场调节类费用"),
            ("other", None, None, -255630.33, "放电结算: 不平衡资金"),
            ("basic_fee", None, None, -4368.00, "容(需)量电费"),
        ]
        return pd.DataFrame(rows, columns=["category", "volume_mwh", "price_cny_kwh", "amount_cny", "notes"])

    def test_components_sum_to_bill_total(self):
        from services.wind_settlement.waterfall import waterfall_components

        comp = waterfall_components(
            self._bill_items(),
            spot_value=9_000_000.0,
            cfd=1_800_000.0,
            green=931_126.49,
        )
        total = sum(v for _, v in comp)
        assert total == pytest.approx(9_000_000 + 1_800_000 + 931_126.49 - 1_213_535.40 - 198_714.19 - 42_227.50 - 255_630.33 - 4_368.00)

    def test_fee_groups_collapsed(self):
        from services.wind_settlement.waterfall import waterfall_components

        comp = waterfall_components(
            self._bill_items(),
            spot_value=9_000_000.0,
            cfd=1_800_000.0,
            green=931_126.49,
        )
        labels = [lbl for lbl, _ in comp]
        assert labels[0] == "现货电能价值"
        assert "合约差价" in labels
        assert "绿电溢价" in labels
        assert "储能分摊" in labels
        assert "调频" in labels
        # 市场调节 + 不平衡资金 + 容(需)量 collapse into 其他费用
        assert "其他费用" in labels

    def test_spot_and_cfd_are_inputs_not_from_bill(self):
        """Replicated spot/cfd replace the bill 现货 row (which nets them)."""
        from services.wind_settlement.waterfall import waterfall_components

        comp = waterfall_components(
            self._bill_items(), spot_value=100.0, cfd=50.0, green=0.0,
        )
        d = dict(comp)
        assert d["现货电能价值"] == 100.0
        assert d["合约差价"] == 50.0


class TestSettlementKpis:
    def test_bill_settle_price(self):
        from services.wind_settlement.waterfall import settle_price

        bill = pd.DataFrame(
            [(47804.8, 10678553.86), (None, 78504.03)],
            columns=["volume_mwh", "amount_cny"],
        )
        # (10678553.86 + 78504.03) / 47804.8 * 1000 → 元/MWh
        assert settle_price(bill) == pytest.approx(10_757_057.89 / 47804.8, rel=1e-6)

    def test_zero_volume(self):
        from services.wind_settlement.waterfall import settle_price

        bill = pd.DataFrame([(None, 100.0)], columns=["volume_mwh", "amount_cny"])
        assert settle_price(bill) is None
