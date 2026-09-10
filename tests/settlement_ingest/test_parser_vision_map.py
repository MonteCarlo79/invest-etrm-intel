"""Tests for the generic settlement category mapper in parser_vision."""
import pytest

from services.settlement_ingest.parser_vision import map_settlement_category


class TestDischargeSide:
    @pytest.mark.parametrize("label,expected", [
        ("现货", "discharge_energy"),
        ("现货交易", "discharge_energy"),
        ("上网电费", "discharge_energy"),
        ("电能电费", "discharge_energy"),
        ("电能量电费", "discharge_energy"),
        ("储能容量补偿费用", "capacity_compensation"),
        ("非市场化电费", "capacity_compensation"),
        ("容量电价补偿", "capacity_compensation"),
        ("调频", "frequency"),
        ("调频里程补偿费用", "frequency"),
        ("辅助服务费用", "frequency"),
        ("补贴", "subsidy"),
        ("偏差考核费用", "penalty"),
        ("系统运行费", "system_operation"),
        ("上网环节线损费用", "system_operation"),
        ("政府性基金及附加", "govt_surcharges"),
        ("输配电费", "transmission"),
        ("某个未知费用", "other"),
    ])
    def test_labels(self, label, expected):
        assert map_settlement_category(label, side="discharge") == expected


class TestChargeSide:
    def test_energy_label_maps_to_charge(self):
        assert map_settlement_category("电能电费", side="charge") == "charge_energy"
        assert map_settlement_category("市场化购电费", side="charge") == "charge_energy"
        assert map_settlement_category("下网电费", side="charge") == "charge_energy"

    def test_coal_capacity_beats_capacity_comp(self):
        # 燃煤/燃气容量电费 is a charge-side cost, NOT capacity compensation
        assert map_settlement_category("系统运行费(燃煤容量电费)", side="charge") == "coal_capacity_charge"


class TestGuangxiGridBill:
    def test_trade_energy_is_discharge_energy_on_discharge_side(self):
        # 电网电费结算单 (scanned 灵山 2026-03..07): 交易电费 = market energy revenue
        assert map_settlement_category("交易电费", side="discharge") == "discharge_energy"

    def test_trade_energy_stays_other_on_charge_side(self):
        assert map_settlement_category("交易电费", side="charge") == "other"

    def test_tuibu_is_rebate_on_both_sides(self):
        # 退补电费 = billing adjustment/refund, not energy (灵山 charge bills: ¥2-5M/month)
        assert map_settlement_category("退补电费", side="charge") == "rebate"
        assert map_settlement_category("(16)退补电费(元)", side="charge") == "rebate"
        assert map_settlement_category("市场损益及分摊返还电费", side="discharge") == "rebate"

    def test_basic_fee(self):
        assert map_settlement_category("功率因数调整电费", side="charge") == "basic_fee"
        assert map_settlement_category("容(需)量电费", side="charge") == "basic_fee"

    def test_rebate_is_side_aware(self):
        # 退补电费 = billing adjustment/refund, not energy — rebate on both sides
        # (changed 2026-09-10: charge-side membership broke 灵山's 价差收入 identity)
        assert map_settlement_category("退补电费", side="charge") == "rebate"
        assert map_settlement_category("退补电费", side="discharge") == "rebate"
