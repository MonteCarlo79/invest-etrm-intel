"""Canonical-source dedup for settlement analytics (口径 A).

The 统推/交易中心结算单 restates the same physical energy as the grid
电费账单 (discharge) and 下网明细 (charge). Its 售电侧合计 is the NET
settlement (verified 定远 2026-09: 3,006,685.53 = 账单合计 5,426,620.62 −
购电侧 2,419,935.09), so summing both sources double-counts.
"""
import pandas as pd
import pytest

from apps.asset_risk.tab_settlement import prefer_canonical_settlements


def _row(cat, amt, notes, month="2026-09", vol=None):
    return {"category": cat, "amount_cny": amt, "notes": notes,
            "month": month, "volume_mwh": vol}


# 定远 2026-09, both sources present (real figures, complete row sets)
SEP_ROWS = [
    # canonical discharge (电费账单)
    _row("discharge_energy", 2154980.79, "放电结算: 日前交易", vol=5343.402),
    _row("discharge_energy", 1274567.68, "放电结算: 实时交易", vol=3147.608),
    _row("other", 238198.99, "放电结算: 市场运营费用"),
    _row("frequency", 152103.16, "放电结算: 两个细则费用"),
    _row("other", 1606770.00, "放电结算: 其他费用"),
    # canonical charge (下网明细 第1期+终期, all fee rows)
    _row("charge_energy", -1774985.43, "充电结算: 市场化交易电费 [第1期]", vol=5768.170),
    _row("transmission", -44.14, "充电结算: 输配电费 [第1期]"),
    _row("coal_capacity_charge", -30.87, "充电结算: 系统运行费用 [第1期]"),
    _row("govt_surcharges", -9.25, "充电结算: 政府性基金及附加 [第1期]"),
    _row("charge_energy", -644933.55, "充电结算: 市场化交易电费 [终期]", vol=4450.190),
    _row("transmission", -49.79, "充电结算: 输配电费 [终期]"),
    _row("system_operation", -6.53, "充电结算: 上网环节线损费用 [终期]"),
    _row("coal_capacity_charge", -34.76, "充电结算: 系统运行费用 [终期]"),
    _row("govt_surcharges", -10.38, "充电结算: 政府性基金及附加 [终期]"),
    _row("basic_fee", 1.96, "充电结算: 力调电费 [终期]"),
    # exchange lump rows (统推)
    _row("discharge_energy", 3006685.53, "放电结算: 交易中心售电侧合计", vol=8491.010),
    _row("charge_energy", -2419935.09, "充电结算: 交易中心购电侧合计", vol=10218.360),
]


class TestPreferCanonical:
    def test_exchange_rows_dropped_when_canonical_exists(self):
        kept, exch = prefer_canonical_settlements(pd.DataFrame(SEP_ROWS))
        assert len(exch) == 2
        assert set(exch["notes"]) == {"放电结算: 交易中心售电侧合计",
                                      "充电结算: 交易中心购电侧合计"}
        # canonical net = 5,426,620.62 − 2,420,102.74 − 其他费用? no: all rows sum
        assert kept["amount_cny"].sum() == pytest.approx(3006517.88, abs=0.01)
        # volumes no longer doubled
        assert kept.loc[kept["category"] == "discharge_energy", "volume_mwh"].sum() \
            == pytest.approx(8491.010, abs=0.001)
        assert kept.loc[kept["category"] == "charge_energy", "volume_mwh"].sum() \
            == pytest.approx(10218.360, abs=0.001)

    def test_exchange_only_month_kept(self):
        rows = [r for r in SEP_ROWS if "交易中心" in r["notes"]]
        kept, exch = prefer_canonical_settlements(pd.DataFrame(rows))
        assert len(kept) == 2 and exch.empty

    def test_sides_dedup_independently(self):
        # canonical charge missing → 购电侧合计 kept; 售电侧合计 still dropped
        rows = [r for r in SEP_ROWS if "[第1期]" not in r["notes"] and "[终期]" not in r["notes"]]
        kept, exch = prefer_canonical_settlements(pd.DataFrame(rows))
        assert "充电结算: 交易中心购电侧合计" in set(kept["notes"])
        assert "放电结算: 交易中心售电侧合计" not in set(kept["notes"])

    def test_non_exchange_books_untouched(self):
        rows = [_row("discharge_energy", 100.0, "放电结算: 现货"),
                _row("charge_energy", -50.0, "电能电费(市场化购电)")]
        kept, exch = prefer_canonical_settlements(pd.DataFrame(rows))
        assert len(kept) == 2 and exch.empty
