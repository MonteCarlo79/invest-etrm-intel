"""口径 A dedup per asset (定远 2026: 统推 statement + grid 上网 bill → RTE ~168% before the fix)."""
import pandas as pd

from libs.asset_risk.settlement.analytics import prefer_canonical_by

SELL, BUY = "放电结算: 交易中心售电侧合计", "充电结算: 交易中心购电侧合计"


def _row(asset, month, cat, vol, amt, note):
    return {"asset": asset, "month": month, "category": cat, "volume_mwh": vol, "amount_cny": amt, "notes": note}


def test_grid_bill_wins_per_side_and_per_asset():
    df = pd.DataFrame([
        _row("定远", "2026-02", "discharge_energy", 10027.82, 3601657.94, SELL),
        _row("定远", "2026-02", "charge_energy", 11871.204, -1376523.94, BUY),
        _row("定远", "2026-02", "discharge_energy", 9570.777, 3585463.46, "放电结算: 日前交易"),
        _row("定远", "2026-02", "discharge_energy", 457.043, 228306.67, "放电结算: 实时交易"),
        _row("其他", "2026-02", "discharge_energy", 500.0, 100000.0, SELL),   # another asset: exchange only
        _row("其他", "2026-02", "charge_energy", 600.0, -50000.0, BUY),
    ])
    out = prefer_canonical_by(df, "asset")
    dy = out[out.asset == "定远"]
    dis = dy[dy.category == "discharge_energy"].volume_mwh.sum()
    chg = dy[dy.category == "charge_energy"].volume_mwh.sum()
    assert abs(dis - 10027.82) < 1e-6 and abs(chg - 11871.204) < 1e-6
    assert abs(dis / chg - 0.8447) < 1e-3                      # was ~1.689 with both sources
    assert SELL not in set(dy.notes) and BUY in set(dy.notes)   # no grid charge bill → keep exchange charge
    assert len(out[out.asset == "其他"]) == 2                   # other asset untouched


def test_empty_frame():
    assert prefer_canonical_by(pd.DataFrame(columns=["asset", "month", "category", "notes"]), "asset").empty
