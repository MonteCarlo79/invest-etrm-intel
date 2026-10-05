import numpy as np

from . import compute as c


def test_toll_variance_is_effectively_zero():
    m = c.merchant_pnl()
    t = c.toll_pnl(1.0 * m.mean())
    assert t.var() / m.var() < 0.05


def test_merchant_mean_and_strip_consistency():
    m = c.merchant_pnl()
    assert abs(m.mean() - 156_800) < 2_000
    assert np.quantile(m, 0.05) < m.mean() - 20_000   # real left tail


def test_deal_zone_ordering():
    m = c.merchant_pnl()
    strip = m.mean()
    fixed_om = 0.60 * strip
    assert fixed_om < strip                              # seller floor < buyer ceiling
    # a toll below strip transfers expected value to the seller at zero variance:
    t = c.toll_pnl(0.85 * strip)
    assert t.mean() < strip and t.std() < 0.15 * m.std()
