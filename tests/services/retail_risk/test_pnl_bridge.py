# tests/services/retail_risk/test_pnl_bridge.py
import pandas as pd
import pytest
from services.retail_risk import pnl_bridge as pb


def test_alpha_rows():
    pos = pd.DataFrame([
        {"channel": "annual", "volume_mwh": 100.0, "pv": 35000.0},       # VWAP 350
        {"channel": "monthly_auction", "volume_mwh": 50.0, "pv": 20000.0},  # VWAP 400
    ])
    out = pb.alpha_rows(pos, spot_vwap=380.0)
    a = out.set_index("channel")
    assert a.loc["annual", "alpha_cny"] == pytest.approx((380 - 350) * 100)
    assert a.loc["monthly_auction", "alpha_cny_mwh"] == pytest.approx(-20.0)


def test_trader_alpha():
    our = pd.DataFrame([{"channel": "monthly_auction", "volume_mwh": 10.0, "vwap": 370.0}])
    bench = pd.DataFrame([{"channel": "集中竞价交易", "avg_price_cny_mwh": 372.0}])
    out = pb.trader_alpha(our, bench)
    row = out.iloc[0]
    assert row["mkt_avg"] == 372.0 and row["trader_alpha_cny"] == pytest.approx(20.0)


def test_sales_alpha_and_identity():
    # retail 40000, channel fee 1000, blended benchmark 370 on 100 MWh
    alpha = pb.sales_alpha(retail_revenue_cny=40000.0, channel_fee_cny=1000.0,
                           blended_benchmark=370.0, retail_vol_mwh=100.0)
    assert alpha == pytest.approx(40000 - 1000 - 37000)
    # identity: trader + sales = retail - fee - our_cost
    trader = (372.0 - 370.0) * 100.0
    identity = trader + alpha
    assert identity == pytest.approx(40000 - 1000 - 37000 + 200)


def test_channel_fee():
    contracts = pd.DataFrame([
        {"annual_mwh": 1000.0, "price_cny_mwh": 400.0, "share_ratio": 0.9},
        {"annual_mwh": 500.0, "price_cny_mwh": 380.0, "share_ratio": None},
    ])
    # fee = revenue x (1 - share) where share present: 1000*400*0.1 = 40000
    assert pb.channel_fee(contracts) == pytest.approx(40000.0)


def test_channel_fee_uses_monthly_forecast_when_available():
    """I4: fee must be monthly, not annual — monthly_forecast month wins; annual/12
    is the fallback."""
    contracts = pd.DataFrame([
        {"annual_mwh": 1200.0, "price_cny_mwh": 400.0, "share_ratio": 0.9,
         "monthly_forecast": '{"3": 100.0}'},
    ])
    # month 3: 100 x 400 x 0.1 = 4000 (not 1200x400x0.1=48000)
    assert pb.channel_fee(contracts, month=3) == pytest.approx(4000.0)
    # fallback: annual / 12
    no_monthly = pd.DataFrame([{"annual_mwh": 1200.0, "price_cny_mwh": 400.0,
                                "share_ratio": 0.9, "monthly_forecast": None}])
    assert pb.channel_fee(no_monthly, month=3) == pytest.approx(1200.0 / 12 * 400.0 * 0.1)


def test_bridge_margin_none_when_only_spot_subtotal():
    """C4: 山东 7021's 合计 is a spot bill subtotal, not the 收益 margin — the bridge
    must report net/revenue as None (honest N/A), never double-count spot."""
    import datetime
    from unittest.mock import MagicMock, patch
    conn = MagicMock()
    with patch("services.retail_risk.pnl_bridge._invoice_by_category",
               return_value={"spot_energy": 75766581.44}), \
         patch("services.retail_risk.reconcile._invoice_totals",
               return_value={"total": None, "settled_vol": 230506.78}):
        b = pb.bridge_month(conn, 42, datetime.date(2026, 3, 1))
    assert b.net is None and b.retail_revenue is None
    assert b.spot_cost == 75766581.44          # costs still shown
    assert b.spread is None
