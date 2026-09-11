# tests/interconnector/test_data.py
from datetime import date
import pytest
from services.interconnector import data

def _t(send="蒙东", recv="江苏", vol=100.0, land=300.0, sendp=150.0, fee=120.0,
       ch=("鲁固直流",), month=date(2026,1,1), raw="蒙东"):
    return dict(send_anchor=send, recv_province=recv, vol_post_mwh=vol, land_price=land,
                send_price=sendp, channel_fee=fee, channel_1=ch[0],
                channel_2=ch[1] if len(ch) > 1 else None, channel_3=None,
                month_start=month, send_raw=raw)

def test_aggregate_flows_vwap_and_volume():
    trades = [_t(vol=100, land=300), _t(vol=300, land=400), _t(recv="上海", vol=50, land=350)]
    flows = {(f["send"], f["recv"]): f for f in data.aggregate_flows(trades)}
    f = flows[("蒙东","江苏")]
    assert f["vol_gwh"] == pytest.approx(0.4)          # 400 MWh → 0.4 GWh
    assert f["land"] == 375                            # (300*100+400*300)/400
    assert f["trades"] == 2 and f["months"] == 1
    assert flows[("蒙东","上海")]["vol_gwh"] == pytest.approx(0.05)

def test_aggregate_flows_skips_nan_price_in_vwap():
    trades = [_t(vol=100, land=300), _t(vol=100, land=None)]
    f = data.aggregate_flows(trades)[0]
    assert f["land"] == 300 and f["vol_gwh"] == pytest.approx(0.2)

def test_per_channel_agg_full_volume_each_channel():
    trades = [_t(vol=100, land=300, ch=("高岭直流","锡泰直流"))]
    agg = data.per_channel_agg(trades)
    assert agg["高岭直流"]["vol_gwh"] == pytest.approx(0.1)
    assert agg["锡泰直流"]["vol_gwh"] == pytest.approx(0.1)   # full vol on each (series path)
    assert agg["锡泰直流"]["trades"] == 1

def test_monthly_volume_by_recv():
    trades = [_t(recv="江苏", vol=100, month=date(2026,1,1)),
              _t(recv="江苏", vol=200, month=date(2026,2,1)),
              _t(recv="上海", vol=50, month=date(2026,1,1))]
    pv = data.monthly_volume_by_recv(trades)
    assert pv.loc[date(2026,1,1), "江苏"] == pytest.approx(0.1)
    assert pv.loc[date(2026,2,1), "江苏"] == pytest.approx(0.2)
    assert pv.loc[date(2026,1,1), "上海"] == pytest.approx(0.05)

def test_balance_of_year_math():
    channels = [dict(name="锡泰直流", gw=10.0), dict(name="云霄直流", gw=2.0)]
    agg = {"锡泰直流": {"vol_gwh": 6139.0, "land": 346, "trades": 93}}
    out = {c["name"]: c for c in data.balance_of_year(channels, agg, date(2026,10,1))}
    xt = out["锡泰直流"]
    assert xt["capability_gwh"] == pytest.approx(10*8760)
    assert xt["traded_gwh"] == pytest.approx(6139.0)
    assert xt["utilization_pct"] == pytest.approx(100*6139/(10*8760), rel=1e-3)
    # remaining hours Oct 1 → Dec 31 = (31+30+31)*24 = 2208
    assert xt["remaining_hours"] == 2208
    assert xt["remaining_capability_gwh"] == pytest.approx(10*2208)
    yx = out["云霄直流"]
    assert yx["traded_gwh"] == 0 and yx["utilization_pct"] == 0

def test_resolve_mlt_pct_priority():
    assert data.resolve_mlt_pct("蒙西", {"蒙西": 85.0}, {"蒙西": 90.0}) == (90.0, "override")
    assert data.resolve_mlt_pct("蒙西", {"蒙西": 85.0}, {}) == (85.0, "rule")
    assert data.resolve_mlt_pct("青海", {}, {}) == (80.0, "default")

def test_year_ago_estimate():
    monthly = {date(2025,7,1): 12000.0, date(2026,7,1): 13500.0}
    assert data.year_ago_estimate(monthly, date(2026,7,1)) == 12000.0
    assert data.year_ago_estimate(monthly, date(2026,3,1)) is None

def test_recycle_gap_math():
    assert data.recycle_gap(1000, 600, 200, 300, 250) == pytest.approx(200*50)
    assert data.recycle_gap(1000, 900, 200, 300, 250) == 0        # no shortfall → 0
    assert data.recycle_gap(1000, 0, 0, None, 250) is None        # missing price → None

def test_backtest_rows_alignment_and_none_policy():
    trades = [_t(send="山西", recv="江苏", vol=100, land=380, fee=70,
                 month=date(2026,7,1))]
    trades[0]["month_end"] = date(2026,7,31)
    month_ahead = {("山西", date(2026,7,1)): 330.0, ("江苏", date(2026,7,1)): 400.0}
    spot = {("山西", date(2026,7,1)): 310.0, ("江苏", date(2026,7,1)): 420.0}
    rows = data.backtest_rows([("山西","江苏")], [date(2026,7,1)], month_ahead, spot, trades)
    r = rows[0]
    assert r["landing"] == 380 and r["premium_over_recv_spot"] == 380-420
    assert r["realized_spread"] == 420-310-70
    # month with no spot → None fields, row still present
    rows2 = data.backtest_rows([("山西","江苏")], [date(2026,8,1)], month_ahead, spot, trades)
    assert rows2[0]["recv_spot"] is None and rows2[0]["realized_spread"] is None

def test_daily_interprov_trend():
    rows = [
        dict(report_date=date(2026,8,1), direction="送出", metric_type="最高均价",
             price_yuan_kwh=0.30, total_vol_100gwh=2.0),
        dict(report_date=date(2026,8,1), direction="送出", metric_type="最高均价",
             price_yuan_kwh=0.34, total_vol_100gwh=None),
        dict(report_date=date(2026,8,1), direction="送出", metric_type="最低均价",
             price_yuan_kwh=0.22, total_vol_100gwh=None),
        dict(report_date=date(2026,8,1), direction="受入", metric_type="最高均价",
             price_yuan_kwh=0.35, total_vol_100gwh=2.0),
        dict(report_date=date(2026,8,2), direction="送出", metric_type="最高均价",
             price_yuan_kwh=0.32, total_vol_100gwh=1.0),
    ]
    df = data.daily_interprov_trend(rows)
    # same date+direction but different metric_type → separate rows, never blended
    day1_out = df[(df.report_date == date(2026,8,1)) & (df.direction == "送出")]
    assert len(day1_out) == 2
    hi = day1_out[day1_out.metric_type == "最高均价"].iloc[0]
    lo = day1_out[day1_out.metric_type == "最低均价"].iloc[0]
    assert hi["price_yuan_kwh"] == pytest.approx(0.32)   # mean within 最高均价 only
    assert lo["price_yuan_kwh"] == pytest.approx(0.22)   # not averaged with 最高均价
    # volume sums only across rows that carry it (None skipped)
    assert hi["total_vol_100gwh"] == pytest.approx(2.0)
    assert len(df) == 4
    assert data.daily_interprov_trend([]).empty

def test_green_premium():
    snap = [
        dict(send_prov="黑龙江", recv_prov="安徽", channel="雁淮直流",
             trade_type="省间绿电交易（市场化交易）", is_subtotal=False,
             volume_100m_kwh=9.07, landing_price=376.58),
        dict(send_prov="黑龙江", recv_prov="安徽", channel="雁淮直流",
             trade_type="其他市场化交易", is_subtotal=False,
             volume_100m_kwh=2.01, landing_price=334.89),
    ]
    out = data.green_premium(snap)
    assert out == [dict(send="黑龙江", recv="安徽", channel="雁淮直流",
                        green_price=376.58, other_price=334.89,
                        premium=pytest.approx(41.69))] or abs(out[0]["premium"] - 41.69) < 0.01
