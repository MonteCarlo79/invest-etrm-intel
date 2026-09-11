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
