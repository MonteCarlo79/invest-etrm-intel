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

def test_price_bands_filters_to_avg_price_metrics():
    rows = [
        dict(report_date=date(2026,8,1), direction="送出", metric_type="最高均价",
             price_yuan_kwh=0.30, total_vol_100gwh=2.0),
        dict(report_date=date(2026,8,1), direction="送出", metric_type="最低均价",
             price_yuan_kwh=0.22, total_vol_100gwh=None),
        dict(report_date=date(2026,8,1), direction="送出", metric_type="最高价",
             price_yuan_kwh=0.45, total_vol_100gwh=None),
        dict(report_date=date(2026,8,1), direction="送出", metric_type="最高电量",
             price_yuan_kwh=0.28, total_vol_100gwh=None),
    ]
    trend = data.daily_interprov_trend(rows)
    assert len(trend) == 4                                # data level keeps all metric types
    bands = data.price_bands(trend)
    assert len(bands) == 2
    assert set(bands["metric_type"]) == {"最高均价", "最低均价"}   # 最高价/最高电量 excluded

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

def test_load_mlt_rules(tmp_path):
    f = tmp_path / "rules.md"
    f.write_text("# MLT rules\n\n- 蒙西: 85%\n- 甘肃: 80%\n")
    rules = data.load_mlt_rules(f)
    assert rules == {"蒙西": 85.0, "甘肃": 80.0}

def test_load_mlt_rules_missing_file(tmp_path):
    assert data.load_mlt_rules(tmp_path / "nope.md") == {}

def test_speculation_premium():
    assert data.speculation_premium(380.0, 330.0) == pytest.approx(50.0)
    assert data.speculation_premium(None, 330.0) is None
    assert data.speculation_premium(380.0, None) is None

def test_monthly_exported_by_sender():
    trades = [_t(send="山西", vol=100, month=date(2026,1,1)),
              _t(send="山西", vol=200, month=date(2026,2,1)),
              _t(send="甘肃", vol=50, month=date(2026,1,1))]
    pv = data.monthly_exported_by_sender(trades)
    assert pv.loc[date(2026,1,1), "山西"] == pytest.approx(0.1)
    assert pv.loc[date(2026,2,1), "山西"] == pytest.approx(0.2)
    assert pv.loc[date(2026,1,1), "甘肃"] == pytest.approx(0.05)
    assert data.monthly_exported_by_sender([]).empty


# ── A4 pattern explorer helpers ──────────────────────────────────────────────

def _snap(send="山西", recv="江苏", ttype="其他市场化交易", ch="雁淮直流",
          vol=10.0, price=330.0, subtotal=False):
    return dict(send_prov=send, recv_prov=recv, trade_type=ttype, channel=ch,
                is_subtotal=subtotal, volume_100m_kwh=vol, landing_price=price)

def test_snapshot_volume_by_trade_type_excludes_subtotals():
    rows = [_snap(ttype="其他市场化交易", vol=10.0),
            _snap(ttype="其他市场化交易", vol=5.0),
            _snap(ttype="省间绿电交易（市场化交易）", vol=2.0),
            _snap(ttype="其他市场化交易", vol=99.0, subtotal=True)]   # excluded
    out = {r["label"]: r["vol_gwh"] for r in data.snapshot_volume_by(rows, "trade_type")}
    assert out["其他市场化交易"] == pytest.approx(1500.0)     # 15 亿kWh × 100
    assert out["省间绿电交易（市场化交易）"] == pytest.approx(200.0)
    assert len(out) == 2                                     # subtotal not a label

def test_snapshot_volume_by_channel_and_pair():
    rows = [_snap(ch="雁淮直流", send="山西", recv="江苏", vol=10.0),
            _snap(ch="雁淮直流", send="山西", recv="浙江", vol=4.0),
            _snap(ch="锡泰直流", send="蒙东", recv="江苏", vol=6.0)]
    by_ch = {r["label"]: r["vol_gwh"] for r in data.snapshot_volume_by(rows, "channel")}
    assert by_ch["雁淮直流"] == pytest.approx(1400.0)
    assert by_ch["锡泰直流"] == pytest.approx(600.0)
    by_pair = {r["label"]: r["vol_gwh"] for r in data.snapshot_volume_by(rows, "pair")}
    assert by_pair["山西→江苏"] == pytest.approx(1000.0)
    assert by_pair["山西→浙江"] == pytest.approx(400.0)

def test_trades_volume_by_channel_and_pair():
    trades = [_t(vol=1000, ch=("雁淮直流",)),          # 1 GWh on 雁淮直流
              _t(vol=500, ch=("雁淮直流", "锡泰直流")),  # full vol on each (series path)
              _t(recv="上海", vol=250, ch=("锡泰直流",))]
    by_ch = {r["label"]: r["vol_gwh"] for r in data.trades_volume_by(trades, "channel")}
    assert by_ch["雁淮直流"] == pytest.approx(1.5)
    assert by_ch["锡泰直流"] == pytest.approx(0.75)
    by_pair = {r["label"]: r["vol_gwh"] for r in data.trades_volume_by(trades, "pair")}
    assert by_pair["蒙东→江苏"] == pytest.approx(1.5)
    assert by_pair["蒙东→上海"] == pytest.approx(0.25)

def test_yoy_volume_compare_inner_join_only():
    snap = [_snap(ch="雁淮直流", vol=10.0), _snap(ch="灵绍直流", vol=5.0)]
    trades = [_t(vol=1000, ch=("雁淮直流",)), _t(vol=500, ch=("锡泰直流",))]
    out = {r["label"]: r for r in data.yoy_volume_compare(snap, trades, "channel")}
    assert set(out) == {"雁淮直流"}                    # only pairings present in both
    assert out["雁淮直流"]["vol_2025_gwh"] == pytest.approx(1000.0)
    assert out["雁淮直流"]["vol_2026_gwh"] == pytest.approx(1.0)
    assert data.yoy_volume_compare(snap, [], "channel") == []

def test_snapshot_price_by_trade_type_vwap():
    rows = [_snap(ttype="其他市场化交易", vol=10.0, price=300.0),
            _snap(ttype="其他市场化交易", vol=30.0, price=400.0),
            _snap(ttype="省间绿电交易（市场化交易）", vol=5.0, price=None),  # no price
            _snap(ttype="其他市场化交易", vol=99.0, price=1.0, subtotal=True)]
    out = {r["trade_type"]: r for r in data.snapshot_price_by_trade_type(rows)}
    m = out["其他市场化交易"]
    assert m["landing_price"] == pytest.approx((300*10 + 400*30)/40)   # VWAP 375
    assert m["vol_gwh"] == pytest.approx(4000.0)    # volume counts all non-subtotal rows
    g = out["省间绿电交易（市场化交易）"]
    assert g["landing_price"] is None and g["vol_gwh"] == pytest.approx(500.0)

# ── MLT explorer filter ──────────────────────────────────────────────────────

def _ft(send="山西", recv="江苏", ch=("雁淮直流",), period="月度",
        ms=date(2026,1,1), me=date(2026,1,31)):
    return dict(send_anchor=send, recv_province=recv, send_raw=send,
                channel_1=ch[0], channel_2=ch[1] if len(ch) > 1 else None,
                channel_3=None, period_type=period, month_start=ms, month_end=me,
                vol_post_mwh=100.0, send_price=150.0, land_price=300.0,
                jingrong_vol_mwh=None, jingrong_price=None, channel_fee=120.0)

def test_filter_trades_no_criteria_returns_all():
    trades = [_ft(), _ft(recv="上海")]
    assert data.filter_trades(trades) == trades
    assert data.filter_trades(trades, recv=[], send=[], channels=[]) == trades

def test_filter_trades_recv_send_channel_period():
    trades = [_ft(send="山西", recv="江苏", ch=("雁淮直流",), period="月度"),
              _ft(send="甘肃", recv="上海", ch=("灵绍直流",), period="年度"),
              _ft(send="山西", recv="上海", ch=("锡泰直流",), period="多月")]
    assert data.filter_trades(trades, recv=["上海"]) == trades[1:]
    assert data.filter_trades(trades, send=["山西"]) == [trades[0], trades[2]]
    # channel matches any of channel_1/2/3
    assert data.filter_trades(trades, channels=["雁淮直流"]) == [trades[0]]
    assert data.filter_trades(trades, periods=["年度", "多月"]) == trades[1:]
    # combined
    assert data.filter_trades(trades, send=["山西"], recv=["上海"]) == [trades[2]]

def test_filter_trades_channel_matches_secondary_slot():
    t = _ft(ch=("雁淮直流", "锡泰直流"))
    assert data.filter_trades([t], channels=["锡泰直流"]) == [t]

def test_filter_trades_month_range_overlap():
    jan = _ft(ms=date(2026,1,1), me=date(2026,1,31))
    year = _ft(ms=date(2026,1,1), me=date(2026,12,31), period="年度")
    jun = _ft(ms=date(2026,6,1), me=date(2026,6,30))
    trades = [jan, year, jun]
    # overlap semantics: a trade is in range if its span intersects the filter span
    assert data.filter_trades(trades, months=(date(2026,6,1), date(2026,6,30))) == [year, jun]
    assert data.filter_trades(trades, months=(date(2026,1,1), date(2026,1,31))) == [jan, year]
    assert data.filter_trades(trades, months=(date(2026,3,1), date(2026,3,31))) == [year]

# ── A5 sub-panel helpers ─────────────────────────────────────────────────────

def _flow(d=date(2026,8,3), direction="送端", metric="最高均价", prov="浙江",
          share=35.0, price=0.30, vol=2.0):
    return dict(report_date=d, direction=direction, metric_type=metric,
                province_cn=prov, province_share=share,
                price_yuan_kwh=price, price_chg_pct=None, total_vol_100gwh=vol)

def test_province_share_table_avg_and_days():
    rows = [_flow(d=date(2026,8,3), prov="浙江", share=30.0),
            _flow(d=date(2026,8,4), prov="浙江", share=40.0),
            _flow(d=date(2026,8,4), prov="江苏", share=20.0),
            _flow(d=date(2026,8,4), prov="蒙东", share=None),        # excluded
            _flow(d=date(2026,8,4), direction="受端", prov="江苏", share=50.0)]
    out = data.province_share_table(rows)
    zj = [r for r in out if r["province"] == "浙江"][0]
    assert zj["avg_share"] == pytest.approx(35.0) and zj["days"] == 2
    assert all(r["province"] != "蒙东" for r in out)
    # sorted: direction asc, then avg_share desc
    recv = [r for r in out if r["direction"] == "受端"]
    assert recv[0]["province"] == "江苏" and recv[0]["avg_share"] == pytest.approx(50.0)
    assert data.province_share_table([]) == []

def test_day_type_split_weekday_vs_weekend():
    # 2026-08-01 = Saturday, 2026-08-03 = Monday, 2026-08-04 = Tuesday
    trend = data.daily_interprov_trend([
        _flow(d=date(2026,8,1), price=0.30, vol=2.0),                 # weekend
        _flow(d=date(2026,8,3), price=0.40, vol=2.0),                 # weekday
        _flow(d=date(2026,8,4), price=0.50, vol=6.0),                 # weekday
        _flow(d=date(2026,8,4), metric="最低均价", price=0.10, vol=None),  # band excluded
    ])
    out = {(r["direction"], r["day_type"]): r for r in data.day_type_split(trend)}
    wd = out[("送端", "工作日")]
    assert wd["days"] == 2
    assert wd["avg_price"] == pytest.approx((0.40*2 + 0.50*6)/8)      # vol-weighted
    assert wd["total_vol_100gwh"] == pytest.approx(8.0)
    we = out[("送端", "周末")]
    assert we["days"] == 1 and we["avg_price"] == pytest.approx(0.30)
    assert data.day_type_split(data.daily_interprov_trend([])) == []


class TestNanTolerance:
    """DB read path via pandas turns NULL into float('nan') — and Series.map
    re-coerces None back to NaN. Aggregations must be NaN-safe regardless."""

    def _nan_row(self, vol=100.0, land=float('nan'), sendp=150.0, fee=120.0):
        return dict(send_anchor="蒙东", recv_province="江苏", vol_post_mwh=vol,
                    land_price=land, send_price=sendp, channel_fee=fee,
                    channel_1="鲁固直流", channel_2=None, channel_3=None,
                    month_start=date(2026, 1, 1), send_raw="蒙东")

    def test_per_channel_agg_nan_price_and_vol_no_crash(self):
        rows = [self._nan_row(), self._nan_row(vol=float('nan'))]
        agg = data.per_channel_agg(rows)
        assert agg["鲁固直流"]["vol_gwh"] == pytest.approx(0.1)  # NaN vol counts as 0
        assert agg["鲁固直流"]["land"] is None

    def test_aggregate_flows_nan_price_excluded_from_vwap(self):
        rows = [self._nan_row(land=300.0), self._nan_row()]
        f = data.aggregate_flows(rows)[0]
        assert f["land"] == 300                     # priced rows only
        assert f["vol_gwh"] == pytest.approx(0.2)   # unpriced volume still counts
        assert f["sendp"] == 150

    def test_aggregate_flows_all_nan_prices_gives_none(self):
        f = data.aggregate_flows([self._nan_row(sendp=float('nan'))])[0]
        assert f["land"] is None and f["sendp"] is None

    def test_pair_vwap_nan_safe(self):
        rows = [self._nan_row(land=300.0), self._nan_row()]
        assert data._pair_vwap(rows, "蒙东", "江苏", "land_price") == 300


class TestProvinceShareWhitelist:
    def test_garbage_province_names_dropped_with_report(self):
        rows = [
            dict(direction="受端", province_cn="上海", province_share=45.1, report_date=date(2026,8,1)),
            dict(direction="受端", province_cn="请上海", province_share=39.0, report_date=date(2026,8,2)),
            dict(direction="受端", province_cn="四川主网", province_share=36.9, report_date=date(2026,8,1)),
            dict(direction="受端", province_cn="河北南网", province_share=32.0, report_date=date(2026,8,1)),
        ]
        out, dropped = data.province_share_table(rows)
        provs = {r["province"] for r in out}
        assert provs == {"上海", "四川主网", "河北南网"}
        assert dropped == ["请上海"]

    def test_all_31_provinces_pass(self):
        rows = [dict(direction="送端", province_cn=p, province_share=10.0,
                     report_date=date(2026,8,1)) for p in ["甘肃","青海","宁夏","新疆","蒙西","蒙东","冀北","京津唐"]]
        out, dropped = data.province_share_table(rows)
        assert len(out) == 8 and dropped == []
