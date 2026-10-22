import datetime as dt
import pytest
from libs.interconnector_pricing import agreements as A


def test_seed_integrity():
    assert len(A.AGREEMENTS_2027) >= 13
    for a in A.AGREEMENTS_2027:
        assert a["volume_100gwh"] > 0
        assert a["resource_mix"] in ("solar", "wind", "mixed", "hydro")
        assert a["confidence"] in ("公告确认", "行业信息", "推算")
        assert a["source_type"] in ("公告", "行业信息")
    nx = [a for a in A.AGREEMENTS_2027 if a["sender"] == "宁夏" and a["receiver"] == "福建"][0]
    assert nx["volume_100gwh"] == 20 and nx["confidence"] == "公告确认"
    qh = [a for a in A.AGREEMENTS_2027 if a["sender"] == "青海" and a["receiver"] == "上海"][0]
    assert qh["volume_100gwh"] == 40 and qh["resource_mix"] == "solar"


def test_seed_shanghai_ppa_table():
    sh = [a for a in A.AGREEMENTS_2027 if a["receiver"] == "上海" and a.get("period") == "2027-2029"]
    assert len(sh) == 9
    assert all(a["confidence"] == "公告确认" for a in sh)
    by_sender = {}
    for a in sh:
        by_sender.setdefault(a["sender"], 0.0)
        by_sender[a["sender"]] += a["volume_100gwh"]
    assert by_sender["甘肃"] == 50.0     # 庆东20 + 宝合20 + 甘浙10
    assert by_sender["陕西"] == 20.0     # 宝合
    assert by_sender["山西"] == 10.0     # 雁淮
    assert by_sender["黑龙江"] == 10.0   # 高岭+锡泰
    assert by_sender["西藏"] == 7.0      # 柴拉+宝合, 6-10月
    assert by_sender["宁夏"] == 15.0     # 威越(甘浙)
    assert by_sender["吉林"] == 3.0      # 高岭+锡泰
    xz = [a for a in sh if a["sender"] == "西藏"][0]
    assert xz["months"] == [6, 7, 8, 9, 10] and xz["resource_mix"] == "hydro"
    assert xz["path_id"] is None         # 西藏 unpriced → no_path scoring


def test_reference_landed_uses_engine():
    a = {"sender": "甘肃", "receiver": "上海", "path_id": "西北—宝合—华东",
         "channel_label": "宝合直流", "volume_100gwh": 20, "resource_mix": "solar"}
    r = A.reference_landed(a, green_total=240.0, env_price=5.0)
    assert r.ok and r.landed == pytest.approx(370.357208258708, abs=1e-9)


def test_monthly_avg_rt_profile_filter():
    rows = [(dt.datetime(2026, 3, 5, 12), 400.0), (dt.datetime(2026, 3, 5, 22), 600.0),
            (dt.datetime(2026, 4, 9, 11), 500.0)]
    assert A.monthly_avg_rt(rows) == [("2026-03", 500.0), ("2026-04", 500.0)]
    assert A.monthly_avg_rt(rows, hours={10, 11, 12}) == [("2026-03", 400.0), ("2026-04", 500.0)]
    assert A.monthly_avg_rt(rows, hours={22}) == [("2026-03", 600.0)]


def test_solar_contract_window_per_month():
    # 2026-2028 PPA contract window: 9–17点 base; Jul–Sep adds hour 18 (延至19点)
    assert A.profile_hours("solar") == {9, 10, 11, 12, 13, 14, 15, 16, 17}
    assert 18 not in A.profile_hours_for_month("solar", 6)
    assert 18 in A.profile_hours_for_month("solar", 7)
    assert 18 in A.profile_hours_for_month("solar", 8)
    assert 18 in A.profile_hours_for_month("solar", 9)
    assert A.profile_hours_for_month("mixed", 8) is None
    rows = [(dt.datetime(2026, 7, 10, 18), 800.0), (dt.datetime(2026, 7, 10, 12), 400.0),
            (dt.datetime(2026, 6, 10, 18), 900.0), (dt.datetime(2026, 6, 10, 12), 500.0)]
    out = A.monthly_avg_rt(rows, hours=lambda m: A.profile_hours_for_month("solar", m))
    assert out == [("2026-06", 500.0), ("2026-07", 600.0)]  # Jul hour-18 counts, Jun's excluded


def test_monthly_avg_rt_missing_months_no_crash():
    rows = [(dt.datetime(2026, 1, 5, 12), 300.0), (dt.datetime(2026, 5, 5, 12), 700.0)]
    out = A.monthly_avg_rt(rows)
    assert out == [("2026-01", 300.0), ("2026-05", 700.0)]


def test_congestion_scores_order_and_no_path():
    ags = [
        {"sender": "甲", "path_id": "西北—宝合—华东", "channel_label": "c1", "volume_100gwh": 10},
        {"sender": "乙", "path_id": None, "channel_label": "c2", "volume_100gwh": 99},
        {"sender": "丙", "path_id": "西北—灵绍—华东", "channel_label": "c3", "volume_100gwh": 5},
    ]
    spreads = {0: 50.0, 2: 100.0}   # index-keyed; 1 has no spread (no path)
    out = A.congestion_scores(ags, spreads)
    assert out[0]["channel_label"] == "c3" and out[0]["score"] == 500.0
    assert out[1]["channel_label"] == "c1" and out[1]["score"] == 500.0
    assert out[2]["no_path"] is True and out[2]["score"] is None


def test_undershoot_flags():
    ags = [{"channel_label": "c1", "volume_100gwh": 100},
           {"channel_label": "c2", "volume_100gwh": 100}]
    flags = A.undershoot_flags({"c1": 70.0, "c2": 90.0}, ags)
    assert flags == {0: True, 1: False}


def test_seasonality_shares():
    hist = [(f"2025-{m:02d}", 10.0 if m == 6 else 5.0) for m in range(1, 13)] + \
           [(f"2026-{m:02d}", 10.0 if m == 6 else 5.0) for m in range(1, 13)]
    shares = A.seasonality_shares(hist)
    assert len(shares) == 12 and sum(shares) == pytest.approx(1.0)
    assert shares[5] == pytest.approx(20.0 / 130.0)
    assert A.seasonality_shares([("2026-03", 7.0)]) == [1 / 12] * 12
