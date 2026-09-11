# tests/interconnector/test_registry.py
from services.interconnector.registry import CHANNELS, get_channels

KV_CLASSES = {"±100","±400","±500","±660","±800","±1100","500kV AC"}
CATEGORIES = {"电源绑定型","大基地型","制度创新型","联网型"}

def test_twenty_six_channels_with_unique_names():
    assert len(CHANNELS) == 26
    assert len({c["name"] for c in CHANNELS}) == 26

def test_required_fields_and_coords():
    for c in CHANNELS:
        assert c["kv"] in KV_CLASSES, c["name"]
        assert c["category"] in CATEGORIES, c["name"]
        assert c["gw"] > 0, c["name"]
        for key in ("send","send_prov","recv","recv_prov","formal","commissioned"):
            assert c[key], (c["name"], key)
        lon, lat = c["sc"]; assert 73 <= lon <= 136 and 17 <= lat <= 54, c["name"]
        lon, lat = c["rc"]; assert 73 <= lon <= 136 and 17 <= lat <= 54, c["name"]

def test_user_corrected_six_channels():
    by = {c["name"]: c for c in CHANNELS}
    assert by["云霄直流"]["kv"] == "±100" and by["云霄直流"]["gw"] == 2.0
    assert by["云霄直流"]["category"] == "制度创新型"
    assert by["宜华直流"]["send_prov"] == "湖北" and by["宜华直流"]["recv_prov"] == "上海"
    assert by["林枫直流"]["send"] == "团林(荆门)" and by["林枫直流"]["gw"] == 3.0
    assert by["坤渝直流"]["send_prov"] == "新疆" and by["坤渝直流"]["recv_prov"] == "重庆"
    assert by["庆东直流"]["send"] == "庆阳" and by["庆东直流"]["recv_prov"] == "山东"
    assert by["宝合直流"]["send_prov"] == "陕西" and by["宝合直流"]["recv_prov"] == "安徽"
    assert by["宝合直流"]["commissioned"] == "2026-06-30"

def test_get_channels_returns_copy():
    a = get_channels(); a[0]["gw"] = -1
    assert get_channels()[0]["gw"] > 0
