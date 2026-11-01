from libs.trading_book.registry import get_stations, get_station

# Task 11 fleet: original 6 full-path BESS + 3 new BESS + 3 wind = 12.
BESS_FULL = {"suyou", "hangjinqi", "siziwangqi", "gushanliang", "bameng", "wulate"}
BESS_NEW = {"wuhai", "wuchuan", "alashan"}
WIND = {"lingt46", "alashanzuoqi", "erlian"}
ALL = BESS_FULL | BESS_NEW | WIND

def test_twelve_stations_registered():
    assert {s.asset_code for s in get_stations()} == ALL
    assert len(get_stations()) == 12

def test_existing_six_unchanged_by_defaults():
    # asset_type/data_path defaults keep the original 6 valid: bess + full.
    for s in get_stations():
        if s.asset_code in BESS_FULL:
            assert s.asset_type == "bess"
            assert s.data_path == "full"

def test_station_price_nodes():
    s = get_station("suyou")
    assert s.plant_name == "景蓝乌尔图储能电站"
    assert s.capacity_mw == 100
    assert any("苏尼特" in n for n in s.price_nodes)

def test_unknown_code_raises():
    import pytest
    with pytest.raises(KeyError):
        get_station("nope")

def test_filter_by_asset_type():
    assert {s.asset_code for s in get_stations(asset_type="bess")} == BESS_FULL | BESS_NEW
    assert {s.asset_code for s in get_stations(asset_type="wind")} == WIND

def test_filter_by_data_path():
    assert {s.asset_code for s in get_stations(data_path="full")} == BESS_FULL
    assert {s.asset_code for s in get_stations(data_path="settlement_only")} == {"wuhai", "lingt46"}
    assert {s.asset_code for s in get_stations(data_path="registry_only")} == \
        {"wuchuan", "alashan", "alashanzuoqi", "erlian"}

def test_combined_filter():
    assert {s.asset_code for s in get_stations(asset_type="wind", data_path="settlement_only")} == {"lingt46"}
    assert {s.asset_code for s in get_stations(asset_type="bess", data_path="registry_only")} == {"wuchuan", "alashan"}

def test_new_bess_entries():
    w = get_station("wuhai")
    assert w.plant_name == "乌海康富储能电站" and w.capacity_mw == 100
    assert w.asset_type == "bess" and w.data_path == "settlement_only"
    assert w.price_nodes == []  # node TBD — no report in repo
    wc = get_station("wuchuan")
    assert wc.plant_name == "武川储能电站" and wc.capacity_mw == 1000
    assert wc.substation == "武川500kV变电站" and wc.data_path == "registry_only"
    assert any("武川站" in n for n in wc.price_nodes)
    al = get_station("alashan")
    assert al.plant_name == "阿拉善储能电站" and al.capacity_mw == 1000
    assert al.substation == "阿拉腾敖包500kV变电站"
    assert any("阿拉腾敖包" in n for n in al.price_nodes)

def test_wind_entries():
    l = get_station("lingt46")
    assert l.plant_name == "零碳46风电" and l.capacity_mw == 405
    assert l.asset_type == "wind" and l.data_path == "settlement_only"
    az = get_station("alashanzuoqi")
    assert az.plant_name == "阿拉善左旗风储电站" and az.capacity_mw == 195
    assert az.asset_type == "wind" and az.data_path == "registry_only"
    e = get_station("erlian")
    assert e.plant_name == "二连风电" and e.capacity_mw == 200
    assert e.data_path == "registry_only"

def test_wulanchabu_excluded():
    # 乌兰察布储能 (B-1) is explicitly out of the fleet per user decision.
    import pytest
    with pytest.raises(KeyError):
        get_station("wulanchabu")
