from libs.trading_book.registry import get_stations, get_station

CODES = {"suyou", "hangjinqi", "siziwangqi", "gushanliang", "bameng", "wulate"}

def test_six_current_stations():
    stations = get_stations()
    assert {s.asset_code for s in stations} == CODES

def test_station_price_nodes():
    s = get_station("suyou")
    assert s.plant_name == "景蓝乌尔图储能电站"
    assert s.capacity_mw == 100
    assert any("苏尼特" in n for n in s.price_nodes)

def test_unknown_code_raises():
    import pytest
    with pytest.raises(KeyError):
        get_station("nope")
