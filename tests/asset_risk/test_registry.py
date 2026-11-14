"""Registry-derived asset identity (Task 9). sqlite-ATTACH harness, same style as test_agent_tools."""
import pytest
from sqlalchemy import create_engine, text

from libs.asset_risk.registry import (
    IDENTITY_COLUMNS, asset_identity, capcomp_rate_map, nom_code_map, plant_map, rm_name_map,
)

_ROWS = [
    (1, "悦杭独贵", "hangjinqi"), (2, "景怡查干哈达", "bameng"), (3, "景蓝乌尔图", "suyou"),
    (4, "裕昭沙子坝", "gushanliang"), (5, "远景乌拉特", "wulate"), (6, "四子王旗", "siziwangqi"),
    (7, "武川", "wuchuan"), (8, "阿拉善储能", "alashan"), (9, "零碳46风电", "lingt46"),
    (10, "灵山", None),
]


@pytest.fixture()
def sqlite_engine():
    eng = create_engine("sqlite:///:memory:")
    with eng.connect() as conn:
        conn.execute(text("ATTACH DATABASE ':memory:' AS marketdata"))
        conn.execute(text("CREATE TABLE marketdata.rm_assets "
                          "(id INTEGER PRIMARY KEY, name TEXT, asset_code TEXT, asset_type TEXT, "
                          "province TEXT, capacity_mw REAL, bess_duration_h REAL, "
                          "commission_date TEXT, invoice_folder TEXT)"))
        for i, name, code in _ROWS:
            conn.execute(text("INSERT INTO marketdata.rm_assets (id, name, asset_code, asset_type, "
                              "capacity_mw) VALUES (:i, :n, :c, 'bess', 100)"),
                         {"i": i, "n": name, "c": code})
        conn.commit()
    yield eng


def test_identity_derives_registry_fields(sqlite_engine):
    df = asset_identity(sqlite_engine)
    assert list(df.columns) == IDENTITY_COLUMNS
    wc = df[df.name == "武川"].iloc[0]
    assert wc.asset_code == "wuchuan" and wc.plant_names == ("旭纯哈乐储能电站", "旭纯哈乐储能电站二") and wc.capcomp_rate == 280.0
    al = df[df.name == "阿拉善储能"].iloc[0]
    assert al.capcomp_rate == 280.0
    ls = df[df.name == "灵山"].iloc[0]
    assert ls.asset_code is None and ls.plant_names == () and ls.capcomp_rate == 350.0
    assert df[df.name == "零碳46风电"].iloc[0].capcomp_rate == 350.0


def test_maps_match_legacy_for_six_stations(sqlite_engine):
    from libs.asset_risk.diagnostics import PLANT_MAP, AUTO_NOM_CODE
    df = asset_identity(sqlite_engine)
    assert {k: v[0] for k, v in plant_map(df).items() if k in PLANT_MAP} == PLANT_MAP
    assert {k: v for k, v in nom_code_map(df).items() if k in AUTO_NOM_CODE} == AUTO_NOM_CODE


def test_map_helpers(sqlite_engine):
    df = asset_identity(sqlite_engine)
    assert "灵山" not in plant_map(df) and "灵山" not in nom_code_map(df)
    assert capcomp_rate_map(df)["灵山"] == 350.0
    assert rm_name_map(df)["wuchuan"] == "武川"


def test_attribution_alias_from_registry():
    from libs.trading_book.waterfall import _ATTR_CODE_ALIAS
    assert _ATTR_CODE_ALIAS == {"bameng": "hetao"}
