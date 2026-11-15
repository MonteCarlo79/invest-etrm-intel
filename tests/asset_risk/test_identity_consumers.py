"""Task 10: chain tabs / wind view / settlement bridge read identity from the
registry frame (rm_assets.asset_code). sqlite-ATTACH harness."""
import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from libs.asset_risk.diagnostics import (
    AUTO_NOM_CODE, CAPCOMP_RATE, PLANT_MAP, chain_asset_names, load_maps,
)
from libs.asset_risk.waterfall import stage_capacity

_LEGACY = list(PLANT_MAP)  # six legacy BESS stations
_EXTRA = [("武川", "wuchuan"), ("阿拉善储能", "alashan"), ("乌海康富", "wuhai")]


def _engine(extra_rows=()):
    eng = create_engine("sqlite:///:memory:")
    code_col = "asset_code TEXT, "
    with eng.connect() as conn:
        conn.execute(text("ATTACH DATABASE ':memory:' AS marketdata"))
        conn.execute(text("CREATE TABLE marketdata.rm_assets "
                          f"(id INTEGER PRIMARY KEY, name TEXT, {code_col}asset_type TEXT, "
                          "capacity_mw REAL, bess_duration_h REAL, commission_date TEXT)"))
        conn.execute(text("CREATE TABLE marketdata.rm_books "
                          "(id INTEGER PRIMARY KEY, name TEXT, book_type TEXT, asset_id INTEGER)"))
        rows = [(n, AUTO_NOM_CODE[n], "bess") for n in _LEGACY] + \
               [(n, c, "bess") for n, c in _EXTRA] + [("零碳46风电", "lingt46", "wind")] + list(extra_rows)
        for i, (name, code, typ) in enumerate(rows, start=1):
            conn.execute(text("INSERT INTO marketdata.rm_assets (id, name, asset_code, asset_type, "
                              "capacity_mw, bess_duration_h) VALUES (:i, :n, :c, :t, 100, 2)"),
                         {"i": i, "n": name, "c": code, "t": typ})
        conn.execute(text("INSERT INTO marketdata.rm_books VALUES (42, 'wind', 'asset', :a)"),
                     {"a": 10})
        conn.commit()
    return eng


@pytest.fixture(params=["diagnostics", "waterfall"])
def load_assets(request):
    if request.param == "diagnostics":
        from apps.asset_risk.tab_diagnostics import _load_assets
    else:
        from apps.asset_risk.tab_waterfall import _load_assets
    return _load_assets


def test_r5_full_stations_only(load_assets):
    eng = _engine()
    names = set(load_assets(eng)["name"])
    assert names == set(_LEGACY) | {"武川", "阿拉善储能"}
    assert "乌海康富" not in names and "零碳46风电" not in names


def test_load_maps_from_identity():
    pm, nom, rate = load_maps(_engine())
    assert {k: pm[k] for k in PLANT_MAP} == PLANT_MAP          # legacy six unchanged
    assert {k: nom[k] for k in AUTO_NOM_CODE} == AUTO_NOM_CODE
    assert all(rate[k] == 350.0 for k in PLANT_MAP)
    assert pm["武川"] == "旭纯哈乐储能电站"                     # first account for prices
    assert rate["阿拉善储能"] == 280.0 and rate["武川"] == 280.0


def test_stage_capacity_uses_rate_map():
    cap = stage_capacity(pd.Series([10.0, 10.0]), pd.Series(["阿拉善储能", "悦杭独贵"]),
                         {"阿拉善储能": 280.0, "悦杭独贵": 350.0})
    assert list(cap) == [2800.0, 3500.0]


def test_settlement_bridge_rm_name():
    from services.mengxi_nodal.settlement_bridge import _rm_name
    assert _rm_name(_engine(), "alashan") == "阿拉善储能"
    assert _rm_name(_engine(), "erlian") is None


def test_wind_book_id():
    from apps.asset_risk.tab_wind_replication import BOOK_ID, _book_id
    assert _book_id(_engine()) == 42
    eng = _engine()
    with eng.begin() as conn:
        conn.execute(text("DELETE FROM marketdata.rm_books"))
    assert _book_id(eng) == BOOK_ID == 6   # no lingt46 book row → literal fallback


def test_unknown_asset_code_logged_and_treated_as_none(capsys):
    from libs.asset_risk.registry import asset_identity
    ident = asset_identity(_engine(extra_rows=[("新资产", "no_such_code", "bess")]))
    row = ident[ident["name"] == "新资产"].iloc[0]
    assert row["asset_code"] is None and row["plant_names"] == ()
    assert "no_such_code" in capsys.readouterr().out
