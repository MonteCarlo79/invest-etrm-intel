import sqlalchemy
from sqlalchemy import text

from libs.asset_risk.prefs import load_pref, pref_key, save_pref


def _engine(tmp_path):
    eng = sqlalchemy.create_engine(f"sqlite:///{tmp_path / 'p.db'}")
    with eng.begin() as c:
        c.execute(text("CREATE TABLE hermes_settings (key TEXT PRIMARY KEY, value TEXT, updated_at TIMESTAMP)"))
    return eng


def test_roundtrip_and_overwrite(tmp_path):
    eng = _engine(tmp_path)
    k = pref_key("asset_risk", "pnl_books", "Someone@Example.com")
    assert k == "asset_risk.pnl_books:someone@example.com"
    assert load_pref(eng, k, []) == []
    assert save_pref(eng, k, ["景蓝乌尔图 Book", "悦杭独贵 Book"])
    assert load_pref(eng, k, []) == ["景蓝乌尔图 Book", "悦杭独贵 Book"]
    assert save_pref(eng, k, [])
    assert load_pref(eng, k, None) == []


def test_missing_table_returns_default(tmp_path):
    eng = sqlalchemy.create_engine(f"sqlite:///{tmp_path / 'none.db'}")
    assert load_pref(eng, "x", ["d"]) == ["d"]
    assert save_pref(eng, "x", [1]) is False
