"""Selection + apply logic of scripts/asset_risk/cleanup_orphan_settlements.py (sqlite)."""
import importlib.util
import pathlib

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.pool import StaticPool

_P = pathlib.Path(__file__).resolve().parents[2] / "scripts/asset_risk/cleanup_orphan_settlements.py"
_spec = importlib.util.spec_from_file_location("cleanup_orphan_settlements", _P)
mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mod)


@pytest.fixture
def eng():
    e = create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})

    @event.listens_for(e, "connect")
    def _attach(c, _):
        c.execute("ATTACH DATABASE ':memory:' AS marketdata")

    with e.begin() as c:
        c.execute(text("CREATE TABLE marketdata.rm_settlements (id INTEGER PRIMARY KEY, book_id INT, "
                       "settlement_month TEXT, file_name TEXT, upload_date TEXT, raw_data TEXT)"))
        c.execute(text("CREATE TABLE marketdata.rm_settlement_items (id INTEGER PRIMARY KEY, settlement_id INT)"))
        rows = [(1, 7, "2026-03-01", "a.pdf"), (2, 7, "2026-03-01", "a.pdf"),  # 1 orphan, 2 twin w/ items
                (3, 8, "2026-03-01", "lone.pdf"),                              # lone zero-item
                (4, 9, "2026-03-01", "b.pdf"), (5, 9, "2026-03-01", "b.pdf")]  # 4 has items, 5 newer zero-item
        for r in rows:
            c.execute(text("INSERT INTO marketdata.rm_settlements VALUES (:i,:b,:m,:f,'2026-11-01','{}')"),
                      {"i": r[0], "b": r[1], "m": r[2], "f": r[3]})
        for sid in (2, 4):
            c.execute(text("INSERT INTO marketdata.rm_settlement_items (settlement_id) VALUES (:s)"), {"s": sid})
    return e


def test_selects_only_orphan_with_itemised_newer_twin(eng):
    with eng.connect() as c:
        assert [r[0] for r in mod.select_orphans(c)] == [1]
        assert sorted(r[0] for r in mod.select_lone_zero_item(c)) == [3, 5]


def test_apply_backs_up_and_deletes(eng, monkeypatch):
    monkeypatch.setattr(mod, "BACKUP", "marketdata.bak")
    with eng.begin() as c:
        assert mod.apply_cleanup(c, [1]) == (1, 1)
    with eng.connect() as c:
        assert c.execute(text("SELECT id FROM marketdata.bak")).fetchall() == [(1,)]
        assert [r[0] for r in c.execute(text("SELECT id FROM marketdata.rm_settlements ORDER BY id"))] == [2, 3, 4, 5]
