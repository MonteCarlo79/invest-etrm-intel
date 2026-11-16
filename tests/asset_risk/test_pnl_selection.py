"""Realised P&L: asset books only, empty default, selection persisted per user (2026-11-16)."""
from pathlib import Path

import sqlalchemy
from sqlalchemy import event, text
from streamlit.testing.v1 import AppTest

SCRIPT = '''
import os, sqlalchemy
from sqlalchemy import event
from apps.asset_risk.tab_pnl import render_pnl
eng = sqlalchemy.create_engine("sqlite:///" + os.environ["PNL_MAIN_DB"])
@event.listens_for(eng, "connect")
def _attach(conn, _):
    conn.execute("ATTACH DATABASE '%s' AS marketdata" % os.environ["PNL_MD_DB"])
render_pnl(eng)
'''


def _setup(tmp_path, monkeypatch):
    main, md = tmp_path / "main.db", tmp_path / "md.db"
    eng = sqlalchemy.create_engine(f"sqlite:///{main}")

    @event.listens_for(eng, "connect")
    def _a(conn, _):
        conn.execute(f"ATTACH DATABASE '{md}' AS marketdata")
    with eng.begin() as c:
        c.execute(text("CREATE TABLE hermes_settings (key TEXT PRIMARY KEY, value TEXT, updated_at TIMESTAMP)"))
        c.execute(text("CREATE TABLE marketdata.rm_assets (id INT, asset_type TEXT, province TEXT)"))
        c.execute(text("CREATE TABLE marketdata.rm_books (id INT, name TEXT, book_type TEXT, asset_id INT)"))
        c.execute(text("INSERT INTO marketdata.rm_assets VALUES (1,'bess','蒙西'),(2,'wind','蒙西')"))
        c.execute(text("INSERT INTO marketdata.rm_books VALUES (1,'A Book','asset',1),(2,'B Book','asset',2),"
                       "(21,'景融售电-冀南','load',NULL)"))
    monkeypatch.setenv("PNL_MAIN_DB", str(main))
    monkeypatch.setenv("PNL_MD_DB", str(md))
    monkeypatch.setenv("AUTH_MODE", "dev")
    monkeypatch.setenv("DEV_USER_EMAIL", "tester@example.com")
    return eng


def _book_widget(at):
    return next(m for m in at.multiselect if m.label.startswith("Book"))


def test_default_empty_asset_books_only_and_selection_persists(tmp_path, monkeypatch):
    eng = _setup(tmp_path, monkeypatch)
    at = AppTest.from_string(SCRIPT, default_timeout=60).run()
    assert not at.exception, [e.message for e in at.exception]
    w = _book_widget(at)
    assert w.value == [] and set(w.options) == {"A Book", "B Book"}   # no load book
    assert any("Select one or more books" in i.value for i in at.info)

    w.set_value(["B Book"]).run()
    with eng.connect() as c:
        assert c.execute(text("SELECT value FROM hermes_settings")).scalar() == "[2]"

    at2 = AppTest.from_string(SCRIPT, default_timeout=60).run()          # a later session
    assert _book_widget(at2).value == ["B Book"]
