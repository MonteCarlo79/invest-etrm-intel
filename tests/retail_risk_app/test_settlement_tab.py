"""Retail-risk Settlement tab lists load books (not customers) from rm_settlements (2026-11-16)."""
import sqlalchemy
from sqlalchemy import event, text
from streamlit.testing.v1 import AppTest

SCRIPT = '''
import os, sqlalchemy
from sqlalchemy import event
from apps.retail_risk.tab_settlement import render_settlement
eng = sqlalchemy.create_engine("sqlite:///" + os.environ["RS_MAIN_DB"])
@event.listens_for(eng, "connect")
def _attach(conn, _):
    conn.execute("ATTACH DATABASE '%s' AS marketdata" % os.environ["RS_MD_DB"])
render_settlement(eng)
'''


def test_settlement_tab_is_per_book(tmp_path, monkeypatch):
    main, md = tmp_path / "main.db", tmp_path / "md.db"
    eng = sqlalchemy.create_engine(f"sqlite:///{main}")

    @event.listens_for(eng, "connect")
    def _a(conn, _):
        conn.execute(f"ATTACH DATABASE '{md}' AS marketdata")
    with eng.begin() as c:
        c.execute(text("CREATE TABLE marketdata.rm_books (id INT, name TEXT, book_type TEXT, asset_id INT)"))
        c.execute(text("CREATE TABLE marketdata.rm_settlements (id INT, book_id INT, settlement_month DATE,"
                       " file_name TEXT, file_type TEXT, upload_date TIMESTAMP, status TEXT, total_amount_cny REAL)"))
        c.execute(text("CREATE TABLE marketdata.rm_settlement_items (settlement_id INT, category TEXT,"
                       " volume_mwh REAL, amount_cny REAL)"))
        c.execute(text("INSERT INTO marketdata.rm_books VALUES (1,'A Book','asset',1),"
                       "(21,'景融售电-冀南','load',NULL),(22,'景融售电-浙江','load',NULL)"))
        c.execute(text("INSERT INTO marketdata.rm_settlements VALUES (1,21,'2026-07-01','a.pdf','pdf',NULL,'parsed',100),"
                       "(2,22,'2026-07-01','b.pdf','pdf',NULL,'parsed',50)"))
        c.execute(text("INSERT INTO marketdata.rm_settlement_items VALUES (1,'retail_revenue',10,1000),"
                       "(1,'spot_energy',8,600),(2,'retail_revenue',5,400)"))
    monkeypatch.setenv("RS_MAIN_DB", str(main))
    monkeypatch.setenv("RS_MD_DB", str(md))
    at = AppTest.from_string(SCRIPT, default_timeout=60).run()
    assert not at.exception, [e.message for e in at.exception]
    sb = at.selectbox(key="settlement_book")
    assert sb.options == ["景融售电-冀南", "景融售电-浙江"]            # load books only, no customers
    assert len(at.dataframe) >= 3                                      # amount, volume, all-books summary
    summary = at.dataframe[-1].value
    assert list(summary.index) == ["景融售电-冀南", "景融售电-浙江"]
    assert float(summary.loc["景融售电-冀南", "合计"]) == 1000.0
