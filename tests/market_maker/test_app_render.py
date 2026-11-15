"""AppTest smoke: every page renders against a sqlite stand-in for marketdata."""
from datetime import date, datetime, timedelta

import pytest
import sqlalchemy
import streamlit as st
from sqlalchemy import event, text
from streamlit.testing.v1 import AppTest

from tests.market_maker.test_refresh import _DDL

from pathlib import Path
APP = str(Path(__file__).resolve().parents[2] / "apps" / "market_maker" / "app.py")
LABELS = ["Positions", "Exposure & MtM", "VaR & Greeks", "Curves", "Risk Agent"]
_real_create_engine = sqlalchemy.create_engine


def _make_db(tmp_path, monkeypatch, seed=None):
    md = tmp_path / "marketdata.db"

    def create_engine(url, **kw):
        kw.pop("pool_pre_ping", None)
        eng = _real_create_engine(url, **kw)

        @event.listens_for(eng, "connect")
        def _attach(conn, _):
            conn.execute(f"ATTACH DATABASE '{md}' AS marketdata")
        return eng

    monkeypatch.setattr(sqlalchemy, "create_engine", create_engine)
    monkeypatch.setenv("PGURL", f"sqlite:///{tmp_path / 'main.db'}")
    monkeypatch.setenv("AUTH_MODE", "dev")
    eng = create_engine(f"sqlite:///{tmp_path / 'main.db'}")
    with eng.begin() as c:
        for s in _DDL:
            c.execute(text(s))
        if seed:
            seed(c)
    st.cache_data.clear()
    st.cache_resource.clear()
    return eng


def _page(label):
    at = AppTest.from_file(APP, default_timeout=60).run()
    assert not at.exception, at.exception
    at.radio(key="mm_page").set_value(label).run()
    assert not at.exception, [e.message for e in at.exception]
    return at


def _texts(at):
    return [x.value for x in (*at.info, *at.warning, *at.caption, *at.markdown, *at.error)]


EMPTY_MSG = {
    "Positions": "No positions for the selected books and provinces.",
    "Exposure & MtM": "No positions for the selected books and provinces — no exposure to show.",
    "VaR & Greeks": "No positions for the selected books and provinces — no delta or VaR to show.",
    "Curves": "No reference curve saved yet. Upload manual points below or run Refresh (Admin).",
    "Risk Agent": "Risk Agent arrives in Task 10.",
}


@pytest.mark.parametrize("label", LABELS)
def test_empty_db_pages(tmp_path, monkeypatch, label):
    _make_db(tmp_path, monkeypatch)
    at = _page(label)
    assert EMPTY_MSG[label] in [i.value for i in at.info]


AS_OF = date.today()


def _seed(c):
    c.execute(text("INSERT INTO marketdata.rm_assets VALUES (2,'武川','bess','蒙西',100,2,'wuchuan')"))
    c.execute(text("INSERT INTO marketdata.rm_books VALUES (7,'bess','asset',2),(21,'load 冀南','load',NULL)"))
    m1 = date(AS_OF.year + (AS_OF.month == 12), AS_OF.month % 12 + 1, 1)
    m2 = date(m1.year + (m1.month == 12), m1.month % 12 + 1, 1)
    for b, p, d, src, price, vol in ((7, "蒙西", "sell", "physical", None, 150),
                                     (7, "蒙西", "buy", "manual", 300, 100),
                                     (21, "冀南", "buy", None, 380, 100)):
        for m in (m1, m2):
            c.execute(text("INSERT INTO marketdata.rm_positions (book_id, instrument_type, province,"
                           " channel, direction, volume_mwh, price_cny_mwh, start_date, end_date, status,"
                           " source, tou_block, basis) VALUES (:b,'forward',:p,'annual',:d,:v,:pr,:s,:s,"
                           "'open',:src,'base',:basis)"),
                      {"b": b, "p": p, "d": d, "v": vol, "pr": price, "s": m, "src": src,
                       "basis": "est" if src == "physical" else None})
    c.execute(text("INSERT INTO marketdata.rm_customers VALUES (1,'A','冀南','active')"))
    c.execute(text("INSERT INTO marketdata.rm_customer_contracts VALUES "
                   "(1,1,'fixed',400,'2026-01-01','2030-12-31',1200,NULL,'active')"))
    # curve marks only for m1 in 蒙西 (m2 unmarked -> "—"); none for 冀南
    for blk, kwh in (("base", 0.3), ("peak", 0.5), ("offpeak", 0.2)):
        c.execute(text("INSERT INTO marketdata.rm_forward_curves (province, product, curve_date,"
                       " delivery_date, price_cny_kwh, source, tou_block) VALUES"
                       " ('蒙西','reference',:cd,:dd,:k,'spot_seasonal',:b)"),
                  {"cd": AS_OF, "dd": m1, "k": kwh, "b": blk})
    # 60 days of 蒙西 spot history -> scenarios; 冀南 none -> VaR exclusion warning
    rows = [{"t": datetime.combine(AS_OF, datetime.min.time()) - timedelta(hours=h), "p": 300 + (h * h) % 97}
            for h in range(1, 24 * 120)]
    c.execute(text("INSERT INTO marketdata.spot_prices_hourly VALUES ('蒙西',:t,:p,NULL)"), rows)


def test_seeded_positions(tmp_path, monkeypatch):
    _make_db(tmp_path, monkeypatch, _seed)
    at = _page("Positions")
    assert len(at.dataframe) == 2  # one table per book
    assert any("physical · 2" in m for m in _texts(at))
    load_tbl = at.dataframe[1].value
    assert set(load_tbl["direction"]) == {"buy", "sell"}  # in-memory load physical (R13) present


def test_seeded_exposure(tmp_path, monkeypatch):
    _make_db(tmp_path, monkeypatch, _seed)
    at = _page("Exposure & MtM")
    assert any(t.startswith("— = no curve mark. Months without a mark") for t in _texts(at))
    mtm = at.dataframe[1].value
    unmarked = mtm["mark_cny_mwh"] == "—"
    assert unmarked.any() and (mtm.loc[unmarked, "mtm_cny"] == "—").all()


def test_seeded_var(tmp_path, monkeypatch):
    _make_db(tmp_path, monkeypatch, _seed)
    at = _page("VaR & Greeks")
    warns = [w.value for w in at.warning]
    assert any(w.startswith("VaR excludes") and "冀南" in w for w in warns), warns
    assert any("no peak/offpeak mark" in t for t in _texts(at))
    hist = {m.label: m.value for m in at.metric}
    assert hist["95% · 1d historical"] not in ("—", "0", "-0")
    assert hist["99% · 10d historical"] not in ("—", "0", "-0")


def test_seeded_curves(tmp_path, monkeypatch):
    _make_db(tmp_path, monkeypatch, _seed)
    at = _page("Curves")
    assert any("Horizon months with no point for 蒙西" in t for t in _texts(at))
    assert any(b.label == "Refresh physical positions & curves" for b in at.button)


def test_merge_manual_keeps_other_points(tmp_path, monkeypatch):
    import pandas as pd

    from apps.market_maker.page_curves import merge_manual, parse_manual_csv
    from libs.curves.store import load_curve, save_curve
    eng = _make_db(tmp_path, monkeypatch, _seed)
    raw = pd.DataFrame({"province": ["内蒙古", "火星", "蒙西"], "delivery_month": ["2030-01-15"] * 3,
                        "tou_block": ["peak", "base", "flat"], "price_cny_mwh": [600, 1, 1]})
    manual, errors = parse_manual_csv(raw)
    assert len(manual) == 1 and len(errors) == 2
    save_curve(eng, merge_manual(eng, manual, AS_OF), curve_date=AS_OF)
    cur = load_curve(eng, "蒙西", AS_OF)
    assert len(cur) == 4 and set(cur["source"]) == {"manual", "spot_seasonal"}
