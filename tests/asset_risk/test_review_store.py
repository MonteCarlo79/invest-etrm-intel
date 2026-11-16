"""Review store idempotency, replacement, filters, reconciliation, and tab smoke."""
from datetime import date

import pandas as pd
import sqlalchemy
from sqlalchemy import event, text
from streamlit.testing.v1 import AppTest

from libs.asset_risk.review import store
from libs.asset_risk.review.recon import reconcile

SEP = date(2026, 9, 1)


def _eng(tmp_path):
    md = tmp_path / "md.db"
    eng = sqlalchemy.create_engine(f"sqlite:///{tmp_path / 'main.db'}")

    @event.listens_for(eng, "connect")
    def _a(conn, _):
        conn.execute(f"ATTACH DATABASE '{md}' AS marketdata")
    store.ensure_table(eng)
    return eng


def _df(rows):
    return pd.DataFrame(rows, columns=["asset_code", "month", "metric", "value", "unit"])


def _count(eng):
    with eng.connect() as c:
        return c.execute(text("SELECT COUNT(*) FROM marketdata.rm_review_metrics")).scalar()


def test_same_hash_idempotent(tmp_path):
    eng = _eng(tmp_path)
    df = _df([("suyou", SEP, "executed_revenue_cny", 100.0, "CNY"), ("suyou", SEP, "daily_cycles", 1.2, "")])
    assert store.save_review(eng, df, "a.pptx", "h1") == 2
    assert store.save_review(eng, df, "a.pptx", "h1") == 0
    assert _count(eng) == 2


def test_corrected_deck_replaces_asset_month(tmp_path):
    eng = _eng(tmp_path)
    store.save_review(eng, _df([("suyou", SEP, "executed_revenue_cny", 100.0, "CNY"),
                                ("suyou", SEP, "daily_cycles", 1.2, ""),
                                ("wulate", SEP, "executed_revenue_cny", 7.0, "CNY")]), "a.pptx", "h1")
    n = store.save_review(eng, _df([("suyou", SEP, "executed_revenue_cny", 120.0, "CNY")]), "a2.pptx", "h2")
    assert n == 1
    out = store.load_review(eng)
    s = out[out.asset_code == "suyou"]
    assert list(s.metric) == ["executed_revenue_cny"] and s.value.iloc[0] == 120.0   # stale cycles gone
    assert out[out.asset_code == "wulate"].value.iloc[0] == 7.0                       # other asset untouched


def test_load_filters(tmp_path):
    eng = _eng(tmp_path)
    store.save_review(eng, _df([("suyou", SEP, "m", 1.0, ""), ("suyou", date(2026, 8, 1), "m", 2.0, ""),
                                ("wulate", SEP, "m", 3.0, "")]), "a.pptx", "h1")
    assert len(store.load_review(eng)) == 3
    assert set(store.load_review(eng, asset_codes=["suyou"]).asset_code) == {"suyou"}
    assert list(store.load_review(eng, months=[SEP]).value.sort_values()) == [1.0, 3.0]
    assert len(store.load_review(eng, asset_codes=["suyou"], months=["2026-08-01"])) == 1
    assert store.load_review(eng, asset_codes=[]).empty


def test_reconcile_like_for_like_pairs():
    rv = _df([("suyou", SEP, "executed_revenue_cny", 100.0, ""), ("suyou", SEP, "total_revenue_cny", 90.0, ""),
              ("suyou", SEP, "daily_cycles", 1.0, ""),
              ("wulate", SEP, "executed_revenue_cny", 100.0, "")])
    act = pd.DataFrame({"asset_code": ["suyou"], "month": ["2026-09"], "arb_cny": [70.0], "cap_cny": [28.0],
                        "fee_cny": [-10.0], "other_cny": [0.0]})
    r = reconcile(rv, act).set_index("asset_code")
    s = r.loc["suyou"]
    assert s.actual_arb_cap_cny == 98.0 and s.diff_exec_cny == 2.0 and abs(s.diff_exec_pct - 2 / 98) < 1e-9
    assert s.actual_total_cny == 88.0 and abs(s.diff_total_pct - 2 / 88) < 1e-9
    assert s.spot_only_cny == 70.0 and not bool(s.flag)            # spot-only never flagged
    assert pd.isna(r.loc["wulate", "actual_arb_cap_cny"]) and not bool(r.loc["wulate", "flag"])
    assert pd.isna(r.loc["wulate", "reviewed_total_cny"])
    bad = reconcile(rv, act.assign(fee_cny=-30.0)).set_index("asset_code").loc["suyou"]   # total actual 68
    assert bool(bad.flag_total) and not bool(bad.flag_exec) and bool(bad.flag)
    assert reconcile(rv.iloc[:0], act).empty


SCRIPT = '''
import os, sqlalchemy
from sqlalchemy import event
from apps.asset_risk.tab_review import render_review
eng = sqlalchemy.create_engine("sqlite:///" + os.environ["RV_MAIN_DB"])
@event.listens_for(eng, "connect")
def _attach(conn, _):
    conn.execute("ATTACH DATABASE '%s' AS marketdata" % os.environ["RV_MD_DB"])
render_review(eng)
'''


def test_tab_smoke(tmp_path, monkeypatch):
    eng = _eng(tmp_path)
    with eng.begin() as c:
        c.execute(text("CREATE TABLE marketdata.rm_assets (id INT, name TEXT, asset_code TEXT)"))
        c.execute(text("CREATE TABLE marketdata.rm_books (id INT, asset_id INT)"))
        c.execute(text("CREATE TABLE marketdata.rm_settlements (id INT, book_id INT, settlement_month TEXT)"))
        c.execute(text("CREATE TABLE marketdata.rm_settlement_items (settlement_id INT, category TEXT, "
                       "amount_cny REAL, volume_mwh REAL)"))
        c.execute(text("INSERT INTO marketdata.rm_assets VALUES (1,'苏右','suyou')"))
        c.execute(text("INSERT INTO marketdata.rm_books VALUES (1,1)"))
        c.execute(text("INSERT INTO marketdata.rm_settlements VALUES (1,1,'2026-09-01')"))
        c.execute(text("INSERT INTO marketdata.rm_settlement_items VALUES (1,'discharge_energy',1000,10),"
                       "(1,'capacity_compensation',200,NULL)"))
    monkeypatch.setenv("RV_MAIN_DB", str(tmp_path / "main.db"))
    monkeypatch.setenv("RV_MD_DB", str(tmp_path / "md.db"))
    monkeypatch.setenv("REVIEW_DECK_DIR", str(tmp_path / "absent"))
    at = AppTest.from_string(SCRIPT, default_timeout=60).run()
    assert not at.exception and any("暂无复盘数据" in i.value for i in at.info)
    assert any("仅限本地" in c.value for c in at.caption)

    store.save_review(eng, _df([("suyou", SEP, "executed_revenue_cny", 1100.0, "CNY")]), "a.pptx", "h1")
    at = AppTest.from_string(SCRIPT, default_timeout=60).run()
    assert not at.exception, [e.message for e in at.exception]
    assert len(at.dataframe) >= 2


def test_parse_many_bad_file_does_not_stop_batch(monkeypatch):
    import apps.asset_risk.tab_review as tr
    errors: list[str] = []
    monkeypatch.setattr(tr.st, "error", lambda m: errors.append(m))
    monkeypatch.setattr(tr.st, "session_state", {})

    def fake_parse(data, name):
        if name == "bad.pptx":
            raise ValueError("corrupt zip")
        return pd.DataFrame({"x": [1]}), []
    monkeypatch.setattr(tr, "parse_bess_deck", fake_parse)
    tr._parse_many([("bad.pptx", b"x"), ("good.pptx", b"y")])
    assert [p["name"] for p in tr.st.session_state["review_parsed"]] == ["good.pptx"]
    assert len(errors) == 1 and "bad.pptx" in errors[0]
