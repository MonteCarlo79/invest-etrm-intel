import datetime as dt

import pandas as pd
from sqlalchemy import create_engine, text

from libs.asset_risk.review import agent as ag
from libs.asset_risk.review.view import review_gap_table


def _seed(tmp_path):
    from sqlalchemy import event
    e = create_engine(f"sqlite:///{tmp_path}/t.db")

    @event.listens_for(e, "connect")
    def _a(conn, _):
        conn.execute(f"ATTACH DATABASE '{tmp_path}/m.db' AS marketdata")
    with e.begin() as c:
        c.execute(text("CREATE TABLE marketdata.rm_review_metrics (asset_code TEXT, month DATE, metric TEXT, "
                       "value REAL, unit TEXT, source_file TEXT, file_hash TEXT, extracted_by TEXT, uploaded_at TEXT)"))
        for code, m, v in [("suyou", "2026-08-01", 1.0), ("suyou", "2026-09-01", 2.0), ("wulate", "2026-09-01", 3.0)]:
            c.execute(text("INSERT INTO marketdata.rm_review_metrics (asset_code, month, metric, value, unit) "
                           "VALUES (:a,:m,'total_revenue_cny',:v,'CNY')"), {"a": code, "m": m, "v": v})
    return e


def test_name_resolves_and_month_filter(tmp_path):
    e = _seed(tmp_path)
    p = ag.review_metrics_payload(e, "景蓝乌尔图", "2026-09", None)
    assert [(r["asset_code"], r["month"]) for r in p["rows"]] == [("suyou", "2026-09-01")]
    assert "spot + capacity compensation before fees" in p["unit_note"] and "total_revenue_cny = after" in p["unit_note"]
    assert len(ag.review_metrics_payload(e, "suyou")["rows"]) == 2
    assert len(ag.review_metrics_payload(e, None, "2026-08", "2026-08")["rows"]) == 1


def test_unknown_asset(tmp_path):
    p = ag.review_metrics_payload(_seed(tmp_path), "nope")
    assert p["rows"] == [] and "Unknown asset" in p["note"]


def test_missing_table():
    p = ag.review_metrics_payload(create_engine("sqlite://"), "suyou")
    assert p["rows"] == [] and "No 复盘 data" in p["note"]


def test_gap_table():
    m = dt.date(2026, 9, 1)
    df = pd.DataFrame([("suyou", m, "strategy_revenue_cny", 100.0, "CNY"),
                       ("suyou", m, "cleared_revenue_cny", 90.0, "CNY"),
                       ("suyou", m, "executed_revenue_cny", 81.0, "CNY")],
                      columns=["asset_code", "month", "metric", "value", "unit"])
    r = review_gap_table(df).iloc[0]
    assert r["cleared−strategy"] == -10 and r["executed−cleared"] == -9
    assert r["cleared gap %"] == -10 and r["asset"].startswith("景蓝乌尔图")


def test_resolve_asset_alias_fallback():
    cases = {"苏右": "suyou", "巴盟": "bameng", "阿拉善": "alashan", "杭锦旗": "hangjinqi",
             "四子王旗": "siziwangqi", "谷山梁": "gushanliang"}
    for name, code in cases.items():
        assert ag.resolve_asset(name) == code, name
    # 阿拉善 is an ambiguous registry substring (阿拉善储能电站 / 阿拉善左旗风储电站)
    from libs.trading_book.registry import STATIONS
    assert sum("阿拉善" in s.plant_name for s in STATIONS) > 1
    assert ag.resolve_asset("nope") is None


def test_missing_table_logs(caplog):
    with caplog.at_level("ERROR"):
        ag.review_metrics_payload(create_engine("sqlite://"), "suyou")
    assert any("load_review failed" in r.message for r in caplog.records)
